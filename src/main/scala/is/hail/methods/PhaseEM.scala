package is.hail.methods

import is.hail.annotations.Annotation
import is.hail.expr.{TArray, TDouble, TInt, TSet, TString, TStruct, TVariant, Type}
import is.hail.keytable.KeyTable
import is.hail.utils.SparseVariantSampleMatrixRRDBuilder
import is.hail.variant.{Variant, VariantDataset}
import org.apache.spark.HashPartitioner
import is.hail.utils._
import org.apache.spark.sql.Row

import scala.collection.mutable

object PhaseEM {

  def apply(vds: VariantDataset, vaKeys: Array[String], saKeys: Array[String], numberPartitions: Int, variantPairs: KeyTable, perSample: Boolean): KeyTable = {
    //Check that variantPairs is a KeyTable with 2  key fields of type variant
    val variantKeys = variantPairs.keyFields.filter(_.typ == TVariant).map(x => x.index)
    if (variantKeys.length != 2)
      fatal(s"variant pairs KeyTable must have exactly two keys of type TVariant")

    val orderedVariantPairs = variantPairs.rdd.map{
      case row =>
        val v1 = row.getAs[Variant](variantKeys(0))
        val v2 = row.getAs[Variant](variantKeys(1))

        if (v1.compare(v2) < 0)
          (v1,v2)
        else
          (v2,v1)

    }.collect().toSet

    info(s"Found ${orderedVariantPairs.size} distinct variant pairs in variantPairs KeyTable.")

    val flatMapOp = (key: Row, svsm: SparseVariantSampleMatrix) => {

      (svsm.getFirstVariant(), svsm.getLastVariant()) match {
        case (Some(firstVariant), Some(lastVariant)) =>
          orderedVariantPairs.toIterator
            .filter { case (v1, v2) => v1.compare(firstVariant) >= 0 && v2.compare(lastVariant) <= 0 }
            .flatMap {
              case (v1, v2) =>
                val v1a = svsm.getVariantAnnotationsAsOption(v1).getOrElse(Annotation.empty)
                val v2a = svsm.getVariantAnnotationsAsOption(v2).getOrElse(Annotation.empty)

                val genotypeCounts = svsm.getGenotypeCounts(v1, v2)
                val haplotypeCounts = Phasing.phaseVariantPairWithEM(genotypeCounts)
                val probOnSameHaplotypeWithEM = Phasing.probOnSameHaplotypeWithEM(haplotypeCounts).getOrElse(null)
                val genotypeCountsIndexedSeq = genotypeCounts.toArray.toIndexedSeq
                val haplotypeCountsIndexedSeq = haplotypeCounts.map(c => c.toArray.toIndexedSeq).getOrElse(null)

                (perSample, svsm.getVariantAsOption(v1), svsm.getVariantAsOption(v2)) match {
                  case (false, _, _) =>
                    Iterator(Row.fromSeq(key.toSeq ++ Seq(v1, v1a, v2, v2a, genotypeCountsIndexedSeq, haplotypeCountsIndexedSeq, probOnSameHaplotypeWithEM)))
                  case (true, Some(s1), Some(s2)) =>
                    val samples = s1.keys.toSet.intersect(s2.keys.toSet)
                    if (samples.isEmpty)
                      Iterator(Row.fromSeq(key.toSeq ++ Seq(v1, v1a, v2, v2a, null, null, genotypeCountsIndexedSeq, haplotypeCountsIndexedSeq, probOnSameHaplotypeWithEM)))
                    else {
                      samples.toIterator.map {
                        case s => Row.fromSeq(key.toSeq ++ Seq(v1, v1a, v2, v2a, s, svsm.getSampleAnnotations(s), genotypeCountsIndexedSeq, haplotypeCountsIndexedSeq, probOnSameHaplotypeWithEM))
                      }
                    }
                  case _ =>
                    Iterator(Row.fromSeq(key.toSeq ++ Seq(v1, v1a, v2, v2a, null, null, genotypeCountsIndexedSeq, haplotypeCountsIndexedSeq, probOnSameHaplotypeWithEM)))
                }

            }
        case _ =>
          Iterator.empty
      }

    }

    vdsToKeyTable(vds, vaKeys, saKeys, numberPartitions, perSample)(flatMapOp)

  }

  def apply(vds: VariantDataset, vaKeys: Array[String], saKeys: Array[String], numberPartitions: Int, bySample: Boolean, perSample: Boolean): KeyTable = {

    val flatMapOp = (key: Row, svm: SparseVariantSampleMatrix) => {
      svm.variants.toIterator.flatMap {
        case v1 =>
          val sampleVariantPairs = svm.getVariant(v1).filter(_._2.isHet).toIterator.flatMap {
            case (s, g1) =>
              svm.getSample(s).filter { case (v2, g2) => g2.isHet && v1.compare(v2) < 0 }.map { case (v2, g2) => (v2, s, g1, g2) }
          }.toIndexedSeq.groupBy(_._1)

          sampleVariantPairs.flatMap {
            case (v2, gts) =>
              val genotypeCounts = svm.getGenotypeCounts(v1, v2)
              if (bySample)
                genotypeCounts(4) -= 1
              val haplotypeCounts = Phasing.phaseVariantPairWithEM(genotypeCounts)

              val res = (
                v1,
                svm.getVariantAnnotations(v1),
                v2,
                svm.getVariantAnnotations(v2),
                genotypeCounts.toArray.toIndexedSeq,
                haplotypeCounts.map(c => c.toArray.toIndexedSeq).getOrElse(null),
                Phasing.probOnSameHaplotypeWithEM(haplotypeCounts).getOrElse(null)
              )

              perSample match {
                case true =>
                  gts.map {
                    case (_, s, g1, g2) =>
                      Row.fromSeq(key.toSeq ++ Seq(res._1, res._2, res._3, res._4, s, svm.getSampleAnnotations(s), res._5, res._6, res._7))
                  }
                case _ =>
                  Iterator(Row.fromSeq(key.toSeq ++ Seq(res._1, res._2, res._3, res._4, res._5, res._6, res._7)))
              }

          }

      }

    }

    vdsToKeyTable(vds, vaKeys, saKeys, numberPartitions, perSample)(flatMapOp)
  }

  def vdsToKeyTable(vds: VariantDataset, vaKeys: Array[String], saKeys: Array[String], number_partitions: Int, perSample: Boolean)
    (flatMapOp: (Row, SparseVariantSampleMatrix) => TraversableOnce[Row]): KeyTable = {


    val sc = vds.sparkContext
    val partitioner = new HashPartitioner(number_partitions)

    //Get key annotations
    val vaKeysQueriers = vaKeys.map(k => vds.queryVA(k))
    val saKeysQueriers = saKeys.map(k => vds.querySA(k))

    val svsmRDD = if (saKeys.isEmpty) {
      info("Building SVSM by VA")
      SparseVariantSampleMatrixRRDBuilder.buildByVA(vds, sc, partitioner)(
        { case (v, va) => Row.fromSeq(vaKeysQueriers.map(q => q._2(va)).toSeq) }
      )
    }
    else {
      info("Building SVSM by VA and SA")
      SparseVariantSampleMatrixRRDBuilder.buildByVAandSA(vds, sc, partitioner)(
        { case (v, va) => Row.fromSeq(vaKeysQueriers.map(q => q._2(va)).toSeq) }, { case sa => Row.fromSeq(saKeysQueriers.map(q => q._2(sa)).toSeq) }
      ).map {
        case ((vak, sak), svsm) => (Row.fromSeq(vak.toSeq ++ sak.toSeq), svsm)
      }
    }

    val result = svsmRDD.flatMap { case (key, svm) => flatMapOp(key, svm) }

    val valueTypes = perSample match {
      case true => Array(
        ("v1", TVariant),
        ("va1", vds.vaSignature),
        ("v2", TVariant),
        ("va2", vds.vaSignature),
        ("s", TString),
        ("sa", vds.saSignature),
        ("genotype_counts", TArray(TInt)),
        ("haplotype_counts", TArray(TDouble)),
        ("prob_same_haplotype", TDouble)
      )
      case _ => Array(
        ("v1", TVariant),
        ("va1", vds.vaSignature),
        ("v2", TVariant),
        ("va2", vds.vaSignature),
        ("genotype_counts", TArray(TInt)),
        ("haplotype_counts", TArray(TDouble)),
        ("prob_same_haplotype", TDouble)
      )
    }

    val ktSignature = TStruct(
      (vaKeys.zip(vaKeysQueriers.map(_._1)) ++
      saKeys.zip(saKeysQueriers.map(_._1)) ++
      valueTypes).toSeq: _*)
    val kt = KeyTable(vds.hc, result, ktSignature, key = vaKeys)
    return kt

  }

}
