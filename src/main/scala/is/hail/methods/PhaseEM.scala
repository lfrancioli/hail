package is.hail.methods

import is.hail.annotations.Annotation
import is.hail.expr.{TArray, TDouble, TSet, TString, TStruct, TVariant, Type}
import is.hail.keytable.KeyTable
import is.hail.utils.SparseVariantSampleMatrixRRDBuilder
import is.hail.variant.{Variant, VariantDataset}
import org.apache.spark.HashPartitioner
import is.hail.utils._
import org.apache.spark.sql.Row

import scala.collection.mutable

object PhaseEM {

  private implicit class Enriched_toTuple_Array[A](val seq: Array[A]) extends AnyVal {
    def toTuple2 = seq match {
      case Array(a, b) => (a, b);
      case x => throw new AssertionError(s"Cannot convert array of length ${ seq.size } into Tuple2: Array(${ x.mkString(", ") })")
    }

    def toTuple3 = seq match {
      case Array(a, b, c) => (a, b, c);
      case x => throw new AssertionError(s"Cannot convert array of length ${ seq.size } into Tuple3: Array(${ x.mkString(", ") })")
    }

    def toTuple4 = seq match {
      case Array(a, b, c, d) => (a, b, c, d);
      case x => throw new AssertionError(s"Cannot convert array of length ${ seq.size } into Tuple4: Array(${ x.mkString(", ") })")
    }

    def toTuple5 = seq match {
      case Array(a, b, c, d, e) => (a, b, c, d, e);
      case x => throw new AssertionError(s"Cannot convert array of length ${ seq.size } into Tuple5: Array(${ x.mkString(", ") })")
    }
  }

  def apply(vds: VariantDataset, keys: Array[String], number_partitions: Int, variantPairs: KeyTable): KeyTable = {
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

    val flatMapOp = (key: Any, svm: SparseVariantSampleMatrix) => {

      orderedVariantPairs.toIterator.flatMap{
        case (v1, v2) =>
          val v1s = v1.toString
          val v2s = v2.toString
          val v1a = svm.getVariantAnnotationsAsOption(v1s).getOrElse(Annotation.empty)
          val v2a = svm.getVariantAnnotationsAsOption(v2s).getOrElse(Annotation.empty)

          val haplotypeCounts = Phasing.phaseVariantPairWithEM(svm.getGenotypeCounts(v1s, v2s))
          val probOnSameHaplotypeWithEM = Phasing.probOnSameHaplotypeWithEM(haplotypeCounts).getOrElse(null)
          val haplotypeCountsIndexedSeq = haplotypeCounts.map(c => c.toArray.toIndexedSeq).getOrElse(null)

          (svm.getVariantAsOption(v1s),svm.getVariantAsOption(v2s)) match{
            case (Some(s1),Some(s2)) =>
              val samples = s1.keys.toSet.intersect(s2.keys.toSet)

              if (samples.isEmpty)
                Iterator((key, v1, v1a, v2, v2a, null, null, haplotypeCountsIndexedSeq, probOnSameHaplotypeWithEM))
              else {
                samples.toIterator.map{
                  case s => (key, v1, v1a, v2, v2a, s, svm.getSampleAnnotations(s), haplotypeCountsIndexedSeq, probOnSameHaplotypeWithEM)
                }
              }
            case _ =>
              Iterator((key, v1, v1a, v2, v2a, null, null, haplotypeCountsIndexedSeq, probOnSameHaplotypeWithEM))
          }

      }

    }

    vdsToKeyTable(vds, keys, number_partitions, flatMapOp)

  }

  def apply(vds: VariantDataset, keys: Array[String], number_partitions: Int): KeyTable = {

    val flatMapOp = (key: Any, svm: SparseVariantSampleMatrix) => {
      svm.variants.toIterator.flatMap {
        case v1 =>
          val sampleVariantPairs = svm.getVariant(v1).filter(_._2.isCalledNonRef).toIterator.flatMap {
            case (s, g1) =>
              svm.getSample(s).filter { case (v2, g2) => g2.isCalledNonRef && v1 < v2 }.map { case (v2, g2) => (v2, s, g1, g2) }
          }.toIndexedSeq.groupBy(_._1)

          sampleVariantPairs.flatMap {
            case (v2, gts) =>
              val haplotypeCounts = Phasing.phaseVariantPairWithEM(svm.getGenotypeCounts(v1, v2))
              val va = Variant.parse(v1)
              val vb = Variant.parse(v2)
              val switch = va.compare(vb) > 0

              val res = (
                if (switch) vb else va,
                if (switch) svm.getVariantAnnotations(v2) else svm.getVariantAnnotations(v1),
                if (switch) va else vb,
                if (switch) svm.getVariantAnnotations(v1) else svm.getVariantAnnotations(v2),
                haplotypeCounts.map(c => c.toArray.toIndexedSeq).getOrElse(null),
                Phasing.probOnSameHaplotypeWithEM(haplotypeCounts).getOrElse(null)
              )

              gts.map {
                case (_, s, g1, g2) =>
                  (key, res._1, res._2, res._3, res._4, s, svm.getSampleAnnotations(s), res._5, res._6)
              }
          }

      }

    }

    vdsToKeyTable(vds, keys, number_partitions, flatMapOp)
  }

  def vdsToKeyTable(vds: VariantDataset, keys: Array[String], number_partitions: Int,
    flatMapOp: (Any, SparseVariantSampleMatrix) => TraversableOnce[(Any, Variant, Annotation, Variant, Annotation, String, Annotation, IndexedSeq[Double],Any)] ): KeyTable = {

    val nkeys = keys.length

    if (nkeys > 5) {
      fatal("Cannot use more than 5 values as keys at the moment.")
    }

    val sc = vds.sparkContext
    val partitioner = new HashPartitioner(number_partitions)

    //Get key annotations
    val key_queriers = keys.map(k => vds.queryVA(k))

    val result = SparseVariantSampleMatrixRRDBuilder.buildByVA(vds, sc, partitioner)(
      { case (v, va) =>
        val annotations = key_queriers.map(q => q._2(va))
        nkeys match {
          case 1 => annotations(0)
          case 2 => annotations.toTuple2
          case 3 => annotations.toTuple3
          case 4 => annotations.toTuple4
          case 5 => annotations.toTuple5
          case _ => fatal("Cannot use more than 5 values as keys at the moment.") // Should never get there
        }
      }
    ).flatMap {
      case (key, svm) =>
        flatMapOp(key, svm).map {
          case (k, v1, va1, v2, va2, s, sa, hc, p) =>
            nkeys match {
              case 1 => Row(k, v1, va1, v2, va2, s, sa, hc, p)
              case 2 => {
                val keys = k.asInstanceOf[Tuple2[Any, Any]]
                Row(keys._1, keys._2, v1, va1, v2, va2, s, sa, hc, p)
              }
              case 3 => {
                val keys = k.asInstanceOf[Tuple3[Any, Any, Any]]
                Row(keys._1, keys._2, keys._3, v1, va1, v2, va2, s, sa, hc, p)
              }
              case 4 => {
                val keys = k.asInstanceOf[Tuple4[Any, Any, Any, Any]]
                Row(keys._1, keys._2, keys._3, keys._4, v1, va1, v2, va2, s, sa, hc, p)
              }
              case 5 => {
                val keys = k.asInstanceOf[Tuple5[Any, Any, Any, Any, Any]]
                Row(keys._1, keys._2, keys._3, keys._4, keys._5, v1, va1, v2, va2, s, sa, hc, p)
              }
              case _ => fatal("How did you get all the way here ?!?")
            }

        }
    }

    val valueTypes = Array(
      ("v1", TVariant),
      ("va1", vds.vaSignature),
      ("v2", TVariant),
      ("va2", vds.vaSignature),
      ("s", TString),
      ("sa", vds.saSignature),
      ("haplotype_counts", TArray(TDouble)),
      ("prob_same_haplotype", TDouble)
    )

    val ktSignature = TStruct((keys.zip(key_queriers.map(_._1)) ++ valueTypes).toSeq: _*)
    val kt = KeyTable(vds.hc, result, ktSignature, key = keys)
    kt.typeCheck()
    info("Keytable types checked!")
    return kt

  }

}
