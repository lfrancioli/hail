package is.hail.methods

import is.hail.expr.{TStruct, TVariant, Type, TSet, TString, TArray, TDouble}
import is.hail.keytable.KeyTable
import is.hail.utils.SparseVariantSampleMatrixRRDBuilder
import is.hail.variant.{Variant, VariantDataset}
import org.apache.spark.HashPartitioner
import is.hail.utils._
import org.apache.spark.sql.Row

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


  def apply(vds: VariantDataset, keys: Array[String], number_partitions: Int): KeyTable = {

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
        val variantPairs = svm.getExistingVariantPairs().toArray
        variantPairs.flatMap {
          case (v1, v2) =>

            val haplotypeCounts = Phasing.phaseVariantPairWithEM(svm.getGenotypeCounts(v1, v2))

            val v1_samples = svm.getVariant(v1).filter(_._2.isHet).keys.toSet
            val v2_samples = svm.getVariant(v2).filter(_._2.isHet).keys.toSet

            v1_samples.intersect(v2_samples).map {
              case sample =>

                val va = Variant.parse(v1)
                val vb = Variant.parse(v2)
                val switch = va.compare(vb) > 0

                (key,
                  if (switch) vb else va,
                  if (switch) svm.getVariantAnnotations(v2) else svm.getVariantAnnotations(v1),
                  if (switch) va else vb,
                  if (switch) svm.getVariantAnnotations(v1) else svm.getVariantAnnotations(v2),
                  sample,
                  svm.getSampleAnnotations(sample),
                  haplotypeCounts.map(c => c.toArray.toIndexedSeq).getOrElse(null),
                  Phasing.probOnSameHaplotypeWithEM(haplotypeCounts).getOrElse(null)
                )
            }

        }.map {
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
