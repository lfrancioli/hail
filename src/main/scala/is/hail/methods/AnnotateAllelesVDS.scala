package is.hail.methods

import is.hail.annotations.{Annotation, Inserter}
import is.hail.expr.{EvalContext, Parser, TArray, TInt32}
import is.hail.sparkextras.{OrderedKeyFunction, OrderedRDD}
import is.hail.utils.ArrayBuilder
import is.hail.variant.{AltAllele, LocusImplicits, VariantDataset}
import is.hail.utils._

object AnnotateAllelesVDS {

  def apply(vds: VariantDataset, other: VariantDataset, expr: String, matchStar: Boolean = true): VariantDataset = {
    import LocusImplicits.orderedKey

    val otherWasSplit = other.wasSplit

    val ec = EvalContext(Map(
      "global" -> (0, vds.globalSignature),
      "v" -> (1, vds.vSignature),
      "va" -> (2, vds.vaSignature),
      "vds" -> (3, TArray(other.vaSignature)),
      "aIndices" -> (4, TArray(TInt32()))
    ))

    val localGlobalAnnotation = vds.globalAnnotation

    val (paths, types, f) = Parser.parseAnnotationExprs(expr, ec, Some(Annotation.VARIANT_HEAD))

    val inserterBuilder = new ArrayBuilder[Inserter]()
    val newSignature = (paths, types).zipped.foldLeft(vds.vaSignature) { case (vas, (ids, signature)) =>
      val (s, i) = vas.insert(signature, ids)
      inserterBuilder += i
      s
    }
    val inserters = inserterBuilder.result()

    if (vds.wasSplit) {
      val otherSplit = if (otherWasSplit) other else other.dropSamples().splitMulti(keepStar = matchStar)
      val (_, aIndexQuerier) = if (otherWasSplit) (null, null) else otherSplit.queryVA("va.aIndex")
      val newRDD = vds.rdd.orderedLeftJoinDistinct(otherSplit.variantsAndAnnotations)
        .mapValuesWithKey { case (v, ((va, gs), annotation)) =>
          val annotations = IndexedSeq(annotation.getOrElse(null))
          val aIndices = IndexedSeq(annotation.map {
            ann =>
              if (otherWasSplit)
                0
              else
                aIndexQuerier(ann).asInstanceOf[Int] - 1
          }.getOrElse(null))

          ec.setAll(localGlobalAnnotation, v, va, annotations, aIndices)
          val newVA = f().zip(inserters).foldLeft(va) { case (va, (ann, inserter)) => inserter(va, ann) }
          (newVA, gs)
        }.asOrderedRDD
      vds.copy(rdd = newRDD, vaSignature = newSignature)
    }
    else {
      val otherLocusRDD = other.rdd.mapMonotonic(OrderedKeyFunction(_.locus), { case (v, (va, gs)) => (v, va) })

      val otherLocusAltAlleleRDD =
        if (otherWasSplit) {
          otherLocusRDD
            .groupByKey()
            .mapPartitions { case it => it.map { case (l, vva) => (l, vva.map { case (v, va) => (v.altAllele, va) }: IndexedSeq[(AltAllele, Annotation)]) } }
            .asOrderedRDD
        }
        else
          otherLocusRDD
            .mapValues { case (v, va) => v.altAlleles.map((_, va)) }
            .asOrderedRDD

      val newRDD = vds.rdd
        .mapMonotonic(OrderedKeyFunction(_.locus), { case (v, vags) => (v, vags) })
        .orderedLeftJoinDistinct(otherLocusAltAlleleRDD)
        .map {
          case (l, ((v, (va, gs)), altAllelesAndAnnotations)) =>
            val aIndices = Array.ofDim[java.lang.Integer](v.nAltAlleles)
            val annotations = Array.ofDim[Annotation](v.nAltAlleles)

            altAllelesAndAnnotations.map(x => x.zipWithIndex.foreach {
              case ((allele, ann), oldIndex) =>
                if (matchStar || allele.alt != "*") {
                  val newIndex = v.altAlleleIndex(allele)
                  if (newIndex > -1) {
                    annotations.update(newIndex, ann)
                    if (otherWasSplit)
                      aIndices.update(newIndex, 0)
                    else
                      aIndices.update(newIndex, oldIndex)
                  }
                }
            })
            ec.setAll(localGlobalAnnotation, v, va, annotations: IndexedSeq[Annotation], aIndices: IndexedSeq[Annotation])
            val newVA = f().zip(inserters).foldLeft(va) { case (va, (ann, inserter)) => inserter(va, ann) }
            (v, (newVA, gs))
        }

      vds.copy(rdd = OrderedRDD(newRDD, vds.rdd.orderedPartitioner), vaSignature = newSignature)
    }
  }
}
