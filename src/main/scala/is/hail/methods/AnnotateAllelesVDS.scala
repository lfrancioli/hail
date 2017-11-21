package is.hail.methods

import is.hail.annotations.{Annotation, Inserter}
import is.hail.expr.{EvalContext, Parser, TArray, TInt32, TStruct}
import is.hail.sparkextras.{OrderedKeyFunction, OrderedRDD}
import is.hail.utils.ArrayBuilder
import is.hail.variant.{AltAllele, LocusImplicits, VariantDataset}
import is.hail.utils._

object AnnotateAllelesVDS {

  def apply(vds: VariantDataset, other: VariantDataset, allelesExpr: String, variantsExpr: Option[String] = None, matchStar: Boolean = true): VariantDataset = {
    import LocusImplicits.orderedKey

    val otherWasSplit = other.wasSplit

    if (vds.wasSplit) {
      vds.annotateVariantsVDS(
        if (!otherWasSplit) other.dropSamples().splitMulti(false, matchStar, false) else other,
        code = variantsExpr match{
          case Some(ve) => Some(allelesExpr + "," + ve)
          case _ => Some(allelesExpr)
        }
      )
    } else {

      val ec = EvalContext(Map(
        "global" -> (0, vds.globalSignature),
        "v" -> (1, vds.vSignature),
        "va" -> (2, vds.vaSignature),
        "vds" -> (3, other.vaSignature),
        "aIndex" -> (4, TInt32())
      ))

      val localGlobalAnnotation = vds.globalAnnotation
      val inserterBuilder = new ArrayBuilder[Inserter]()

      val parsedVariantsExpr = variantsExpr match{
        case Some(ve) =>
          Some(Parser.parseAnnotationExprs(ve, ec, Some(Annotation.VARIANT_HEAD)))
        case _ => None
      }


      val (allelesExprPaths, allelesExprTypes, allelesF) = Parser.parseAnnotationExprs(allelesExpr, ec, Some(Annotation.VARIANT_HEAD))


      val signatureVariantsExpr = parsedVariantsExpr match {
        case Some((variantsExprPaths, variantsExprTypes, variantsF)) =>
          (variantsExprPaths, variantsExprTypes).zipped.foldLeft(vds.vaSignature) { case (vas, (ids, signature)) =>
            val (s, i) = vas.insert(signature, ids)
            inserterBuilder += i
            s
          }
        case _ => vds.vaSignature
      }
      val variantsExprInserters = inserterBuilder.result()
      inserterBuilder.clear()

      val signatureAllelesExpr = (allelesExprPaths, allelesExprTypes).zipped.foldLeft(signatureVariantsExpr) { case (vas, (ids, signature)) =>
        val (s, i) = vas.insert(TArray(signature), ids)
        inserterBuilder += i
        s
      }

      val newSignature = signatureAllelesExpr match {
        case t: TStruct =>
          allelesExprPaths.foldLeft(t) { case (res, path) =>
            res.setFieldAttributes(path, Map("Number" -> "A"))
          }
        case _ => signatureAllelesExpr
      }
      val allelesExprInserters = inserterBuilder.result()

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

            val allelesAnnotations = Array.ofDim[Annotation](v.nAltAlleles)
            var annotationsForVariants = None: Option[Annotation]

            altAllelesAndAnnotations.map(x => x.zipWithIndex.foreach {
              case ((allele, ann), oldIndex) =>
                if (matchStar || allele.alt != "*") {
                  val newIndex = v.altAlleleIndex(allele)
                  if (newIndex > -1) {
                    ec.setAll(localGlobalAnnotation, v, va, ann,
                      if (otherWasSplit) 1 else oldIndex + 1)
                    allelesAnnotations.update(newIndex, allelesF(): IndexedSeq[Any])
                    annotationsForVariants match {
                      case Some(x) =>
                      case None => annotationsForVariants = Some(ann)
                    }
                  }
                }
            })


            val newVA = parsedVariantsExpr match {
              case Some((variantsExprPaths, variantsExprTypes, variantsF)) =>
                ec.setAll(localGlobalAnnotation, v, va, annotationsForVariants.orNull)
                (variantsF() ++ allelesAnnotations).zip(variantsExprInserters ++ allelesExprInserters).foldLeft(va) {
                  case (va, (ann, inserter)) => inserter(va, ann)
                }

              case _ => allelesAnnotations.zip(allelesExprInserters).foldLeft(va) {
                case (va, (ann, inserter)) => inserter(va, ann)
              }

            }
            (v, (newVA, gs))
        }

      vds.copy(rdd = OrderedRDD(newRDD, vds.rdd.orderedPartitioner), vaSignature = newSignature)
    }
  }
}
