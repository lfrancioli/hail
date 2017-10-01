package is.hail.methods

import is.hail.utils.richUtils.RichRDD
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel
import is.hail.annotations._
import is.hail.utils.{SparseVariantSampleMatrix, SparseVariantSampleMatrixRRDBuilder}
import is.hail.variant._
import is.hail.utils._
import is.hail.expr.{TArray, TBoolean, TDouble, TGenotype, TString, TStruct, TVariant, Type}
import is.hail.keytable.KeyTable
import org.apache.spark.sql.Row

import scala.collection.mutable
import scala.language.postfixOps

object PhaseTrios {

  //Returns all pairs of variants for which the parent parentID is het at both sites, along with
  //whether each of the variant pairs are on the same haplotype or not based on the transmission of alleles in the trio (or None if ambiguous / missing data)
  private def getHetPhasedVariantPairs(trios: SparseVariantSampleMatrix, kidID: String, parentID: String, otherParentID: String) : Set[(Variant,Variant,Option[Boolean])] = {
    val genotypes = trios.getSample(parentID)

    (for((variant1, gt1) <- genotypes if gt1.isHet; (variant2,gt2) <- genotypes if gt2.isHet && variant1.compare(variant2) < 0) yield{
      //info("Found variant pair: " + variant1 + "/" + variant2 + " in parent " + parentID)
      (variant1, variant2, isOnSameParentalHaplotype(trios,variant1,variant2,kidID,otherParentID))
    }).toSet

  }

  //Given a site that is Het in parent1,
  //returns whether the site was transmitted from parent1 or from the otherParent
  //Returns None if ambiguous or missing data
  private def isHetSiteTransmitted(kidGT: Option[Genotype], otherParentGT: Option[Genotype]): Option[Boolean] = {
    kidGT match {
      case Some(kid) => {
        if(kid.isHomRef){ return Some(false)}
        else if(kid.isHomVar){return Some(true)}
        else if(kid.isHet){
          otherParentGT match {
            case Some(gt) => {
              if(gt.isHomRef){ return Some(true)}
              else if(gt.isHomVar){return Some(false)}
            }
            case None => None
          }
        }
      }
      case None => None
    }
    None
  }


  //Given two variants that are het in a parent, computes whether they are on the same haplotype or not based on
  //the child and the otherParent in the trio.
  //Returns None if ambiguous or missing datta
  private def isOnSameParentalHaplotype(trios: SparseVariantSampleMatrix, variant1: Variant, variant2: Variant, kidID: String, otherParentID: String): Option[Boolean] = {

    val v1POO = isHetSiteTransmitted(trios.getGenotype(variant1, kidID), trios.getGenotype(variant1, otherParentID))
    val v2POO = isHetSiteTransmitted(trios.getGenotype(variant2, kidID), trios.getGenotype(variant2, otherParentID))

    (v1POO, v2POO) match {
      case (Some(v1poo), Some(v2poo)) => Some(v1poo == v2poo)
      case _ => None
    }

  }

  def apply(trioVDS: VariantDataset, ped : Pedigree, vaKeys: Array[String],  number_partitions: Int) : KeyTable = {

    //Get SparkContext
    val sc = trioVDS.sparkContext

    //Get annotations
    val vaKeysQueriers = vaKeys.map(vak => trioVDS.queryVA(vak))

    val partitioner = new HashPartitioner(number_partitions)

    val ped_in_vds = ped.filterTo(trioVDS.sampleIds.map(_.asInstanceOf[String]).toSet).completeTrios

    info(s"Found ${ped_in_vds.length} complete trios in VDS.")

    val triosRDD = SparseVariantSampleMatrixRRDBuilder.buildByVA(trioVDS, sc, partitioner)(
      { case (v, va) => Row.fromSeq(vaKeysQueriers.map(q => q._2(va)).toSeq) }
    ).flatMap {
      case (key, svm) =>

        ped_in_vds.flatMap {
          case trio =>
            val phasePairs = getHetPhasedVariantPairs(svm, trio.kid, trio.mom.get, trio.dad.get) ++
              getHetPhasedVariantPairs(svm, trio.kid, trio.dad.get, trio.mom.get)

            phasePairs.filter(_._3.isDefined).map {
              case (v1, v2, onSameHaplotype) =>
                val switch = v1.compare(v2) > 0

                Row.fromSeq(key.toSeq ++
                  Seq(
                    if (switch) v2 else v1,
                    if (switch) svm.getVariantAnnotations(v2) else svm.getVariantAnnotations(v1),
                    if (switch) v1 else v2,
                    if (switch) svm.getVariantAnnotations(v1) else svm.getVariantAnnotations(v2),
                    trio.kid,
                    svm.getGenotype(v1,trio.kid).getOrElse(null),
                    svm.getGenotype(v2,trio.kid).getOrElse(null),
                    svm.getSampleAnnotations(trio.kid),
                    trio.mom.get,
                    svm.getGenotype(v1,trio.mom.get).getOrElse(null),
                    svm.getGenotype(v2,trio.mom.get).getOrElse(null),
                    svm.getSampleAnnotations(trio.mom.get),
                    trio.dad.get,
                    svm.getGenotype(v1,trio.dad.get).getOrElse(null),
                    svm.getGenotype(v2,trio.dad.get).getOrElse(null),
                    svm.getSampleAnnotations(trio.dad.get),
                    onSameHaplotype.get
                  )
                )
            }
        }
    }

    val valueTypes = Array(
      ("v1", TVariant),
      ("va1", trioVDS.vaSignature),
      ("v2", TVariant),
      ("va2", trioVDS.vaSignature),
      ("kid", TString),
      ("kid_v1", TGenotype),
      ("kid_v2", TGenotype),
      ("kidSA", trioVDS.saSignature),
      ("mom", TString),
      ("mom_v1", TGenotype),
      ("mom_v2", TGenotype),
      ("momSA", trioVDS.saSignature),
      ("dad", TString),
      ("dad_v1", TGenotype),
      ("dad_v2", TGenotype),
      ("dadSA", trioVDS.saSignature),
      ("same_haplotype", TBoolean)
    )

    val ktSignature = TStruct(
      (vaKeys.zip(vaKeysQueriers.map(_._1)) ++
        valueTypes).toSeq: _*)

    val kt = KeyTable(trioVDS.hc, triosRDD, ktSignature, key = vaKeys)
    kt.typeCheck()
    info("Keytable types checked!")
    return kt

  }

}
