package org.broadinstitute.hail.methods

import org.apache.spark.rdd.RDD
import org.broadinstitute.hail.methods.PhasedGenotypeType.PhasedGenotypeType
import org.broadinstitute.hail.utils.SparseVariantSampleMatrix
import org.broadinstitute.hail.variant.{Genotype, GenotypeType}
import org.broadinstitute.hail.variant.GenotypeType._
import org.apache.commons.math3.stat.inference
import org.apache.commons.math3.stat.inference.{AlternativeHypothesis, BinomialTest}

import scala.collection.mutable

//Super-ugly!
object PhasedGenotypeType extends  Enumeration{
  type PhasedGenotypeType = Value
  val HomRef = Value(0)
  val Het = Value(1)
  val HomVar = Value(2)
  val NoCall = Value(-1)
  val CompHet = Value(3)

}

//class GeneBurden(val caseCounts : mutable.HashMap[PhasedGenotypeType,Int],val controlCounts : mutable.HashMap[PhasedGenotypeType,Int], val cumulAF : Double)  {
class GeneBurden(val cases: SparseVariantSampleMatrix, val controls: SparseVariantSampleMatrix){

  /**
  //Compute counts
  val casesCounts = getCounts(cases, controls)
  val controlsCounts = getCounts(controls, controls,2)
  val cumulAF = controls.cumulativeAF //TODO Correct for in-phase hets

  //Compute p-values
  var pValRec = computeRecessiveEnrichment()
  var pValDom = 1

  //Stores variants / genotype info for all significantly enriched genes
  val casesVariants = if(pValRec < 5e-5 || pValDom < 5e-5) cases.variants else mutable.Map[String, mutable.Map[String,Genotype]]()
  val controlsVariants = if(pValRec < 5e-5 || pValDom < 5e-5) controls.variants else mutable.Map[String, mutable.Map[String,Genotype]]()

  /** Public Functions **/
  override def toString(): String ={
    return ""
  }

  /** Private Functions **/
  private def computeRecessiveEnrichment(): Double = {
    val rate = cumulAF * cumulAF
    new BinomialTest().binomialTest(cases.nSamples, casesCounts(PhasedGenotypeType.HomVar) + casesCounts(PhasedGenotypeType.CompHet), rate, AlternativeHypothesis.GREATER_THAN)
  }

  private def isCompoundHet(variantIDs: List[String], phasingData: SparseVariantSampleMatrix, minPhasingSamples: Int = 1) : Boolean = {

    val phasingVariants = phasingData.variants

    //Loop over all pairs of variants (ordered lexicographically)
    (for( i<-variantIDs; j <- variantIDs; if(i < j && phasingVariants.contains(i) && phasingVariants.contains(j))) yield{
      //For each pair of variants, count how many samples carry both
      (phasingVariants(i).filter({case (k,v) => v == GenotypeType.Het || v == GenotypeType.HomVar}).keySet.intersect(
        phasingVariants(j).filter({case (k,v) => v == GenotypeType.Het || v == GenotypeType.HomVar}).keySet).size)

    }).max >= minPhasingSamples
  }

  private def getCounts(vs: SparseVariantSampleMatrix, phasingData: SparseVariantSampleMatrix, minPhasingSamples: Int = 1) : mutable.HashMap[PhasedGenotypeType,Int] = {
    val res = new mutable.HashMap[PhasedGenotypeType,Int]()

    vs.samples.foreach({ case (sampleID, variants) =>
      //Goes from Variant -> GT to GT -> Variants -- TODO: Unless Variant -> GT used elsewhere, change the representation in SVSM
      val byGT = variants.foldRight(Map[GenotypeType, List[String]]())({
        case (variant, acc) => acc updated(variant._2.gtType, variant._1 :: acc.getOrElse(variant._2.gtType, List[String]()))
      })

      //Count genotypes in order of deleteriousness
      if (byGT.contains(GenotypeType.HomVar)) {
        res.update(PhasedGenotypeType.HomVar, res.getOrElse(PhasedGenotypeType.HomVar, 0) + 1)
      }
      else if (byGT.contains(GenotypeType.Het)) {
        if (byGT.get(GenotypeType.Het).size > 2 && isCompoundHet(byGT(GenotypeType.Het), phasingData, minPhasingSamples)) {
          res.update(PhasedGenotypeType.Het, res.getOrElse(PhasedGenotypeType.CompHet, 0) + 1)
        }
        else {
          res.update(PhasedGenotypeType.Het, res.getOrElse(PhasedGenotypeType.Het, 0) + 1)
        }
      }
      //Missing data is accumulated -- inconsistent with other genotypes but might be helpful to have..
      if(byGT.contains(GenotypeType.NoCall)){
        res.update(PhasedGenotypeType.HomVar, res.getOrElse(PhasedGenotypeType.HomVar, 0) + byGT(GenotypeType.NoCall).size)
      }

    })

    res
  }
**/

}

