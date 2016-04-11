package org.broadinstitute.hail.methods

import breeze.linalg.{Vector, DenseVector, max, sum}
import breeze.numerics._

/**
  * Created by laurent on 6/20/16.
  */
object Phasing {


  /** Computes the probability of 2 variants being on the same haplotype
    * using the EM from phaseVariantPairWithEM
    *
    * @param genotypeCounts The genotype counts for the 9 possible genotype combination between the 2 variants:
    *                       HomRef / HomRef
    *                       Het / HomRef
    *                       HomVar / HomRef
    *                       HomRef / Het
    *                       Het / Het
    *                       HomVar / Het
    *                       HomRef / HomVar
    *                       Het / HomVar
    *                       HomVar / HomVar
    * @return The probability of both variants being on the same haplotype
    */
  def probOnSameHaplotypeWithEM(genotypeCounts : Vector[Int]) : Option[Double] = {
    phaseVariantPairWithEM(genotypeCounts) match{
      case Some(haplotypes) =>
        return Some(haplotypes(0) * haplotypes(3) / (haplotypes(1) * haplotypes(2) + haplotypes(0) * haplotypes(3)))
      case None => None
    }
  }

  /** Implementents the EM discribed in
    * Maximum-Likelihood  Estimation  of Molecular  Haplotype  Frequencies  in a Diploid  Population
    * Escoffier & Slatkin, Mol. Biol. Evol. 1995
    *
    * @param gtCounts The genotype counts for the 9 possible genotype combination between the 2 variants:
    *                       HomRef / HomRef
    *                       Het / HomRef
    *                       HomVar / HomRef
    *                       HomRef / Het
    *                       Het / Het
    *                       HomVar / Het
    *                       HomRef / HomVar
    *                       Het / HomVar
    *                       HomVar / HomVar
    * @return The estimated number of haplotypes carrying the variants:
    *         AB (no variants)
    *         Ab (variant b only)
    *         aB (variant a only)
    *         ab (both variants)
    */
  def phaseVariantPairWithEM(gtCounts : Vector[Int]) : Option[DenseVector[Double]] = {

    assert(gtCounts.size == 9, "phaseVariantPairWithEM requires genotype count for the 9 possible genotype combinations.")

    val nSamples = sum(gtCounts)

    //Needs some non-ref samples to compute
    if(gtCounts(0) >= nSamples){ return None}

    val nHaplotypes = 2.0*nSamples.toDouble

    /**
      * Constant quantities for each of the different haplotypes:
      * n.AB => 2*n.AABB + n.AaBB + n.AABb
      * n.Ab => 2*n.AAbb + n.Aabb + n.AABb
      * n.aB => 2*n.aaBB + n.AaBB + n.aaBb
      * n.ab => 2*n.aabb + n.aaBb + n.Aabb
      */
    val const_counts = new DenseVector(Array[Double](
      2.0*gtCounts(0) + gtCounts(1) + gtCounts(3), //n.AB
      2.0*gtCounts(6) + gtCounts(3) + gtCounts(7), //n.Ab
      2.0*gtCounts(2) + gtCounts(1) + gtCounts(5), //n.aB
      2.0*gtCounts(8) + gtCounts(5) + gtCounts(7)  //n.ab
    ))

    //Initial estimate with AaBb contributing equally to each haplotype
    var p_next = (const_counts :+ new DenseVector(Array.fill[Double](4)(gtCounts(4)/2.0))) :/ nHaplotypes
    var p_cur = p_next :+ 1.0

    var i = 0

    //EM
    while(max(abs(p_next :- p_cur)) > 1e-7){
      i += 1
      p_cur = p_next

      p_next = (const_counts :+
        (new DenseVector(Array[Double](
          p_cur(0)*p_cur(3), //n.AB
          p_cur(1)*p_cur(2), //n.Ab
          p_cur(1)*p_cur(2), //n.aB
          p_cur(0)*p_cur(3)  //n.ab
        )) :* gtCounts(4) / ((p_cur(0)*p_cur(3))+(p_cur(1)*p_cur(2))) )
        ) :/ nHaplotypes

    }

    return Some(p_next :* nHaplotypes)
  }





}
