package is.hail

import is.hail.stats._
import breeze.linalg.{Vector, DenseVector, max, sum}
import breeze.numerics._
import is.hail.expr.types._

package object experimental {

  def findMaxAC(af: Double, an: Int, ci: Double = .95): Int = {
   if (af == 0)
      0
    else {
      val quantile_limit = ci // ci for one-sided, 1-(1-ci)/2 for two-sided
      val max_ac = qpois(quantile_limit, an * af)
      max_ac
    }
  }

  def calcFilterAlleleFreq(ac: Int, an: Int, ci: Double = .95, lower: Double = 1e-10, upper: Double = 2, tol: Double = 1e-7, precision: Double = 1e-6): Double = {
    if (ac <= 0 || an == 0)
      0.0
    else {
      var f = (af: Double) => ac.toDouble - 1 - qpois(ci, an.toDouble * af)
      val root = uniroot(f, lower, upper, tol)
      val rounder = 1d / (precision / 100d)
      var max_af = math.round(root.getOrElse(0.0) * rounder) / rounder
      while (findMaxAC(max_af, an) < ac) {
        max_af += precision
      }
      max_af - precision
    }
  }

  def calcFilterAlleleFreq(ac: Int, an: Int, ci: Double): Double = calcFilterAlleleFreq(ac, an, ci, lower = 1e-10, upper = 2, tol = 1e-7, precision = 1e-6)


  def probSameHapEM(n00: Int, n01: Int, n02: Int, n10: Int, n11: Int, n12: Int, n20: Int, n21: Int, n22: Int) : Option[Double] = {
  //def probSameHapEM(gtCounts: Array[Double]) : Option[Double] = {


    val _gtCounts = DenseVector(n00, n01, n02, n10, n11, n12, n20, n21, n22)
    //val _gtCounts = new DenseVector(gtCounts)
    assert(_gtCounts.size == 9, "haplotypeFreqEM requires genotype counts for the 9 possible genotype combinations.")

    val nSamples = sum(_gtCounts)

    //Needs some non-ref samples to compute

    val nHaplotypes = 2.0*nSamples.toDouble
    if(_gtCounts(0) >= nSamples){ return None}

    /**
      * Constant quantities for each of the different haplotypes:
      * n.AB => 2*n.AABB + n.AaBB + n.AABb
      * n.Ab => 2*n.AAbb + n.Aabb + n.AABb
      * n.aB => 2*n.aaBB + n.AaBB + n.aaBb
      * n.ab => 2*n.aabb + n.aaBb + n.Aabb
      */
    val const_counts = new DenseVector(Array[Double](
      2.0*_gtCounts(0) + _gtCounts(1) + _gtCounts(3), //n.AB
      2.0*_gtCounts(6) + _gtCounts(3) + _gtCounts(7), //n.Ab
      2.0*_gtCounts(2) + _gtCounts(1) + _gtCounts(5), //n.aB
      2.0*_gtCounts(8) + _gtCounts(5) + _gtCounts(7)  //n.ab
    ))

    //Initial estimate with AaBb contributing equally to each haplotype
    var p_next = (const_counts :+ new DenseVector(Array.fill[Double](4)(_gtCounts(4)/2.0))) :/ nHaplotypes
    var p_cur = p_next :+ 1.0

    //EM
    while(max(abs(p_next :- p_cur)) > 1e-7){
      p_cur = p_next

      p_next = (const_counts :+
        (new DenseVector(Array[Double](
          p_cur(0)*p_cur(3), //n.AB
          p_cur(1)*p_cur(2), //n.Ab
          p_cur(1)*p_cur(2), //n.aB
          p_cur(0)*p_cur(3)  //n.ab
        )) :* _gtCounts(4) / ((p_cur(0)*p_cur(3))+(p_cur(1)*p_cur(2))) )
        ) :/ nHaplotypes

    }

    val haplotypes = (p_next :* nHaplotypes)
    return Some(haplotypes(0) * haplotypes(3) / (haplotypes(1) * haplotypes(2) + haplotypes(0) * haplotypes(3)))
  }

}
