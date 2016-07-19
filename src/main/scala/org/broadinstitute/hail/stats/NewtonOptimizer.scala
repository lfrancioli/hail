package org.broadinstitute.hail.stats

import breeze.linalg._

class NewtonOptimizer(gradient: DenseVector[Double] => DenseVector[Double], hessian: DenseVector[Double] => DenseMatrix[Double]) {

  def optimize(x0: DenseVector[Double], tolerance: Double = 1e-3, maxIter: Int = 10): Option[DenseVector[Double]] = {
    val x = x0.copy
    val g = gradient(x0)
    val h = hessian(x0)

    println(s"0: $x, $g")

    for (i <- 1 to maxIter) {
      x := x - h \ g  // FIXME: use Cholesky (even better, for lease squares, don't compute h!)
      g := gradient(x)

      println(s"$i: $x, $g")

      if (norm(g) < tolerance)
        return Some(x)

      h := hessian(x)
    }

    None
  }
}