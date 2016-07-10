package org.broadinstitute.hail.stats

import breeze.linalg._
import org.broadinstitute.hail.Utils._

class NewtonOptimizer(gradient: DenseVector[Double] => DenseVector[Double], hessian: DenseVector[Double] => DenseMatrix[Double]) {

  def optimize(x0: DenseVector[Double], tolerance: Double = 1e-3, maxIter: Int = 10): DenseVector[Double] = {
    val x = x0
    val g = gradient(x0)
    val h = hessian(x0)

    println(s"0: $x, $g")

    for (i <- 1 to maxIter) {
      x := x - h \ g  // FIXME: use Cholesky
      g := gradient(x)

      println(s"$i: $x, $g")

      if (norm(g) < tolerance)
        return x

      h := hessian(x)
    }

    fatal("Failed to converge!")
  }
}