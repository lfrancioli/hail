package org.broadinstitute.hail.stats

import breeze.linalg._
import breeze.numerics._
import org.apache.commons.math3.distribution.ChiSquaredDistribution

// need to catch linalg exceptions like singular matrix inversion
class LogisticRegressionModel(X: DenseMatrix[Double], y: DenseVector[Double]) {
  require(y.length == X.rows)

  val n = X.rows
  val m = X.cols

  //move to stat utils?
  def logodds(x: Double): Double = math.log(x / (1 - x))

  //possibly remove once handled by general case
  def loglkInterceptOnly(): Double = {
    val avg = sum(y) / n
    sum(log(y * avg + (1d - y) * (1d - avg)))
  }

  def bInterceptOnly(): DenseVector[Double] = {
    val b0 = DenseVector.zeros[Double](m)
    b0(0) = logodds(sum(y) / n)
    b0
  }

  // intended per variant, starting from fit without genotype
  def fit(b0: DenseVector[Double] = DenseVector.zeros[Double](m), maxIter: Int = 100, tol: Double = 1E-10): LogisticRegressionFit = {
    require(X.cols == b0.length)
    require(maxIter > 0)

    var b = b0.copy
    var mu = DenseVector.zeros[Double](n)
    var score = DenseVector.zeros[Double](m)
    var fisher = DenseMatrix.zeros[Double](m, m)
    var iter = 0
    var converged = false

    while (!converged && iter < maxIter) {
      iter += 1

      mu = sigmoid(X * b)
      score = X.t * (mu - y) // check sign
      fisher = X.t * (X(::, *) :* (mu :* (1d - mu))) // would mu.map(x => x * (1 - x)) be faster?

//      alternative algorithm avoids both mult by X.t and direct inversion
//      val qrRes = qr.reduced(diag(sqrt(mu :* (1d - mu))) * X)
//      solve qrRes.R * bDiff = qrRes.Q.t * (y - mu) with R upper triangular
//      return diagonal of inverse as well, which is diagonal of inv(R)^T * inv(R)

//      println(s"b = $b")
//      println(s"mu = $mu")
//      println(s"score = $score")
//      println(s"fisher = $fisher")

      // catch singular here ... need to recognize when singular implies fit versus other issues
      val bDiff = fisher \ score // could also return bDiff if last adjustment improves Wald accuracy. Conceptually better to have b, mu, and fisher correspond.

      if (norm(bDiff) < tol)
        converged = true
      else
        b -= bDiff
    }

    LogisticRegressionFit(b, mu, fisher, converged, iter)
  }

  // could start from mu
  // one chiSqDist per partition
  def scoreTest(b: DenseVector[Double], chiSqDist: ChiSquaredDistribution): ScoreStat = {
    require(X.cols == b.length)

    val mu = sigmoid(X * b)
    val y0 = X.t * (y - mu)
    val chi2 = y0 dot ((X.t * (X(::, *) :* (mu :* (1d - mu)))) \ y0)

    //alternative approach using QR:
    //val sqrtW = sqrt(mu :* (1d - mu))
    //val Qty0 = qr.reduced.justQ(X(::, *) :* sqrtW).t * ((y - mu) :/ sqrtW)
    //val chi2 = Qty0 dot Qty0  // better to create normSq Ufunc

    val p = 1d - chiSqDist.cumulativeProbability(chi2)

    ScoreStat(chi2, p)
  }
}

case class LogisticRegressionFit(
  b: DenseVector[Double],
  mu: DenseVector[Double],
  fisher: DenseMatrix[Double],
  converged: Boolean,
  nIter: Int) {

  def loglk(y: DenseVector[Double]): Double = sum(log((y :* mu) + ((1d - y) :* (1d - mu))))

  def waldTest(): WaldStat = {
    val se = sqrt(diag(inv(fisher))) // breeze uses LU to invert, dgetri...for Wald, better to pass through from fit?  if just gt, can solve fisher \ (1,0,...,0) or use schur complement
    val z = b :/ se
    val sqrt2 = math.sqrt(2)
    val p = z.map(zi => 1 + erf(-abs(zi) / sqrt2))

    WaldStat(b, se, z, p)
  }

  // one chiSqDist per partition
  def likelihoodRatioTest(y: DenseVector[Double], loglk0: Double, chiSqDist: ChiSquaredDistribution): LikelihoodRatioStat = {
    val chi2 = 2 * (loglk(y) - loglk0)
    val p = 1d - chiSqDist.cumulativeProbability(chi2)

    LikelihoodRatioStat(b, chi2, p)
  }
}

case class WaldStat(b: DenseVector[Double], se: DenseVector[Double], z: DenseVector[Double], p: DenseVector[Double])

case class ScoreStat(chi2: Double, p: Double)

case class LikelihoodRatioStat(b: DenseVector[Double], chi2: Double, p: Double)
