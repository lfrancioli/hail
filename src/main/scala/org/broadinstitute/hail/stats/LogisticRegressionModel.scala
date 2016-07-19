package org.broadinstitute.hail.stats

import breeze.linalg._
import breeze.numerics._
import org.apache.commons.math3.distribution.ChiSquaredDistribution

// how to handle intercept?
class LogisticRegressionModel(X: DenseMatrix[Double], y: DenseVector[Double]) {
  val n = y.length

  def logit(x: Double): Double = 1 / (1 + math.exp(-x))

  // intended for initial fit without genotype
  def fitNull(maxIter: Int = 50, tol: Double = 10E-6): LogisticRegressionFit = {
    val b0 = DenseVector.zeros[Double](n)
    b0(0) = logit(sum(y) / n)
    fit(b0, maxIter, tol)
  }

  // intended per variant, starting from fit without genotype
  // could be slightly more efficient by starting from mu0 instead of b0
  def fit(b0: DenseVector[Double], maxIter: Int = 50, tol: Double = 10E-6): LogisticRegressionFit = {
    // check maxIter > 0

    var b = b0.copy
    var mu = DenseVector.zeros[Double](n)
    var score = DenseVector.zeros[Double](n)
    var fisher = DenseMatrix.zeros[Double](n, n)
    var iter = 0
    var converged = false

    while (!converged && iter < maxIter) {
      iter += 1

      mu = sigmoid(X * b)
      score = X.t * (y - mu)
      fisher = X.t * diag(mu :* (1d - mu)) * X

      // alternative algorithm avoids both mult by X.t and direct inversion
      // val qrRes = qr.reduced(diag(sqrt(mu :* (1d - mu))) * X)
      // solve qrRes.R * bDiff = qrRes.Q.t * (y - mu) with R upper triangular
      // return diagonal of inverse as well, diagonal of inv(R)^T * inv(R)

      val bDiff = fisher \ score

      if (norm(bDiff) < tol)
        converged = true
      else
        b -= bDiff
    }

    LogisticRegressionFit(b, mu, fisher, converged, iter)
  }

  // mu0 is from nullFit, degrees of freedom is 1
  // one chiSqDist per partition
  def scoreTest(mu0: DenseVector[Double], chiSqDist: ChiSquaredDistribution): ScoreStat = {
    val Qty = qr.reduced.justQ(diag(sqrt(mu0 :* (1d - mu0))) * X).t * (y - mu0)
    val z = norm(Qty) // better to compute square directly
    val chisq = z * z
    val p = 1d - chiSqDist.cumulativeProbability(chisq)

    ScoreStat(chisq, p)
  }
}

case class LogisticRegressionFit(
  b: DenseVector[Double],
  mu: DenseVector[Double],
  fisher: DenseMatrix[Double],
  converged: Boolean,
  nIter: Int) {

  def loglk(y: DenseVector[Double]): Double = sum(log(y :* mu) + ((1d - y) :* (1d - mu)))

  def waldTest: WaldStat = {
    val se = sqrt(diag(inv(fisher))) // breeze uses LU to invert, dgetri...for Wald, better to pass through from fit?
    val z = b :/ se
    val sqrt2 = math.sqrt(2)
    val p = z.map(zi => 1 + erf(-abs(zi) / sqrt2))

    WaldStat(b, se, z, p)
  }

  // one chiSqDist per partition
  def likelihoodRatioTest(y: DenseVector[Double], loglk0: Double, chiSqDist: ChiSquaredDistribution): LikelihoodRatioStat = {
    val chisq = 2 * (loglk(y) - loglk0)
    val p = 1d - chiSqDist.cumulativeProbability(chisq)

    LikelihoodRatioStat(b, chisq, p)
  }
}

case class WaldStat(b: DenseVector[Double], se: DenseVector[Double], z: DenseVector[Double], p: DenseVector[Double])

case class ScoreStat(chisq: Double, p: Double)

case class LikelihoodRatioStat(b: DenseVector[Double], chisq: Double, p: Double)
