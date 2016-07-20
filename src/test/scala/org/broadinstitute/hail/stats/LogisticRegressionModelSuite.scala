package org.broadinstitute.hail.stats

import org.scalatest.testng.TestNGSuite
import org.testng.annotations.Test
import breeze.linalg._
import org.broadinstitute.hail.Utils._
import org.apache.commons.math3.distribution.ChiSquaredDistribution

class LogisticRegressionModelSuite extends TestNGSuite {

  @Test def modelTest() = {

    /* R code:
    y0 = c(0, 0, 1, 1, 1, 1)
    c1 = c(0, 2, 1, -2, -2, 4)
    c2 = c(-1, 3, 5, 0, -4, 3)
    logfit <- glm(y0 ~ c1 + c2, family=binomial(link="logit"))
    summary(logfit)["coefficients"]
    */

    val y = DenseVector(0d, 0d, 1d, 1d, 1d, 1d)

    val X = DenseMatrix(
      (1.0, 0.0, -1.0),
      (1.0, 2.0, 3.0),
      (1.0, 1.0, 5.0),
      (1.0, -2.0, 0.0),
      (1.0, -2.0, -4.0),
      (1.0, 4.0, 3.0))

    val model = new LogisticRegressionModel(X,y)
    val fit = model.fitInit()
    val waldStat = fit.waldTest()

    val b0 = model.bInterceptOnly()
    val loglk0 = model.loglkInterceptOnly()

    val chiSqDist = new ChiSquaredDistribution(X.cols - 1)
    val scoreStat = model.scoreTest(b0, chiSqDist)

    val loglk = fit.loglk(y)
    val lrStat = fit.likelihoodRatioTest(y, loglk0, chiSqDist)

    assert(D_==(waldStat.b(0), 0.7245034, tolerance = 1.0E-6))
    assert(D_==(waldStat.b(1), -0.3585773, tolerance = 1.0E-6))
    assert(D_==(waldStat.b(2), 0.1922622, tolerance = 1.0E-6))

    assert(D_==(waldStat.se(0), 0.9396654, tolerance = 1.0E-6))
    assert(D_==(waldStat.se(1), 0.6246568, tolerance = 1.0E-6))
    assert(D_==(waldStat.se(2), 0.4559844, tolerance = 1.0E-6))

    assert(D_==(waldStat.z(0), 0.7710228, tolerance = 1.0E-6))
    assert(D_==(waldStat.z(1), -0.5740389, tolerance = 1.0E-6))
    assert(D_==(waldStat.z(2), 0.4216421, tolerance = 1.0E-6))

    assert(D_==(waldStat.p(0), 0.4406934, tolerance = 1.0E-6))
    assert(D_==(waldStat.p(1), 0.5659415, tolerance = 1.0E-6))
    assert(D_==(waldStat.p(2), 0.6732863, tolerance = 1.0E-6))

    assert(D_==(loglk0, -3.81908501,tolerance = 1.0E-6))
    assert(D_==(loglk, -3.643316979, tolerance = 1.0E-6))
    println(lrStat)
    assert(D_==(lrStat.p, 0.8388125392, tolerance = 1.0E-5))

    assert(D_==(scoreStat.chisq, 0.346648, tolerance = 1.0E-5))
    assert(D_==(scoreStat.p, 0.8408652791, tolerance = 1.0E-5))
  }

}