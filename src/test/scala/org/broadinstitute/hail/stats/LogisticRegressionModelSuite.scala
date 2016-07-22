package org.broadinstitute.hail.stats

import org.scalatest.testng.TestNGSuite
import org.testng.annotations.Test
import breeze.linalg._
import org.broadinstitute.hail.Utils._
import org.apache.commons.math3.distribution.ChiSquaredDistribution

class LogisticRegressionModelSuite extends TestNGSuite {

  @Test def covariatesVersusInterceptOnlyTest() = {

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
    val fit = model.fit()
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
    assert(D_==(lrStat.chi2, 0.351536062, tolerance = 1.0E-6))
    assert(D_==(lrStat.p, 0.8388125392, tolerance = 1.0E-5))

    assert(D_==(scoreStat.chi2, 0.346648, tolerance = 1.0E-5))
    assert(D_==(scoreStat.p, 0.8408652791, tolerance = 1.0E-5))
  }

  @Test def covariatesAndGtsVersusCovariatesOnlyTest() = {

    /* R code:
    y0 = c(0, 0, 1, 1, 1, 1)
    c1 = c(0, 2, 1, -2, -2, 4)
    c2 = c(-1, 3, 5, 0, -4, 3)
    gts = c(0, 1, 2, 0, 0, 1)
    logfit <- glm(y0 ~ c1 + c2 + gts, family=binomial(link="logit"))
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

    val gts = DenseVector(0d, 1d, 2d, 0d, 0d, 1d)

    val gtsX = DenseMatrix.horzcat(gts.asDenseMatrix.t, X)

    val nullModel = new LogisticRegressionModel(X, y)
    val nullFit = nullModel.fit(nullModel.bInterceptOnly())
    val loglk0 = nullFit.loglk(y)

    val model = new LogisticRegressionModel(gtsX, y)
    val b0 = DenseVector.vertcat(DenseVector(0d), nullFit.b)
    val fit = model.fit(b0)
    val loglk = fit.loglk(y)

    val chiSqDist = new ChiSquaredDistribution(1)

    val waldStat = fit.waldTest()
    val scoreStat = model.scoreTest(b0, chiSqDist)
    val lrStat = fit.likelihoodRatioTest(y, loglk0, chiSqDist)

//    println(waldStat)

    assert(D_==(waldStat.b(0), 3.1775729, tolerance = 1.0E-6))
    assert(D_==(waldStat.b(1), -0.4811418, tolerance = 1.0E-6))
    assert(D_==(waldStat.b(2), -0.4293547, tolerance = 1.0E-6))
    assert(D_==(waldStat.b(3), -0.4214875, tolerance = 1.0E-6))

    // loss of precision here relative to R... 4.10942073414 ???
    assert(D_==(waldStat.se(0), 4.1093314, tolerance = 1.0E-4))
    assert(D_==(waldStat.se(1), 1.6203512, tolerance = 1.0E-4))
    assert(D_==(waldStat.se(2), 0.7256555, tolerance = 1.0E-4))
    assert(D_==(waldStat.se(3), 0.9223513, tolerance = 1.0E-4))

    assert(D_==(waldStat.z(0), 0.7732579, tolerance = 1.0E-4))
    assert(D_==(waldStat.z(1), -0.2969367, tolerance = 1.0E-4))
    assert(D_==(waldStat.z(2), -0.5916785, tolerance = 1.0E-4))
    assert(D_==(waldStat.z(3), -0.4569707, tolerance = 1.0E-4))

    assert(D_==(waldStat.p(0), 0.4393698, tolerance = 1.0E-4))
    assert(D_==(waldStat.p(1), 0.7665148, tolerance = 1.0E-4))
    assert(D_==(waldStat.p(2), 0.5540659, tolerance = 1.0E-4))
    assert(D_==(waldStat.p(3), 0.6476921, tolerance = 1.0E-4))

    assert(D_==(loglk0, -3.6433170,tolerance = 1.0E-6))
    assert(D_==(loglk, -3.2066547, tolerance = 1.0E-6))
    assert(D_==(lrStat.chi2, 0.8733246, tolerance = 1.0E-6))
    assert(D_==(lrStat.p, 0.35003656, tolerance = 1.0E-5))

//    Need something to test against
//    assert(D_==(scoreStat.chi2, ???, tolerance = 1.0E-5))
//    assert(D_==(scoreStat.p, ???, tolerance = 1.0E-5))
  }
}