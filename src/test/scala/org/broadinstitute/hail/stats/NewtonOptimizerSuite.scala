package org.broadinstitute.hail.stats

import breeze.numerics.{abs, erf, exp, sigmoid, sqrt}
import org.scalatest.testng.TestNGSuite
import org.testng.annotations.Test
import org.broadinstitute.hail.Utils._
import breeze.linalg._
import org.apache.commons.math3.distribution.{TDistribution, ChiSquaredDistribution}

class NewtonOptimizerSuite extends TestNGSuite {

  @Test def glmTest() = {

    /* R code:
    y0 = c(0, 0, 1, 1, 1, 1)
    c1 = c(0, 2, 1, -2, -2, 4)
    c2 = c(-1, 3, 5, 0, -4, 3)

    linfit <- glm(y0 ~ c1 + c2, family=gaussian(link = "identity"))
    logfit <- glm(y0 ~ c1 + c2, family=binomial(link="logit"))
    firthfit <- logistf(y ~ c1 + c2, pl=FALSE)
    poisfit <- glm(y0 ~ c1 + c2, family=poisson(link = "log"))

    summary(linfit)["coefficients"]
    summary(logfit)["coefficients"]
    summary(firthfit)["coefficients"]
    summary(poisfit)["coefficients"]
    */

    val t = DenseVector(0d, 0d, 1d, 1d, 1d, 1d)

    val X = DenseMatrix(
      (1.0, 0.0, -1.0),
      (1.0, 2.0, 3.0),
      (1.0, 1.0, 5.0),
      (1.0, -2.0, 0.0),
      (1.0, -2.0, -4.0),
      (1.0, 4.0, 3.0))

    def linGrad(w: DenseVector[Double]): DenseVector[Double] = {
      val y = X * w
      X.t * (y - t)
    }

    def linHess(w: DenseVector[Double]): DenseMatrix[Double] = {
      X.t * X
    }

    def logGrad(w: DenseVector[Double]): DenseVector[Double] = {
      val y = sigmoid(X * w)
      X.t * (y - t)
    }

    def logHess(w: DenseVector[Double]): DenseMatrix[Double] = {
      val y = sigmoid(X * w)
      X.t * diag(y :* (1d - y)) * X
    }

    def logLogLk(w: DenseVector[Double], X: DenseMatrix[Double], t: DenseVector[Double]): Double = {
      val y = sigmoid(X * w)
      sum(breeze.numerics.log((t :* y) + ((1d - t) :* (1d - y))))
    }

    // could combine
    def logLogLkY(y: DenseVector[Double], t: DenseVector[Double]): Double =
      sum(breeze.numerics.log((t :* y) + ((1d - t) :* (1d - y))))

    def logScoreStat(X: DenseMatrix[Double], t: DenseVector[Double]): Double = {
      val n = t.length
      val mu = sum(t) / n
      val y = DenseVector.fill[Double](n, mu)
      val Qty = qr.reduced.justQ(X).t * (t - y)
      val z = norm(Qty)
      z * z / (mu * (1 - mu))

      //val W = diag(DenseVector.fill[Double](n, mu * (1 - mu)))
      //(X.t * (t - y)).t * inv(X.t * W * X) * (X.t * (t - y))
    }

    def firthGrad(w: DenseVector[Double]): DenseVector[Double] = {
      val y = sigmoid(X * w)
      val XsqrtW = diag(sqrt(y :* (1d - y))) * X
      val Q = qr.reduced.justQ(XsqrtW)
      //val k = norm(Q(*, ::))
      //val h = k :* k
      val h = DenseVector((0 until 6).map(i => Q(i,::) dot Q(i,::)).toArray)

      X.t * (y - t + (h :* (y - .5)))
    }

    def firthHess(w: DenseVector[Double]) = logHess(w)

    // p39 of http://www.ucl.ac.uk/~ucakiko/files/ikosmidis_thesis.pdf
    // also bias corrected but appears to converge to same place as logistic
    def firthGrad2(w: DenseVector[Double]) = {
      val y = sigmoid(X * w)
      val W = diag(y :* (1d - y))
      val Xfirth = sqrt(W) * X
      val Q = qr.reduced.justQ(Xfirth)
      //val k = norm(Q(*, ::))
      //val h = k :* k
      val H = Q * Q.t
      val h = DenseVector((0 until 6).map(i => H(i,i)).toArray)

      X.t * ((y - t) + ((H * inv(W) * (t - y) * (t - y).t).t * (h :* (y - .5))))
    }

    def poisGrad(w: DenseVector[Double]): DenseVector[Double] = {
      val y = exp(X * w)
      X.t * (y - t)
    }

    def poisHess(w: DenseVector[Double]): DenseMatrix[Double] = {
      val y = exp(X * w)
      X.t * diag(y) * X
    }

    val b0 = DenseVector(0d, 0d, 0d)

    var b = new NewtonOptimizer(linGrad, linHess).optimize(b0, tolerance = 1.0E-10, maxIter = 10).get

    val dof = X.rows - X.cols
    var sigma = sqrt((t - X * b).t * (t - X * b) / dof)
    var se = sigma * sqrt(diag(inv(linHess(b))))
    var z = b :/ se
    var tdist = new TDistribution(dof)
    var p = z.map(zi => 2 * tdist.cumulativeProbability(-math.abs(zi)))

    assert(D_==(b(0), 0.66524013314, tolerance = 1.0E-5))
    assert(D_==(b(1), -0.07703281027, tolerance = 1.0E-5))
    assert(D_==(b(2), 0.03994293866, tolerance = 1.0E-5))

    assert(D_==(se(0), 0.2784968181, tolerance = 1.0E-5))
    assert(D_==(se(1), 0.1796073122, tolerance = 1.0E-5))
    assert(D_==(se(2), 0.1281720944, tolerance = 1.0E-5))

    assert(D_==(z(0), 2.3886812700, tolerance = 1.0E-5))
    assert(D_==(z(1), -0.4288957354, tolerance = 1.0E-5))
    assert(D_==(z(2), 0.3116352187, tolerance = 1.0E-5))

    assert(D_==(p(0), 0.09685640909, tolerance = 1.0E-5))
    assert(D_==(p(1), 0.69693267208, tolerance = 1.0E-5))
    assert(D_==(p(2), 0.77571957353, tolerance = 1.0E-5))

    b = new NewtonOptimizer(logGrad, logHess).optimize(b0, tolerance = 1.0E-10, maxIter = 10).get
    se = sqrt(diag(inv(logHess(b))))
    z = b :/ se
    val sqrt2 = sqrt(2)
    p = z.map(c => 1 + erf(-abs(c) / sqrt2))

    val n = t.length
    val mu = sum(t) / n
    val y0 = DenseVector.fill[Double](n, mu)
    val logll0 = logLogLkY(y0, t)
    val logll = logLogLk(b, X, t)
    val loglrs = 2 * (logll - logll0)

    println(s"logll0 = $logll0")
    println(s"loglrs = $loglrs")

    val logscs = logScoreStat(X, t)

    var chisqdist = new ChiSquaredDistribution(X.cols - 1)

    val loglrp = 1 - chisqdist.cumulativeProbability(loglrs)
    val logscp = 1 - chisqdist.cumulativeProbability(logscs)

    println(s"loglrp = $loglrp")
    println(s"logscp = $logscp")

    assert(D_==(b(0), 0.7245034, tolerance = 1.0E-6))
    assert(D_==(b(1), -0.3585773, tolerance = 1.0E-6))
    assert(D_==(b(2), 0.1922622, tolerance = 1.0E-6))

    assert(D_==(se(0), 0.9396654, tolerance = 1.0E-6))
    assert(D_==(se(1), 0.6246568, tolerance = 1.0E-6))
    assert(D_==(se(2), 0.4559844, tolerance = 1.0E-6))

    assert(D_==(z(0), 0.7710228, tolerance = 1.0E-6))
    assert(D_==(z(1), -0.5740389, tolerance = 1.0E-6))
    assert(D_==(z(2), 0.4216421, tolerance = 1.0E-6))

    assert(D_==(p(0), 0.4406934, tolerance = 1.0E-6))
    assert(D_==(p(1), 0.5659415, tolerance = 1.0E-6))
    assert(D_==(p(2), 0.6732863, tolerance = 1.0E-6))


    assert(D_==(logll0, -3.81908501,tolerance = 1.0E-6))
    assert(D_==(logll, -3.643316979, tolerance = 1.0E-6))
    assert(D_==(loglrp, 0.8388125392, tolerance = 1.0E-5))

    assert(D_==(logscs, 0.346648, tolerance = 1.0E-5))
    assert(D_==(logscp, 0.8408652791, tolerance = 1.0E-5))

    b = new NewtonOptimizer(firthGrad, firthHess).optimize(b0, tolerance = 1.0E-6, maxIter = 50).get
    se = sqrt(diag(inv(logHess(b))))
    z = b :/ se
    p = z.map(c => 1 + erf(-abs(c) / sqrt2))

    assert(D_==(b(0), 0.4630531613, tolerance = 1.0E-5))
    assert(D_==(b(1), -0.2068672819, tolerance = 1.0E-5))
    assert(D_==(b(2), 0.1069276506, tolerance = 1.0E-5))

    assert(D_==(se(0), 0.8912271200, tolerance = 1.0E-5))
    assert(D_==(se(1), 0.5800514573, tolerance = 1.0E-5))
    assert(D_==(se(2), 0.4177184603, tolerance = 1.0E-5))

//    assert(D_==(z(0), 0.7710228, tolerance = 1.0E-5))
//    assert(D_==(z(1), -0.5740389, tolerance = 1.0E-5))
//    assert(D_==(z(2), 0.4216421, tolerance = 1.0E-5))

    assert(D_==(p(0), 0.6033646525, tolerance = 1.0E-5))
    assert(D_==(p(1), 0.7213642582, tolerance = 1.0E-5))
    assert(D_==(p(2), 0.7979661342, tolerance = 1.0E-5))

    b = new NewtonOptimizer(poisGrad, poisHess).optimize(b0, tolerance = 1.0E-10, maxIter = 10).get
    se = sqrt(diag(inv(poisHess(b))))
    z = b :/ se
    p = z.map(c => 1 + erf(-abs(c) / sqrt2))

    assert(D_==(b(0), -0.42042704, tolerance = 1.0E-5))
    assert(D_==(b(1), -0.11829133, tolerance = 1.0E-5))
    assert(D_==(b(2), 0.05918721, tolerance = 1.0E-5))

    assert(D_==(se(0), 0.5328265, tolerance = 1.0E-5))
    assert(D_==(se(1), 0.3492719, tolerance = 1.0E-5))
    assert(D_==(se(2), 0.2361830, tolerance = 1.0E-5))

    assert(D_==(z(0), -0.7890506, tolerance = 1.0E-5))
    assert(D_==(z(1), -0.3386798, tolerance = 1.0E-5))
    assert(D_==(z(2), 0.2505990, tolerance = 1.0E-5))

    assert(D_==(p(0), 0.4300824, tolerance = 1.0E-5))
    assert(D_==(p(1), 0.7348509, tolerance = 1.0E-5))
    assert(D_==(p(2), 0.8021242, tolerance = 1.0E-5))

  }


  @Test def quadraticTest() = {
    val d = 2
    val h = DenseMatrix.eye[Double](d)

    def gradient(x: DenseVector[Double]): DenseVector[Double] =
      x

    def hessian(x: DenseVector[Double]): DenseMatrix[Double] =
      h

    val no = new NewtonOptimizer(gradient, hessian)
    val x0 = DenseVector.fill[Double](d, 10.0)
    val xmin = no.optimize(x0, tolerance = 1.0E-10, maxIter = 100).get

    assert(D_==(xmin(0), 0, 1.0E-6))
    assert(D_==(xmin(1), 0, 1.0E-6))
  }

  @Test def cubicTest() = {
    val d = 1

    def gradient(x: DenseVector[Double]): DenseVector[Double] =
      DenseVector(3 * x(0) * x(0) - 1)

    def hessian(x: DenseVector[Double]): DenseMatrix[Double] =
      new DenseMatrix(1, 1, Array[Double](6 * x(0)))

    val no = new NewtonOptimizer(gradient, hessian)
    val x0 = DenseVector(0.01)
    val xmin = no.optimize(x0, tolerance = 1.0E-10, maxIter = 100).get

    assert(D_==(xmin(0), scala.math.sqrt(1.0 / 3), 1.0E-6))
  }
}