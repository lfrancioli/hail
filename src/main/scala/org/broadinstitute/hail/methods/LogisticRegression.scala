package org.broadinstitute.hail.methods

import breeze.linalg._
import breeze.numerics._
import org.apache.spark.rdd.RDD
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations.Annotation
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.stats.NewtonOptimizer
import org.broadinstitute.hail.variant._

object LogRegStats {
  def `type`: Type = TStruct(
    ("nMissing", TInt),
    ("beta", TDouble),
    ("se", TDouble),
    ("zstat", TDouble),
    ("pval", TDouble))
}

case class LogRegStats(nMissing: Int, beta: Double, se: Double, t: Double, p: Double) {
  def toAnnotation: Annotation = Annotation(nMissing, beta, se, t, p)
}

object LogisticRegression {
  def name = "LogisticRegression"

  def apply(vds: VariantDataset, y: DenseVector[Double], cov: Option[DenseMatrix[Double]]): LogisticRegression = {
    require(cov.forall(_.rows == y.size))

    // FIXME: improve message (or require Boolean and place in command)
    if (! y.forall(yi => yi == 0d || yi == 1d))
      fatal(s"For logistic regression, each phenotype value must be 0 or 1.")

    val n = y.size
    val k = if (cov.isDefined) cov.get.cols else 0
    val d = n - k - 2

    if (d < 1)
      fatal(s"$n samples and $k ${plural(k, "covariate")} with intercept implies $d degrees of freedom.")

    info(s"Running logreg on $n samples with $k sample ${plural(k, "covariate")}...")

    val covAndOnes: DenseMatrix[Double] = cov match {
      case Some(dm) => DenseMatrix.horzcat(dm, DenseMatrix.ones[Double](n, 1))
      case None => DenseMatrix.ones[Double](n, 1)
    }

    val sc = vds.sparkContext
    val yBc = sc.broadcast(y)
    val covAndOnesBc = sc.broadcast(covAndOnes)

    // FIXME: worth making a version of aggregateByVariantWithKeys using sample index rather than sample name?
    val sampleIndexBc = sc.broadcast(vds.sampleIds.zipWithIndex.toMap)

    new LogisticRegression(vds.rdd
      .map{ case (v, a, gs) =>
        val (nCalled, gtSum) = gs.flatMap(_.gt).foldRight((0,0))((gt, acc) => (acc._1 + 1, acc._2 + gt))

        println(v)
        println(gs.flatMap(_.gt))

        val logregstatsOpt =  // FIXME: improve this catch
          if (gtSum == 0 || gtSum == 2 * nCalled || (gtSum == nCalled && gs.flatMap(_.gt).forall(_ == 1)) || nCalled == 0)
            None
          else {
            val gtMean = gtSum.toDouble / nCalled

            val gtArray = gs.map(_.gt.map(_.toDouble).getOrElse(gtMean)).toArray


            val t = yBc.value
            val X = DenseMatrix.horzcat(new DenseMatrix(n, 1, gtArray), covAndOnesBc.value) // FIXME: make more efficient

            val b0 = DenseVector.fill(X.cols)(0d)

            def logGrad(w: DenseVector[Double]): DenseVector[Double] = {
              val r = sigmoid(X * w)
              X.t * (r :- t)
            }

            def logHess(w: DenseVector[Double]): DenseMatrix[Double] = {
              val r = sigmoid(X * w)
              X.t * diag(r :* (1d - r)) * X
            }

            val bOpt = new NewtonOptimizer(logGrad, logHess).optimize(b0, tolerance = 1.0E-10, maxIter = 10)

            // FIXME: deal with difference between perfect fit and other lack of convergence
            // FIXME: catch breeze.linalg.MatrixSingularException

            if (bOpt.isEmpty)
              None
            else {
              val b = bOpt.get
              val se = sqrt(diag(inv(logHess(b))))
              val z = b :/ se
              val sqrt2 = sqrt(2)
              val p = z.map(c => 1 + erf(-abs(c) / sqrt2))
              Some(LogRegStats(n - nCalled, b(0), se(0), z(0), p(0)))
            }
          }
        (v, logregstatsOpt)
      }
    )
  }
}

case class LogisticRegression(rdd: RDD[(Variant, Option[LogRegStats])])