package is.hail.methods

import breeze.linalg.{DenseMatrix, DenseVector}
import is.hail.expr.{TInt, TNumeric, TString, TStruct}
import is.hail.keytable.KeyTable
import is.hail.stats.{LogisticRegressionModel, LogisticRegressionTest, RegressionUtils}
import is.hail.utils.{fatal, info, plural}
import is.hail.variant.VariantDataset
import org.apache.spark.sql.Row

object LogisticRegressionBurdenMultiPheno {
  def apply(vds: VariantDataset,
    keyName: String,
    variantKeys: String,
    singleKey: Boolean,
    aggExpr: String,
    test: String,
    ysName: Array[String],
    ysExpr: Array[String],
    covExpr: Array[String]): (KeyTable, KeyTable) = {

    if (ysName.length != ysExpr.length)
      fatal(s"Found ${ysName} phenotype names but ${ysExpr.length} phenotype expressions.")

    val logRegTest = LogisticRegressionTest.tests.getOrElse(test,
      fatal(s"Supported tests are ${ LogisticRegressionTest.tests.keys.mkString(", ") }, got: $test"))

    val reservedFields = logRegTest.schema.asInstanceOf[TStruct].fields.map(_.name).toSet + ("pheno", "nsamples")
    if (reservedFields(keyName))
      fatal(s"Key name '$keyName' clashes with reserved columns $reservedFields")

    val ysCovSamples = ysName.zip(ysExpr).map{
      case (yName, yExpr) =>
        val (y, cov, completeSamples) = RegressionUtils.getPhenoCovCompleteSamples(vds, yExpr, covExpr)
        val completeSamplesSet = completeSamples.toSet
        val sampleMask = vds.sampleIds.map(completeSamplesSet).toArray
        val completeSampleIndex = (0 until vds.nSamples)
          .filter(i => completeSamplesSet(vds.sampleIds(i)))
          .toArray

        val n = y.size
        val k = cov.cols
        val d = n - k - 1

        if (d < 1)
          fatal(s"$n samples and $k ${ plural(k, "covariate") } including intercept implies $d degrees of freedom.")

        val nullModel = new LogisticRegressionModel(cov, y)
        val nullFit = nullModel.fit()

        if (!nullFit.converged)
          fatal("Failed to fit (unregulatized) logistic regression null model (covariates only): " + (
            if (nullFit.exploded)
              s"exploded at Newton iteration ${ nullFit.nIter }"
            else
              "Newton iteration failed to converge"))


        (yName, completeSampleIndex, y, cov, n, k, nullFit)
    }

    info(s"Aggregating variants by '$keyName' for ${vds.nSamples} samples...")

    def sampleKT = vds.aggregateBySamplePerVariantKey(keyName, variantKeys, aggExpr, singleKey)
      .cache()

    val keyType = sampleKT.fields(0).typ

    // d > 0 implies at least 1 sample
    val numericType = sampleKT.fields(1).typ

    if (!numericType.isInstanceOf[TNumeric])
      fatal(s"aggregate_expr type must be numeric, found $numericType")

    info(s"Running $test logistic regression burden test for ${ysExpr.length} ${ plural(ysExpr.length, "phenotype") } on ${sampleKT.count} keys on ${vds.nSamples} samples with ${covExpr.length} ${ plural(covExpr.length, "covariate") } ...")

    val sc = sampleKT.hc.sc
    val logRegTestBc = sc.broadcast(logRegTest)
    val bcVars = sc.broadcast(ysCovSamples.map{
      case (yName, completeSampleIndex, y, cov, n, k, nullFit) =>
        val x = new DenseMatrix[Double](n, k + 1, cov.toArray ++ Array.ofDim[Double](n))
        (yName, completeSampleIndex, x, y, nullFit)
    })

    val (logregSignature, merger) = TStruct(keyName -> keyType, "pheno" -> TString, "nsamples" -> TInt).merge(logRegTest.schema.asInstanceOf[TStruct])

    val logregRDD = sampleKT.rdd.flatMap{
      keyedRow =>
        val x_all = RegressionUtils.keyedRowToVectorDouble(keyedRow)
        bcVars.value.map{
          case (yName, completeSampleIndex, x, y, nullFit) =>
            val X = x.copy
            X(::, -1) := DenseVector(completeSampleIndex.map(i => x_all(i)))
            merger(
              Row(keyedRow.get(0), yName, completeSampleIndex.length),
              logRegTestBc.value.test(X, y, nullFit).toAnnotation).asInstanceOf[Row]
        }
    }

    val logregKT = new KeyTable(sampleKT.hc, logregRDD, signature = logregSignature, key = Array(keyName))

    (logregKT, sampleKT)
  }
}
