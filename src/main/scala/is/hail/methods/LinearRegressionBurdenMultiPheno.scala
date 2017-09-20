package is.hail.methods

import breeze.linalg.{DenseVector, qr}
import is.hail.annotations.Annotation
import is.hail.expr.{TInt, TNumeric, TString, TStruct}
import is.hail.keytable.KeyTable
import is.hail.stats.{LinearRegressionModel, RegressionUtils}
import is.hail.utils.{fatal, info, plural}
import is.hail.variant.VariantDataset
import org.apache.spark.sql.Row

object LinearRegressionBurdenMultiPheno {

  def apply(vds: VariantDataset,
    keyName: String,
    variantKeys: String,
    singleKey: Boolean,
    aggExpr: String,
    ysName: Array[String], //Hacky -- should use named exprs
    ysExpr: Array[String],
    covExpr: Array[String]): (KeyTable, KeyTable) = {

    if (ysName.length != ysExpr.length)
      fatal(s"Found ${ysName} phenotype names but ${ysExpr.length} phenotype expressions.")

    val reservedFields = LinearRegression.schema.fields.map(_.name).toSet + ("pheno", "nsamples")
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
          fatal(s"$n samples and $k ${ plural(k, "covariate") } including intercept for expr '${ yExpr }' implies $d degrees of freedom.")

        (yName, completeSampleIndex, y, cov, d)
    }

    info(s"Aggregating variants by '$keyName' ...")

    if (vds.sampleIds.toSet.contains(keyName))
      fatal(s"Key name '$keyName' clashes with a sample name")

    if (ysName.contains(keyName))
      fatal(s"Key name '$keyName' clashes with one of the phenotype names")

    def sampleKT = vds.aggregateBySamplePerVariantKey(keyName, variantKeys, aggExpr, singleKey)
      .cache()

    val keyType = sampleKT.fields(0).typ

    // d > 0 implies at least 1 sample
    val numericType = sampleKT.fields(1).typ

    if (!numericType.isInstanceOf[TNumeric])
      fatal(s"aggregate_expr type must be numeric, found $numericType")

    info(s"Running linear regression burden test for ${ysExpr.length} ${ plural(ysExpr.length, "phenotype") } on ${sampleKT.count} keys on ${vds.nSamples} samples with ${covExpr.length} ${ plural(covExpr.length, "covariate") } ...")

    val sc = sampleKT.hc.sc
    val vars = ysCovSamples.map{
      case (yName, completeSampleIndex, y, cov, d) =>
        val Qt = qr.reduced.justQ(cov).t
        val Qty = Qt * y
        val yyp = (y dot y) - (Qty dot Qty)

        (yName, completeSampleIndex, y, Qt, Qty, yyp, d)
    }
    val bcVars = sc.broadcast(vars)

//    val linregFields = (keyName, keyType) +: ysName.map(n => (n,LinearRegression.schema))
//    val linregSignature = TStruct(linregFields : _*)

    val (linregSignature, merger) = TStruct(keyName -> keyType, "pheno" -> TString, "nsamples" -> TInt).merge(LinearRegression.schema)

    val linregRDD = sampleKT.rdd.flatMap{ keyedRow =>
      val x_all = RegressionUtils.keyedRowToVectorDouble(keyedRow)
        bcVars.value.map{
          case (yName, completeSampleIndex, y, qt, qty, yyp, d) =>
            val x = DenseVector(completeSampleIndex.map(i => x_all(i)))
            merger(
              Row(keyedRow.get(0), yName, completeSampleIndex.length),
              LinearRegressionModel.fit(x, y, yyp, qt, qty, d)).asInstanceOf[Row]
        }
    }

//    val linregRDD = sampleKT.mapAnnotations { keyedRow =>
//      val x_all = RegressionUtils.keyedRowToVectorDouble(keyedRow)
//      val linRegFits =
//        bcVars.value.map{
//          case (completeSampleIndex, y, qt, qty, yyp, d) =>
//            val x = DenseVector(completeSampleIndex.map(i => x_all(i)))
//            LinearRegressionModel.fit(x, y, yyp, qt, qty, d)
//        }
//      val annotations = keyedRow.get(0) +: linRegFits
//
//        Row(Annotation(annotations : _* ))
//
//    }
    val linregKT = new KeyTable(sampleKT.hc, linregRDD, signature = linregSignature, key = Array(keyName))

    (linregKT, sampleKT)
  }
}
