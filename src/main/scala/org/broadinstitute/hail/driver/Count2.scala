package org.broadinstitute.hail.driver

import org.apache.spark.sql.functions._
import org.broadinstitute.hail.Utils._

object Count2 extends Command {

  class Options extends BaseOptions

  def newOptions = new Options

  def name = "count2"

  def description = "Print number of samples, variants, and called genotypes in current dataset"

  def supportsMultiallelic = true

  def requiresVDS = false

  def run(state: State, options: Options): State = {
    val vsm2 = state.vsm2
    val df = vsm2.df
    def udfGenotypeCountNoCall = udf((gts: Seq[java.lang.Integer]) => gts.count(g => g == null))

    val nVariants = df.count()
    val nSamples = vsm2.nSamples
    val nNoCalls = df.withColumn("nNoCalls", udfGenotypeCountNoCall(df("gs").apply("gt")))
      .agg(sum("nNoCalls")).collect().apply(0).getLong(0)

    val nGenotypes = nSamples * nVariants
    val nCalled = nGenotypes - nNoCalls
    val callRate = divOption(nCalled, nGenotypes)

    info(
      s"""count:
          |  nSamples = ${vsm2.nSamples}
          |  nVariants = $nVariants
          |  nCalled = $nCalled
          |  callRate = ${callRate.map(r => (r * 100).formatted("%.3f%%")).getOrElse("NA")}""".stripMargin)

    state
  }
}

