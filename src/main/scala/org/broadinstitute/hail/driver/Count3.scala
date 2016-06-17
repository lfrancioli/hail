package org.broadinstitute.hail.driver

import org.apache.spark.sql.functions._
import org.broadinstitute.hail.Utils._

object Count3 extends Command {

  class Options extends BaseOptions

  def newOptions = new Options

  def name = "count3"

  def description = "Print number of samples, variants, and called genotypes in current dataset"

  def supportsMultiallelic = true

  def requiresVDS = false

  def run(state: State, options: Options): State = {
    val vsm2 = state.vsm2
    val df = vsm2.df

    val nVariants = df.count()
    val nSamples = vsm2.nSamples
    val gtCounts = df.select(df("v"), explode(df("gs").apply("gt")).as("gt"))
      .groupBy("gt").count().as("count")

    val nNoCalls = gtCounts.where("gt IS NULL").collect().apply(0).getAs[Long](1)


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


