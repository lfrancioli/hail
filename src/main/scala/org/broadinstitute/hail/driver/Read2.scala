package org.broadinstitute.hail.driver

import org.broadinstitute.hail.variant.VariantSampleMatrix2
import org.kohsuke.args4j.{Option => Args4jOption}

object Read2 extends Command {
  def name = "read2"

  def description = "Load file .vds as the current dataset"

  class Options extends BaseOptions {
    @Args4jOption(required = true, name = "-i", aliases = Array("--input"), usage = "Input .vds file")
    var input: String = _

    @Args4jOption(name = "--skip-genotypes", usage = "Don't load genotypes")
    var skipGenotypes: Boolean = false

  }

  def newOptions = new Options

  def supportsMultiallelic = true

  def requiresVDS = false

  def run(state: State, options: Options): State = {
    val input = options.input

    val newVSM2 = VariantSampleMatrix2.read(state.sqlContext, input, options.skipGenotypes)
    state.copy(vsm2 = newVSM2)
  }
}

