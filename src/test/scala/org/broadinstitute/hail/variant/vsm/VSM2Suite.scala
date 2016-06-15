package org.broadinstitute.hail.variant.vsm

import org.broadinstitute.hail.SparkSuite
import org.testng.annotations.Test
import org.broadinstitute.hail.driver._

class VSM2Suite extends SparkSuite {
  @Test def test1() {
    var s = State(sc, sqlContext, null)
    val testVCF = "src/test/resources/sample.vcf"
    val output = tmpDir.createTempFile("testDataFrame", ".vds")
    s = ImportVCF.run(s, Array(testVCF))
    s = Write.run(s, Array("-o", output))
    s = Read.run(s, Array("-i", output))

  }
}
