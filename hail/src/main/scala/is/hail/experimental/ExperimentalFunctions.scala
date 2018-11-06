package is.hail.experimental

import is.hail.expr.types._
import is.hail.expr.ir.functions._
import is.hail.annotations.{Region, StagedRegionValueBuilder}
import is.hail.asm4s.{AsmFunction3, Code}
import is.hail.expr.ir._

object ExperimentalFunctions extends RegistryFunctions {

  def registerAll() {
    val experimentalPackageClass = Class.forName("is.hail.experimental.package$")

    registerScalaFunction("filtering_allele_frequency", TInt32(), TInt32(), TFloat64(), TFloat64())(experimentalPackageClass, "calcFilterAlleleFreq")
    registerScalaFunction("prob_same_hap_em", TInt32(), TInt32(), TInt32(), TInt32(), TInt32(), TInt32(), TInt32(), TInt32(), TInt32(), TFloat64())(experimentalPackageClass, "probSameHapEM")

  }

}
