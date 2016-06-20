package org.broadinstitute.hail

import scala.language.implicitConversions

package object hailql {
  implicit def toRichSeqNamedExpr(s: Seq[NamedExpr]): RichSeqNamedExpr = new RichSeqNamedExpr(s)
}
