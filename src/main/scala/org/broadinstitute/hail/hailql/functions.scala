package org.broadinstitute.hail.hailql

object functions {
  def col(name: String): NamedExpr = Attribute(name)

  def lit(a: Any, t: Type) = Literal(a, t)

  def lit(b: Boolean) = Literal(b, TBoolean)

  def lit(i: Int) = Literal(i, TInt)

  def lit(s: String) = Literal(s, TString)

  def lit(d: Double) = Literal(d, TDouble)

  // aggregators:
  def count(name: String): Aggregator = Count(name, lit(true, TBoolean))

  def count(name: String, predicate: Expression): Aggregator = Count(name, predicate)
}
