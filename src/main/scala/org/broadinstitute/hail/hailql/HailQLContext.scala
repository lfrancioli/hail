package org.broadinstitute.hail.hailql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}
import org.broadinstitute.hail.driver.{Command, State}
import org.broadinstitute.hail.expr
import org.broadinstitute.hail.variant._

object HailQLContext {
  def fromExprType(t: expr.Type): Type = t match {
    case expr.TInt => TInt
    case expr.TBoolean => TBoolean
    case expr.TDouble => TDouble
    case expr.TString => TString
    case expr.TArray(elementType) => TArray(fromExprType(elementType))
    case expr.TSet(elementType) => TArray(fromExprType(elementType))
    case expr.TStruct(fields) => TStruct.from(
      fields.map(f => (f.name, fromExprType(f.`type`))): _*)
  }

  def fromExprValue(v: Any, t: expr.Type): Any = t match {
    case expr.TBoolean | expr.TInt | expr.TDouble | expr.TString => v
    case expr.TArray(elementType) => v.asInstanceOf[Seq[_]].map(u => fromExprValue(u, elementType))
    case expr.TSet(elementType) =>
      v.asInstanceOf[Set[_]].map(u => fromExprValue(u, elementType)).toSeq
    case expr.TStruct(fields) =>
      v.asInstanceOf[Row].toSeq.zip(fields).map { case (u, f) =>
        fromExprValue(u, f.`type`)
      }
  }
}

class HailQLContext(val sc: SparkContext,
  val sqlContext: SQLContext) {

  import HailQLContext._

  def matrixFromVariantDataset(vds: VariantDataset): Matrix = {
    val vaExprType = vds.vaSignature
    val vaType = fromExprType(vaExprType)
    Matrix(RelationalLiteral(MatrixValue(
      TMatrix(
        TStruct.from("contig" -> TString,
          "start" -> TInt,
          "ref" -> TString,
          "alt" -> TString,
          "va" -> vaType),
        TStruct.from(
          "s" -> TString),
        TStruct.from("gt" -> TInt,
          "ad" -> TArray(TInt),
          "dp" -> TInt,
          "gq" -> TInt,
          "pl" -> TArray(TInt),
          "fakeRef" -> TBoolean)),
      vds.sampleIds.map { s =>
        Seq[Any](s)
      }.toArray[Any],
      vds.rdd.map { case (v, va, gs) =>
        (Seq[Any](v.contig, v.start, v.ref, v.alt, fromExprValue(va, vaExprType)),
          gs.map(g =>
            Seq[Any](g.gt.orNull,
              g.ad.map(_.toSeq).orNull,
              g.dp.orNull,
              g.gq.orNull,
              g.pl.map(_.toSeq).orNull,
              g.fakeRef)).toSeq)
      }
    )))
  }

  def readMatrixParquet(filename: String): Table = ???
}

object HailQLCommand extends Command {

  class Options extends BaseOptions

  def newOptions = new Options

  def name = "hailql"

  def description = "HailQL test"

  def supportsMultiallelic = true

  def requiresVDS = true

  def run(state: State, options: Options): State = {
    val hc = new HailQLContext(state.sc, state.sqlContext)

    val m = hc.matrixFromVariantDataset(state.vds)

    import functions._

    m.select("gt").printSchema()

    val n = m
      .select("gt")
      .filterRows(col("va").getField("pass"))
      .agg(count("count", col("gt") >= lit(0)))
      .eval(hc)
      .rdd.first()
      .asInstanceOf[Seq[Int]].head

    println(s"count = $n")

    state
  }
}