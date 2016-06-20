package org.broadinstitute.hail.hailql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext}

/* FIXME how to query over rows/columns in, say, Aggregate or FilterKey
   maybe m.filterKey(RowAxis, $"geno".agg(fraction(isCalled($"gt"))))?
   */

// FIXME convert expressions to UDFs for Matrix execution
// FIXME lift joins ... operators for adding extra keys?

// FIXME collect, read, write: write action nodes
// FIXME backend
// FIXME _simple_ optimizer!
// FIXME print plan

// FIXME get bind for aggregate right

// FIXME in terms of dataset, [T]
// FIXME (un)zip, (un)wrap, explode
// FIXME table join that keeps one copy of join column(s)

class BaseRelationExpr(val schema1: TStruct) extends TableExpr {
  def children = Seq()

  def toDF: DataFrame = ???
}

object implicits {

  implicit class StringToColumn(val sc: StringContext) {
    def $(args: Any*): NamedExpr = {
      Attribute(sc.s(args: _*))
    }
  }

}

object functions {
  def col(name: String): NamedExpr = Attribute(name)

  def lit(a: Any, t: Type) = Literal(a, t)
}

object Example {
  def main(args: Array[String]) {
    import implicits._

    val t1 = Table(
      new BaseRelationExpr(TStruct(Map(
        "chrom" -> TString,
        "pos" -> TInt,
        "ref" -> TString,
        "alts" -> TArray(TString),
        "sample" -> TString,
        "genotype" -> TStruct(Map(
          "gt" -> TInt,
          "ad" -> TArray(TInt),
          "dp" -> TInt,
          "gq" -> TInt,
          "pl" -> TArray(TInt)))))))

    t1.printSchema()

    val m1 = t1.groupAsMatrix(
      Seq($"chrom", $"pos", $"ref", $"alts"),
      Seq($"sample"))
      .agg($"genotype".getField("gt").as("call"))

    m1.printSchema()
  }
}

class HailQLContext(sc: SparkContext,
  sqlContext: SQLContext) {
  def readTableParquet(filename: String): Table = ???

  def readMatrixParquet(filename: String): Table = ???
}

sealed abstract class Type {
  def pretty() {
    pretty(0)
    print("\n")
  }

  def pretty(indent: Int)
}

case object TInt extends Type {
  def pretty(indent: Int) {
    print("Int")
  }
}

case object TString extends Type {
  def pretty(indent: Int) {
    print("String")
  }
}

case object TUnit extends Type {
  def pretty(indent: Int) {
    print("Unit")
  }
}

case class TArray(elementType: Type) extends Type {
  def pretty(indent: Int) {
    print("Array[")
    elementType.pretty(indent)
    print("]")
  }
}

case class TStruct(fields: Map[String, Type]) extends Type {
  def pretty(indent: Int) {
    println("Struct {")
    fields.foreach { case (name, t) =>
      print(" " * (indent + 2))
      print(name)
      print(": ")
      t.pretty(indent + 2)
      println()
    }
    print(" " * indent)
    print("}")
  }

  // FIXME check name collision
  def union(other: TStruct): TStruct = TStruct(fields ++ other.fields)
}

case class TTensor(
  rowKeySchema: TStruct,
  colKeySchema: TStruct,
  schema: TStruct) {

  def pretty() {
    println("Tensor {")

    print("  rowKey: ")
    rowKeySchema.pretty(2)
    println()

    print("  colKey: ")
    colKeySchema.pretty(2)
    println()

    print("  cellSchema: ")
    schema.pretty(2)
    println()

    println("}")
  }

  def selectSchema = rowKeySchema.union(colKeySchema).union(schema)
}

sealed abstract class Axis

case object RowAxis extends Axis

case object ColumnAxis extends Axis

class RichSeqNamedExpr(val s: Seq[NamedExpr]) extends AnyVal {
  def toTStruct: TStruct =
    TStruct(s.map(e => (e.name, e.t)).toMap)
}

sealed abstract class JoinType

case object Inner extends JoinType

case object LeftOuter extends JoinType

case object RightOuter extends JoinType

case object FullOuter extends JoinType

case object Broadcast extends JoinType

abstract class TreeNode[BaseType <: TreeNode[BaseType]] {
  def children: Seq[BaseType]
}

abstract class Expression extends TreeNode[Expression] {
  def t: Type

  def bind(rexpr: RelationalExpr) {
    children.foreach(_.bind(rexpr))
  }

  def getField(name: String): NamedExpr = FieldRef(this, name)

  def as(name: String): NamedExpr = Alias(this, name)

  def toColumn(df: DataFrame): Column
}

case class FieldRef(child: Expression, name: String) extends UnaryExpr with NamedExpr {
  // FIXME check
  def t = child.t.asInstanceOf[TStruct].fields(name)

  def toColumn(df: DataFrame) = child.toColumn(df).getField(name)
}

trait NamedExpr extends Expression {
  def name: String
}

abstract class LeafExpr extends Expression {
  def children = Seq()
}

abstract class UnaryExpr extends Expression {
  def child: Expression

  def children = Seq(child)
}

abstract class BinaryExpr extends Expression {
  def left: Expression

  def right: Expression

  def children = Seq(left, right)
}

case class Attribute(name: String) extends LeafExpr with NamedExpr {
  // FIXME integrate
  var rexpr: RelationalExpr = null

  def t: Type = {
    assert(rexpr != null)
    // FIXME symbol not found exception
    rexpr.selectSchema.fields(name)
  }

  override def bind(re: RelationalExpr) {
    super.bind(re)
    rexpr = re
  }

  def toColumn(df: DataFrame) = df(name)
}

case class Alias(child: Expression, name: String) extends UnaryExpr with NamedExpr {
  def t = child.t

  def toColumn(df: DataFrame) = child.toColumn(df).as(name)
}

case class Literal(value: Any, t: Type) extends LeafExpr {
  def toColumn(df: DataFrame) = org.apache.spark.sql.functions.lit(value)
}

case class Add(left: Expression, right: Expression) extends BinaryExpr {
  // FIXME
  def t = TInt

  def toColumn(df: DataFrame) = left.toColumn(df) + right.toColumn(df)
}

trait RelationalExpr {
  def selectSchema: TStruct
}

abstract class TableExpr extends TreeNode[TableExpr] with RelationalExpr {
  def bind() {}

  lazy val schema: TStruct = {
    bind()
    schema1
  }

  def schema1: TStruct

  def selectSchema = schema

  def toDF: DataFrame
}

abstract class LeafTableExpr extends TableExpr {
  def children = Seq()
}

abstract class UnaryTableExpr extends TableExpr {
  def child: TableExpr

  def children = Seq(child)
}

abstract class BinaryTableExpr extends TableExpr {
  def left: TableExpr

  def right: TableExpr

  def children = Seq(left, right)
}

case class TableSelect(child: TableExpr, predicate: Expression) extends UnaryTableExpr {
  override def bind() {
    super.bind()
    predicate.bind(this)
  }

  def schema1 = child.schema

  def toDF: DataFrame = {
    val df = child.toDF
    df.filter(predicate.toColumn(df))
  }
}

case class TableProject(child: TableExpr, exprs: Seq[NamedExpr]) extends UnaryTableExpr {
  override def bind() {
    super.bind()
    exprs.foreach(_.bind(child))
  }

  def schema1 = TStruct(exprs.map(e => (e.name, e.t)).toMap)

  def toDF = {
    val df = child.toDF
    df.select(exprs.map(_.toColumn(df)): _*)
  }
}

case class TableJoin(left: TableExpr, right: TableExpr,
  predicate: Expression,
  joinType: JoinType) extends BinaryTableExpr {
  def schema1 = left.schema.union(right.schema)

  def toDF = {
    val df = left.toDF.join(right.toDF)
    df.filter(predicate.toColumn(df))
  }
}

case class TableAggregateTable(child: TableExpr, groupingExprs: Seq[NamedExpr], aggExprs: Seq[NamedExpr]) extends UnaryTableExpr {
  override def bind() {
    groupingExprs.foreach(_.bind(child))

    // FIXME no
    aggExprs.foreach(_.bind(child))
  }

  def schema1 = (groupingExprs ++ aggExprs).toTStruct

  def toDF = {
    val df = child.toDF
    df.groupBy(groupingExprs.map(_.toColumn(df)): _*)
      .agg(aggExprs.head.toColumn(df),
        aggExprs.tail.map(_.toColumn(df)): _*)
  }
}

case class MatrixAggregateTable(mexpr: MatrixExpr, groupingExprs: Seq[NamedExpr], aggExprs: Seq[NamedExpr]) extends LeafTableExpr {
  override def bind() {
    groupingExprs.foreach(_.bind(mexpr))

    // FIXME no
    aggExprs.foreach(_.bind(mexpr))
  }

  def schema1 = (groupingExprs ++ aggExprs).toTStruct

  def toDF = ???
}

abstract class TableAction[T] {
  def execute(): T
}

class TableCollect(texpr: TableExpr) extends TableAction[Array[Row]] {
  def execute(): Array[Row] = ???
}

class TableWriteParquet(texpr: TableExpr, path: String) extends TableAction[Unit] {
  def execute(): Unit = ???
}

class TableGroupedDataTable(texpr: TableExpr, groupingExprs: Seq[NamedExpr]) {
  def agg(expr: NamedExpr, exprs: NamedExpr*): Table =
    Table(TableAggregateTable(texpr, groupingExprs, expr +: exprs))
}

class MatrixGroupedDataTable(mexpr: MatrixExpr, groupingExprs: Seq[NamedExpr]) {
  def agg(expr: NamedExpr, exprs: NamedExpr*): Table =
    Table(MatrixAggregateTable(mexpr, groupingExprs, expr +: exprs))
}

class TableGroupedDataMatrix(texpr: TableExpr,
  rowGroupingExprs: Seq[NamedExpr],
  colGroupingExprs: Seq[NamedExpr]) {
  def agg(expr: NamedExpr, exprs: NamedExpr*): Matrix =
    Matrix(TableAggregateMatrix(texpr, rowGroupingExprs, colGroupingExprs, expr +: exprs))
}

class MatrixGroupedDataMatrix(mexpr: MatrixExpr,
  rowGroupingExprs: Seq[NamedExpr],
  colGroupingExprs: Seq[NamedExpr]) {
  def agg(expr: NamedExpr, exprs: NamedExpr*): Matrix =
    Matrix(MatrixAggregateMatrix(mexpr, rowGroupingExprs, colGroupingExprs, expr +: exprs))
}

object Table {
  def apply(texpr: TableExpr): Table = new Table(texpr)
}

class Table(texpr: TableExpr) {

  def schema: TStruct = texpr.schema

  def printSchema() {schema.pretty()}

  def apply(attrName: String): NamedExpr = Attribute(attrName)

  def select(attrName: String, attrNames: String*): Table =
    Table(TableProject(texpr, (attrName +: attrNames).map(Attribute)))

  def withAttribute(attrName: String, attr: Expression): Table =
    withAttribute(Alias(attr, attrName))

  def withAttribute(attr: NamedExpr, attrs: NamedExpr*): Table =
    Table(TableProject(texpr,
      schema.fields.map { case (name, t) =>
        Attribute(name)
      }.toSeq ++ attrs :+ attr))

  def drop(attr: String, attrs: String*): Table = {
    val exclude = (attrs :+ attr).toSet

    Table(TableProject(texpr,
      schema.fields.filter { case (name, t) => !exclude.contains(name) }
        .map { case (name, t) => Attribute(name) }
        .toSeq))
  }

  def filter(predicate: Expression): Table = Table(TableSelect(texpr, predicate))

  def groupBy(groupingExpr: NamedExpr, groupingExprs: NamedExpr*): TableGroupedDataTable =
    new TableGroupedDataTable(texpr, groupingExpr +: groupingExprs)

  def agg(aggExpr: NamedExpr, aggExprs: NamedExpr*): Table =
    new TableGroupedDataTable(texpr, Seq()).agg(aggExpr, aggExprs: _*)

  def groupAsMatrix(rowGroupingExprs: Seq[NamedExpr],
    colGroupingExprs: Seq[NamedExpr]): TableGroupedDataMatrix =
    new TableGroupedDataMatrix(texpr, rowGroupingExprs, colGroupingExprs)

  // FIXME
  def collect(): Array[Row] = ???

  def writeParquet(filename: String): Unit = ???
}

abstract class MatrixExpr extends TreeNode[MatrixExpr] with RelationalExpr {
  def schema1: TTensor

  def bind() {}

  lazy val schema: TTensor = {
    bind()
    schema1
  }

  def selectSchema = schema.selectSchema
}

abstract class LeafMatrixExpr extends MatrixExpr {
  def children = Seq()
}

abstract class UnaryMatrixExpr extends MatrixExpr {
  def child: MatrixExpr

  def children = Seq(child)
}

abstract class BinaryMatrixExpr extends MatrixExpr {
  def left: MatrixExpr

  def right: MatrixExpr

  def children = Seq(left, right)
}

case class MatrixSelect(child: MatrixExpr, predicate: Expression) extends UnaryMatrixExpr {
  def schema1 = child.schema
}

case class MatrixProject(child: MatrixExpr, exprs: Seq[NamedExpr]) extends UnaryMatrixExpr {
  override def bind() {
    super.bind()
    exprs.foreach(_.bind(child))
  }

  def schema1 = {
    val childSchema = child.schema
    childSchema.copy(
      schema = exprs.toTStruct)
  }
}

case class MatrixJoin(left: MatrixExpr,
  right: MatrixExpr,
  rowJoinType: JoinType,
  colJoinType: JoinType) extends BinaryMatrixExpr {
  def schema1 = {
    // check keys match
    val leftSchema = left.schema
    val rightSchema = right.schema
    leftSchema.copy(
      schema = leftSchema.schema.union(rightSchema.schema))
  }
}

case class TableAggregateMatrix(texpr: TableExpr,
  rowExprs: Seq[NamedExpr],
  colExprs: Seq[NamedExpr],
  aggExprs: Seq[NamedExpr]) extends LeafMatrixExpr {
  override def bind() {
    super.bind()
    rowExprs.foreach(_.bind(texpr))
    colExprs.foreach(_.bind(texpr))
    aggExprs.foreach(_.bind(texpr))
  }

  def schema1 = TTensor(rowExprs.toTStruct,
    colExprs.toTStruct,
    aggExprs.toTStruct)
}

case class MatrixAggregateMatrix(child: MatrixExpr,
  rowExprs: Seq[NamedExpr],
  colExprs: Seq[NamedExpr],
  aggExprs: Seq[NamedExpr]) extends UnaryMatrixExpr {
  override def bind() {
    super.bind()
    rowExprs.foreach(_.bind(child))
    colExprs.foreach(_.bind(child))
    aggExprs.foreach(_.bind(child))
  }

  def schema1 = TTensor(rowExprs.toTStruct,
    colExprs.toTStruct,
    aggExprs.toTStruct)
}

case class FilterAxis(child: MatrixExpr,
  axis: Axis,
  predicate: Expression) extends UnaryMatrixExpr {
  override def bind() {
    predicate.bind(child)
  }

  def schema1 = child.schema
}

object Matrix {
  def apply(mexpr: MatrixExpr): Matrix = new Matrix(mexpr)
}

class Matrix(mexpr: MatrixExpr) {
  def schema: TTensor = mexpr.schema

  def printSchema() {schema.pretty()}

  def apply(attrName: String): NamedExpr = Attribute(attrName)

  def select(attrName: String, attrNames: String*): Matrix =
    Matrix(MatrixProject(mexpr, (attrNames :+ attrName).map(Attribute)))

  def withAttribute(attrName: String, attr: Expression): Matrix =
    withAttribute(Alias(attr, attrName))

  def withAttribute(attr: NamedExpr, attrs: NamedExpr*): Matrix =
    Matrix(MatrixProject(mexpr,
      schema.schema.fields.map { case (name, t) =>
        Attribute(name)
      }.toSeq ++ attrs :+ attr))

  def drop(attr: String, attrs: String*): Matrix = {
    val exclude = (attrs :+ attr).toSet

    Matrix(MatrixProject(mexpr,
      schema.schema.fields.filter { case (name, t) => !exclude.contains(name) }
        .map { case (name, t) => Attribute(name) }
        .toSeq))
  }

  def groupBy(groupingExpr: NamedExpr, groupingExprs: NamedExpr*): MatrixGroupedDataTable =
    new MatrixGroupedDataTable(mexpr, groupingExpr +: groupingExprs)

  def agg(aggExpr: NamedExpr, aggExprs: NamedExpr*): Table =
    new MatrixGroupedDataTable(mexpr, Seq()).agg(aggExpr, aggExprs: _*)

  def groupAsMatrix(rowGroupingExprs: Seq[NamedExpr],
    colGroupingExprs: Seq[NamedExpr]): MatrixGroupedDataMatrix =
    new MatrixGroupedDataMatrix(mexpr, rowGroupingExprs, colGroupingExprs)

  def filterAxis(axis: Axis, predicate: Expression): Matrix =
    Matrix(FilterAxis(mexpr, axis, predicate))

  def filter(predicate: Expression): Matrix = Matrix(MatrixSelect(mexpr, predicate))

  // FIXME collect

  def writeParquet(path: String): Unit = ???
}
