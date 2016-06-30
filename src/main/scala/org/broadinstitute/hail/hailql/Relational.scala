package org.broadinstitute.hail.hailql

abstract class Relational {
  private[hailql] def rexpr: RelationalExpr

  private [hailql] def ctor(newRExpr: RelationalExpr): this.type

  def schema: TRelational = rexpr.t

  def printSchema(): Unit = schema.pretty()

  def apply(attrName: String): NamedExpr = Attribute(attrName)

  def select(attrName: String, attrNames: String*): this.type =
    ctor(Project(rexpr, (attrNames :+ attrName).map(Attribute)))

  def withAttribute(attrName: String, attr: Expression): this.type =
    withAttribute(Alias(attr, attrName))

  def withAttribute(attr: NamedExpr, attrs: NamedExpr*): this.type =
    ctor(Project(rexpr,
      schema.schema.fields.map { f =>
        Attribute(f.name)
      } ++ attrs :+ attr))

  def drop(attr: String, attrs: String*): this.type = {
    val exclude = (attrs :+ attr).toSet

    ctor(Project(rexpr,
      schema.schema.fields.filter { f => !exclude.contains(f.name) }
        .map { f => Attribute(f.name) }))
  }

  def filter(predicate: Expression): this.type = ctor(Select(rexpr, predicate))

  def expand: Table =
    Table(Expand(rexpr))

  def groupBy(groupingExprs: NamedExpr*): GroupedTable =
    expand.groupBy(groupingExprs: _*)

  def agg(aggExpr: Aggregator, aggExprs: Aggregator*): Table =
    groupBy().agg(aggExpr, aggExprs: _*)

  def groupAsVector(groupingExprs: Seq[NamedExpr]): VectorGroupedTable =
    expand.groupAsVector(groupingExprs)

  def groupAsMatrix(rowGroupingExprs: Seq[NamedExpr],
    colGroupingExprs: Seq[NamedExpr]): MatrixGroupedTable =
    expand.groupAsMatrix(rowGroupingExprs, colGroupingExprs)

  def eval(hc: HailQLContext): RelationalValue = {
    rexpr.bind()
    rexpr.eval(hc)
  }
}

object Table {
  def apply(rexpr: RelationalExpr): Table = new Table(rexpr)
}

class Table(val rexpr: RelationalExpr) extends Relational {
  def ctor(newRExpr: RelationalExpr) = Table(rexpr).asInstanceOf[this.type]

  override def schema: TTable = rexpr.t.asInstanceOf[TTable]

  override def groupBy(groupingExprs: NamedExpr*): GroupedTable =
    new GroupedTable(rexpr, groupingExprs)

  override def groupAsVector(groupingExprs: Seq[NamedExpr]): VectorGroupedTable =
    new VectorGroupedTable(rexpr, groupingExprs)

  override def groupAsMatrix(rowGroupingExprs: Seq[NamedExpr],
    colGroupingExprs: Seq[NamedExpr]): MatrixGroupedTable =
    new MatrixGroupedTable(rexpr, rowGroupingExprs, colGroupingExprs)

  // natural join
  def join(right: Table, joinType: JoinType): Table =
    Table(TableJoin(rexpr, right.rexpr, joinType))

  override def eval(hc: HailQLContext): TableValue = super.eval(hc).asInstanceOf[TableValue]
}

object Vector {
  def apply(rexpr: RelationalExpr): Vector = new Vector(rexpr)
}

class Vector(val rexpr: RelationalExpr) extends Relational {
  def ctor(newRExpr: RelationalExpr) = Vector(newRExpr).asInstanceOf[this.type]

  override def schema: TVector = super.schema.asInstanceOf[TVector]

  override def eval(hc: HailQLContext): VectorValue = super.eval(hc).asInstanceOf[VectorValue]

  def join(right: Vector, joinType: JoinType) =
    Vector(VectorJoin(rexpr, right.rexpr, joinType))

  def index(expr: Expression): Expression = VectorIndex(rexpr, expr)
}

object Matrix {
  def apply(rexpr: RelationalExpr): Matrix = new Matrix(rexpr)
}

class Matrix(val rexpr: RelationalExpr) extends Relational {
  def ctor(newRExpr: RelationalExpr) = Matrix(newRExpr).asInstanceOf[this.type]

  override def schema: TMatrix = rexpr.t.asInstanceOf[TMatrix]

  def filterRows(predicate: Expression): Matrix =
    Matrix(FilterRows(rexpr, predicate))

  def filterColumns(predicate: Expression): Matrix =
    Matrix(FilterColumns(rexpr, predicate))

  def aggByRows(): RowGroupedMatrix = new RowGroupedMatrix(rexpr)

  def aggByColumns(): ColumnGroupedMatrix = new ColumnGroupedMatrix(rexpr)

  def join(right: Matrix, rowJoinType: JoinType, colJoinType: JoinType): Matrix =
    Matrix(MatrixJoin(rexpr, right.rexpr, rowJoinType, colJoinType))

  def index(rowExpr: Expression, colExpr: Expression): Expression =
    MatrixIndex(rexpr, rowExpr, colExpr)
}

class GroupedTable(rexpr: RelationalExpr, groupingExprs: Seq[NamedExpr]) {
  def agg(expr: Aggregator, exprs: Aggregator*): Table =
    Table(AggregateTable(rexpr, groupingExprs, expr +: exprs))
}

class VectorGroupedTable(rexpr: RelationalExpr, groupingExprs: Seq[NamedExpr]) {
  def agg(expr: Aggregator, exprs: Aggregator*): Vector =
    Vector(AggregateVector(rexpr, groupingExprs, expr +: exprs))
}

class MatrixGroupedTable(rexpr: RelationalExpr,
  rowGroupingExprs: Seq[NamedExpr],
  colGroupingExprs: Seq[NamedExpr]) {
  def agg(expr: Aggregator, exprs: Aggregator*): Matrix =
    Matrix(AggregateMatrix(rexpr, rowGroupingExprs, colGroupingExprs, expr +: exprs))
}

class RowGroupedMatrix(rexpr: RelationalExpr) {
  def agg(expr: Aggregator, exprs: Aggregator*): Vector =
    Vector(AggregateRows(rexpr, expr +: exprs))
}

class ColumnGroupedMatrix(rexpr: RelationalExpr) {
  def agg(expr: Aggregator, exprs: Aggregator*): Vector =
    Vector(AggregateColumns(rexpr, expr +: exprs))
}
