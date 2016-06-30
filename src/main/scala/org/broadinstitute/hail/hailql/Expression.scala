package org.broadinstitute.hail.hailql

import org.broadinstitute.hail.Utils._

import scala.collection.mutable
import scala.reflect.ClassTag

/* FIXME how to query over rows/columns in, say, Aggregate or FilterKey
   maybe m.filterKey(RowAxis, $"geno".agg(fraction(isCalled($"gt"))) < 0.9)?
   maybe m.filterRows(m.aggRows(fraction(isCalled($"gt")).index(m.rowKey, unit()) < 0.9)

   index must be:
    - from a single table
    - only in terms of one of rowKey or colKey in a matrix
    - essentially from a single, 1-dimensional object
   */

// FIXME current plan:

// FIXME rewriters
// FIXME lift index

// FIXME pretty everything
// FIXME read/write interface (on HailQL, relational, ...)

// FIXME add "internal" annotations to TMatrix

// FIXME support intervals

// FIXME wrap, unwrap, explode, zip, unzip, ...
// FIXME sort, orderBy, union, intersection, difference, distinct, toDF, toRDD, take, head, collect, ...

// FIXME put HailQL in RelationalExpr, RelationalValue?

// FIXME parse column split dots into fieldRef
// FIXME typecheck
// FIXME matrix: SelectRowKey, SelectColKey, vector: SelectKey (for drop, add, etc.)
// FIXME add types, operators: pull from AST

// FIXME lift joins ... operators for adding extra keys?

// FIXME collect, read, write: write action nodes

// FIXME _simple_ optimizer!
// FIXME compose nested select, filter
// FIXME push filter below select
// FIXME push filter into join -- tests for dependence

// FIXME print plan

// FIXME put back matrix agg matrix, etc.
// FIXME (un)zip, (un)wrap, explode
// FIXME table join that keeps one copy of join column(s)

// FIXME in terms of dataset, [T] -- or codegen?

// FIXME support this syntax?
object implicits {

  implicit class StringToColumn(val sc: StringContext) {
    def $(args: Any*): NamedExpr = {
      Attribute(sc.s(args: _*))
    }
  }

}

class RichSeqNamedExpr(val s: Seq[NamedExpr]) extends AnyVal {
  def toTStruct: TStruct =
    TStruct.from(s.map(e => (e.name, e.t)): _*)
}

sealed abstract class JoinType

case object Inner extends JoinType

case object LeftOuter extends JoinType

case object RightOuter extends JoinType

case object FullOuter extends JoinType

abstract class BaseExpr {
  def t: Type

  def children: Seq[BaseExpr]
}

trait LeafExpr extends BaseExpr {
  def children = Seq()
}

trait UnaryExpr extends BaseExpr {
  def child: BaseExpr

  def children = Seq(child)
}

trait BinaryExpr extends BaseExpr {
  def left: BaseExpr

  def right: BaseExpr

  def children = Seq(left, right)
}

trait NamedExpr extends Expression {
  def name: String
}

abstract class Expression extends BaseExpr {
  def children: Seq[BaseExpr]

  def bind(rexpr: RelationalExpr) {
    children.foreach(_.asInstanceOf[Expression].bind(rexpr))
  }

  def compile(): Any => Any

  def getField(name: String): NamedExpr = FieldRef(this, name)

  def as(name: String): NamedExpr = Alias(this, name)

  def >=(other: Expression): Expression = GtEq(this, other)

  def +(other: Expression): Expression = Add(this, other)
}

case class FieldRef(child: Expression, name: String) extends Expression with UnaryExpr with NamedExpr {
  def field = child.t.asInstanceOf[TStruct].field(name)

  def t = field.t

  def compile() = {
    val i = field.i
    val f = child.compile()

    { (a: Any) =>
      val v = f(a)
      if (v != null) {
        v.asInstanceOf[Seq[_]](i)
      } else
        null
    }
  }
}

case class Attribute(name: String) extends Expression with LeafExpr with NamedExpr {
  // FIXME integrate (what does this mean?)
  var rexpr: RelationalExpr = null

  def t: Type = {
    assert(rexpr != null)
    // FIXME symbol not found exception
    rexpr.selectSchema.field(name).t
  }

  override def bind(re: RelationalExpr) {
    super.bind(re)
    rexpr = re
  }

  def compile() = {
    val i = rexpr.selectSchema.field(name).i
    // println(s"name=$name, i=$i")
    (a: Any) =>
      if (a != null) {
        // println(s"name=$name, i=$i, a=$a")
        a.asInstanceOf[Seq[_]](i)
      } else
        null
  }
}

case class VectorIndex(rexpr: RelationalExpr, index: Expression) extends Expression {
  def children = Seq(rexpr, index)

  def t = rexpr.t.schema

  def compile() = throw new UnsupportedOperationException
}

case class MatrixIndex(rexpr: RelationalExpr, rowIndex: Expression, colIndex: Expression) extends Expression {
  def children = Seq(rexpr, rowIndex, colIndex)

  def t = rexpr.t.schema

  def compile() = throw new UnsupportedOperationException
}

case class Alias(child: Expression, name: String) extends Expression with UnaryExpr with NamedExpr {
  def t = child.t

  def compile() = child.compile()
}

case class Literal(value: Any, t: Type) extends Expression with LeafExpr {

  def compile() = _ => value
}

case class Add(left: Expression, right: Expression) extends Expression with BinaryExpr {
  // FIXME
  def t = TInt

  def compile() = {
    val lf = left.compile()
    val lr = right.compile()

    { (v: Any) =>
      val lv = lf(v)
      val rv = lr(v)
      if (lv != null && rv != null)
        lv.asInstanceOf[Int] + rv.asInstanceOf[Int]
      else
        null
    }
  }
}

case class GtEq(left: Expression, right: Expression) extends Expression with BinaryExpr {
  // FIXME
  def t = TBoolean

  def compile() = {
    val lf = left.compile()
    val lr = right.compile()

    { (v: Any) =>
      val lv = lf(v)
      val rv = lr(v)
      // println(s"lv=$lv, rv=$rv")
      if (lv != null && rv != null)
        lv.asInstanceOf[Int] >= rv.asInstanceOf[Int]
      else
        null
    }
  }
}

// FIXME unbox
abstract class Combiner extends Serializable {
  def zeroValue: Any

  def combine(acc: Any, v: Any): Any

  def merge(acc1: Any, acc2: Any): Any
}

trait Aggregator extends NamedExpr {
  def compile() = throw new UnsupportedOperationException

  def combiner(): Combiner
}

case class Sum(name: String, child: Expression) extends Expression with UnaryExpr with Aggregator {
  def t = child.t

  def combiner() = new Combiner {
    def zeroValue = 0

    def combine(acc: Any, v: Any) = acc.asInstanceOf[Int] + v.asInstanceOf[Int]

    def merge(acc1: Any, acc2: Any) = acc1.asInstanceOf[Int] + acc2.asInstanceOf[Int]
  }
}

case class Count(name: String, child: Expression) extends Expression with UnaryExpr with Aggregator {
  // FIXME TLong?
  def t = TInt

  def combiner() = {
    val f = child.compile()

    new Combiner {
      def zeroValue = 0

      def combine(acc: Any, v: Any) = {
        val pv = f(v)
        acc.asInstanceOf[Int] + (pv != null && pv.asInstanceOf[Boolean]).toInt
      }

      def merge(acc1: Any, acc2: Any) = acc1.asInstanceOf[Int] + acc2.asInstanceOf[Int]
    }
  }
}

trait RelationalExpr extends BaseExpr {
  def t: TRelational

  def selectSchema: TStruct = t.selectSchema

  def bind(): Unit

  def eval(hc: HailQLContext): RelationalValue
}

case class Select(child: RelationalExpr, predicate: Expression) extends RelationalExpr {
  def children = Seq(child, predicate)

  def bind() = {
    child.bind()
    predicate.bind(child)
  }

  lazy val t = {
    bind()
    child.t
  }

  def eval(hc: HailQLContext) = {
    val childrval = child.eval(hc)
    val p: Any => Any = predicate.compile()
    child.eval(hc).filter(hc, predicate.compile())
  }
}

case class Project(child: RelationalExpr, exprs: Seq[NamedExpr]) extends RelationalExpr {
  def children = child +: exprs

  def bind() {
    child.bind()
    exprs.foreach(_.bind(child))
  }

  def t = {
    bind()
    child.t.withSchema(exprs.toTStruct)
  }

  def eval(hc: HailQLContext) = {
    val fs = exprs.map(_.compile())
    child.eval(hc).project(hc, fs)
  }
}

case class AggregateTable(child: RelationalExpr, groupingExprs: Seq[NamedExpr], aggExprs: Seq[Aggregator]) extends RelationalExpr {
  def children = (child +: groupingExprs) ++ aggExprs

  override def bind() {
    child.bind()
    groupingExprs.foreach(_.bind(child))
    aggExprs.foreach(_.bind(child))
  }

  lazy val t = {
    bind()
    // FIXME including grouping?
    TTable((groupingExprs ++ aggExprs).toTStruct)
  }

  def eval(hc: HailQLContext) = {
    val rval = child.eval(hc).asInstanceOf[TableValue]

    val gfs = groupingExprs.map(_.compile())
    val combiners = aggExprs.map(_.combiner())
    val zeroValues = combiners.map(_.zeroValue)

    TableValue(t,
      rval.rdd
        .map(v => (gfs.map(f => f(v)), v))
        .aggregateByKey(zeroValues)(
          (acc, v) => combiners.zip(acc).map { case (comb, acc) =>
            comb.combine(acc, v)
          },
          (acc1, acc2) => combiners.zip(acc1).zip(acc2).map { case ((comb, acc1), acc2) =>
            comb.merge(acc1, acc2)
          })
        .map { case (k, v) =>
          k.asInstanceOf[Seq[_]] ++ v.asInstanceOf[Seq[_]]
        })
  }
}

case class AggregateVector(child: RelationalExpr, groupingExprs: Seq[NamedExpr], aggExprs: Seq[Aggregator]) extends RelationalExpr {
  def children = (child +: groupingExprs) ++ aggExprs

  override def bind() {
    child.bind()
    groupingExprs.foreach(_.bind(child))
    aggExprs.foreach(_.bind(child))
  }

  lazy val t = {
    bind()
    TVector(
      groupingExprs.toTStruct,
      aggExprs.toTStruct)
  }

  def eval(hc: HailQLContext) = {
    val rval = child.eval(hc).asInstanceOf[TableValue]

    val gfs = groupingExprs.map(_.compile())
    val combiners = aggExprs.map(_.combiner())
    val zeroValues = combiners.map(_.zeroValue)

    VectorValue(t,
      rval.rdd
        .map(v => (gfs.map(f => f(v)), v))
        .aggregateByKey(zeroValues)(
          (acc, v) => combiners.zip(acc).map { case (comb, acc) =>
            comb.combine(acc, v)
          },
          (acc1, acc2) => combiners.zip(acc1).zip(acc2).map { case ((comb, acc1), acc2) =>
            comb.merge(acc1, acc2)
          })
        .map { case (k, v) => (k: Any, v: Any) })
  }
}

case class AggregateMatrix(child: RelationalExpr,
  rowGroupingExprs: Seq[NamedExpr],
  colGroupingExprs: Seq[NamedExpr],
  aggExprs: Seq[Aggregator]) extends RelationalExpr {
  def children = (child +: rowGroupingExprs) ++ colGroupingExprs ++ aggExprs

  override def bind() {
    child.bind()
    rowGroupingExprs.foreach(_.bind(child))
    colGroupingExprs.foreach(_.bind(child))
    aggExprs.foreach(_.bind(child))
  }

  lazy val t = {
    bind()
    TMatrix(
      rowGroupingExprs.toTStruct,
      colGroupingExprs.toTStruct,
      aggExprs.toTStruct)
  }

  def eval(hc: HailQLContext) = {
    val rval = child.eval(hc).asInstanceOf[TableValue]

    val rowgfs = rowGroupingExprs.map(_.compile())
    val colgfs = colGroupingExprs.map(_.compile())
    val combiners = aggExprs.map(_.combiner())

    val colKeys = rval.rdd
      .map(v => colgfs.map(f => f(v)))
      .distinct()
      .collect()

    val colKeyIndex = colKeys.zipWithIndex.toMap
    val nColKeys = colKeyIndex.size
    val colKeyIndexBc = hc.sc.broadcast(colKeyIndex)

    MatrixValue(t,
      colKeys,
      rval.rdd
        .map(v => (rowgfs.map(f => f(v)), v))
        .groupByKey()
        .map { case (rowKey, values) =>
          val a = Array.fill(nColKeys)(combiners.map(_.zeroValue))
          for (v <- values) {
            val colKey = colgfs.map(f => f(v))
            val i = colKeyIndexBc.value(colKey)
            a(i) = combiners.zip(a(i)).map { case (comb, acc) =>
              comb.combine(acc, v)
            }
          }
          (rowKey: Any, a.toSeq: Seq[Any])
        })
  }
}

case class RelationalLiteral(rval: RelationalValue) extends RelationalExpr with LeafExpr {
  def bind() {}

  def t = rval.t

  def eval(hc: HailQLContext) = rval
}

case class Expand(child: RelationalExpr) extends RelationalExpr with UnaryExpr {
  def bind() {
    child.bind()
  }

  lazy val t = {
    bind()
    TTable(child.t.selectSchema)
  }

  def eval(hc: HailQLContext) = child.eval(hc).expand(hc)
}

// natural join
case class TableJoin(left: RelationalExpr,
  right: RelationalExpr,
  joinType: JoinType) extends RelationalExpr with BinaryExpr {

  def bind() {
    left.bind()
    right.bind()
  }

  def t = {
    bind()

    val lt = left.t.asInstanceOf[TTable].schema
    val rt = right.t.asInstanceOf[TTable].schema

    val commonNames = lt.fields.map(_.name).toSet
      .intersect(rt.fields.map(_.name).toSet)

    val common = lt.fields
      .filter(f => !commonNames.contains(f.name))
      .map(f => (f.name, f.t))

    val commonIndex = common.zipWithIndex.toMap

    TTable(TStruct.from(
      common
        ++ lt.fields.filter(f => !commonNames.contains(f.name)).map(f => (f.name, f.t))
        ++ rt.fields.filter(f => !commonNames.contains(f.name)).map(f => (f.name, f.t)): _*))
  }

  def eval(hc: HailQLContext) = {
    val ltv = left.eval(hc).asInstanceOf[TableValue]
    val rtv = right.eval(hc).asInstanceOf[TableValue]

    // dup
    val commonNames = ltv.t.schema.fields.map(_.name).toSet
      .intersect(rtv.t.schema.fields.map(_.name).toSet)

    val common = ltv.t.schema.fields
      .filter(f => !commonNames.contains(f.name))
      .map(f => (f.name, f.t))
    val nCommon = common.size

    val leftRDD = ltv.rdd.map { x =>
      val xseq = x.asInstanceOf[Seq[_]]
      val k = mutable.Seq.fill[Any](nCommon)(null)
      common.zipWithIndex.foreach { case ((name, t), i) =>
        val j = ltv.t.schema.field(name).i
        k(j) = xseq(i)
      }
      (k: Any, x)
    }

    val rightRDD = rtv.rdd.map { x =>
      val xseq = x.asInstanceOf[Seq[_]]
      val k = mutable.Seq.fill[Any](nCommon)(null)
      common.zipWithIndex.foreach { case ((name, t), i) =>
        val j = ltv.t.schema.field(name).i
        k(j) = xseq(i)
      }
      (k: Any, x)
    }

    val joinedRDD = joinType match {
      case Inner =>
        leftRDD.join(rightRDD)
      case LeftOuter =>
        leftRDD.leftOuterJoin(rightRDD)
          .map { case (k, (lv, rv)) =>
            (k, (lv, rv.getOrElse(Seq.fill[Any](rtv.t.schema.size)(null))))
          }
      case RightOuter =>
        leftRDD.rightOuterJoin(rightRDD)
          .map { case (k, (lv, rv)) =>
            (k, (lv.getOrElse(Seq.fill[Any](ltv.t.schema.size)(null)), rv))
          }
      case FullOuter =>
        leftRDD.fullOuterJoin(rightRDD)
          .map { case (k, (lv, rv)) =>
            (k, (lv.getOrElse(Seq.fill[Any](ltv.t.schema.size)(null)),
              rv.getOrElse(Seq.fill[Any](rtv.t.schema.size)(null))))
          }
    }

    TableValue(t,
      joinedRDD.map { case (k, (lv, rv)) =>
        (k.asInstanceOf[Seq[_]]
          ++ lv.asInstanceOf[Seq[_]]
          ++ rv.asInstanceOf[Seq[_]])
      })
  }
}

case class VectorJoin(left: RelationalExpr,
  right: RelationalExpr,
  joinType: JoinType) extends RelationalExpr with BinaryExpr {

  def bind() {
    left.bind()
    right.bind()
  }

  def t = {
    bind()

    val lt = left.t.asInstanceOf[TVector]
    val rt = right.t.asInstanceOf[TVector]

    TVector(lt.keySchema,
      lt.schema.union(rt.schema))
  }

  def eval(hc: HailQLContext) = {
    val lv = left.eval(hc).asInstanceOf[VectorValue]
    val rv = right.eval(hc).asInstanceOf[VectorValue]

    // FIXME cleanup
    val newRDD = joinType match {
      case Inner =>
        lv.rdd.join(rv.rdd)
          .map[(Any, Any)] { case (k, (lx, rx)) =>
          (k, lx.asInstanceOf[Seq[_]] ++ rx.asInstanceOf[Seq[_]])
        }
      case LeftOuter =>
        lv.rdd.leftOuterJoin(rv.rdd)
          .map[(Any, Any)] { case (k, (lx, rx)) =>
          (k, lx.asInstanceOf[Seq[_]]
            ++ rx.map(_.asInstanceOf[Seq[_]])
            .getOrElse(Seq.fill[Any](rv.t.schema.size)(null)))
        }
      case RightOuter =>
        lv.rdd.rightOuterJoin(rv.rdd)
          .map[(Any, Any)] { case (k, (lx, rx)) =>
          (k, lx.map(_.asInstanceOf[Seq[_]])
            .getOrElse(Seq.fill[Any](rv.t.schema.size)(null))
            ++ rx.asInstanceOf[Seq[_]])
        }
      case FullOuter =>
        lv.rdd.fullOuterJoin(rv.rdd)
          .map[(Any, Any)] { case (k, (lx, rx)) =>
          (k, lx.map(_.asInstanceOf[Seq[_]])
            .getOrElse(Seq.fill[Any](rv.t.schema.size)(null))
            ++ rx.map(_.asInstanceOf[Seq[_]])
            .getOrElse(Seq.fill[Any](rv.t.schema.size)(null)))
        }
    }

    VectorValue(t, newRDD)
  }
}

case class MatrixJoin(left: RelationalExpr,
  right: RelationalExpr,
  rowJoinType: JoinType,
  colJoinType: JoinType) extends RelationalExpr with BinaryExpr {

  def bind() {
    left.bind()
    right.bind()
  }

  def t = {
    bind()

    val lt = left.t.asInstanceOf[TMatrix]
    val rt = right.t.asInstanceOf[TMatrix]

    TMatrix(lt.rowKeySchema,
      lt.colKeySchema,
      lt.schema.union(rt.schema))
  }

  def eval(hc: HailQLContext) = {
    val lmv = left.eval(hc).asInstanceOf[MatrixValue]
    val rmv = right.eval(hc).asInstanceOf[MatrixValue]

    val colKeys = colJoinType match {
      case Inner =>
        lmv.colKeys.intersect(rmv.colKeys)
      case LeftOuter =>
        lmv.colKeys
      case RightOuter =>
        rmv.colKeys
      case FullOuter =>
        lmv.colKeys.union(rmv.colKeys)
    }
    val nColumns = colKeys.size

    val colKeyIndex = colKeys.zipWithIndex.toMap

    val joinedRDD = rowJoinType match {
      case Inner =>
        lmv.rdd.join(lmv.rdd)

      case LeftOuter =>
        lmv.rdd.leftOuterJoin(rmv.rdd).map { case (k, (lv, rv)) =>
          (k, (lv,
            rv.getOrElse(Seq.fill[Any](nColumns)(Seq.fill[Any](rmv.t.schema.size)(null)))))
        }

      case RightOuter =>
        lmv.rdd.rightOuterJoin(rmv.rdd).map { case (k, (lv, rv)) =>
          (k, (lv.getOrElse(Seq.fill[Any](nColumns)(Seq.fill[Any](lmv.t.schema.size)(null))),
            rv))
        }

      case FullOuter =>
        lmv.rdd.fullOuterJoin(rmv.rdd).map { case (k, (lv, rv)) =>
          (k, (lv.getOrElse(Seq.fill[Any](nColumns)(Seq.fill[Any](lmv.t.schema.size)(null))),
            rv.getOrElse(Seq.fill[Any](nColumns)(Seq.fill[Any](rmv.t.schema.size)(null)))))
        }
    }

    MatrixValue(t,
      colKeys,
      joinedRDD.map { case (k, (lvalues, rvalues)) =>
        (k, lvalues.zip(rvalues).map { case (lv, rv) =>
          lv.asInstanceOf[Seq[_]] ++ rv.asInstanceOf[Seq[_]]
        })
      }
    )
  }
}

case class FilterRows(child: RelationalExpr,
  predicate: Expression) extends RelationalExpr {
  def children = Seq(child, predicate)

  override def bind() = {
    child.bind()
    predicate.bind(child)
  }

  lazy val t = {
    bind()
    child.t.asInstanceOf[TMatrix]
  }

  def eval(hc: HailQLContext) = {
    val mval = child.eval(hc).asInstanceOf[MatrixValue]

    // FIXME in predicate context
    val pf = predicate.compile()
    MatrixValue(t,
      mval.colKeys,
      mval.rdd.filter { case (rowKey, values) =>
        val u = rowKey.asInstanceOf[Seq[_]] ++ Seq.fill[Any](t.colKeySchema.size + t.schema.size)(null)
        val keep = pf(u)
        keep != null && keep.asInstanceOf[Boolean]
      })
  }

}

object FilterColumns {
  def vectorIndex[T](a: Seq[T], indices: Seq[Int])(implicit tct: ClassTag[T]): Seq[T] = {
    val r = new Array[T](indices.length)
    indices.zipWithIndex.foreach { case (i, j) => r(j) = a(i) }
    r.toSeq
  }
}

case class FilterColumns(child: RelationalExpr,
  predicate: Expression) extends RelationalExpr {
  def children = Seq(child, predicate)

  def bind() = {
    child.bind()
    predicate.bind(child)
  }

  lazy val t = {
    bind()
    child.t.asInstanceOf[TMatrix]
  }

  def eval(hc: HailQLContext) = {
    val mval = child.eval(hc).asInstanceOf[MatrixValue]

    // FIXME in predicate context
    val pf = predicate.compile()
    val keepIndices = mval.colKeys
      .zipWithIndex
      .filter { case (colKey, i) =>
        val u = Seq.fill[Any](t.rowKeySchema.size)(null) ++ colKey.asInstanceOf[Seq[_]] ++ Seq.fill[Any](t.schema.size)(null)
        val keep = pf(u)
        keep != null && keep.asInstanceOf[Boolean]
      }.map { case (colKey, i) => i }

    MatrixValue(t,
      FilterColumns.vectorIndex(mval.colKeys, keepIndices),
      mval.rdd.map { case (rowKey, values) =>
        (rowKey, FilterColumns.vectorIndex(values, keepIndices))
      })
  }

}

case class AggregateRows(rexpr: RelationalExpr,
  aggExprs: Seq[Aggregator]) extends RelationalExpr {
  def children = rexpr +: aggExprs

  def bind() {
    rexpr.bind()
    aggExprs.foreach(_.bind(rexpr))
  }

  def t = {
    bind()

    val rexprt = rexpr.t.asInstanceOf[TMatrix]
    TVector(rexprt.rowKeySchema,
      aggExprs.toTStruct)
  }

  def eval(hc: HailQLContext) = {
    val mval = rexpr.eval(hc).asInstanceOf[MatrixValue]

    val combiners = aggExprs.map(_.combiner())

    VectorValue(t,
      mval.rdd.map { case (rowKey, values) =>
        (rowKey, values
          .aggregate(combiners.map(_.zeroValue))({ case (acc, x) =>
            combiners.zip(acc).map { case (comb, acc) =>
              comb.combine(acc, x)
            }
          }, { case (acc1, acc2) =>
            combiners.zip(acc1).zip(acc2).map { case ((comb, acc1), acc2) =>
              comb.merge(acc1, acc2)
            }
          }))
      })
  }
}

case class AggregateColumns(rexpr: RelationalExpr,
  aggExprs: Seq[Aggregator]) extends RelationalExpr {
  def children = rexpr +: aggExprs

  def bind() {
    rexpr.bind()
    aggExprs.foreach(_.bind(rexpr))
  }

  def t = {
    bind()

    val rexprt = rexpr.t.asInstanceOf[TMatrix]
    TVector(rexprt.colKeySchema,
      aggExprs.toTStruct)
  }

  def eval(hc: HailQLContext) = {
    val mval = rexpr.eval(hc).asInstanceOf[MatrixValue]
    val nColumns = mval.colKeys.size
    val combiners = aggExprs.map(_.combiner())

    VectorValue(t,
      hc.sc.parallelize(
        mval.colKeys.zip(mval.rdd
          .aggregate(Seq.fill[Seq[Any]](nColumns)(combiners.map(_.zeroValue)))({ case (acc, (rowKey, values)) =>
            acc.zip(values).map { case (acc, v) =>
              combiners.zip(acc).map { case (comb, acc) =>
                comb.combine(acc, v)
              }
            }
          }, { case (acc1, acc2) =>
            acc1.zip(acc2).map { case (acc1, acc2) =>
              combiners.zip(acc1).zip(acc2).map { case ((comb, acc1), acc2) =>
                comb.merge(acc1, acc2)
              }
            }
          }))
      ))
  }
}
