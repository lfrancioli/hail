package org.broadinstitute.hail.hailql

import org.apache.spark.rdd.RDD

abstract class RelationalValue {
  def t: TRelational

  def filter(hc: HailQLContext, predicate: Any => Any): RelationalValue

  def project(hc: HailQLContext, fs: Seq[Any => Any]): RelationalValue

  def expand(hc: HailQLContext): TableValue
}

case class TableValue(t: TTable, rdd: RDD[Any]) extends RelationalValue {
  def filter(hc: HailQLContext, predicate: Any => Any): TableValue =
    TableValue(t,
      rdd
        .filter { x =>
          val keep = predicate(x)
          keep != null && keep.isInstanceOf[Boolean]
        })

  def project(hc: HailQLContext, fs: Seq[Any => Any]): TableValue =
    TableValue(t,
      rdd.map { x =>
        fs.map(f => f(x))
      })

  def expand(hc: HailQLContext) = this
}

case class VectorValue(t: TVector,
  rdd: RDD[(Any, Any)]) extends RelationalValue {
  def filter(hc: HailQLContext, predicate: Any => Any) =
    VectorValue(t,
      rdd
        .filter { case (k, v) =>
          val u = k.asInstanceOf[Seq[_]] ++ v.asInstanceOf[Seq[_]]
          val keep = predicate(u)
          keep != null && keep.isInstanceOf[Boolean]
        })

  def project(hc: HailQLContext, fs: Seq[Any => Any]): VectorValue =
    VectorValue(t,
      rdd.map { case (k, v) =>
        val u = k.asInstanceOf[Seq[_]] ++ v.asInstanceOf[Seq[_]]
        (k, fs.map(f => f(u)))
      })

  def expand(hc: HailQLContext) = {
    TableValue(TTable(t.selectSchema),
      rdd.map { case (k, v) =>
        k.asInstanceOf[Seq[_]] ++ v.asInstanceOf[Seq[_]]
      }
    )
  }
}

case class MatrixValue(t: TMatrix,
  colKeys: Seq[Any],
  // FIXME Iterable[Any]
  rdd: RDD[(Any, Seq[Any])]) extends RelationalValue {
  def filter(hc: HailQLContext, predicate: Any => Any): MatrixValue = {
    val colKeysBc = hc.sc.broadcast(colKeys)
    MatrixValue(t,
      colKeys,
      rdd.map { case (rowKey, values) =>
        (rowKey, values.zipWithIndex.map { case (v, i) =>
          val u = rowKey.asInstanceOf[Seq[_]] ++
            colKeysBc.value(i).asInstanceOf[Seq[_]] ++
            v.asInstanceOf[Seq[_]]
          val keep = predicate(u)
          if (keep != null && keep.asInstanceOf[Boolean])
            v
          else
            null
        })
      })
  }

  def project(hc: HailQLContext, fs: Seq[Any => Any]): MatrixValue = {
    val colKeysBc = hc.sc.broadcast(colKeys)
    MatrixValue(t,
      colKeys,
      rdd.map { case (rowKey, values) =>
        (rowKey, values.zipWithIndex.map { case (v, i) =>
          val u = rowKey.asInstanceOf[Seq[_]] ++
            colKeysBc.value(i).asInstanceOf[Seq[_]] ++
            v.asInstanceOf[Seq[_]]
          fs.map(f => f(u))
        }: Seq[Any])
      })
  }

  def expand(hc: HailQLContext) = {
    val colKeysBc = hc.sc.broadcast(colKeys)
    TableValue(TTable(t.selectSchema),
      rdd.flatMap { case (rowKey, values) =>
        values.zipWithIndex.map { case (v, i) =>
          rowKey.asInstanceOf[Seq[_]] ++
            colKeysBc.value(i).asInstanceOf[Seq[_]] ++
            v.asInstanceOf[Seq[_]]
        }
      }
    )
  }
}
