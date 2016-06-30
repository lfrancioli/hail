package org.apache.spark.sql

import org.apache.spark.sql.types.DataType

object UserDefinedFunctionBuilder {
  def apply(f: AnyRef,
    dataType: DataType,
    inputTypes: Seq[DataType] = Nil): UserDefinedFunction = UserDefinedFunction(f, dataType, inputTypes)
}
