package org.broadinstitute.hail.hailql

sealed abstract class Type {
  def pretty() {
    pretty(0)
    print("\n")
  }

  def pretty(indent: Int)
}

case object TBoolean extends Type {
  def pretty(indent: Int) = print("Boolean")
}

case object TInt extends Type {
  def pretty(indent: Int) = print("Int")
}

case object TDouble extends Type {
  def pretty(indent: Int) = print("Double")
}

case object TString extends Type {
  def pretty(indent: Int) = print("String")
}

case object TUnit extends Type {
  def pretty(indent: Int) = print("Unit")
}

case class TArray(elementType: Type) extends Type {
  def pretty(indent: Int) {
    print("Array[")
    elementType.pretty(indent)
    print("]")
  }
}

case class Field(name: String, t: Type, i: Int)

object TStruct {
  def empty: TStruct = TStruct(Array.empty[Field])

  def from(args: (String, Type)*): TStruct =
    TStruct(args
      .zipWithIndex
      .map { case ((n, t), i) => Field(n, t, i) })

}

case class TStruct(fields: Seq[Field]) extends Type {
  assert(fields.zipWithIndex.forall { case (f, i) =>
    f.i == i
  })

  val fieldIndex: Map[String, Int] = fields.zipWithIndex.map { case (f, i) => (f.name, i) }.toMap

  def field(name: String): Field = fields(fieldIndex(name))

  def fieldOption(name: String): Option[Field] = fieldIndex.get(name).map(i => fields(i))

  def pretty(indent: Int) {
    println("Struct {")
    fields.foreach { f =>
      print(" " * (indent + 2))
      print(f.name)
      print(": ")
      f.t.pretty(indent + 2)
      println()
    }
    print(" " * indent)
    print("}")
  }

  // FIXME check name collision
  def union(other: TStruct): TStruct = TStruct.from((fields ++ other.fields).map(f => (f.name, f.t)): _*)

  def size: Int = fields.size
}

abstract class TRelational extends Type {
  def selectSchema: TStruct

  def schema: TStruct

  def withSchema(newSchema: TStruct): TRelational
}

case class TTable(schema: TStruct) extends TRelational {
  def pretty(indent: Int) {
    println("Table {")

    print("  schema: ")
    schema.pretty(indent + 2)
    println()

    println("}")
  }

  def selectSchema = schema

  def withSchema(newSchema: TStruct) = TTable(newSchema)
}

case class TVector(
  keySchema: TStruct,
  schema: TStruct) extends TRelational {

  def pretty(indent: Int) {
    println("Matrix {")

    print("  rowKeySchema: ")
    keySchema.pretty(indent + 2)
    println()

    print("  schema: ")
    schema.pretty(indent + 2)
    println()

    println("}")
  }

  def selectSchema = keySchema.union(schema)

  def withSchema(newSchema: TStruct) = TVector(keySchema, newSchema)
}

case class TMatrix(
  rowKeySchema: TStruct,
  colKeySchema: TStruct,
  schema: TStruct) extends TRelational {

  def pretty(indent: Int) {
    println("Matrix {")

    print("  rowKeySchema: ")
    rowKeySchema.pretty(indent + 2)
    println()

    print("  colKeySchema: ")
    colKeySchema.pretty(indent + 2)
    println()

    print("  schema: ")
    schema.pretty(indent + 2)
    println()

    println("}")
  }

  def selectSchema = rowKeySchema.union(colKeySchema).union(schema)

  def withSchema(newSchema: TStruct) = TMatrix(rowKeySchema, colKeySchema, newSchema)
}