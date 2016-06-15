package org.broadinstitute.hail.variant

import net.jpountz.lz4.LZ4Factory
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.mutable

//case class Genotype2(gt: Int, ad: Array[Int], dp: Int, gq: Int, pl: Array[Int], fakeRef: Boolean)

//case class GenotypeArrays(gt: Array[Int], ad: Array[Array[Int]], dp: Array[Int], gq: Array[Int], pl: Array[Array[Int]], fr: Array[Boolean]) {
//  val size = gt.length
//  assert(size == ad.length &&
//    size == dp.length &&
//    size == gq.length &&
//    size == pl.length)
//
//  def toRow: Row = {
//    Row.fromSeq(Array(gt, ad, dp, gq, pl, fr))
//  }
//}

//class GenotypeArrayBuilder {
//  private val gtb = new mutable.ArrayBuilder.ofInt
//  private val adb = new mutable.ArrayBuilder.ofRef[Array[Int]]
//  private val dpb = new mutable.ArrayBuilder.ofInt
//  private val gqb = new mutable.ArrayBuilder.ofInt
//  private val plb = new mutable.ArrayBuilder.ofRef[Array[Int]]
//  private val frb = new mutable.ArrayBuilder.ofBoolean
//
//  def +=(gt: Int, ad: Array[Int], dp: Int, gq: Int, pl: Array[Int], fr: Boolean = false): GenotypeArrayBuilder.this.type = {
//    gtb += gt
//    adb += ad
//    dpb += dp
//    gqb += gq
//    plb += pl
//    frb += fr
//    this
//  }
//
//  def result(): GenotypeArrays =
//    GenotypeArrays(gtb.result(), adb.result(), dpb.result(), gqb.result(), plb.result(), frb.result())
//
//  def clear(): GenotypeArrayBuilder.this.type = {
//    gtb.clear()
//    adb.clear()
//    dpb.clear()
//    gqb.clear()
//    plb.clear()
//    frb.clear()
//    this
//  }
//}

// FIXME use zipWithIndex
class GenotypeStreamIterator(arr: Array[Genotype]) extends Iterator[Genotype] {
  var i: Int = 0
  override val size = arr.size

  override def hasNext: Boolean = i < size

  override def next(): Genotype = {
    val g = arr(i)
    i += 1
    g
  }
}

object LZ4Utils {
  val factory = LZ4Factory.fastestInstance()
  val compressor = factory.highCompressor()
  val decompressor = factory.fastDecompressor()

  def compress(a: Array[Byte]): Array[Byte] = {
    val decompLen = a.length

    val maxLen = compressor.maxCompressedLength(decompLen)
    val compressed = Array.ofDim[Byte](maxLen)
    val compressedLen = compressor.compress(a, 0, a.length, compressed, 0, maxLen)

    compressed.take(compressedLen)
  }

  def decompress(decompLen: Int, a: Array[Byte]) = {
    val decomp = Array.ofDim[Byte](decompLen)
    val compLen = decompressor.decompress(a, 0, decomp, 0, decompLen)
    assert(compLen == a.length)

    decomp
  }
}

case class GenotypeStream(variant: Variant, arr: Array[Genotype])
  extends Iterable[Genotype] {

  override def iterator: GenotypeStreamIterator = {
    new GenotypeStreamIterator(arr)
  }

  override def newBuilder: mutable.Builder[Genotype, GenotypeStream] = {
    new GenotypeStreamBuilder(variant)
  }

  //  def decompressed: GenotypeStream = {
  //    decompLenOption match {
  //      case Some(decompLen) =>
  //        GenotypeStream(variant, None, LZ4Utils.decompress(decompLen, a))
  //      case None => this
  //    }
  //  }
  //
  //  def compressed: GenotypeStream = {
  //    decompLenOption match {
  //      case Some(_) => this
  //      case None =>
  //        GenotypeStream(variant, Some(a.length), LZ4Utils.compress(a))
  //    }
  //  }

}

object GenotypeStream {

  def schema: Array[StructType] = {
    Array(StructType(Array(
      StructField("gt", ArrayType(IntegerType), nullable = true),
      StructField("ad", ArrayType(ArrayType(IntegerType)), nullable = true),
      StructField("dp", ArrayType(IntegerType), nullable = true),
      StructField("gq", ArrayType(IntegerType), nullable = true),
      StructField("pl", ArrayType(ArrayType(IntegerType)), nullable = true),
      StructField("fr", ArrayType(BooleanType), nullable = false)
    )))
  }

//  def schema: StructType = {
//    StructType(Array(
//      StructField("gt", ArrayType(IntegerType), nullable = true),
//      StructField("ad", ArrayType(ArrayType(IntegerType)), nullable = true),
//      StructField("dp", ArrayType(IntegerType), nullable = true),
//      StructField("gq", ArrayType(IntegerType), nullable = true),
//      StructField("pl", ArrayType(ArrayType(IntegerType)), nullable = true),
//      StructField("fr", ArrayType(BooleanType), nullable = false)
//    ))
//  }

  def fromRow(v: Variant, genotypes: Array[Row]): GenotypeStream = {
//    println(row)
    GenotypeStream(v, genotypes.map(r => Genotype(r)))
  }
}

class GenotypeStreamBuilder(variant: Variant)
  extends mutable.Builder[Genotype, GenotypeStream] {

  val gab = new mutable.ArrayBuilder.ofRef[Genotype]
  lazy val gb = new GenotypeBuilder(variant)

  override def +=(g: Genotype): GenotypeStreamBuilder.this.type = {
    gb.clear()
    gb.set(g)
    gab += new Genotype(gb.getGT, gb.getAD, gb.getDP, gb.getGQ, gb.getPL, gb.getFR)
    this
  }


  def write(gb: GenotypeBuilder) {
    gab += new Genotype(gb.getGT, gb.getAD, gb.getDP, gb.getGQ, gb.getPL, gb.getFR)}

  def ++=(i: Iterator[Genotype]): GenotypeStreamBuilder.this.type = {
    i.foreach(this += _)
    this
  }

  override def clear() {
    gab.clear()
  }

  override def result(): GenotypeStream = {
    GenotypeStream(variant, gab.result())
  }
}
