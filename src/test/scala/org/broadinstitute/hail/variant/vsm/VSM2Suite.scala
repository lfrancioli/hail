package org.broadinstitute.hail.variant.vsm

import org.broadinstitute.hail.SparkSuite
import org.testng.annotations.Test
import org.broadinstitute.hail.driver._
import org.broadinstitute.hail.variant._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row


//Make user defined types
// GenotypeType
// VariantType



class VSM2Suite extends SparkSuite {
  @Test def test1() {
    var s = State(sc, sqlContext, null)
    val testVCF = "src/test/resources/sample.vcf"
    val output = tmpDir.createTempFile("testDataFrame", ".vds")
    s = ImportVCF.run(s, Array(testVCF))
    var v = Count.run(s, Array.empty[String])
//    s = Write.run(s, Array("-o", output) )
//    sys.exit()
    val vsm2 = VariantSampleMatrix2.convertToVSM2(s.vds)
    val df = vsm2.df
    val sampleIds = vsm2.sampleIds
    println(df.printSchema())
    println(df.count())

    println(df.select(df("v")).show(5))
    println(df.select(df("v.contig")).show(5))
    println(df.groupBy("v.contig").count().show())

    println(df.select("gs").show(5))
    println(df.select(explode(df("gs"))).show(5))
    println(df.select(df("gs").apply("gt")).show(5))
    println(df.select(df("gs").apply(0)).show(5))
    println(df.select(df("v"), explode(df("gs").apply("gt")).as("gt")).show(5))
    println(df.select(df("gs").apply("gt").as("gt")).show(5))

    case class Genotype2(gt: Int) {
      def isHomRef: Boolean = gt == 0
    }

//    class AltAlleleUDT extends UserDefinedType[AltAllele] {
//      def dataType = StructType(Array(
//        StructField("ref", StringType, nullable = false),
//        StructField("alt", StringType, nullable = false)))
//
//      def serialize(a: AltAllele) = Row(a.ref, a.alt)
//
//      def deserialize(r: Row) = AltAllele(r.getString(0), r.getString(1))
//    }
//
//    class VariantUDT extends UserDefinedType[Variant] {
//      def dataType = StructType( Seq (
//        StructField("contig", StringType, false),
//        StructField("start", IntegerType, false),
//        StructField("ref", StringType, nullable = false),
//        StructField("altAlleles", ArrayType(AltAllele.schema, containsNull = false),
//          nullable = false)))
//
//      def serialize(v: Variant) = Row(v.contig, v.start, v.ref, v.altAlleles)
//
//      def deserialize(r: Row) = Variant(r.getString(0), r.getInt(1), r.getString(2), r.getSeq[AltAllele](3).toArray)
//    }

    println(df.select(df("v"), df("gs").apply("gt")).rdd.map{r => (r.getAs[Variant](0), r.getSeq[Genotype2](1))}.take(5).mkString("\n"))
    //FIXME: Why Empty Row always being printed out?

    println(df.select(df("v"), df("gs").apply("gt")).rdd
      .map{r => (Variant.fromRow(r.getStruct(0)), r.getSeq(1))}
      .map{case (v, gs) => v.toString}
      .take(5).mkString("\n"))


    def udfGenotypeCountNoCall = udf((gts: Seq[Int]) => gts.count(g => g == null))

    println(df.withColumn("nNoCalls", udfGenotypeCountNoCall(df("gs").apply("gt"))).show(5))

    println(df.withColumn("nNoCalls", udfGenotypeCountNoCall(df("gs").apply("gt"))).agg(sum("nNoCalls")).show(5))

    println(df.select(df("v"), explode(df("gs").apply("gt")).as("gt")).groupBy("gt").count().as("count").show())

    class CountGenotypeType() extends UserDefinedAggregateFunction {
      // Schema you get as an input
      def inputSchema = new StructType().add("gt", IntegerType)
      // Schema of the row which is used for aggregation
      def bufferSchema = new StructType().add("nNoCall", IntegerType)
      // Returned type
      def dataType = IntegerType
      // Self-explaining
      def deterministic = true
      // zero value
      def initialize(buffer: MutableAggregationBuffer) = buffer.update(0, 0) //called once for each partition
      // Similar to seqOp in aggregate
      def update(buffer: MutableAggregationBuffer, input: Row) = {
        if (input.isNullAt(0))
          buffer.update(0, buffer.getInt(0) + input.getInt(0))
      }
      // Similar to combOp in aggregate
      def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
        buffer1.update(0, buffer1.getInt(0) | buffer2.getInt(0))
      }
      // Called on exit to get return value
      def evaluate(buffer: Row) = buffer.getInt(0)
    }

    sqlContext.udf.register("countNoCallGT", new CountGenotypeType)

//    df.select(gtSelectors:_*).rdd.foreach{r => println(r.toSeq)}

    //s = Write.run(s, Array("-o", output))
    //s = Read.run(s, Array("-i", output))

  }
}
