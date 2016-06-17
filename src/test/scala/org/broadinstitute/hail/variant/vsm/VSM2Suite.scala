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
    s = Write.run(s, Array("-o", output))
    s = Read.run(s, Array("-i", output)) // reads vds on disk to VDS
    s = Read2.run(s, Array("-i", output)) // reads vds on disk to VSM2
//    s = Count.run(s, Array.empty[String])
//    sys.exit()
//
//    var v = Count.run(s, Array.empty[String])
////    s = Write.run(s, Array("-o", output) )
////    sys.exit()
//    sys.exit()
//    val vsm2 = VariantSampleMatrix2.convertToVSM2(s.vds)
//    s = s.copy(vsm2 = vsm2)
    s = Count.run(s, Array.empty[String]) // from RDD
    s = Count2.run(s, Array.empty[String]) // user defined function
    s = Count3.run(s, Array.empty[String]) // explode and groupby
//
//    def udfGenotypeCountNoCall = udf((gts: Seq[java.lang.Integer]) => gts.count(g => g == null))
//
//    val numSamples = vsm2.nSamples
//    val numVariants = df.count()
//    val numNoCalls = df.withColumn("nNoCalls", udfGenotypeCountNoCall(df("gs").apply("gt")))
//      .agg(sum("nNoCalls")).collect().apply(0).getLong(0)
//
//    val numGenotypes = numSamples * numVariants
//    val numCalled = numGenotypes - numNoCalls
//
//    println("Count with dataframe:")
//    println(s"numSamples: ${numSamples}")
//    println(s"numVariants: ${numVariants}")
//    println(s"numCalled: ${numGenotypes - numNoCalls}")
//    println(s"callRate: ${numCalled / numGenotypes.toDouble}")
//    //println(df.select(df("v"), explode(df("gs").apply("gt")).as("gt")).groupBy("gt").count().as("count").show())
//
//    class CountGenotypeType() extends UserDefinedAggregateFunction {
//      // Schema you get as an input
//      def inputSchema = new StructType().add("gt", IntegerType)
//      // Schema of the row which is used for aggregation
//      def bufferSchema = new StructType().add("nNoCall", IntegerType)
//      // Returned type
//      def dataType = IntegerType
//      // Self-explaining
//      def deterministic = true
//      // zero value
//      def initialize(buffer: MutableAggregationBuffer) = buffer.update(0, 0) //called once for each partition
//      // Similar to seqOp in aggregate
//      def update(buffer: MutableAggregationBuffer, input: Row) = {
//        if (input.isNullAt(0))
//          buffer.update(0, buffer.getInt(0) + input.getInt(0))
//      }
//      // Similar to combOp in aggregate
//      def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
//        buffer1.update(0, buffer1.getInt(0) | buffer2.getInt(0))
//      }
//      // Called on exit to get return value
//      def evaluate(buffer: Row) = buffer.getInt(0)
//    }
//
//    sqlContext.udf.register("countNoCallGT", new CountGenotypeType)
//
////    df.select(gtSelectors:_*).rdd.foreach{r => println(r.toSeq)}
//
//    //s = Write.run(s, Array("-o", output))
//    //s = Read.run(s, Array("-i", output))

  }
}
