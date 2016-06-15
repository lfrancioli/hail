package org.broadinstitute.hail.variant

import java.nio.ByteBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkContext, SparkEnv}
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.check.Gen
import org.broadinstitute.hail.expr._

import scala.io.Source
import scala.language.implicitConversions
import scala.reflect.ClassTag

object VariantSampleMatrix2 {
  final val magicNumber: Int = 0xe51e2c58
  final val fileVersion: Int = 3

  def apply(metadata: VariantMetadata, df: DataFrame): VariantSampleMatrix2 = new VariantSampleMatrix2(metadata, df)

  def convertToVSM2(vds: VariantDataset): VariantSampleMatrix2 = {
    val vaRequiresConversion = vds.vaSignature.requiresConversion
    val vaSignature = vds.vaSignature

    VariantSampleMatrix2(
    vds.metadata,
    vds.rdd.map{case (v, va, gs) =>
      Row.fromSeq(Array(v.toRow, if (vaRequiresConversion) vaSignature.makeSparkWritable(va) else va,
      Row.fromSeq(gs.toGenotypeStream(v).arr.map(g => g.toRow))))
    }.toDF
  )
  }

  def read(sqlContext: SQLContext, dirname: String, skipGenotypes: Boolean = false): VariantSampleMatrix2 = {
    if (!dirname.endsWith(".vds") && !dirname.endsWith(".vds/"))
      fatal(s"input path ending in `.vds' required, found `$dirname'")

    val hConf = sqlContext.sparkContext.hadoopConfiguration

    if (!hadoopExists(hConf, dirname))
      fatal(s"no VDS found at `$dirname'")

    val vaSchema = dirname + "/va.schema"
    val saSchema = dirname + "/sa.schema"
    val globalSchema = dirname + "/global.schema"
    val pqtSuccess = dirname + "/rdd.parquet/_SUCCESS"
    val metadataFile = dirname + "/metadata.ser"

    if (!hadoopExists(hConf, metadataFile))
      fatal("corrupt VDS: no metadata.ser file.  Recreate VDS.")

    val (sampleIds, sampleAnnotations, globalAnnotation, wasSplit) =
      readDataFile(dirname + "/metadata.ser", hConf) { dis =>
        try {
          val serializer = SparkEnv.get.serializer.newInstance()
          val ds = serializer.deserializeStream(dis)

          val m = ds.readObject[Int]
          if (m != magicNumber)
            fatal("Invalid VDS: invalid magic number")

          val v = ds.readObject[Int]
          if (v != fileVersion)
            fatal("Old VDS version found")

          val sampleIds = ds.readObject[IndexedSeq[String]]
          val sampleAnnotations = ds.readObject[IndexedSeq[Annotation]]
          val globalAnnotation = ds.readObject[Annotation]
          val wasSplit = ds.readObject[Boolean]

          ds.close()
          (sampleIds, sampleAnnotations, globalAnnotation, wasSplit)

        } catch {
          case e: Exception =>
            println(e)
            fatal(s"Invalid VDS: ${e.getMessage}\n  Recreate with current version of Hail.")
        }
      }

    if (!hadoopExists(hConf, pqtSuccess))
      fatal("corrupt VDS: no parquet success indicator, meaning a problem occurred during write.  Recreate VDS.")

    if (!hadoopExists(hConf, vaSchema, saSchema, globalSchema))
      fatal("corrupt VDS: one or more .schema files missing.  Recreate VDS.")

    val vaSignature = readFile(dirname + "/va.schema", hConf) { dis =>
      val schema = Source.fromInputStream(dis)
        .mkString
      Parser.parseType(schema)
    }

    val saSignature = readFile(dirname + "/sa.schema", hConf) { dis =>
      val schema = Source.fromInputStream(dis)
        .mkString
      Parser.parseType(schema)
    }

    val globalSignature = readFile(dirname + "/global.schema", hConf) { dis =>
      val schema = Source.fromInputStream(dis)
        .mkString
      Parser.parseType(schema)
    }


    val metadata = VariantMetadata(sampleIds, sampleAnnotations, globalAnnotation,
      saSignature, vaSignature, globalSignature, wasSplit)


    val df = sqlContext.read.parquet(dirname + "/rdd.parquet")

    val vaRequiresConversion = vaSignature.requiresConversion

    if (skipGenotypes)
      new VariantSampleMatrix2(
        metadata.copy(sampleIds = IndexedSeq.empty[String],
          sampleAnnotations = IndexedSeq.empty[Annotation]),
        df.select("variant", "annotations"))
    else
      new VariantSampleMatrix2(metadata, df)
  }
}

class VariantSampleMatrix2(val metadata: VariantMetadata, val df: DataFrame) {
  def sampleIds: IndexedSeq[String] = metadata.sampleIds

  //lazy val sampleIdsBc = sparkContext.broadcast(sampleIds)

  def nSamples: Int = metadata.sampleIds.length

  def vaSignature: Type = metadata.vaSignature

  def saSignature: Type = metadata.saSignature

  def globalSignature: Type = metadata.globalSignature

  def globalAnnotation: Annotation = metadata.globalAnnotation

  def sampleAnnotations: IndexedSeq[Annotation] = metadata.sampleAnnotations

  def sampleIdsAndAnnotations: IndexedSeq[(String, Annotation)] = sampleIds.zip(sampleAnnotations)

  //lazy val sampleAnnotationsBc = sparkContext.broadcast(sampleAnnotations)

  def wasSplit: Boolean = metadata.wasSplit

}

class RichVSM2(vsm2: VariantSampleMatrix2) {
  def makeSchema(): StructType =
    StructType(Array(
      StructField("variant", Variant.schema, nullable = false),
      StructField("annotations", vsm2.vaSignature.schema, nullable = false),
      StructField("gs", GenotypeStream.schema, nullable = false)
    ))

  def write(sqlContext: SQLContext, sparkContext: SparkContext, dirname: String, compress: Boolean = true) {
    if (!dirname.endsWith(".vds") && !dirname.endsWith(".vds/"))
      fatal(s"output path ending in `.vds' required, found `$dirname'")

    val hConf = sparkContext.hadoopConfiguration
    hadoopMkdir(dirname, hConf)

    val sb = new StringBuilder
    writeTextFile(dirname + "/sa.schema", hConf) { out =>
      vsm2.saSignature.pretty(sb, 0, printAttrs = true)
      out.write(sb.result())
    }

    sb.clear()
    writeTextFile(dirname + "/va.schema", hConf) { out =>
      vsm2.vaSignature.pretty(sb, 0, printAttrs = true)
      out.write(sb.result())
    }

    sb.clear()
    writeTextFile(dirname + "/global.schema", hConf) { out =>
      vsm2.globalSignature.pretty(sb, 0, printAttrs = true)
      out.write(sb.result())
    }

    writeDataFile(dirname + "/metadata.ser", hConf) {
      dos => {
        val serializer = SparkEnv.get.serializer.newInstance()
        val ss = serializer.serializeStream(dos)
        ss
          .writeObject(VariantSampleMatrix.magicNumber)
          .writeObject(VariantSampleMatrix.fileVersion)
          .writeObject(vsm2.sampleIds)
          .writeObject(vsm2.sampleAnnotations)
          .writeObject(vsm2.globalAnnotation)
          .writeObject(vsm2.wasSplit)
        ss.close()
      }
    }

    val vaSignature = vsm2.vaSignature
    val vaRequiresConversion = vaSignature.requiresConversion


    vsm2.df.write.parquet(dirname + "/rdd.parquet")

    //    println(makeSchema())
    //    val rowRDD = vds.rdd
    //      .map {
    //        case (v, va, gs) =>
    //          val r = Row.fromSeq(Array(v.toRow,
    //            if (vaRequiresConversion) vaSignature.makeSparkWritable(va) else va,
    //            gs.toGenotypeStream(v, compress).toRow))
    //          //          println(r.getAs[Row](2).toSeq.map(_.asInstanceOf[Array[_]].take(3): IndexedSeq[Any]))
    //          r
    //      }
    //    sqlContext.createDataFrame(rowRDD, makeSchema())
    //      .write.parquet(dirname + "/rdd.parquet")
    // .saveAsParquetFile(dirname + "/rdd.parquet")
  }
}