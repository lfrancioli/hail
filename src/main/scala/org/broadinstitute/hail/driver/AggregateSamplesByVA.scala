package org.broadinstitute.hail.driver

import java.nio.ByteBuffer

import org.apache.spark.SparkEnv
import org.broadinstitute.hail.RichRDD
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.io.annotators.IntervalListAnnotator
import org.broadinstitute.hail.methods._
import org.broadinstitute.hail.utils.{Interval, MultiArray2}
import org.broadinstitute.hail.variant._
import org.kohsuke.args4j.{Option => Args4jOption}

import scala.collection.mutable

object AggregateSamplesByVA extends Command {

  class Options extends BaseOptions {

    @Args4jOption(required = true, name = "-o", aliases = Array("--output"),
      usage = "path of output file")
    var output: String = _

    @Args4jOption(required = true, name = "-c", aliases = Array("--condition"),
      usage = ".columns file, or comma-separated list of fields/computations")
    var condition: String = _

    @Args4jOption(required = true, name = "--va", usage = "path to variant annotation to aggregate on starting with `va`")
    var aggAnn: String = _

    @Args4jOption(required = true, name = "--as-matrix", usage = "When using this option, only a single condition can " +
      "be passed. If set, the output is a matrix of variants x samples with each cell containing the value of the condition.")
    var asMatrix: Boolean = false

  }

  def newOptions = new Options

  def name = "aggregatesamplesbyVA"

  def description = "Aggregate and export samples information over variant annnotation"

  def supportsMultiallelic = true

  def requiresVDS = true

  def run(state: State, options: Options): State = {
    val vds = state.vds
    val sc = vds.sparkContext
    val cond = options.condition
    val output = options.output
    val vas = vds.vaSignature
    val sas = vds.saSignature
    val aggAnn = vds.queryVA(options.aggAnn)._2


    val aggregationEC = EvalContext(Map(
      "v" -> (0, TVariant),
      "va" -> (1, vds.vaSignature),
      "s" -> (2, TSample),
      "sa" -> (3, vds.saSignature),
      "global" -> (4, vds.globalSignature)))

    val symTab = Map(
      "s" -> (0, TSample),
      "sa" -> (1, vds.saSignature),
      "global" -> (2, vds.globalSignature),
      "gs" -> (-1, BaseAggregable(aggregationEC, TGenotype)))

    val ec = EvalContext(symTab)
    ec.set(2, vds.globalAnnotation)
    aggregationEC.set(4, vds.globalAnnotation)

    val (header, parseResults) = if (cond.endsWith(".columns")) {
      Parser.parseColumnsFile(ec, cond, vds.sparkContext.hadoopConfiguration)
    } else {
      val ret = Parser.parseNamedArgs(cond, ec)
      (ret.map(_._1), ret.map(x => (x._2, x._3)))
    }

    if (header.isEmpty)
      fatal("this module requires one or more named expr arguments")

    //val (zVals, seqOp, combOp, resultOp) = Aggregators.makeFunctions(aggregationEC)

  //  val zvf: () => Array[Any] = () => zVals.indices.map(zVals).toArray

    val aggregators = ec.aggregationFunctions.toArray
    val aggregatorA = ec.a

    val localSamplesBc = vds.sampleIdsBc
    val localAnnotationsBc = vds.sampleAnnotationsBc

    val nAggregations = aggregators.length
    val nSamples = vds.nSamples



    val mapOp :  (Variant, Annotation) => Annotation =  {case (v,va) => aggAnn(va)}
    val seqOp : (MultiArray2[Any], Int, Genotype) => MultiArray2[Any] = {
      case (arr, i, g) =>
        for (j <- 0 until nAggregations) {
          arr.update(i, j, aggregators(j).seqOp(g, arr(i, j)))
        }
        arr
    }
    val combOp : (MultiArray2[Any],MultiArray2[Any]) => MultiArray2[Any] = {
      case (arr1, arr2) =>
        for (i <- 0 until nSamples; j <- 0 until nAggregations) {
          arr1.update(i, j, aggregators(j).combOp(arr1(i, j), arr2(i, j)))
        }
        arr1
    }

    val res = vds.rdd
      .map { case (v, (va, gs)) =>

          val baseArray = MultiArray2.fill[Any](nSamples, nAggregations)(null)
          for (i <- 0 until nSamples; j <- 0 until nAggregations) {
            baseArray.update(i, j, aggregators(j).zero)
          }

          aggregatorA(0) = v
          aggregatorA(1) = va

          (mapOp(v,va), gs.iterator.zipWithIndex.map {
            case (g, i) => (localSamplesBc.value(i), localAnnotationsBc.value(i),i, g) }
            .foldLeft(baseArray) { case (acc, (s, sa, i, g)) =>
              aggregatorA(2) = s
              aggregatorA(3) = sa
              seqOp(acc, i, g)
            })
      }
      .reduceByKey(combOp)

    new RichRDD(res.map({
      case(ann, values) =>
        val annStr = ann.toString
        values.rows.zipWithIndex.foldLeft("")({
          case(str, (row,i)) => str + "\n" +
            row.foldLeft(s"$annStr\t${localSamplesBc.value(i)}")({
              case(s, v) => s + "\t" + v.toString()
            })
        })
    })).writeTable(options.output, header = Some(header.foldLeft(s"${options.aggAnn}\tSample")((str, col) => s"$str\t$col")))


    state
  }
}
