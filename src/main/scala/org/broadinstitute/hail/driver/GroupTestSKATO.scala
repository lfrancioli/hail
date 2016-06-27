package org.broadinstitute.hail.driver

import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.annotations.Annotation
import org.json4s._
import org.kohsuke.args4j.{Option => Args4jOption}
import org.broadinstitute.hail.Utils._
import org.json4s.jackson.JsonMethods._
import java.io.{FileInputStream, IOException}
import java.util.Properties
import scala.collection.JavaConverters._

object GroupTestSKATO extends Command {

  class Options extends BaseOptions {
    @Args4jOption(name = "--block-size", usage = "Groups per SKAT-O invocation")
    var blockSize = 1000

    @Args4jOption(required = true, name = "--config", usage = "SKAT-O configuration file")
    var config: String = _

    @Args4jOption(required = true, name = "-y", aliases = Array("--y"), usage = "Response sample annotation")
    var ySA: String = _

//    @Args4jOption(required = true, name = "-q", aliases = Array("--quantitative"), usage = "y is a quantitative phenotype")
//    var yType: Boolean = false

    @Args4jOption(required = true, name = "-o", aliases = Array("--output"), usage = "path of output tsv")
    var output: String = _

    @Args4jOption(required = false, name = "-c", aliases = Array("--covariates"), usage = "Covariate sample annotations, comma-separated")
    var covSA: String = ""

    @Args4jOption(required = true, name = "-k", aliases = Array("--groupkeys"),
      usage = "comma-separated list of annotations to be used as grouping variable(s) (must be attribute of va)")
    var groupKeys: String = _
  }

  def newOptions = new Options

  def name = "grouptest skato"

  def description = "Run SKAT-O on groups"

  def supportsMultiallelic = false

  def requiresVDS = true

  val skatoSignature = TArray(TStruct(
      "groupName" -> TString,
      "pValue" -> TDouble,
      "pValueNoAdj" -> TDouble,
      "nMarker" -> TInt,
      "nMarkerTest" -> TInt
  ))

  val header = "groupName\tpValue\tpValueNoAdj\tnMarker\tnMarkerTest"

  def run(state: State, options: Options): State = {
    val vds = state.vds
    val group = state.group

    val properties = try {
      val p = new Properties()
      val is = new FileInputStream(options.config)
      p.load(is)
      is.close()
      p
    } catch {
      case e: IOException =>
        fatal(s"could not open file: ${e.getMessage}")
    }

    val R = properties.getProperty("hail.skato.R", "/usr/bin/Rscript")
    val skatoScript = properties.getProperty("hail.skato.script", "src/test/resources/testSKATO.r")
    val kernel = properties.getProperty("hail.skato.kernel", "linear.weighted")
    val method = properties.getProperty("hail.skato.method", "davies")
    val weightsBeta = properties.getProperty("hail.skato.weightsBeta", "1,25")
    //val weights
    val imputeMethod = properties.getProperty("hail.skato.imputeMethod", "fixed")
    val rCorr = properties.getProperty("hail.skato.rCorr", "0")
    val checkGenotype = properties.getProperty("hail.skato.checkGenotype", "true")
    val isDosage = properties.getProperty("hail.skato.isDosage", "false")
    val missingCutoff = properties.getProperty("hail.skato.missingCutoff", "0.15")
    val estimateMAF = properties.getProperty("hail.skato.estimateMAF", "1")


//    if (group == null)
//      fatal("No group has been created. Use the `creategroup` command.")

    def toDouble(t: BaseType, code: String): Any => Double = t match {
      case TInt => _.asInstanceOf[Int].toDouble
      case TLong => _.asInstanceOf[Long].toDouble
      case TFloat => _.asInstanceOf[Float].toDouble
      case TDouble => _.asInstanceOf[Double]
      case TBoolean => _.asInstanceOf[Boolean].toDouble
      case _ => fatal(s"Sample annotation `$code' must be numeric or Boolean, got $t")
    }

    val queriers = options.groupKeys.split(",").map{vds.queryVA(_)._2}

    val symTab = Map(
      "s" -> (0, TSample),
      "sa" -> (1, vds.saSignature))

    val ec = EvalContext(symTab)
    val a = ec.a

    val (yT, yQ) = Parser.parse(options.ySA, ec)

    val yToDouble = toDouble(yT, options.ySA)
    val ySA = vds.sampleIdsAndAnnotations.map { case (s, sa) =>
      a(0) = s
      a(1) = sa
      yQ().map(yToDouble)
    }

    val (covT, covQ) = Parser.parseExprs(options.covSA, ec).unzip
    val covToDouble = (covT, options.covSA.split(",").map(_.trim)).zipped.map(toDouble)
    val covSA = vds.sampleIdsAndAnnotations.map { case (s, sa) =>
      a(0) = s
      a(1) = sa
      (covQ.map(_()), covToDouble).zipped.map(_.map(_))
    }

    val cmd = Array(R, skatoScript)

    val localBlockSize = options.blockSize

    val ySABC = state.sc.broadcast(ySA)

    def printContext(w: (String) => Unit) = {
      val y = pretty(JArray(ySABC.value.map{y => if (y.isDefined) JDouble(y.get) else JNull}.toList))
      val cov = JArray(covSA.map{cov => JArray(cov.map{c => if (c.isDefined) JDouble(c.get) else JNull}.toList)}.toList)
      w(s"""{"Y":$y, "groups":{""")
    }

    def printSep(w: (String) => Unit) = {
      w(",")
    }

    def printFooter(w: (String) => Unit) = {
      w("}}")
    }

    def printElement(w: (String) => Unit, g: (IndexedSeq[Any], Iterable[Iterable[Int]])) = {
      val a = JArray(g._2.map(a => JArray(a.map(JInt(_)).toList)).toList)
      w(s""""${g._1.map(_.toString).mkString(",")}":${pretty(a)}""")
    }

    val groups = vds.rdd.map{case (v, va, gs) =>
      (queriers.map{_(va).get}.toIndexedSeq, gs.map{g => g.nNonRefAlleles.getOrElse(9)})
    }.groupByKey().persist(StorageLevel.MEMORY_AND_DISK)

    groups.mapPartitions{ it =>
      val pb = new java.lang.ProcessBuilder(cmd.toList.asJava)

      it.grouped(localBlockSize)
        .flatMap{_.iterator
          .pipe(pb, printContext, printElement, printFooter, printSep)
          .map{result =>
            val a = Annotation.fromJson(parse(result), skatoSignature, "<root>")
            a.asInstanceOf[IndexedSeq[Any]].map(_.asInstanceOf[Row].mkString("\t")).mkString("\n")
          }
        }
    }.writeTable(options.output, Some(header), deleteTmpFiles = true)

    groups.unpersist()

    state
  }
}
