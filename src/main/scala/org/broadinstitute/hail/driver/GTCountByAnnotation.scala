package org.broadinstitute.hail.driver

import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.utils.MultiArray2
import org.broadinstitute.hail.variant._
import org.kohsuke.args4j.{Argument, Option => Args4jOption}
import scala.collection.JavaConverters._

import scala.collection.immutable.Map

/**
  * Created by laurent on 4/11/16.
  */
object GTCountByAnnotation extends Command {

  def name = "GTcountbyannotation"

  def description = "Count number of alternate alleles per sample per given annotation. At the moment, the binning is naive => each value gets a bin!"

  override def supportsMultiallelic = true

  class Options extends BaseOptions {
    @Args4jOption(required = true, name = "-o", aliases = Array("--output"), usage = "Output filename")
    var output: String = _

    @Args4jOption(required = true, name = "-a", aliases = Array("--annotation"), usage = "annotations ... ")
    var annotations: Array[String] = _

    //@Argument(required = true, name = "-a", aliases = Array("--annotation"), usage = "<annotations...>")
    //var annotations: java.util.ArrayList[String] = _
  }
  def newOptions = new Options

 /** def getAnnotationType(vds: VariantDataset, annotation: String) : Type = vds.metadata.vaSignature.getOption(annotation) match {
   * case None => fatal("annotation" + annotation + "not found in input VDS.")
   * case Some(s) =>
   * }**/

  object GTCounter{

   val header = "HomRef\tHet\tHomVar\tMissing"

 }


 class GTCounter extends Serializable{

   var nRef = 0;
   var nHet = 0;
   var nVar = 0;
   var nMissing = 0;

  def merge(gc: GTCounter) : GTCounter = {
    nRef += gc.nRef
    nHet += gc.nHet
    nVar += gc.nVar
    nMissing += gc.nMissing
    this
  }
   //def seqOpU, Variant, Annotation, String, Annotation, T) => U,

   def addGenotype(g: Genotype) : GTCounter ={

     if(g.isHomRef){nRef += 1}
     else if(g.isHet){nHet += 1}
     else if(g.isHomVar){nVar += 1}
     else{nMissing +=1}

     this
   }

   def toLine() : String = {
     nRef + "\t" + nHet + "\t" + nVar + "\t" + nMissing
   }

 }

  def run(state: State, options: Options): State = {
    val vds = state.vds


    val annList = options.annotations.map(a => vds.queryVA(a)._2).toSeq


    val annAgg = vds.aggregateByAnnotation(new GTCounter())({
      case(counter,v,va,s,sa,i,g) =>
        counter.addGenotype(g)},{
      case(c1,c2) => c1.merge(c2)},
      {case (v,va) =>
        annList.map(a => a(va))
        }
    ).collect()

    val outLines = annAgg.map{case (ann,gc) =>
        ann.map{case a => a.getOrElse("NA").toString()}.reduce[String]{case (a1,a2) => a1+"\t"+a2} + "\t" + gc.toLine()
    }

    val annHeader = options.annotations.reduce((a1,a2) => a1 + "\t" + a2)


    writeTable(options.output,state.sc.hadoopConfiguration, outLines, Some(annHeader+"\t"+GTCounter.header))

   state
  }

}
