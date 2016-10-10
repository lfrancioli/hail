package org.broadinstitute.hail.driver

import org.broadinstitute.hail.utils._
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.expr.{EvalContext, Field, Parser, TAggregable, TBoolean, TGenotype, TSample, TStruct, TVariant, Type}
import org.broadinstitute.hail.methods.{Aggregators, Filter}
import org.broadinstitute.hail.variant.{Genotype, Variant, VariantDataset, VariantSampleMatrix}
import org.kohsuke.args4j.{Option => Args4jOption}


object Join extends Command {

  class Options extends BaseOptions {
    @Args4jOption(required = false, name = "-i", aliases = Array("--input"),
      usage = "VDS to join. If --sa_root is not specified, the sample annotations will be merged under the assumption that " +
        "annotations with the same name are the same in both VDSs.")
    var input: String = _
    @Args4jOption(required = true, name = "--va_root",
      usage = "Period-delimited path starting with `va'")
    var vr: String = _
    @Args4jOption(required = true, name = "--ga_root",
      usage = "Period-delimited path starting with `ga'")
    var gr: String = _
    @Args4jOption(required = false, name = "--drop-sa",
      usage = "Removes SA")
    var dropSA: Boolean = false
    @Args4jOption(required = false, name = "--sa_root",
      usage = "Period-delimited path starting with `sa'")
    var sr: String = "sa"
    @Args4jOption(required = false, name = "--sa_suffix",
      usage = "Suffix to add to samples IDs that are already present in the current VDS.")
    var suffix: String = "_2"


  }

  def newOptions = new Options

  def name = "join"

  def description = "Inner join two VDSs"

  def supportsMultiallelic = true

  def requiresVDS = true

  def run(state: State, options: Options): State = {

    def getSplitVDSs(vds: VariantDataset, otherVDS: VariantDataset) : (VariantDataset,VariantDataset) = {
      if (vds.wasSplit == otherVDS.wasSplit)
        (vds, otherVDS)
      else if (vds.wasSplit && !otherVDS.wasSplit)
        (vds, SplitMulti.run(state.copy(state.sc, state.sqlContext, otherVDS)).vds)
      else
        fatal("Cannot join a multi-allelic VDS with a VDS where multi-allelic sites were split. Please run splitmulti before joining.")
    }

    val (vds,otherVDS) = getSplitVDSs(state.vds, VariantSampleMatrix.read(state.sqlContext, options.input))

    val (newVASignature, vaInserter) = vds.insertVA(otherVDS.vaSignature, Parser.parseAnnotationRoot(options.vr,"va"))
    val (newGASignature, gaInserter) = vds.insertGlobal(otherVDS.globalSignature, Parser.parseAnnotationRoot(options.gr,"ga"))


    def mergeSA(signature: Type, otherSignature: Type, sa: IndexedSeq[Annotation]) : (Type, IndexedSeq[Annotation]) = {

      val currentFields = signature.asInstanceOf[TStruct].fields.map(f => f.name -> f).toMap

     otherSignature.asInstanceOf[TStruct].fields
        .filter(f =>
          if(currentFields.contains(f.name)){
          if(currentFields(f.name).`type` != f.`type`) fatal(s"Sample annotation ${f.name} was found in both VDSs with different types and cannot be merged.")
           false
        }
          else true)
       .foldLeft((signature,sa))({
         case ((s, sa), f) =>
          val inserter = signature.insert(f.`type`, f.name)
         (inserter._1, sa.map(a => inserter._2(a,None)) )
       })
    }

    val sampleSet = vds.sampleIds.toSet
    def getNewSampleName(s: String, suffix: String) : String = {
      if(sampleSet.contains(s)) getNewSampleName(s + suffix, suffix)
      else s
    }
    val newSamplesIDs = vds.sampleIds ++ otherVDS.sampleIds.map(s => getNewSampleName(s,options.suffix))

    def getNewSA(): (Type, IndexedSeq[Annotation]) = {
      if(options.dropSA){
        (TStruct.empty, Array.fill(vds.sampleAnnotations.length + otherVDS.sampleAnnotations.length)(Annotation.empty))
      } else {
        //TODO FIXME This doesn't work
        val sa1 = mergeSA(vds.saSignature.asInstanceOf[TStruct], otherVDS.saSignature.asInstanceOf[TStruct], vds.sampleAnnotations)
        val sa2 = mergeSA(otherVDS.saSignature.asInstanceOf[TStruct], vds.saSignature.asInstanceOf[TStruct], otherVDS.sampleAnnotations)
        (sa1._1, sa1._2 ++ sa2._2)
      }
    }

    val (newSaSignature, newSA) = getNewSA()


    val newRDD = vds.rdd.orderedInnerJoinDistinct(
      otherVDS.rdd).mapValues({
      case((va1,gt1),(va2,gt2)) =>
        (vaInserter(va1, Some(va2)), gt1 ++ gt2)
    }
    ).asOrderedRDD


    state.copy(vds = vds.copy(
      rdd = newRDD,
      vaSignature = newVASignature,
      saSignature = newSaSignature,
      globalSignature = newGASignature,
      sampleIds = newSamplesIDs,
      sampleAnnotations = newSA,
      globalAnnotation = gaInserter(state.vds.globalAnnotation,Some(otherVDS.globalAnnotation))
    ))
  }
}
