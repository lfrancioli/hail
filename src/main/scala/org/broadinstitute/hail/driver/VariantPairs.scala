package org.broadinstitute.hail.driver

import breeze.linalg.DenseVector
import com.google.common.collect.MultimapBuilder.ListMultimapBuilder
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel
import org.broadinstitute.hail.RichRDD
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.expr.BaseType
import org.broadinstitute.hail.methods.{GeneBurden, Phasing}
import org.broadinstitute.hail.variant.{Genotype, Variant, VariantDataset, VariantSampleMatrix}
import org.kohsuke.args4j.{Option => Args4jOption}
import org.broadinstitute.hail.utils.{SparseVariantSampleMatrix, SparseVariantSampleMatrixRRDBuilder}
import org.broadinstitute.hail.Utils._

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ArrayBuilder, ListBuffer}


/**
  * Created by laurent on 4/8/16.
  */
object VariantPairs extends Command {

  def name = "variantPairs"

  def description = "For each variant, separate samples in that gene and count the number of variants in category 2. Category 1 and 2 have to be mutually exclusive"

  def supportsMultiallelic = false

  def requiresVDS = true

  class Options extends BaseOptions {
    @Args4jOption(required = true, name = "-a", aliases = Array("--gene_annotation"), usage = "Annotation storing the gene information for aggregation")
    var gene_annotation: String = _
    @Args4jOption(required = true, name = "-o", aliases = Array("--output"), usage = "Output filename")
    var output: String = _
    @Args4jOption(required = false, name = "-vaStrat", aliases = Array("--va_stratification"), usage = "Stratify results based on variant annotations. Comma-separated list of annotations.")
    var vaStrat: String = ""
    @Args4jOption(required = false, name = "-cat2", aliases = Array("--category2"), usage = "Boolean variant annotation for cat2")
    var cat2: String = ""
    //@Args4jOption(required = false, name = "-saStrat", aliases = Array("--sa_stratification"), usage = "Stratify results based on sample annotations. Comma-separated list of annotations.")
    //var saStrat: String = ""
    @Args4jOption(required = false, name = "-p", aliases = Array("--partitions_number"), usage = "Number of partitions to use for gene aggregation.")
    var number_partitions: Int = 1000
  }

  def newOptions = new Options

  object VariantPairs {

    def getHeader(variantAnnotations: Array[String]) : String = {
      (Array("CHROM","POS","REF","ALT") ++
        variantAnnotations ++
        Array("nCarriers", "nNonCarriers", "sumCat2Carriers","sumCat2NonCarriers")).mkString("\t")
    }

  }

  class VariantPairs(svsm: SparseVariantSampleMatrix, val vaStrat: Array[String], val cat2: String){

    private val cat2Querier = svsm.queryVA(cat2)

    private val nCat2BySample = Array.ofDim[Int](svsm.nSamples)

    private val variantsCounts = ArrayBuffer[VariantCounts]()

    private class VariantCounts(val annotations : Array[String]) {

      var nCarriers = 0
      var nNonCarriers = 0
      var sumCarriers = 0
      var sumNonCarriers = 0

    }

    //Get annotation queriers
    var vaQueriers = Array.ofDim[(BaseType,Querier)](vaStrat.size)
    vaStrat.indices.foreach({ i =>
      vaQueriers(i) = svsm.queryVA(vaStrat(i))
    })

    //Count number of cat2 by sample
    svsm.sampleIDs.indices.foreach({
      i =>   svsm.getSampleAsList(i).foreach({
          case (v, g) =>
            if(g.isHet || g.isHomVar) {
              cat2Querier._2(svsm.variantsAnnotations(svsm.variantsIndex(v))) match {
                case Some(ann) => if( ann.asInstanceOf[Boolean] ) {nCat2BySample(i) += 1}
                case None =>
            }
        }
      })
    })

    //Go through each of the variants of interest and get the number of missense by carrier / nonCarrier
    svsm.variants.indices.foreach({
      i =>
        cat2Querier._2(svsm.variantsAnnotations(i)) match {
        case Some(ann) =>
          if(!ann.asInstanceOf[Boolean]) {
            val nonHomRefSamples = new mutable.HashSet[Int]()
            val counts = new VariantCounts(svsm.variants(i).split(":") ++ vaQueriers.map({ q => q._2(svsm.variantsAnnotations(i)).getOrElse("NA").toString }))
            svsm.getVariant(i).foreach({
              case (s, g) =>
                val si = svsm.sampleIDs.indexOf(s)
                if (g.isHet || g.isHomVar) {
                  counts.nCarriers += 1
                  counts.sumCarriers += nCat2BySample(si)
                }
                //Should not happen but safer to keep
                else if (g.isHomRef) {
                  counts.nNonCarriers += 1
                  counts.sumNonCarriers += nCat2BySample(si)
                }
                nonHomRefSamples += si
            })
            //Add all hom ref samples that weren't accounted for
            svsm.sampleIDs.indices.filter({
              si => !nonHomRefSamples.contains(si)
            }).foreach({
              si =>
                counts.nNonCarriers += 1
                counts.sumNonCarriers += nCat2BySample(si)
            })
            variantsCounts += counts
          }
        case None =>
      }
    })


    //Methods
    def toString(group_name : String) : String = {
      variantsCounts.map(counts => counts.annotations.mkString("\t") + "\t%d\t%d\t%d\t%d".format(counts.nCarriers,counts.nNonCarriers, counts.sumCarriers, counts.sumNonCarriers)).mkString("\n")
    }


  }


  def run(state: State, options: Options): State = {

    //Get sample and variant stratifications
    //val saStrats = state.sc.broadcast(if(!options.saStrat.isEmpty()) options.saStrat.split(",") else Array[String]())
    val vaStrats = state.sc.broadcast(if(!options.vaStrat.isEmpty()) options.vaStrat.split(",") else Array[String]())

    val partitioner = new HashPartitioner(options.number_partitions)

    //Get annotations
    val geneAnn = state.vds.queryVA(options.gene_annotation)._2

    val cat2 = state.sc.broadcast(options.cat2)

    info("Computing gene burden")

    val gb = SparseVariantSampleMatrixRRDBuilder.buildByVAstoreVA(
      vsm = state.vds,
      sc = state.sc,
      partitioner = partitioner,
      variantAnnotations = vaStrats.value ++ Array(cat2.value)
    )({case (v,va) => geneAnn(va).get.toString}).mapValues(
      {case svsm => new VariantPairs(svsm, vaStrats.value, cat2.value)}
    ).persist(StorageLevel.MEMORY_AND_DISK)

    info("Writing out results")

    //Write out pair variants stats
    new RichRDD(gb.map(
      {case (gene,vpr) => vpr.toString(gene)}
    )).writeTable(options.output,
      Some("gene\t" + VariantPairs.getHeader(vaStrats.value)))

    state
  }
}
