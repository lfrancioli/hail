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
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


/**
  * Created by laurent on 4/8/16.
  */
object LinkedVariantPairs extends Command {

  def name = "linkedPairs"

  def description = "For each pair of variants in a gene found in at least one sample, returns the inferred counts of AB Ab aB and ab haplotypes."

  def supportsMultiallelic = false

  def requiresVDS = true

  class Options extends BaseOptions {
    @Args4jOption(required = true, name = "-a", aliases = Array("--gene_annotation"), usage = "Annotation storing the gene information for aggregation")
    var gene_annotation: String = _
    @Args4jOption(required = true, name = "-o", aliases = Array("--output"), usage = "Output filename")
    var output: String = _
    @Args4jOption(required = false, name = "-vaStrat", aliases = Array("--va_stratification"), usage = "Stratify results based on variant annotations. Comma-separated list of annotations.")
    var vaStrat: String = ""
    //@Args4jOption(required = false, name = "-saStrat", aliases = Array("--sa_stratification"), usage = "Stratify results based on sample annotations. Comma-separated list of annotations.")
    //var saStrat: String = ""
    @Args4jOption(required = false, name = "-p", aliases = Array("--partitions_number"), usage = "Number of partitions to use for gene aggregation.")
    var number_partitions: Int = 1000
  }

  def newOptions = new Options

  object LinkedVariantPairResult {

    def getHeader(variantAnnotations: Array[String]) : String = {
      (Array("CHROM1","POS1","REF1","ALT1","CHROM2","POS2","REF2","ALT2") ++
        variantAnnotations.map(x => x +"1") ++
        variantAnnotations.map(x => x +"2") ++
      Array("nAB","nAb","naB","nab")).mkString("\t")
    }

  }

  class LinkedVariantPairResult(svsm: SparseVariantSampleMatrix, val vaStrat: Array[String]){

    //Stores all variants that are already done (v1,v2,annotations)
    private val alreadyDone = mutable.HashSet[(String,String,String)]()
    //Caches variant annotations
    private val vaCache = mutable.Map[String,Array[String]]()

    //Get annotation queriers
    var vaQueriers = Array.ofDim[(BaseType,Querier)](vaStrat.size)
    vaStrat.indices.foreach({ i =>
      vaQueriers(i) = svsm.queryVA(vaStrat(i))
    })

    //Compute burden
    val variantPairHaplotypes = computePairs()


    //Simple Result class
    class VariantPairHaplotypes(val v1: String, val v2 : String, val va1: Array[String], val va2: Array[String], val haplotypes: Option[DenseVector[Double]]){

      override def toString() : String ={
        val haplotypeCountStr = haplotypes match{
          case Some(hapCounts) => hapCounts.foldLeft("")({case(str,count) => str + "\t" + count.round})
          case None => "NA\tNA\tNA\tNA"
        }

        (v1.split(":") ++
          v2.split(":") ++
        va1 ++
        va2).mkString("\t") + haplotypeCountStr
      }
    }

    //Methods
    def toString(group_name : String) : String = {
      variantPairHaplotypes.map(hap => group_name + "\t" + hap.toString()).mkString("\n")
    }

    private def getVAStrats(variant : String) :  Array[String] = {
      vaCache.get(variant) match {
        case Some(ann) => ann
        case None =>
          val va =vaQueriers.map({querier =>
            svsm.getVariantAnnotation(variant,querier._2).getOrElse("NA").toString()
          })
          //addVAValues(va)
          vaCache(variant) = va
          va
      }
    }

    private def computePairs() : List[VariantPairHaplotypes] = {

      var variantPairHaplotypes = List.newBuilder[VariantPairHaplotypes]

      svsm.getExistingVariantPairs().foreach({
        case(v1,v2) => variantPairHaplotypes += new VariantPairHaplotypes(v1, v2,getVAStrats(v1), getVAStrats(v2),
          Phasing.phaseVariantPairWithEM(svsm.getGenotypeCounts(v1,v2)))
      })

      variantPairHaplotypes.result()
    }

  }


  def run(state: State, options: Options): State = {

    //Get sample and variant stratifications
    //val saStrats = state.sc.broadcast(if(!options.saStrat.isEmpty()) options.saStrat.split(",") else Array[String]())
    val vaStrats = state.sc.broadcast(if(!options.vaStrat.isEmpty()) options.vaStrat.split(",") else Array[String]())

    /**
    //Get annotation queriers
    val saQueriers = for (strat <- saStrats.value) yield {
      state.vds.querySA(strat)
    }

    //Group samples by stratification
    val samplesByStrat = state.sc.broadcast(for (i <- state.vds.sampleIds.indices) yield {
      saQueriers.map({case(basetype,querier) => querier(state.vds.sampleAnnotations(i))}).mkString("\t")
    })

    //Unique sample strats
    val uniqueSaStrats = state.sc.broadcast(samplesByStrat.value.toSet)**/

    //Filter variants that have a MAF higher than what we're looking for in ALL stratifications
    /**val maxAF = state.sc.broadcast(if(options.mraf > options.mdaf) options.mraf else options.mdaf)

    def rareVariantsFilter = {(v: Variant, va: Annotation, gs: Iterable[Genotype]) =>

      //Get AN and AC for each of the strats
      val an = uniqueSaStrats.value.foldLeft(mutable.Map[String,Int]()){case(m, strat) => m.update(strat,0); m}
      val ac = uniqueSaStrats.value.foldLeft(mutable.Map[String,Int]()){case(m, strat) => m.update(strat,0); m}

      gs.zipWithIndex.foreach({ case (genotype, i) =>
        if (genotype.isHomRef) {
          an(samplesByStrat.value(i)) += 2
        }
        else if(genotype.isHet){
          an(samplesByStrat.value(i)) += 2
          ac(samplesByStrat.value(i)) += 1
        }
        else if(genotype.isHomVar){
          an(samplesByStrat.value(i)) += 2
          ac(samplesByStrat.value(i)) += 2
        }
      })

      //Check that the max AF isn't above the threshold
      uniqueSaStrats.value.foldLeft(0) {
        (ag, st) =>
          val af = if (an(st) > 0) ac(st) / an(st) else 0
          if (af > ag) af else ag
      } < maxAF.value

    }**/

    val partitioner = new HashPartitioner(options.number_partitions)

    //Get annotations
    val geneAnn = state.vds.queryVA(options.gene_annotation)._2

    info("Computing gene burden")

    val gb = SparseVariantSampleMatrixRRDBuilder.buildByAnnotation(
      vsm = state.vds,
      sc = state.sc,
      partitioner = partitioner,
      variantAnnotations = vaStrats.value
    )({case (v,va) => geneAnn(va).get.toString}).mapValues(
      {case svsm => new LinkedVariantPairResult(svsm, vaStrats.value)}
    ).persist(StorageLevel.MEMORY_AND_DISK)

    info("Writing out results")

    //Write out pair variants stats
    new RichRDD(gb.map(
      {case (gene,vpr) => vpr.toString(gene)}
    )).writeTable(options.output,
      Some("gene\t" + LinkedVariantPairResult.getHeader(vaStrats.value)))

    state
  }
}
