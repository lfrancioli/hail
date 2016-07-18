package org.broadinstitute.hail.driver

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
import scala.collection.mutable.ArrayBuffer


/**
  * Created by laurent on 4/8/16.
  */
object RareVariantsBurden extends Command {

    def name = "rareBurden"

    def description = "Computes per-gene counts of rare variations for dominant and recessive modes"

    def supportsMultiallelic = false

    def requiresVDS = true

    class Options extends BaseOptions {
      @Args4jOption(required = true, name = "-a", aliases = Array("--gene_annotation"), usage = "Annotation storing the gene information for aggregation")
      var gene_annotation: String = _
      @Args4jOption(required = true, name = "-o", aliases = Array("--output"), usage = "Output filename prefix")
      var output: String = _
      @Args4jOption(required = false, name = "-mraf", aliases = Array("--max-recessive-af"), usage = "Maximum allele frequency considered for recessive enrichment test")
      var mraf : Double = 0.01
      @Args4jOption(required = false, name = "-mdaf", aliases = Array("--max-dominant-af"), usage = "Maximum allele frequency considered for dominant enrichment test")
      var mdaf : Double = 0.001
      @Args4jOption(required = false, name = "-vaStrat", aliases = Array("--va_stratification"), usage = "Stratify results based on variant annotations. Comma-separated list of annotations.")
      var vaStrat: String = ""
      @Args4jOption(required = false, name = "-saStrat", aliases = Array("--sa_stratification"), usage = "Stratify results based on sample annotations. Comma-separated list of annotations.")
      var saStrat: String = ""
      @Args4jOption(required = false, name = "-p", aliases = Array("--partitions_number"), usage = "Number of partitions to use for gene aggregation.")
      var number_partitions: Int = 1000
    }

    def newOptions = new Options

  object GeneBurdenResult {

    def getSingleVariantHeaderString(variantAnnotations: Array[String], sampleAnnotations: Array[String]) : String = {
      (sampleAnnotations ++
        variantAnnotations ++
        Array("nHets","nHomVar")).mkString("\t")
    }

    def getVariantPairHeaderString(variantAnnotations: Array[String], sampleAnnotations: Array[String]) : String = {
      (sampleAnnotations ++
        variantAnnotations.map(ann => ann + "1") ++
        variantAnnotations.map(ann => ann + "2") ++
        Array("nCHets","nCHetsD")).mkString("\t")
    }

    /**private def zipVA(va1: Array[String],va2: Array[String]) : Array[String] = {
      * va1.zip(va2).map({case(a1,a2) => a1+"/"+a2})
      * }**/

  }

    class GeneBurdenResult(svsm: SparseVariantSampleMatrix, val vaStrat: Array[String], val saStrat: Array[String]){

      private val vaCache = mutable.Map[String,Array[String]]()
      private val phaseCache = mutable.Map[(String,String),Option[Double]]()

      //Get annotation queriers
      var vaQueriers = Array.ofDim[(BaseType,Querier)](vaStrat.size)
      vaStrat.indices.foreach({ i =>
        vaQueriers(i) = svsm.queryVA(vaStrat(i))
      })

      var saQueriers = Array.ofDim[(BaseType,Querier)](saStrat.size)
      saStrat.indices.foreach({ i =>
        saQueriers(i) = svsm.querySA(saStrat(i))
      })

      //Stores all values found for variant annotations
      //val vaValues = new Array[mutable.Set[String]](vaStrat.size)

      //Compute burden
      val results = new GBResults()

      //Loop through all samples
      svsm.sampleIDs.indices.foreach({ i =>

        //Get sample stratification
        val saStrat = saQueriers.map({querier =>
          svsm.getSampleAnnotation(i,querier._2).getOrElse("NA").toString()
        })
        //Compute and add sample results
        results.addSampleResults( computeIndividualBurden(svsm.getSampleAsList(i), new GBSampleResults(saStrat)) )

      })

      //Result classes
      class GBResults() {
        val singleVariantsStats = mutable.Map[String, (Int,Int)]()
        val variantPairsStats = mutable.Map[String, (Int,Double)]()

        def addSampleVariantResult(current: mutable.Map[String, Int], sample: mutable.Map[String, Int]) = {
          sample.foreach({ case(va,n) =>
            if(current.contains(va)){
              current(va) += 1
            }else{
              current(va) = 1
            }
          })
        }

        def addSampleResults(sRes : GBSampleResults) : GBResults = {
          //Add single-variant results
          sRes.singleVariantsStats.foreach({
            case(ann,(nHet,nHomVar)) =>
              singleVariantsStats.get(ann) match{
                case Some((het,hvar)) => singleVariantsStats(ann) = (het+nHet,hvar+nHomVar)
                case None => singleVariantsStats(ann) = (nHet,nHomVar)
              }
          })

          sRes.variantPairsStats.foreach({
            case(ann,(nCHet,nCHetD)) =>
              variantPairsStats.get(ann) match{
                case Some((chet,chetD)) => variantPairsStats(ann) = (chet+nCHet,chetD+nCHetD)
                case None => variantPairsStats(ann) = (nCHet,nCHetD)
              }
          })
          this
        }

      }

      private class GBSampleResults(val sampleStrat : Array[String]) extends GBResults {

        def addVariant(variant: String, gt : Genotype): GBSampleResults ={
          //Sanity check -- should be assert?
          if(gt.isHet || gt.isHomVar) {
            val strat = (sampleStrat ++ getVAStrats(variant)).mkString("\t")
            singleVariantsStats.get(strat) match {
              case Some((nHet, nHomVar)) => singleVariantsStats(strat) = if(gt.isHet) (nHet + 1, nHomVar) else (nHet, nHomVar+1)
              case None => singleVariantsStats(strat) = if(gt.isHet) (1, 0) else (0, 1)
            }
          }
          this
        }

        def addCompoundHets(v1: String, previousVariants: List[String]): GBSampleResults ={
          val v1Ann = getVAStrats(v1)
          val v1AnnStr = v1Ann.mkString("")

          previousVariants.foreach({v2 =>
            val pPhase = getPhase(v1,v2)

            //Create annotation String
            val v2Ann = getVAStrats(v2)
            val ann =
              if(v1AnnStr < v2Ann.mkString("")) (sampleStrat ++ v1Ann ++ v2Ann).mkString("\t")
              else (sampleStrat ++ v2Ann ++ v1Ann).mkString("\t")

            //If the variants are compound hets, then only keep most likely compound het (smallest pPhase) for
            //each stratification
            pPhase match {
              case Some(p) =>
                variantPairsStats.get(ann) match {
                  case Some((nOld, pOld)) => if (pOld > p) {
                    variantPairsStats(ann) = if(p > 0.5) (1,p) else (0,p)
                  }
                  case None => variantPairsStats(ann) = if(p > 0.5) (1,p) else (0,p)
                }
              case None =>
            }
          })
          this
        }
      }

      //Methods
      /**private def addVAValues(values : Array[String]) = {
        * values.indices.foreach(i =>
        * vaValues(i).add(values(i))
        * )
        * }*/

      def getSingleVariantsStats(group_name : String) : String = {

        results.singleVariantsStats.map({case (ann,(nHets,nHoms)) =>
          "%s%s\t%d\t%d".format(group_name,if(ann.isEmpty) "" else "\t"+ann,nHets,nHoms)
        }).mkString("\n")
      }

      def getVariantPairsStats(group_name : String) : String = {
        results.variantPairsStats.map({case (ann,(nCHets,nCHetsD)) =>
          "%s%s\t%d\t%.3f".format(group_name,if(ann.isEmpty) "" else "\t"+ann,nCHets,nCHetsD)
        }).mkString("\n")
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

      private def getPhase(v1: String, v2: String) : Option[Double] = {
        phaseCache.get((v1,v2)) match{
          case Some(p) => p
          case None =>
            val pPhase = Phasing.probOnSameHaplotypeWithEM(svsm.getGenotypeCounts(v1,v2))
            phaseCache.update((v1,v2),pPhase)
            pPhase
        }
      }

      private def computeIndividualBurden(variants: List[(String,Genotype)], res : GBSampleResults, previousHetVariants: List[String] = List[String]()): GBSampleResults = {

        variants match {
          case Nil => res
          case (v,gt)::vgts =>
            if(gt.isHet){
              computeIndividualBurden(vgts,res.addVariant(v,gt).addCompoundHets(v,previousHetVariants),v::previousHetVariants)
            }else{
              computeIndividualBurden(vgts,res.addVariant(v,gt),previousHetVariants)
            }
        }
      }

    }


    def run(state: State, options: Options): State = {

      //Get sample and variant stratifications
      val saStrats = if(!options.saStrat.isEmpty()) options.saStrat.split(",") else Array[String]()
      val vaStrats = if(!options.vaStrat.isEmpty()) options.vaStrat.split(",") else Array[String]()

      //Get annotation queriers
      val saQueriers = for (strat <- saStrats) yield {
        state.vds.querySA(strat)
      }

      //Group samples by stratification
      val samplesByStrat = state.sc.broadcast(for (i <- state.vds.sampleIds.indices) yield {
        saQueriers.map({case(basetype,querier) => querier(state.vds.sampleAnnotations(i))}).mkString("\t")
      })

      //Unique sample strats
      val uniqueSaStrats = state.sc.broadcast(samplesByStrat.value.toSet)

      //Filter variants that have a MAF higher than what we're looking for in ALL stratifications
      val maxAF = if(options.mraf > options.mdaf) options.mraf else options.mdaf
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
        } < maxAF

      }

      val partitioner = new HashPartitioner(options.number_partitions)

      //Get annotations
      val geneAnn = state.vds.queryVA(options.gene_annotation)._2

      info("Computing gene burden")

      val gb = SparseVariantSampleMatrixRRDBuilder.buildByVAstoreVAandSA(
        state.vds.filterVariants(rareVariantsFilter),
        state.sc,
        partitioner,
        vaStrats,
        saStrats
      )({case (v,va) => geneAnn(va).get.toString}).mapValues(
        {case svsm => new GeneBurdenResult(svsm, vaStrats, saStrats)}
      ).persist(StorageLevel.MEMORY_AND_DISK)

      info("Writing out results")

      //Write out single variant stats
      new RichRDD(gb.map(
        {case (gene,gr) => gr.getSingleVariantsStats(gene)}
      )).writeTable(options.output +".single.txt",
        Some("gene\t" + GeneBurdenResult.getSingleVariantHeaderString(vaStrats,saStrats)))

      //Write out pair variants stats
      new RichRDD(gb.map(
        {case (gene,gr) => gr.getVariantPairsStats(gene)}
      )).writeTable(options.output +".pair.txt",
        Some("gene\t" + GeneBurdenResult.getVariantPairHeaderString(vaStrats,saStrats)))

      state
    }
}
