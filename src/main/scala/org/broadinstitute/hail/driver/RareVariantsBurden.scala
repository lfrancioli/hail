package org.broadinstitute.hail.driver

import breeze.linalg.DenseVector
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel
import org.broadinstitute.hail.RichRDD
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.expr.{BaseType, Parser, TDouble, TStruct}
import org.broadinstitute.hail.methods.{GeneBurden, Phasing}
import org.broadinstitute.hail.variant.{Genotype, Variant, VariantDataset, VariantSampleMatrix}
import org.kohsuke.args4j.{Option => Args4jOption}
import org.broadinstitute.hail.utils.{SparseVariantSampleMatrix, SparseVariantSampleMatrixRRDBuilder}
import org.broadinstitute.hail.Utils._

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ArrayBuilder}


/**
  * Created by laurent on 4/8/16.
  */
object RareVariantsBurden extends Command {

    def name = "rareBurden"

    def description = "Computes per-gene counts of rare variations for dominant and recessive modes. " +
      "Recessive mode takes all MAF by default, so filtering should be done upstream or mPopRAF option used."

    def supportsMultiallelic = false

    def requiresVDS = true

    class Options extends BaseOptions {
      @Args4jOption(required = true, name = "--gene_annotation", usage = "Annotation storing the gene information for aggregation")
      var gene_annotation: String = _
      @Args4jOption(required = true, name = "--maf_annotation", usage = "Annotation storing the MAF [Double] information to use. " +
        "All sites will be used for the recessive test, only sites with MAF smaller than --mDAF will be considered " +
        "for the dominant test.")
      var maf_annotation: String = _
      @Args4jOption(required = true, name = "-o", aliases = Array("--output"), usage = "Output filename prefix")
      var output: String = _
      @Args4jOption(required = false, name = "-mDAF", aliases = Array("--max-dominant-af"), usage = "Maximum allele frequency considered for dominant enrichment test. Not used if negative")
      var mDAF : Double = -1.0
      @Args4jOption(required = false, name = "-vaStrat", aliases = Array("--va_stratification"), usage = "Stratify results based on variant annotations. Comma-separated list of annotations.")
      var vaStrat: String = ""
      @Args4jOption(required = false, name = "-saStrat", aliases = Array("--sa_stratification"), usage = "Stratify results based on sample annotations. Comma-separated list of annotations.")
      var saStrat: String = ""
      @Args4jOption(required = false, name = "--cpg", usage = "Annotation storing the CpG status information")
      var cpg_annotation: String = _
      @Args4jOption(required = false, name = "-p", aliases = Array("--partitions_number"), usage = "Number of partitions to use for gene aggregation.")
      var number_partitions: Int = 1000
      @Args4jOption(required = false, name = "--controls", usage = "Input control cohort .vds file for phasing only")
      var controls: String = _
      @Args4jOption(required = false, name = "--controls-gene_annotation",
        usage = "Annotation storing the gene information in controls for aggregation. By default uses the same " +
          "annotations specified with --gene-annotation")
      var controls_gene_annotation: String = _
      @Args4jOption(required = false, name = "--keep-missing-AF", usage = "If set, sites with missing AF annotations " +
        "are kept and considered singletons.")
      var keepMissing: Boolean = false

      //Hard-coded for now
      /**@Args4jOption  (required = false, name = "-p", aliases = Array("--partitions_number"),
        *               usage = "Prior for 2 singletons to be in a compound het state.")
        *               var cHet00Prior: Double = 0.15
        * @Args4jOption (required = false, name = "-p", aliases = Array("--partitions_number"),
        *               usage = "Prior for 1 singleton and a non-singleton site to be in a compound het state.")
        *               var cHet01Prior: Double = 0.4 **/
    }

    def newOptions = new Options

  object GeneBurdenResult {

    def getSingleVariantHeaderString(variantAnnotations: Array[String], sampleAnnotations: Array[String]) : String = {
      (sampleAnnotations ++
        variantAnnotations ++
        Array("nHets","nHomVar","nDomNonRef")).mkString("\t")
    }

    def getVariantPairHeaderString(variantAnnotations: Array[String], sampleAnnotations: Array[String]) : String = {
      (sampleAnnotations ++
        variantAnnotations.map(ann => ann + "1") ++
        variantAnnotations.map(ann => ann + "2") ++
        Array("nCompHets","totProbCompHets")).mkString("\t")
    }

    /**private def zipVA(va1: Array[String],va2: Array[String]) : Array[String] = {
      * va1.zip(va2).map({case(a1,a2) => a1+"/"+a2})
      * }**/

  }

    class GeneBurdenResult(val svsm: SparseVariantSampleMatrix, val vaStrat: Array[String], val saStrat: Array[String], val uniqueSaStrat: Array[String], val samplesStrat: IndexedSeq[Int], val mDAF: Double, val afAnn : String){

      //HARD-CODED Priors -- will need some work
      val cHet00Prior = 0.126
      val cHet01Prior = 0.474

      //Caches variant information
      private val vaCache = mutable.Map[Int,String]()
      private val phaseCache = mutable.Map[(String,String),Option[Double]]()

      //Stores results
      //Stores nHet, nHomVar, nDomVars
      private val singleVariantsCounts = mutable.Map[String,(Int,Int,Int)]()
      //Stores het sites for each sample
      private val hetSites = mutable.Map[Int,Set[Int]]()

      //Stores the number of coumpound hets
      private val compoundHetcounts = mutable.Map[String,(Int,Double)]()
      //private val compoundHetProbs = mutable.Map[String,Double]()

      //Get annotation queriers
      var vaQueriers = Array.ofDim[(BaseType,Querier)](vaStrat.size)
      vaStrat.indices.foreach({ i =>
        vaQueriers(i) = svsm.queryVA(vaStrat(i))
      })

      var saQueriers = Array.ofDim[(BaseType,Querier)](saStrat.size)
      saStrat.indices.foreach({ i =>
        saQueriers(i) = svsm.querySA(saStrat(i))
      })

      var afQuerier = svsm.queryVA(afAnn)

      //Compute the MAFs
      private val nSAStrats = uniqueSaStrat.size
      private val nSamplesPerStrat = samplesStrat.foldLeft(Array.fill(nSAStrats)(0))({(ag,s) => ag(s)+=1; ag})
      private val maf = svsm.variantsAnnotations.map({
        a => afQuerier._2(a)
          .orElse(Option(0.0)) //This should only happen if --keep-missing-AF options is set
            .get.asInstanceOf[Double]
      })
      //private val singletons =  mutable.Set[Int]()

      //Compute and record the per-sample counts
      computePerSampleCounts()

      def getHetSites() : Set[String] = {
        hetSites.values
          .foldLeft(mutable.Set[Int]())({ case (sites, newSites) => sites ++ newSites })
          .map(i => svsm.variants(i))
          .toSet
      }

      private def computePerSampleCounts() : Unit = {

        //Private class to store sample-level information
        class sampleVariants{
          //var homRefVars = mutable.HashSet[String]()
          var hetVars = mutable.HashSet[String]()
          var homVarVars = mutable.HashSet[String]()
          //var NoCallVars = mutable.HashSet[String]()
          var domVars = mutable.HashSet[String]()
        }

        //Creates the sets of het variants within a sample to compute compound hets later
        val s_hetVariantBuilder = mutable.Set[Int]()
        //s_hetVariantBuilder.sizeHint(svsm.variants.size+1) //TODO: Remove then updating to Scala 2.12+

        svsm.foreachSample({
          (s, si, variants, genotypes) =>
            //Get the sample stratification
            val saStrat = uniqueSaStrat(samplesStrat(si))

            //Save sampleCounts for this sample
            val sampleCounts = new sampleVariants()

            //Compute results for single variants
            s_hetVariantBuilder.clear()
            variants.indices.foreach({
              i =>
                val vi = variants(i)
                var addHet = 0
                var addHomVar = 0
                var addDomVar = 0
                val va = concatStrats(saStrat,getVAStrats(vi))

                genotypes(i) match{
                  //case -1 => sampleCounts.NoCallVars.add(concatStrats(saStrat,getVAStrats(vi)))
                  case 1 =>
                    if(sampleCounts.hetVars.add(va)){ addHet += 1 }
                    s_hetVariantBuilder += vi
                    if(maf(vi) < mDAF){
                      if(sampleCounts.domVars.add(va)){ addDomVar +=1 }
                    }
                  case 2 =>
                    if(sampleCounts.homVarVars.add(va)){ addHomVar += 1 }
                    if(maf(vi) < mDAF){
                      if(sampleCounts.domVars.add(va)){ addDomVar +=1 }
                    }
                  case _ =>
                }

                if(addHet + addHomVar + addDomVar > 0){
                  singleVariantsCounts.get(va) match {
                    case Some((nHets, nHomVars, nDomVars)) => singleVariantsCounts(va) = (nHets+addHet,nHomVars+addHomVar,nDomVars+addDomVar)
                    case None => singleVariantsCounts(va) = (addHet,addHomVar,addDomVar)
                  }
                }

            })

            //Store het sites for later phasing
            hetSites(si) = s_hetVariantBuilder.result().toSet

        })

      }

      private def concatStrats(strats: String*): String ={
        strats.filter({x => !x.isEmpty}).mkString("\t")
      }

      def getSingleVariantsStats(group_name : String) : String = {
        val str = singleVariantsCounts.map({case (ann,(nHets,nHoms,nDoms)) =>
          "%s%s\t%d\t%d\t%d".format(group_name,if(ann.isEmpty) "" else "\t"+ann,nHets,nHoms,nDoms)
        }).mkString("\n")
        if(str.isEmpty){
          group_name + "\t" + Array.fill(3 + saStrat.size + vaStrat.size)("NA").mkString("\t")
        }else{
          str
        }
      }

      def getVariantPairsStats(group_name : String) : String = {
        val str = compoundHetcounts.map({case (ann,(nCHets,pCHets)) =>
          "%s%s\t%d\t%.3f".format(group_name,if(ann.isEmpty) "" else "\t"+ann,nCHets,pCHets)
        }).mkString("\n")
        if(str.isEmpty){
          group_name + "\t" + Array.fill(saStrat.size + 2*vaStrat.size + 2)("NA").mkString("\t")
        }else{
          str
        }
      }

      private def getVAStrats(variantIndex : Int) :  String = {
        vaCache.get(variantIndex) match {
          case Some(ann) => ann
          case None =>
            val va =vaQueriers.map({querier =>
              svsm.getVariantAnnotation(variantIndex,querier._2).getOrElse("NA").toString()
            }).mkString("\t")
            //addVAValues(va)
            vaCache(variantIndex) = va
            va
        }
      }

      def phaseCompoundHets(pop : SparseVariantSampleMatrix) : GeneBurdenResult = {
        phaseCompoundHets(pop,
          v => !pop.variantsIndex.contains(v)
        )
      }

      def phaseCompoundHets() : GeneBurdenResult = {
        val singletons = svsm.getSingletons()
        phaseCompoundHets(svsm,
          v => singletons.contains(v))
      }

      private def phaseCompoundHets(pop : SparseVariantSampleMatrix, isSingleton: String => Boolean) : GeneBurdenResult = {
        hetSites.foreach({
          case (si, variants) =>

            val compHetVars = mutable.Set[String]()
            val compHetProbs = mutable.Map[String, Double]()
            val saStrat = uniqueSaStrat(samplesStrat(si)) //TODO Cache from above?

            val s_variants = variants.toArray //TODO clean this up as not efficient

            s_variants.indices.foreach({
              i1 =>
                val v1i = s_variants(i1)
                val v1 = svsm.variants(v1i)
                val va1 = getVAStrats(v1i)
                Range(i1 + 1, s_variants.size).foreach({
                  i2 =>
                    val v2i = s_variants(i2)
                    val v2 = svsm.variants(v2i)
                    val va2 = getVAStrats(v2i)
                    //Sort annotations before insertion to prevent duplicates
                    val compHetVA = if (va1 < va2) concatStrats(saStrat, va1, va2) else concatStrats(saStrat, va2, va1)

                    if (isSingleton(v1)) {
                      if (isSingleton(v2))
                        compHetProbs.withDefaultValue(0.0)(compHetVA) += cHet00Prior
                      else
                        compHetProbs.withDefaultValue(0.0)(compHetVA) += cHet01Prior
                    }
                    else if (isSingleton(v2))
                      compHetProbs.withDefaultValue(0.0)(compHetVA) += cHet01Prior
                    else {
                      getPhase(v1, v2, pop) match {
                        case Some(sameHap) =>
                          if (sameHap < 0.5) {
                            compHetVars.add(compHetVA)
                          }
                          compHetProbs.withDefaultValue(0.0)(compHetVA) += (1 - sameHap)
                        case None =>
                      }
                    }
                })
            })
            compHetProbs.foreach({
              case (strat, prob) =>
                val sampleTotProb = Math.min(prob, 1.0)
                val addCompHet = if (compHetVars.contains(strat)) 1 else 0
                compoundHetcounts.get(strat) match {
                  case Some(counts) =>
                    compoundHetcounts(strat) = (counts._1 + addCompHet, counts._2 + sampleTotProb)
                  case None =>
                    compoundHetcounts(strat) = (addCompHet, sampleTotProb)
                }
            })
        })

        this
      }

      private def getPhase(v1: String, v2: String, pop: SparseVariantSampleMatrix) : Option[Double] = {
        phaseCache.get((v1,v2)) match{
          case Some(p) => p
          case None =>
            val pPhase = Phasing.probOnSameHaplotypeWithEM(pop.getGenotypeCounts(v1,v2))
            phaseCache.update((v1,v2),pPhase)
            pPhase
        }
      }
    }


    def run(state: State, options: Options): State = {

      val afAnnotation = options.maf_annotation
      val afAnn = state.vds.queryVA(options.maf_annotation)

      //Check that MAF annotation is fine
      afAnn._1 match {
        case TDouble =>
        case _ => fatal("MAF annotation needs to be of type Double.")
      }

      val vds = if(options.keepMissing) state.vds else state.vds.filterVariants((v,va,gts) => afAnn._2(va).isDefined )

      //Get sample and variant stratifications
      val saStrats = if(!options.saStrat.isEmpty()) options.saStrat.split(",") else Array[String]()
      val vaStrats = if(!options.vaStrat.isEmpty()) options.vaStrat.split(",") else Array[String]()

      val partitioner = new HashPartitioner(options.number_partitions)

      //Get annotations
      val geneAnn = vds.queryVA(options.gene_annotation)._2

      //Get annotation queriers
      val saQueriers = for (strat <- saStrats) yield {
        vds.querySA(strat)
      }

      //Group samples by stratification
      val uniqueSaStrats = new ArrayBuffer[String]()
      val samplesStrat = state.sc.broadcast(for (i <- vds.sampleIds.indices) yield {
        val strat = saQueriers.map({case(basetype,querier) => querier(vds.sampleAnnotations(i)).getOrElse("NA")}).mkString("\t")
        val stratIndex = uniqueSaStrats.indexOf(strat)
        if(stratIndex < 0){
          uniqueSaStrats += strat
          uniqueSaStrats.size - 1
        }else{
          stratIndex
        }
      })
      val uniqueSaStratsBr = state.sc.broadcast(uniqueSaStrats.toArray)

      val mDAF = options.mDAF

      val gb = SparseVariantSampleMatrixRRDBuilder.buildByVA(
        vds.filterVariants((v,va,gts) => geneAnn(va).isDefined),
        state.sc,
        partitioner
      )({case (v,va) => geneAnn(va).get.toString}).mapValues({
        svsm => new GeneBurdenResult(svsm, vaStrats, saStrats, uniqueSaStratsBr.value, samplesStrat.value, mDAF, afAnnotation)
      }).persist(StorageLevel.MEMORY_AND_DISK)
/**
  * if(Option(options.controls).isDefined)
  * gb.mapValues( gbr )
  * else{
  * val hetSites = gb.map(_._2.getHetSites()).reduce((s1,s2) => s1 ++ s2)
  * val controlVDS = State(state.sc,state.sqlContext,VariantSampleMatrix.read(state.sqlContext, options.controls)).vds
  * .filterVariants({case (v,va,gt) => hetSites.contains(v.toString) })
  * }
**/

      //Write out single variant stats
      new RichRDD(gb.map(
        {case (gene,gr) => gr.getSingleVariantsStats(gene)}
      )).writeTable(options.output +".single.txt",
        Some("gene\t" + GeneBurdenResult.getSingleVariantHeaderString(vaStrats,saStrats)))

      val gbPair =
        if(Option(options.controls).isDefined){
          val hetSites = gb.map(_._2.getHetSites()).reduce((s1,s2) => s1 ++ s2)
          val controlVDS = State(state.sc,state.sqlContext,VariantSampleMatrix.read(state.sqlContext, options.controls)).vds
            .filterVariants({case (v,va,gt) => hetSites.contains(v.toString) })
          val controlsGeneAnn = controlVDS.queryVA(
            if(Option(options.controls_gene_annotation).isDefined) options.controls_gene_annotation
            else options.gene_annotation
          )._2
          val controlSVSM = SparseVariantSampleMatrixRRDBuilder.buildByVA(
            controlVDS.filterVariants((v,va,gts) => controlsGeneAnn(va).isDefined),
            state.sc,
            partitioner
          )({case (v,va) => controlsGeneAnn(va).get.toString})

          gb.join(controlSVSM).mapValues({
            case (g,svsm) =>
              g.phaseCompoundHets(svsm)
              g
          })

        }
        else{ gb.mapValues({
          g => g.phaseCompoundHets()
            g
        }) }

      //Write out pair variants stats
      new RichRDD(gbPair.map(
        {case (gene,gr) => gr.getVariantPairsStats(gene)}
      )).writeTable(options.output +".pair.txt",
        Some("gene\t" + GeneBurdenResult.getVariantPairHeaderString(vaStrats,saStrats)))

      state
    }
}
