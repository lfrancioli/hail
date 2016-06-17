package org.broadinstitute.hail.driver

import breeze.linalg.{DenseVector, max, sum}
import breeze.numerics.abs
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel
import org.broadinstitute.hail.{Logging, RichRDD}
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.methods.Pedigree
import org.broadinstitute.hail.utils.{SparseVariantSampleMatrix, SparseVariantSampleMatrixRRDBuilder}
import org.broadinstitute.hail.variant._
import org.kohsuke.args4j.{Option => Args4jOption}
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.expr.BaseType
import org.broadinstitute.hail.variant.GenotypeType._

import scala.collection.mutable
import scala.language.postfixOps

object SingletonLDinTrios extends Command {

  def name = "singletonLDinTrios"
  def description = "Looks at pairs of rare variants where both variants are both in at least one of the parents of one of the trios" +
    "and the other isn't. Compute how many are in/out of phase, stratified by the AC in ExAC (no binning here so filter wisely!)"

  class Options extends BaseOptions {
    @Args4jOption(required = true, name = "-i", aliases = Array("--controls"), usage = "Input ExAC .vds file")
    var controls_input: String = _

    @Args4jOption(required = true, name = "-o", aliases = Array("--output"), usage = "Output filename")
    var output: String = _

    @Args4jOption(required = true, name = "-f", aliases = Array("--fam"), usage = ".fam file")
    var famFilename: String = _

    @Args4jOption(required = true, name = "-g", aliases = Array("--gene_annotation"), usage = "Annotation storing the gene information for aggregation")
    var gene_annotation: String = _

    //@Args4jOption(required = true, name = "-c", aliases = Array("--consequence_annotation"), usage = "Annotation storing the gene information for aggregation")
    //var consequence_annotation: String = _

    //@Args4jOption(required = false, name = "-cexac", aliases = Array("--condition_AC"), usage = "Condition to apply to ExAC for filtering down the number of variants. Default is AC < 200")
    //var cexac: String = "va.info.AC[0] < 200"

    @Args4jOption(required = false, name = "-p", aliases = Array("--partitions_number"), usage = "Number of partitions to use for gene aggregation.")
    var number_partitions: Int = 200

    @Args4jOption(required = false, name = "-nocoseg", aliases = Array("--no_coseg"), usage = "Do not run co-seggregation analysis")
    var nocoseg: Boolean = false

    @Args4jOption(required = false, name = "-noem", aliases = Array("--no_EM"), usage = "Do not run haplotype analysis using EM algorithm")
    var noem: Boolean = false

    @Args4jOption(required = false, name = "-vaStrat", aliases = Array("--va_stratification"), usage = "Stratify results based on variant annotations. Comma-separated list of annotations.")
    var vaStrat: String = ""

    @Args4jOption(required = false, name = "-saStrat", aliases = Array("--sa_stratification"), usage = "Stratify results based on sample annotations. Comma-separated list of annotations.")
    var saStrat: String = ""

  }
  def newOptions = new Options

  /**
    * Records:
    * Number of variant pairs
    *
    **/

   object VariantPairsCounter{
    def getHeaderString(coseg: Boolean, em : Boolean, variantAnnotations: Array[String], sampleAnnotations: Array[String]) : String = {

      variantAnnotations.foldLeft("AC1\tAC2")({(str,ann) => str + "\t" + ann + "1"}) +
        variantAnnotations.foldLeft("")({(str,ann) => str + "\t" + ann + "2"}) +
      sampleAnnotations.foldLeft("")({(str,ann) => str + "\t" + ann}) +
      "\tsameTrioHap\tdiffTrioHap" +
        (if(coseg) "\tcoInExAC\tnotCoInExAC\tsameHapTrioAndExAC\tdiffHapTrioAndExAC" else "") +
        (if(em) "\tsameHapExACEM\tdiffHapExACEM\tsameHapTrioAndExACEM\tdiffHapTrioAndExACEM" else "" )
    }
  }

  class VariantPairsCounter(val trios: SparseVariantSampleMatrix, val ped : Pedigree, val vaStrat: Array[String] = Array[String](), val saStrat: Array[String] = Array[String]()) extends Logging{

    //Small class to cache variant stats
    object VariantStats{
      def apply(v1: String, v2: String, AC1: Int, AC2: Int, ann1: String, ann2: String) : VariantStats = {
        if(v1<v2){
          //Note that either both ann1 and ann2 should be emtpy or non-empty
          new VariantStats(v1,v2,AC1,AC2,if(ann1.isEmpty() || ann2.isEmpty()) "" else ann1+"\t"+ann2)
        }
        new VariantStats(v1,v2,AC2,AC1, if(ann1.isEmpty() || ann2.isEmpty()) "" else ann2+"\t"+ann1)
      }
    }

    case class VariantStats private(val v1: String, val v2: String, val AC1: Int, val AC2: Int, val variantAnnotations: String)  {
      var sameExacHap : Option[Boolean] = None
      var sameEMHap: Option[Boolean] = None

      def setEMHap(probSameHap: Option[Double]) = {
        probSameHap match {
          case Some(prob) => sameEMHap = Some(prob > 0.5)
          case None =>
        }
      }

      def getKey(sampleAnnotations: String) : (Int, Int, String) = {

        if(variantAnnotations.isEmpty){return (AC1,AC2,sampleAnnotations)}

        return (AC1,AC2, variantAnnotations + (if(!variantAnnotations.isEmpty) "\t" else "")  + sampleAnnotations)
      }
    }

    /** Stores the following results:
      * #sites on the same trio haplotype,
      * #Sites on different trio haplotypes,
      * #Sites found co-seggregating in ExAC,
      * #Sites found in ExAC but not co-seggregating
      * #Sites on the same haplotype and found co-seggregating in ExAC
      * #Sites on different haplotype and not co-seggregating in ExAC, although present
      * #Sites on the same haplotype in ExAC based on EM
      * #Sites on different haplotype in ExAC based on EM
      * #Sites on the same haplotype in ExAC based on EM and on the same trio haplotype
      * #Sites on different haplotype in ExAC based on EM and on different trio haplotype
      * */
    class VPCResult(val coseg :Boolean = true, val em : Boolean = true) {

      //Numbers from trio inheritance
      var nSameTrioHap = 0
      var nDiffTrioHap = 0

      //Numbers from naive found/not found in ExAC
      var nCoSegExAC = 0
      var nNonCoSegExac = 0
      var nCoSegExACandSameTrioHap = 0
      var nNonCoSegExACandDiffTrioHap = 0

      //Numbers from EM
      var nSameHapExAC = 0
      var nDiffHapExac = 0
      var nSameHapExACandSameHapTrio = 0
      var nDiffHapExACandDiffHapTrio = 0

      def this(sameTrioHap : Boolean, variantStats: VariantStats, coseg :Boolean = true, em : Boolean = true){
        this(coseg,em)
        nSameTrioHap = if(sameTrioHap) 1 else 0
        nDiffTrioHap = if(sameTrioHap) 0 else 1

        variantStats.sameExacHap match {
          case Some(sameHap) => coSegExAC(sameHap,sameTrioHap)
          case None =>
        }
        variantStats.sameEMHap match {
          case Some(sameHap) => sameEMHap(sameHap,sameTrioHap)
          case None =>
        }
      }

      def coSegExAC(coseg : Boolean, sameTrioHap : Boolean) ={
        if(coseg){
          nCoSegExAC += 1
          if(sameTrioHap){
            nCoSegExACandSameTrioHap += 1
          }
        }else{
          nNonCoSegExac +=1
          if(!sameTrioHap){
            nNonCoSegExACandDiffTrioHap += 1
          }
        }
      }

      def sameEMHap(sameHap : Boolean, sameTrioHap : Boolean) ={
        if(sameHap){
          nSameHapExAC += 1
          if(sameTrioHap){
            nSameHapExACandSameHapTrio += 1
          }
        }else{
          nDiffHapExac +=1
          if(!sameTrioHap){
            nDiffHapExACandDiffHapTrio += 1
          }
        }
      }

      def add(that: VPCResult): VPCResult ={
         nSameTrioHap += that.nSameTrioHap
         nDiffTrioHap += that.nDiffTrioHap

        //Numbers from naive += that.naive found/not found in ExAC
         nCoSegExAC += that.nCoSegExAC
         nNonCoSegExac += that.nNonCoSegExac
         nCoSegExACandSameTrioHap += that.nCoSegExACandSameTrioHap
         nNonCoSegExACandDiffTrioHap += that.nNonCoSegExACandDiffTrioHap

        //Numbers from EM
         nSameHapExAC += that.nSameHapExAC
         nDiffHapExac += that.nDiffHapExac
         nSameHapExACandSameHapTrio += that.nSameHapExACandSameHapTrio
         nDiffHapExACandDiffHapTrio += that.nDiffHapExACandDiffHapTrio
        this
      }

      override def toString() : String = {
        "%d\t%d".format(nSameTrioHap, nDiffTrioHap) +
          (if(coseg) "\t%d\t%d\t%d\t%d".format(nCoSegExAC, nNonCoSegExac, nCoSegExACandSameTrioHap, nNonCoSegExACandDiffTrioHap) else "" ) +
          (if(em) "\t%d\t%d\t%d\t%d".format(nSameHapExAC, nDiffHapExac, nSameHapExACandSameHapTrio, nDiffHapExACandDiffHapTrio) else "")
      }

    }

    var res = mutable.Map[(Int, Int, String), VPCResult]()

    //Get sample annotation queriers
    private var saQueriers = Array.ofDim[(BaseType,Querier)](saStrat.size)
    saStrat.indices.foreach({
      i => saQueriers(i) = trios.querySA(saStrat(i))
    })

    val variantPairs = (for(trio <- ped.completeTrios) yield{

      //Get the sample annotations for the kid as a string
      //For now rely on toString(). Later might need binning by type
      var kidSA = saQueriers.map({ querier =>
        trios.getSampleAnnotation(trio.kid,querier._2).getOrElse("NA").toString()
      }).mkString("\t")

      getHetPhasedVariantPairs(trio.kid,trio.dad,trio.mom,kidSA) ++ getHetPhasedVariantPairs(trio.kid,trio.mom,trio.dad,kidSA)
    }).flatten.filter(_._3.isDefined)



    //Public functions
    override def toString() : String = {
      //Check if any stratification
      if(saStrat.isEmpty && vaStrat.isEmpty){
        res.map({case ((ac1,ac2,annotations),result) =>
          ("%d\t%d\t").format(ac1,ac2) + result.toString()
        }).mkString("\r")
      }else{
        res.map({case ((ac1,ac2,annotations),result) =>
          ("%d\t%d\t%s\t").format(ac1,ac2,annotations) + result.toString()
        }).mkString("\r")
      }
    }

    def toString(group_name : String) : String = {
      //Check if any stratification
      if(saStrat.isEmpty && vaStrat.isEmpty) {
        res.map({ case ((ac1, ac2, annotations), result) =>
          ("%s\t%d\t%d\t").format(group_name, ac1, ac2) + result.toString()
        }).mkString("\n")
      }else{
        res.map({ case ((ac1, ac2, annotations), result) =>
          ("%s\t%d\t%d\t%s\t").format(group_name, ac1, ac2, annotations) + result.toString()
        }).mkString("\n")
      }
    }

    def getHetSites() : Set[String] = {
      variantPairs.flatMap({case (v1,v2,phase, kidSA) => List(v1,v2)}).toSet
    }

    def this(trios: SparseVariantSampleMatrix, exac : SparseVariantSampleMatrix , ped : Pedigree, coseg: Boolean = false, em: Boolean = true) = {
      this(trios,ped)
      computeExACphase(exac,coseg,em)
    }

    def addExac(exac: SparseVariantSampleMatrix, coseg: Boolean = false, em: Boolean = true) = {
      computeExACphase(exac,coseg,em)
    }


    //Private functions
    private def computeExACphase(exac : SparseVariantSampleMatrix, coseg: Boolean, em: Boolean) = {

      //Get annotation queriers
      var vaQueriers = Array.ofDim[(BaseType,Querier)](vaStrat.size)
      vaStrat.indices.foreach({ i =>
        vaQueriers(i) = trios.queryVA(vaStrat(i))
      })

      def computeVariantStats(v1: String, v2: String) : VariantStats = {

        //Get variants annotation as String
        //For now rely on toString() but might be good to have type-specific functions
        val v1Ann = vaQueriers.map({querier =>
          trios.getVariantAnnotation(v1,querier._2).getOrElse("NA").toString()
        }).mkString("\t")
        val v2Ann = vaQueriers.map({querier =>
          trios.getVariantAnnotation(v2,querier._2).getOrElse("NA").toString()
        }).mkString("\t")

        //Get AC for both variants
        val AC1 = exac.getAC(v1)
        val AC2 = exac.getAC(v2)

        val v = VariantStats(v1,v2,AC1,AC2,v1Ann,v2Ann)
        //Check if could be found in ExAC and how it seggregates
        //info("Computing ExAC segregation for variant-pair:" + v1 +" | "+v2)
        if(coseg) { v.sameExacHap = foundInSameSampleInExAC(exac, v1, v2) }

        //Compute whether on the same haplotype based on EM using ExAC
        //info("Computing ExAC phase for variant-pair:" + v1 +" | "+v2)
        if(em) { v.setEMHap(probOnSameHaplotypeWithEM(exac, v1, v2)) }

        return v
      }

      //Cache variants that have already been processed
      var variantCache = mutable.Map[(String,String),VariantStats]()

      //info("Computing ExAC phase for "+variantPairs.size+" variant pairs...")
      variantPairs.foreach({ case (v1, v2, sameTrioHap, kidSA) =>

        //Only store results where sites could be trio-phased
        if (sameTrioHap.isDefined) {

          val variantStats = variantCache.get((v1,v2)) match {
            case Some(cachedResult) => {
              cachedResult
            }
            case None => {
              val vStats = computeVariantStats(v1,v2)
              variantCache((v1,v2)) = vStats
              vStats
            }
          }

          val k = variantStats.getKey(kidSA)
          val v = new VPCResult(sameTrioHap.get, variantStats, coseg, em)

          //Add results
          res.get(k) match{
            case Some(pv) => res.update(k, pv.add(v))
            case None => res.update(k,v)
          }

        }
      })
    }

    //Returns all pairs of variants for which the parent parentID is het at both sites, along with
    //whether each of the variant pairs are on the same haplotype or not based on the transmission of alleles in the trio (or None if ambiguous / missing data)
    private def getHetPhasedVariantPairs(kidID: String, parentID: String, otherParentID: String, kidSA : String = "") : Set[(String,String,Option[Boolean],String)] = {
      val genotypes = trios.getSample(parentID)

      (for((variant1, gt1) <- genotypes if gt1.isHet; (variant2,gt2) <- genotypes if gt2.isHet && variant1 < variant2) yield{
        //info("Found variant pair: " + variant1 + "/" + variant2 + " in parent " + parentID)
            (variant1, variant2, isOnSameParentalHaplotype(variant1,variant2,kidID,otherParentID), kidSA)
      }).toSet

    }

    //Given a site that is Het in parent1,
    //returns whether the site was transmitted from parent1 or from the otherParent
    //Returns None if ambiguous or missing data
    private def isHetSiteTransmitted(kidGT: Option[Genotype], otherParentGT: Option[Genotype]): Option[Boolean] = {
      kidGT match {
        case Some(kid) => {
          if(kid.isHomRef){ return Some(false)}
          else if(kid.isHomVar){return Some(true)}
          else if(kid.isHet){
            otherParentGT match {
              case Some(gt) => {
                if(gt.isHomRef){ return Some(true)}
                else if(gt.isHomVar){return Some(false)}
              }
              case None => None
            }
          }
        }
        case None => None
      }
      None
    }


    //Given two variants that are het in a parent, computes whether they are on the same haplotype or not based on
    //the child and the otherParent in the trio.
    //Returns None if ambiguous or missing datta
    private def isOnSameParentalHaplotype(variantID1: String, variantID2: String, kidID: String, otherParentID: String): Option[Boolean] = {

      val v1POO = isHetSiteTransmitted(trios.getGenotype(variantID1, kidID), trios.getGenotype(variantID1, otherParentID))
      val v2POO = isHetSiteTransmitted(trios.getGenotype(variantID2, kidID), trios.getGenotype(variantID2, otherParentID))

      (v1POO, v2POO) match {
        case (Some(v1poo), Some(v2poo)) => Some(v1poo == v2poo)
        case _ => None
      }

    }

    //Given two variants, check if any sample in ExAC carries both of these variants. Then compares the number of samples carrying
    //both variants to the minSamples.
    private def foundInSameSampleInExAC(exac: SparseVariantSampleMatrix, variantID1: String, variantID2: String, minSamples: Int = 1): Option[Boolean] = {

      (exac.getVariantAsOption(variantID1), exac.getVariantAsOption(variantID2)) match {
        case (Some(v1), Some(v2)) => Some((v1.filter({
          case (k, v) => v.isHet || v.isHomVar
        }).keySet.intersect(
          v2.filter({
            case (k, v) => v.isHet || v.isHomVar
          }).keySet).size) >= minSamples)
        case _ => None
      }

    }

    private def probOnSameHaplotypeWithEM(exac: SparseVariantSampleMatrix, variantID1: String, variantID2: String) : Option[Double] = {
      phaseWithEM(exac,variantID1,variantID2) match{
        case Some(haplotypes) =>
          return Some(haplotypes(0) * haplotypes(3) / (haplotypes(1) * haplotypes(2) + haplotypes(0) * haplotypes(3)))
        case None => None
      }

    }

    /**Returns a vector containing the estimated number of haplotypes
      * (0) AB
      * (1) Ab
      * (2) aB
      * (3) ab
      */
    private def phaseWithEM(exac: SparseVariantSampleMatrix, variantID1: String, variantID2: String) : Option[DenseVector[Double]] = {

      /**Count the number of individuals with different genotype combinations
        * (0) AABB
        * (1) AaBB
        * (2) aaBB
        * (3) AABb
        * (4) AaBb
        * (5) aaBb
        * (6) AAbb
        * (7) Aabb
        * (8) aabb
        */
        def getIndex(g1: GenotypeType, g2: GenotypeType) : Int = {
        (g1, g2) match {
          case (HomRef, HomRef) => 0
          case (Het, HomRef) => 1
          case (HomVar, HomRef) => 2
          case (HomRef, Het) => 3
          case (Het, Het) => 4
          case (HomVar, Het) => 5
          case (HomRef, HomVar) => 6
          case (Het, HomVar) => 7
          case (HomVar,HomVar) => 8
          case _ => -1
        }
      }

      val gtCounts = new DenseVector(new Array[Int](9))

      val v1_gt = exac.getVariant(variantID1)
      val v2_gt = exac.getVariant(variantID2)

      //Add all HomRef/HomRef counts
      gtCounts(0) += exac.nSamples - (v1_gt.keys.toSet ++ v2_gt.keys.toSet ).size

      //Add all non-homref genotype counts from v1
      v1_gt.foreach({
        case (s,g1) =>
          val index = v2_gt.get(s) match {
            case Some(g2) =>
              v2_gt.remove(s)
              getIndex(g1.gtType, g2.gtType)
            case None =>
              getIndex(g1.gtType,GenotypeType.HomRef)
          }
          if(index > -1){ gtCounts(index) += 1 }
      })

      //Add all v2-specific counts
      v2_gt.foreach({
        case (s,g2) => if(g2.isCalled){ gtCounts(getIndex(GenotypeType.HomRef,g2.gtType)) += 1 }
      })

      val nSamples = sum(gtCounts)

      //Needs some non-ref samples to compute
      if(gtCounts(0) >= nSamples){ return None}

      val nHaplotypes = 2.0*nSamples.toDouble

      /**
        * Constant quantities for each of the different haplotypes:
        * n.AB => 2*n.AABB + n.AaBB + n.AABb
        * n.Ab => 2*n.AAbb + n.Aabb + n.AABb
        * n.aB => 2*n.aaBB + n.AaBB + n.aaBb
        * n.ab => 2*n.aabb + n.aaBb + n.Aabb
        */
      val const_counts = new DenseVector(Array[Double](
        2.0*gtCounts(0) + gtCounts(1) + gtCounts(3), //n.AB
        2.0*gtCounts(6) + gtCounts(3) + gtCounts(7), //n.Ab
        2.0*gtCounts(2) + gtCounts(1) + gtCounts(5), //n.aB
        2.0*gtCounts(8) + gtCounts(5) + gtCounts(7)  //n.ab
      ))

      //info("EM initialization done.")

      //Initial estimate with AaBb contributing equally to each haplotype
      var p_next = (const_counts :+ new DenseVector(Array.fill[Double](4)(gtCounts(4)/2.0))) :/ nHaplotypes
      var p_cur = p_next :+ 1.0

      var i = 0

      //EM
      while(max(abs(p_next :- p_cur)) > 1e-7){
        i += 1
        p_cur = p_next

        p_next = (const_counts :+
          (new DenseVector(Array[Double](
            p_cur(0)*p_cur(3), //n.AB
            p_cur(1)*p_cur(2), //n.Ab
            p_cur(1)*p_cur(2), //n.aB
            p_cur(0)*p_cur(3)  //n.ab
          )) :* gtCounts(4) / ((p_cur(0)*p_cur(3))+(p_cur(1)*p_cur(2))) )
          ) :/ nHaplotypes

      }
      //info("EM converged after " + i.toString + " iterations.")
      return Some(p_next :* nHaplotypes)

    }

  }


  def run(state: State, options: Options): State = {
    
    //Read PED file
    val ped = state.sc.broadcast(Pedigree.read(options.famFilename, state.hadoopConf, state.vds.sampleIds))

    //Get annotations
    val triosGeneAnn = state.vds.queryVA(options.gene_annotation)._2

    //List of contigs to consider
    val autosomes = Range(1,23).map({c: Int => c.toString}).toSet

    //Filter variants that are on autosomes and have a gene annotation
    def autosomeFilter = {(v: Variant, va: Annotation) => autosomes.contains(v.contig) && triosGeneAnn(va).isDefined}

    //List individuals from trios where all family members are present
    //In case of multiple offspring, keep only one
    val samplesInTrios = ped.value.completeTrios.foldLeft(Set[String]())({case (acc,trio) => acc ++ Set(trio.mom,trio.dad,trio.kid)})

    //Filter variants to keep autosomes only and samples to keep only complete trios
    val trioVDS = state.vds.filterVariants(autosomeFilter).filterSamples((s: String, sa: Annotation) => samplesInTrios.contains(s))

    val partitioner = new HashPartitioner(options.number_partitions)

    val variantAnnotations = state.sc.broadcast(if(!options.vaStrat.isEmpty()) options.vaStrat.split(",") else Array[String]())

    val sampleAnnotations = state.sc.broadcast(if(!options.saStrat.isEmpty()) options.saStrat.split(",") else Array[String]())

    val triosRDD = SparseVariantSampleMatrixRRDBuilder.buildByAnnotation(trioVDS,state.sc , partitioner, variantAnnotations.value, sampleAnnotations.value)(
      {case (v,va) => triosGeneAnn(va).get.toString}
    ).mapValues({
      case svm => new VariantPairsCounter(svm,ped.value,variantAnnotations.value, sampleAnnotations.value)
    }).persist(StorageLevel.MEMORY_AND_DISK)

    //Get unique variants that are found in pairs in our samples
    //TODO: Can this be replaced by a fold?
    val uniqueVariants = triosRDD.map({
      case(gene,svm) => svm.getHetSites()
      }).reduce(
      {case(v1,v2) => v1 ++ v2}
    )


    info(triosRDD.map({
      case(gene,vs) => ("Gene: %s\tnVariantPairs: %d").format(gene,vs.variantPairs.size)
    }).collect().mkString("\n"))

    info("Found " + uniqueVariants.size.toString + " variants in pairs in samples.")

    val bcUniqueVariants = state.sc.broadcast(uniqueVariants)

    //Load ExAC VDS, filter common samples and sites based on exac condition (AC)
    val exacVDS = State(state.sc,state.sqlContext,VariantSampleMatrix.read(state.sqlContext, options.controls_input)).vds

    val exacGeneAnn = exacVDS.queryVA(options.gene_annotation)._2

    //Only keep variants that are of interest and have a gene annotation (although they should match those of trios!)
    def variantsOfInterestFilter = {(v: Variant, va: Annotation) => exacGeneAnn(va).isDefined && bcUniqueVariants.value.contains(v.toString)}

    val exacRDD = SparseVariantSampleMatrixRRDBuilder.buildByAnnotation(exacVDS.filterVariants(variantsOfInterestFilter).
      filterSamples((s: String, sa: Annotation) => !trioVDS.sampleIds.contains(s)), state.sc, partitioner)(
      {case (v,va) => exacGeneAnn(va).get.toString}
    )
      //.persist(StorageLevel.MEMORY_AND_DISK)

    /**info(exacRDD.map({
      * case(gene,vs) => ("Gene: %s\tnVariants: %d\tnSamples: %d\tnGenotypes: %d").format(gene,vs.variants.size, vs.nSamples, vs.nGenotypes())
      * }).collect().mkString("\n"))*/

  // TODO: Print out partitioner
    val callsByGene = triosRDD.join(exacRDD,partitioner)
    //.persist(StorageLevel.MEMORY_AND_DISK)

    /**info(callsByGene.map({
      * case(gene,(trios,exac)) => ("Gene: %s\tnVariantPairs: %d").format(gene,trios.variantPairs.size)
      * }).collect().mkString("\n"))**/


    //val x = new SparseVector()

    //write results
    val run_coseg = state.sc.broadcast(!options.nocoseg)
    val run_em = state.sc.broadcast(!options.noem)

    new RichRDD(callsByGene.map(
      {case(gene,(trios,exac)) =>
        val now = System.nanoTime
        trios.addExac(exac,run_coseg.value,run_em.value)
        info("Gene %s phasing done in %.1f seconds.".format(gene,(System.nanoTime - now) / 10e9))
        trios.toString(gene)
      })).writeTable(options.output,header = Some("gene\t" + VariantPairsCounter.getHeaderString(run_coseg.value, run_em.value, variantAnnotations.value, sampleAnnotations.value)))

    state
  }
}
