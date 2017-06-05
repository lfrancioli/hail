package is.hail.methods

import is.hail.utils.richUtils.RichRDD
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel
import is.hail.annotations._
import is.hail.utils.{SparseVariantSampleMatrix, SparseVariantSampleMatrixRRDBuilder}
import is.hail.variant._
import is.hail.utils._
import is.hail.expr.{Type}

import scala.collection.mutable
import scala.language.postfixOps

object PhaseTrios {

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

  class VariantPairsCounter(val trios: SparseVariantSampleMatrix, val ped : Pedigree, val vaStrat: Array[String] = Array[String](), val saStrat: Array[String] = Array[String]()) {

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
    class VPCResult(val coseg :Boolean, val em : Boolean) {

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
    private var saQueriers = Array.ofDim[(Type,Querier)](saStrat.size)
    saStrat.indices.foreach({
      i => saQueriers(i) = trios.querySA(saStrat(i))
    })

    val variantPairs = (for(trio <- ped.filterTo(trios.sampleIDs.toSet).completeTrios) yield{

      //Get the sample annotations for the kid as a string
      //For now rely on toString(). Later might need binning by type
      var kidSA = saQueriers.map({ querier =>
        trios.getSampleAnnotation(trio.kid,querier._2).getOrElse("NA").toString()
      }).mkString("\t")

      getHetPhasedVariantPairs(trio.kid,trio.dad.get,trio.mom.get,kidSA) ++ getHetPhasedVariantPairs(trio.kid,trio.mom.get,trio.dad.get,kidSA)
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

    def addExac(exac: SparseVariantSampleMatrix, coseg: Boolean = false, em: Boolean = true) = {
      computeExACphase(exac,coseg,em)
    }


    //Private functions
    private def computeExACphase(exac : SparseVariantSampleMatrix, coseg: Boolean, em: Boolean) = {

      //Get annotation queriers
      var vaQueriers = Array.ofDim[(Type,Querier)](vaStrat.size)
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
        if(em) { v.setEMHap( Phasing.probOnSameHaplotypeWithEM(exac.getGenotypeCounts(v1, v2))) }

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

  }


  def apply(trioVDS: VariantDataset, referenceVDS: VariantDataset , ped: Pedigree, gene_annotation: String, output: String, number_partitions: Int,
    variantAnnotations: Array[String] = Array[String](), sampleAnnotations: Array[String] = Array[String](),
    run_coseg : Boolean, run_em: Boolean) = {

    //Get SparkContext
    val sc = trioVDS.sparkContext

    //Get annotations
    val triosGeneAnn = trioVDS.queryVA(gene_annotation)._2

    val partitioner = new HashPartitioner(number_partitions)

    val ped_in_vds = ped.filterTo(trioVDS.sampleIds.map(_.asInstanceOf[String]).toSet).completeTrios

    info(s"Found ${ped_in_vds.length} complete trios in VDS.")

    val triosRDD = SparseVariantSampleMatrixRRDBuilder.buildByVA(trioVDS, sc , partitioner)(
      {case (v,va) => triosGeneAnn(va).toString}
    ).mapValues({
      case svm => new VariantPairsCounter(svm, ped, variantAnnotations, sampleAnnotations)
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

    val bcUniqueVariants = sc.broadcast(uniqueVariants)

    //Only keep variants that are in the trios
    def variantsOfInterestFilter = {(v: Variant, va: Annotation, gs: Iterable[Genotype]) => bcUniqueVariants.value.contains(v.toString)}


    trioVDS.typecheck()
    info("trioVDS typechecked!")

    referenceVDS.typecheck()
    info("reference VDS typechecked!")

    val filteredReference = referenceVDS
      .filterVariants(variantsOfInterestFilter)
      .annotateVariantsVDS(trioVDS, code = Some(s"$gene_annotation = vds.${gene_annotation.substring(3)}"))

    filteredReference.typecheck()
    info("filteredReference typechecked!")

    val variants = filteredReference.queryVariants("variants.map(v => isDefined(va.gene)).counter()")

    info(variants._1.toString())

    val referenceGeneAnn = filteredReference.queryVA(gene_annotation)

    val referenceRDD = SparseVariantSampleMatrixRRDBuilder.buildByVA(filteredReference, sc, partitioner)(
      {case (v,va) => referenceGeneAnn._2(va).toString}
    )

    val callsByGene = triosRDD.join(referenceRDD,partitioner)

    val results = callsByGene.map{
      case(gene,(trios,exac)) =>
        val now = System.nanoTime
        trios.addExac(exac,run_coseg,run_em)
        info("Gene %s phasing done in %.1f seconds.".format(gene,(System.nanoTime - now) / 10e9))
        trios.toString(gene)
      }.filter(s => !s.isEmpty).persist(StorageLevel.MEMORY_AND_DISK)

    info(s"Found ${results.count()} resulting genes containing variant pair results.")

    //write results
    results.writeTable(output, trioVDS.hc.tmpDir,
      header = Some("gene\t" + VariantPairsCounter.getHeaderString(run_coseg, run_em, variantAnnotations, sampleAnnotations)))

  }
}
