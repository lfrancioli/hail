package org.broadinstitute.hail.utils

import breeze.linalg.{DenseVector}
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.variant.{Genotype, GenotypeType, Variant, VariantSampleMatrix}
import org.broadinstitute.hail.variant.GenotypeType._

import scala.collection.{immutable, mutable}
import scala.collection.mutable.{ArrayBuffer, ArrayBuilder, Map}
import scala.reflect.ClassTag
import org.broadinstitute.hail.expr
import org.broadinstitute.hail.expr.{BaseType, EvalContext, Parser, TStruct, Type}


/**
  * Created by laurent on 4/19/16.
  */

object SparseVariantSampleMatrixRRDBuilder {

  //Given a mapping from a variant and its annotations to use as the key to the resulting PairRDD,
  //Aggregates the data in a SparseSampleVariantMatrix
  def buildByAnnotation[K](vsm: VariantSampleMatrix[Genotype], sc: SparkContext, partitioner : Partitioner, sampleAnnotations: Array[String] = Array[String]())(
    mapOp: (Variant, Annotation)  => K)(implicit uct: ClassTag[K]): RDD[(K, SparseVariantSampleMatrix)] = {

    //Broadcast sample IDs
    val bcSampleIds = sc.broadcast(vsm.sampleIds)

    //Build sample annotations
    val sa = sc.broadcast(buildSamplesAnnotations(vsm,sampleAnnotations))

    vsm.rdd
      .mapPartitions { (it: Iterator[(Variant, Annotation, Iterable[Genotype])]) =>
        val gtBuilder = new mutable.ArrayBuilder.ofByte()
        val siBuilder = new ArrayBuilder.ofInt()
        it.map { case (v, va, gs) =>
          gtBuilder.clear()
          siBuilder.clear()
          val sg = gs.iterator.zipWithIndex.foldLeft((siBuilder,gtBuilder))({
            case (acc,(g,i)) => if(!g.isHomRef) (acc._1 += i,  acc._2 += g.gt.getOrElse(-1).toByte) else acc
          })
          (mapOp(v,va), (v.toString,siBuilder.result(),gtBuilder.result()))
        }
      }.aggregateByKey(new SparseVariantSampleMatrix(bcSampleIds.value, saSignature = sa.value._1, sampleAnnotations = sa.value._2), partitioner) (
      { case (svsm, (v,sampleIndices,genotypes)) => svsm.addVariant(v,sampleIndices,genotypes) },
      { (svsm1,svsm2) => svsm1.merge(svsm2) })
  }

  //Given a mapping from a variant and its annotations to use as the key to the resulting PairRDD,
  //Aggregates the data in a SparseSampleVariantMatrix
  def buildByAnnotation[K](vsm: VariantSampleMatrix[Genotype], sc: SparkContext, partitioner : Partitioner, variantAnnotations : Array[String], sampleAnnotations: Array[String] = Array[String]())(
    mapOp: (Variant, Annotation)  => K)(implicit uct: ClassTag[K]): RDD[(K, SparseVariantSampleMatrix)] = {

    if(variantAnnotations.isEmpty){
      return buildByAnnotation(vsm,sc,partitioner,sampleAnnotations)(mapOp)
    }

    //Broadcast sample IDs
    val bcSampleIds = sc.broadcast(vsm.sampleIds)

    //Build sample annotations
    val sa = sc.broadcast(buildSamplesAnnotations(vsm,sampleAnnotations))

    //Create annotations signature / querier / inserter
    var newVA : Type = TStruct.empty
    val inserterBuilder = mutable.ArrayBuilder.make[Inserter]
    val querierBuilder = mutable.ArrayBuilder.make[Querier]
    variantAnnotations.foreach({a =>
      val (atype, aquerier) = vsm.queryVA(a)
      querierBuilder += aquerier
      val (s,i) = newVA.insert(atype.asInstanceOf[Type],expr.Parser.parseAnnotationRoot(a,"va"))
      inserterBuilder += i
      newVA = s

    })
    val queriers = sc.broadcast(querierBuilder.result())
    val inserters = sc.broadcast(inserterBuilder.result())
    val newVAbc = sc.broadcast(newVA)

    vsm.rdd
      .mapPartitions { (it: Iterator[(Variant, Annotation, Iterable[Genotype])]) =>
        val gtBuilder = new mutable.ArrayBuilder.ofByte()
        val siBuilder = new ArrayBuilder.ofInt()
        it.map { case (v, va, gs) =>
          gtBuilder.clear()
          siBuilder.clear()
          val reducedVA = queriers.value.map({qa => qa(va)})
          val sg = gs.iterator.zipWithIndex.foldLeft((siBuilder,gtBuilder))({
            case (acc,(g,i)) => if(!g.isHomRef) (acc._1 += i,  acc._2 += g.gt.getOrElse(-1).toByte) else acc
          })
          (mapOp(v,va), (v.toString,reducedVA,siBuilder.result(),gtBuilder.result()))
        }
      }.aggregateByKey(new SparseVariantSampleMatrix(bcSampleIds.value, newVAbc.value, sa.value._1, sa.value._2), partitioner) (
      { case (svsm, (v,reducedVA,sampleIndices,genotypes)) =>
        var va = Annotation.empty
        reducedVA.indices.foreach({ i =>
          va = inserters.value(i)(va,reducedVA(i))
        })
        svsm.addVariant(v,va,sampleIndices,genotypes) },
      { (svsm1,svsm2) => svsm1.merge(svsm2) })
  }


  private def buildSamplesAnnotations(vsm: VariantSampleMatrix[Genotype], sampleAnnotations: Array[String]) : (Type,IndexedSeq[Annotation]) = {
    if(sampleAnnotations.isEmpty){ return (TStruct.empty,IndexedSeq[Annotation]())}

    var newSA : Type = TStruct.empty
    val inserterBuilder = mutable.ArrayBuilder.make[Inserter]
    val querierBuilder = mutable.ArrayBuilder.make[Querier]
    sampleAnnotations.foreach({a =>
      val (atype, aquerier) = vsm.querySA(a)
      querierBuilder += aquerier
      val (s,i) = newSA.insert(atype.asInstanceOf[Type],expr.Parser.parseAnnotationRoot(a,"sa"))
      inserterBuilder += i
      newSA = s

    })

    val queriers = querierBuilder.result()
    val inserters = inserterBuilder.result()


    val saBuilder = mutable.ArrayBuilder.make[Annotation]
    vsm.sampleAnnotations.indices.foreach({ ai =>
      var sa = Annotation.empty
      queriers.indices.foreach({ i =>
        val ann  = queriers(i)(vsm.sampleAnnotations(ai))
        sa = inserters(i)(sa,ann)
      })
      saBuilder += sa;
    })


    return (newSA,saBuilder.result().toIndexedSeq)
  }

}

class SparseVariantSampleMatrix(val sampleIDs: IndexedSeq[String], val vaSignature:Type = TStruct.empty, val saSignature: Type = TStruct.empty, val sampleAnnotations: IndexedSeq[Annotation] = IndexedSeq[Annotation]()) extends Serializable {

  val nSamples = sampleIDs.length
  lazy val samplesIndex = sampleIDs.zipWithIndex.toMap

  val variants = ArrayBuffer[String]()
  val variantsAnnotations = ArrayBuffer[Annotation]()
  lazy val variantsIndex = variants.zipWithIndex.toMap

  //Stores the variants -> sample mappings
  //Populated when adding variants
  private val v_sindices = ArrayBuffer[Array[Int]]()
  private val v_genotypes = ArrayBuffer[Array[Byte]]()
  //private val vindices = ArrayBuffer[Int]()

  //Stores the samples -> variants mappings
  //Lazily computed from variants -> sample mappings
  //when accessing per-sample data
  private val s_vindices = ArrayBuffer[Array[Int]]()
  private val s_genotypes = ArrayBuffer[Array[Byte]]()
  //private val sindices = ArrayBuffer[Int]()

  def nGenotypes() : Int = {
    v_genotypes.size
  }

  def addVariant(variant: String, samples: Array[Int], genotypes: Array[Byte]) : SparseVariantSampleMatrix = {

    variants += variant
    v_sindices += samples
    v_genotypes += genotypes

    this
  }

  def addVariant(variant: String, variantAnnotations: Annotation, samples: Array[Int], genotypes: Array[Byte]) : SparseVariantSampleMatrix = {

    variants += variant
    variantsAnnotations += variantAnnotations
    v_sindices += samples
    v_genotypes += genotypes

    this
  }


  /**def addGenotype(variant: String, index: Int, genotype: Genotype) : SparseVariantSampleMatrix ={

    if(!genotype.isHomRef){

      variantsIndex.get(variant) match {
        case Some(v) =>
          v_sindices.update(v,v_sindices(v):+index)
          v_genotypes.update(v,v_genotypes(v):+genotype.gt.getOrElse(-1).toByte)
        case None =>
          v_sindices.append(Array(index))
          v_genotypes.append(Array(genotype.gt.getOrElse(-1).toByte))
      }

    }
    this
  }*/

  def merge(that: SparseVariantSampleMatrix): SparseVariantSampleMatrix = {

    this.variants ++= that.variants
    this.variantsAnnotations ++= that.variantsAnnotations
    this.v_sindices ++= that.v_sindices
    this.v_genotypes ++= that.v_genotypes
    this
  }

  //Returns None in case the variant is not present
  def getVariantAsOption(variantID: String) : Option[Map[String,Genotype]] = {
    variantsIndex.get(variantID) match{
      case Some(vindex) => Some(getVariant(vindex))
      case None => None
    }
  }

  //Return an empty map in case the variant is not present
 def getVariant(variantID: String): Map[String,Genotype] = {
   getVariant(variantsIndex.getOrElse(variantID, -1))
 }

  def getVariant(variantIndex: Int): Map[String,Genotype] = {

    val variant = mutable.Map[String,Genotype]()

    if(variantIndex > -1) {
      Range(0, v_sindices(variantIndex).size).foreach({
        case i => variant.update(sampleIDs(v_sindices(variantIndex)(i)), Genotype(v_genotypes(variantIndex)(i)))
      })
    }

    return variant

  }

  //Returns None if the sample is absent,
  // a Map of Variants -> Genotypes for that sample otherwise
  def getSampleAsOption(sampleID: String) : Option[Map[String,Genotype]] = {

    val sampleIndex = samplesIndex.getOrElse(sampleID,-1)

    if(sampleIndex < 0) { return None }

    Some(getSample(sampleIndex))

  }

  //Returns a Map of Variants -> Genotype for that sample
  //In case of an absent sample, returns an empty map
  def getSample(sampleID: String): Map[String,Genotype] = {
    getSample(samplesIndex.getOrElse(sampleID,-1))
 }

  //Returns a Map of Variants -> Genotype for that sample
  //In case of an absent sample, returns an empty map
  def getSample(sampleIndex: Int): Map[String,Genotype] = {

    val sample = mutable.Map[String,Genotype]()

    if(variants.isEmpty){return sample}

    if(sampleIndex < 0) { return sample }

    if(s_vindices.isEmpty){ buildSampleView() }

    Range(0,s_vindices(sampleIndex).size).foreach({
      i => sample.update(variants(s_vindices(sampleIndex)(i)), Genotype(s_genotypes(sampleIndex)(i)))
    })

    return sample
  }

  //Returns a Map of Variants -> Genotype for that sample
  //In case of an absent sample, returns an empty map
  def getSampleAsList(sampleID: String): List[(String,Genotype)] = {
    getSampleAsList(samplesIndex.getOrElse(sampleID,-1))
  }

  //Returns a Map of Variants -> Genotype for that sample
  //In case of an absent sample, returns an empty map
  def getSampleAsList(sampleIndex: Int): List[(String,Genotype)] = {

    if(variants.isEmpty){return List[(String,Genotype)]()}

    if(sampleIndex < 0) { return List[(String,Genotype)]() }

    if(s_vindices.isEmpty){ buildSampleView() }

    (for (i <- s_vindices(sampleIndex).indices) yield{
      (variants(s_vindices(sampleIndex)(i)), Genotype(s_genotypes(sampleIndex)(i)))
    }).toList
  }

  def queryVA(code: String): (BaseType, Querier) = {

    val st = immutable.Map(Annotation.VARIANT_HEAD ->(0, vaSignature))
    val ec = EvalContext(st)
    val a = ec.a

    val (t, f) = Parser.parse(code, ec)

    val f2: Annotation => Option[Any] = { annotation =>
      a(0) = annotation
      f()
    }

    (t, f2)
  }

  def querySA(code: String): (BaseType, Querier) = {

    val st = immutable.Map(Annotation.SAMPLE_HEAD ->(0, saSignature))
    val ec = EvalContext(st)
    val a = ec.a

    val (t, f) = Parser.parse(code, ec)

    val f2: Annotation => Option[Any] = { annotation =>
      a(0) = annotation
      f()
    }

    (t, f2)
  }

  def getSampleAnnotation(sampleID: String, annotation: String): Option[Any] ={
    val qsa = queryVA(annotation)._2
    qsa(sampleAnnotations(samplesIndex(sampleID)))

  }

  def getSampleAnnotation(sampleID: String, querier: Querier): Option[Any] ={
    querier(sampleAnnotations(samplesIndex(sampleID)))
  }

  def getSampleAnnotation(sampleIndex: Int, querier: Querier): Option[Any] ={
    querier(sampleAnnotations(sampleIndex))
  }

  def getVariantAnnotation(variantID: String, annotation: String) : Option[Any] = {
    val qva = queryVA(annotation)._2
    qva(variantsAnnotations(variantsIndex(variantID)))
  }

  def getVariantAnnotation(variantID: String, querier: Querier) : Option[Any] = {
    querier(variantsAnnotations(variantsIndex(variantID)))
  }


  private def buildSampleView() = {



    //Loop through all variants and collect (variant, samples, genotype) then groupBy sample
    // and add variant/genotype info
    val vsg = (for( v <-Range(0,v_sindices.size); i <- Range(0,v_sindices(v).size)) yield {
      (v,v_sindices(v)(i),v_genotypes(v)(i))
    }).groupBy({case (vindex,sindex,gt) => sindex})

    val vBuilder = new ArrayBuilder.ofInt
    val gBuilder = new ArrayBuilder.ofByte

    Range(0,sampleIDs.size).foreach({
      si =>
        vBuilder.clear()
        gBuilder.clear()
        if(vsg.contains(si)){
          vsg(si).foreach({
            case(v,s,g) =>
              vBuilder += v
              gBuilder += g
          })
        }
        s_vindices += vBuilder.result()
        s_genotypes += gBuilder.result()
    })

  }

 def getGenotype(variantID: String, sampleID:String) : Option[Genotype] = {

   val sampleIndex = samplesIndex.getOrElse(sampleID,-1)
   if(sampleIndex < 0){ return None}

   val variantIndex = variantsIndex.getOrElse(variantID,-1)
   if(variantIndex < 0){ return None}

   Range(0,v_sindices(variantIndex).size).foreach({
     case i => if(v_sindices(variantIndex)(i) == sampleIndex){ return Some(Genotype(v_genotypes(variantIndex)(i))) }
   })

   return Some(Genotype(0)) //TODO would be best not to hardcode

  }

 def getAC(variantID: String) : Int ={

   val variantIndex = variantsIndex.getOrElse(variantID,-1)
   if(variantIndex < 0){ return 0}

   v_genotypes(variantIndex).foldLeft(0)({
     case (acc, gt) =>
       val genotype = Genotype(gt)
       if(genotype.isHet){acc +1}
       else if(genotype.isHomVar){acc + 2}
       else{acc}
   })

 }


  /** Compute genotype counts over a pair of variants
    *
    * @param variantID1 ID of the first variant
    * @param variantID2 ID of the second variant
    * @return Counts of individuals with different genotype combinations
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
  def getGenotypeCounts(variantID1: String, variantID2: String) : DenseVector[Int] = {


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

    val v1_gt = getVariant(variantID1)
    val v2_gt = getVariant(variantID2)

    //Add all HomRef/HomRef counts
    gtCounts(0) += this.nSamples - (v1_gt.keys.toSet ++ v2_gt.keys.toSet ).size

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

    return(gtCounts)

  }

 /**def cumulativeAF: Double = {

 variants.aggregate(0.0)({(acc, variant) =>
      //Count the number of called samples and the number of non-ref alleles
      val counts = variant._2.foldLeft((0.0,0.0))({(acc2,g) =>
    g match {
          case GenotypeType.NoCall => (acc2._1, acc2._2 + 1)
          case GenotypeType.Het => (acc2._1 + 1, acc2._2)
          case GenotypeType.HomVar => (acc2._1 + 2, acc2._2)
          case GenotypeType.HomRef => acc2 //This is only here for completeness sake and should never be used
    }
      })
      counts._1/(nSamples - counts._2)
    },
      {(acc1,acc2) => (acc1 + acc2)
    })

 }**/

}
