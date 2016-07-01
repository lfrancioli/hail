package org.broadinstitute.hail.utils

import breeze.linalg.{CSCMatrix, DenseVector, sum}
import org.apache.avro.SchemaBuilder.MapBuilder
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
        gtBuilder.sizeHint(bcSampleIds.value.size+1) //TODO: Remove when updating to Scala 2.12+
        val siBuilder = new ArrayBuilder.ofInt()
        siBuilder.sizeHint(bcSampleIds.value.size+1) //TODO: Remove when updating to Scala 2.12+
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
        gtBuilder.sizeHint(bcSampleIds.value.size+1) //TODO: Remove when updating to Scala 2.12+
        val siBuilder = new ArrayBuilder.ofInt()
        siBuilder.sizeHint(bcSampleIds.value.size+1) //TODO: Remove when updating to Scala 2.12+
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

  //Given a mapping from a variant and its annotations to use as the key to the resulting PairRDD,
  //Aggregates the data in a SparseSampleVariantMatrix
  def buildByAnnotationWithSA[K,K2](vsm: VariantSampleMatrix[Genotype], sc: SparkContext, partitioner : Partitioner, variantAnnotations : Array[String], sampleAnnotations: Array[String] = Array[String]())(
    mapOp: (Variant, Annotation)  => K, saMapOp: Annotation => K2)(implicit uct: ClassTag[K]): RDD[((K,K2), SparseVariantSampleMatrix)] = {

    /**if(variantAnnotations.isEmpty){
      * return buildByAnnotation(vsm,sc,partitioner,sampleAnnotations)(mapOp)
      * }**/ //TODO Might be worth implementing

    //Broadcast sample IDs
    val bcSampleIds = sc.broadcast(vsm.sampleIds)

    //Build sample annotations
    val sa = sc.broadcast(buildSamplesAnnotations(vsm,sampleAnnotations))

    //Create a map of sampleID => saMapOp()
    val saMap = Map.newBuilder[Int,K2]
      sa.value._2.indices.foreach({
        i => saMap += ((i,saMapOp(sa.value._2(i))))
      })
    val saMapbc = sc.broadcast(saMap.result())


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
        gtBuilder.sizeHint(bcSampleIds.value.size+1) //TODO: Remove when updating to Scala 2.12+
        val siBuilder = new ArrayBuilder.ofInt()
        siBuilder.sizeHint(bcSampleIds.value.size+1) //TODO: Remove when updating to Scala 2.12+
        it.map { case (v, va, gs) =>
          val reducedVA = queriers.value.map({qa => qa(va)})
          val gtBySA = gs.iterator.zipWithIndex.filter({
            case (g,i) => !g.isHomRef}).map({
            case (g,i) => (saMapbc.value(i),i,g.gt.getOrElse(-1).toByte)
          }).toList.groupBy({case (sa,i,gt) => sa}).map({
            case (k,ls) =>
              gtBuilder.clear()
              siBuilder.clear()
             ls.foreach(
               {case (sa,i,gt) => siBuilder += i
               gtBuilder += gt}
             )
              (k,v.toString,reducedVA,siBuilder.result(),gtBuilder.result())
            })
          (mapOp(v,va),gtBySA)
        }
      }.flatMap({case (g,ls) =>
        for((sa,v,va,si,gt) <- ls) yield{
          ((g,sa),(v,va,si,gt))
        }
      }).aggregateByKey(new SparseVariantSampleMatrix(bcSampleIds.value, newVAbc.value, sa.value._1, sa.value._2), partitioner) (
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

  //Computes the variant pairs that exist in the samples and returns them
  def getExistingVariantPairs() : Set[(String,String)] = {

    //Stores whether each variant pair was observed
    val result = mutable.HashSet[(String,String)]()

    //Go through each of the samples and fill the matrix
    if(s_vindices.isEmpty){ buildSampleView() }

    //Creates the sets of variants
    val s_variantBuilder = new ArrayBuilder.ofInt()
    s_variantBuilder.sizeHint(variants.size) //TODO: Remove then updating to Scala 2.12+

    sampleIDs.indices.foreach({
      si =>
        //Get all variants with het/homvar genotype
        s_variantBuilder.clear()
        s_vindices(si).indices.foreach({
          vi => if(s_genotypes(si)(vi) > -1) { s_variantBuilder += s_vindices(si)(vi) }
        })

        //Computes all pairs of variants
        val s_variants = s_variantBuilder.result()
        s_variants.indices.foreach({
          v1i =>
            Range(v1i+1,s_variants.size).foreach({
              v2 => result.add(variants(s_variants(v1i)),variants(s_variants(v2)))
            })
        })
    })
    result.toSet
  }


  private def buildSampleView() = {



    //Loop through all variants and collect (variant, samples, genotype) then groupBy sample
    // and add variant/genotype info
    val vsg = (for( v <-Range(0,v_sindices.size); i <- Range(0,v_sindices(v).size)) yield {
      (v,v_sindices(v)(i),v_genotypes(v)(i))
    }).groupBy({case (vindex,sindex,gt) => sindex})

    val vBuilder = new ArrayBuilder.ofInt
    vBuilder.sizeHint(variants.size) //TODO: Remove then updating to Scala 2.12+
    val gBuilder = new ArrayBuilder.ofByte
    gBuilder.sizeHint(variants.size) //TODO: Remove then updating to Scala 2.12+

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

    val gtCounts = new DenseVector(new Array[Int](9))
    var n_noCalls = 0

    def addCount(gt1: Byte, gt2:Byte): Unit ={
      if(gt1 < 0 || gt2 < 0){
        n_noCalls += 1
      }
      else{
        gtCounts(gt1 + 3*gt2) +=1
      }
    }

    //Get sample/genotype pairs sorted by sample
    val v1_gts = v_sindices(variantsIndex(variantID1)).zip(v_genotypes(variantsIndex(variantID1))).sortBy({case (s,gt) => s})
    val v2_gts = v_sindices(variantsIndex(variantID2)).zip(v_genotypes(variantsIndex(variantID2))).sortBy({case (s,gt) => s})
    var v2_index = 0

    //Add all gt1 counts
    v1_gts.foreach({
      case(s1,gt1) =>
        //first samples for v2 is the same
        while(v2_index < v2_gts.size && v2_gts(v2_index)._1 < s1){
          addCount(0,v2_gts(v2_index)._2)
          v2_index += 1
        }
        if(v2_index < v2_gts.size && v2_gts(v2_index)._1 == s1){
          addCount(gt1,v2_gts(v2_index)._2)
          v2_index += 1
        }else{
          addCount(gt1,0)
        }
    })

    //Add all remaining gt2 counts
    Range(v2_index,v2_gts.size).foreach(
      i => addCount(0, v2_gts(i)._2 )
    )

    //Add all HomRef/HomRef counts
    gtCounts(0) += this.nSamples - n_noCalls - sum(gtCounts)

    return(gtCounts)

  }
}
