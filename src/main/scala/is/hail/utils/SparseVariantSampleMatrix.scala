package is.hail.utils

import breeze.linalg.{DenseVector, sum}
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD
import is.hail.annotations._
import is.hail.variant.{Genotype, Variant, VariantSampleMatrix}

import scala.collection.{immutable, mutable}
import scala.collection.mutable.{ArrayBuffer, ArrayBuilder, Map}
import scala.reflect.ClassTag
import is.hail.expr.{Type, EvalContext, Parser, TStruct}


/**
  * Created by laurent on 4/19/16.
  */

object SparseVariantSampleMatrixRRDBuilder {

  //Given a mapping from a variant and its annotations to use as the key to the resulting PairRDD,
  //Aggregates the data in a SparseSampleVariantMatrix
  def buildByVA[K](vsm: VariantSampleMatrix[Genotype], sc: SparkContext, partitioner : Partitioner)(
    mapOp: (Variant, Annotation)  => K)(implicit uct: ClassTag[K]): RDD[(K, SparseVariantSampleMatrix)] = {

    //Broadcast sample IDs
    val bcSampleIds = sc.broadcast(vsm.sampleIds.map(_.asInstanceOf[String]))

    //Broadcast sample annotations
    val sa = sc.broadcast(vsm.saSignature,vsm.sampleAnnotations)

    vsm.rdd
      .mapPartitions { (it: Iterator[(Variant, (Annotation, Iterable[Genotype]))]) =>
        val gtBuilder = new mutable.ArrayBuilder.ofByte()
        gtBuilder.sizeHint(bcSampleIds.value.size+1) //TODO: Remove when updating to Scala 2.12+
      val siBuilder = new ArrayBuilder.ofInt()
        siBuilder.sizeHint(bcSampleIds.value.size+1) //TODO: Remove when updating to Scala 2.12+
        it.map { case (v, (va, gs)) =>
          gtBuilder.clear()
          siBuilder.clear()
          val sg = gs.iterator.zipWithIndex.foldLeft((siBuilder,gtBuilder))({
            case (acc,(g,i)) => if(!g.isHomRef) (acc._1 += i,  acc._2 += g.gt.getOrElse(-1).toByte) else acc
          })
          (mapOp(v,va), (v.toString,va,siBuilder.result(),gtBuilder.result()))
        }
        //}.aggregateByKey(new SparseVariantSampleMatrix(bcSampleIds.value, newVA, sa.value._1, sa.value._2), partitioner) ( // too slow
      }.aggregateByKey(new SparseVariantSampleMatrix(bcSampleIds.value, vsm.vaSignature), partitioner) (
      { case (svsm, (v,va,sampleIndices,genotypes)) =>
        svsm.addVariant(v,va,sampleIndices,genotypes) },
      { (svsm1,svsm2) => svsm1.merge(svsm2) }).mapValues({svsm =>
      svsm.addSA(sa.value._1,sa.value._2)
      svsm
    })
  }

  //Given a mapping from a variant and its annotations to use as the key to the resulting PairRDD,
  //Aggregates the data in a SparseSampleVariantMatrix
  def buildByVAandSA[K,K2](vsm: VariantSampleMatrix[Genotype], sc: SparkContext, partitioner : Partitioner)(
    mapOp: (Variant, Annotation)  => K, saMapOp: Annotation => K2)(implicit uct: ClassTag[K]): RDD[((K,K2), SparseVariantSampleMatrix)] = {

    //Broadcast sample IDs
    val bcSampleIds = sc.broadcast(vsm.sampleIds.map(_.asInstanceOf[String]))

    //Broadcast sample annotations
    val sa = sc.broadcast(vsm.saSignature,vsm.sampleAnnotations)

    //Create a map of sampleID => saMapOp()
    val saMapBuilder = Map.newBuilder[Int,K2]
    sa.value._2.indices.foreach({
      i => saMapBuilder += ((i,saMapOp(sa.value._2(i))))
    })
    val saMap = saMapBuilder.result()

    vsm.rdd
      .mapPartitions { (it: Iterator[(Variant, (Annotation, Iterable[Genotype]))]) =>
        val gtBuilder = new mutable.ArrayBuilder.ofByte()
        gtBuilder.sizeHint(bcSampleIds.value.size+1) //TODO: Remove when updating to Scala 2.12+
      val siBuilder = new ArrayBuilder.ofInt()
        siBuilder.sizeHint(bcSampleIds.value.size+1) //TODO: Remove when updating to Scala 2.12+
        it.map { case (v, (va, gs)) =>
          val gtBySA = gs.iterator.zipWithIndex.filter({
            case (g,i) => !g.isHomRef}).map({
            case (g,i) => (saMap(i),i,g.gt.getOrElse(-1).toByte)
          }).toList.groupBy({case (sa,i,gt) => sa}).map({
            case (k,ls) =>
              gtBuilder.clear()
              siBuilder.clear()
              ls.foreach(
                {case (sa,i,gt) => siBuilder += i
                  gtBuilder += gt}
              )
              (k,v.toString,va,siBuilder.result(),gtBuilder.result())
          })
          (mapOp(v,va),gtBySA)
        }
      }.flatMap({case (g,ls) =>
      for((sa,v,va,si,gt) <- ls) yield{
        ((g,sa),(v,va,si,gt))
      }
    }).aggregateByKey(new SparseVariantSampleMatrix(bcSampleIds.value, vsm.vaSignature), partitioner) ( //TODO: Fix this if useful
      { case (svsm, (v,va,sampleIndices,genotypes)) =>
        svsm.addVariant(v,va,sampleIndices,genotypes) },
      { (svsm1,svsm2) => svsm1.merge(svsm2) })
      .mapValues({svsm =>
        svsm.addSA(sa.value._1,sa.value._2)
        svsm
      })
  }

}

class SparseVariantSampleMatrix(val sampleIDs: IndexedSeq[String], val vaSignature:Type = TStruct.empty) extends Serializable {

  private var saSignature: Type = TStruct.empty
  private var sampleAnnotations: IndexedSeq[Annotation] = IndexedSeq[Annotation]()

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

  def addSA(signature: Type = TStruct.empty,annotations: IndexedSeq[Annotation] = IndexedSeq[Annotation]()) = {
    saSignature = signature
    sampleAnnotations = annotations
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

  def queryVA(code: String): (Type, Querier) = {

    val st = immutable.Map(Annotation.VARIANT_HEAD ->(0, vaSignature))
    val ec = EvalContext(st)
    val a = ec.a

    val (t, f) = Parser.parseExpr(code, ec)

    val f2: Annotation => Any = { annotation =>
      a(0) = annotation
      f()
    }

    (t, f2)
  }

  def querySA(code: String): (Type, Querier) = {

    val st = immutable.Map(Annotation.SAMPLE_HEAD ->(0, saSignature))
    val ec = EvalContext(st)
    val a = ec.a

    val (t, f) = Parser.parseExpr(code, ec)

    val f2: Annotation => Any = { annotation =>
      a(0) = annotation
      f()
    }

    (t, f2)
  }

  def getSampleAnnotation(sampleID: String, annotation: String): Option[Any] ={
    getSampleAnnotation(samplesIndex(sampleID), querySA(annotation)._2)
  }

  def getSampleAnnotation(sampleID: String, querier: Querier): Option[Any] ={
    getSampleAnnotation(samplesIndex(sampleID),querier)
  }

  def getSampleAnnotation(sampleIndex: Int, querier: Querier): Option[Any] ={
    val sa = querier(sampleAnnotations(sampleIndex))
    sa match {
      case null => None
      case x => Some(x)
    }
  }

  def getSampleAnnotations(sampleID: String) : Annotation = {
    sampleAnnotations(samplesIndex(sampleID))
  }

  def getVariantAnnotation(variantID: String, annotation: String) : Option[Any] = {
    getVariantAnnotation(variantsIndex(variantID), queryVA(annotation)._2)
  }

  def getVariantAnnotation(variantID: String, querier: Querier) : Option[Any] = {
    getVariantAnnotation(variantsIndex(variantID), querier)
  }

  def getVariantAnnotation(variantIndex: Int, querier: Querier) : Option[Any] = {
    val va = querier(variantsAnnotations(variantIndex))
    va match {
      case null => None
      case x => Some(x)
    }
  }

  def getVariantAnnotations(variantID: String) : Annotation = {
    variantsAnnotations(variantsIndex(variantID))
  }

  //Applies an operation on each sample
  //The map operation accesses the sample name and index and arrays of variant ID and genotypes.
  def mapSamples[R](mapOp: (String, Int, Array[Int],Array[Byte]) => R)(implicit uct: ClassTag[R]): Array[R] = {

    //Build samples view if not created yet
    if(s_vindices.isEmpty){ buildSampleView() }

    sampleIDs.zipWithIndex.map({
      case(s,si) =>
        mapOp(s, si, s_vindices(si),s_genotypes(si))
    }).toArray

  }

  //Applies an operation on each variant
  //The map operation accesses the variant name and index and arrays of sample Index and genotypes.
  def mapVariants[R](mapOp: (String, Int, Array[Int],Array[Byte]) => R)(implicit uct: ClassTag[R]): Array[R] = {

    variants.zipWithIndex.map({
      case(v,vi) =>
        mapOp(v, vi, v_sindices(vi),v_genotypes(vi))
    }).toArray

  }

  def foreachSample(f: (String, Int, Array[Int],Array[Byte]) => Unit) : Unit = {

    //Build samples view if not created yet
    if(s_vindices.isEmpty){ buildSampleView() }

    sampleIDs.zipWithIndex.foreach({
      case(s,si) =>
        f(s, si, s_vindices(si),s_genotypes(si))
    })

  }

  def foreachVariant(f: (String, Int, Array[Int],Array[Byte]) => Unit): Unit = {

    variants.zipWithIndex.foreach({
      case(v,vi) =>
        f(v, vi, v_sindices(vi),v_genotypes(vi))
    })

  }

  def foldLeftVariant[R](z: R)(op: (R,(String, Int, Array[Int],Array[Byte])) => R)(implicit uct: ClassTag[R]): R = {

    variants.zipWithIndex.foldLeft(z)({
      case(agg,(v,vi)) =>
        op(agg,(v, vi, v_sindices(vi),v_genotypes(vi)))
    })

  }

  //Computes the variant pairs that exist in the samples and returns them
  def getExistingVariantPairs() : Set[(String,String)] = {

    //Stores whether each variant pair was observed
    val result = mutable.HashSet[(Int,Int)]()

    //Go through each of the samples and fill the matrix
    if(s_vindices.isEmpty){ buildSampleView() }

    //Creates the sets of variants
    val s_variantBuilder = new ArrayBuilder.ofInt()
    s_variantBuilder.sizeHint(variants.size+1) //TODO: Remove then updating to Scala 2.12+

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
              v2i => result.add(s_variants(v1i),s_variants(v2i))
            })
        })
    })
    result.toSet.map{case (v1i, v2i) => (variants(v1i), variants(v2i))}
  }


  private def buildSampleView() = {



    //Loop through all variants and collect (variant, samples, genotype) then groupBy sample
    // and add variant/genotype info
    val vsg = (for( v <-Range(0,v_sindices.size); i <- Range(0,v_sindices(v).size)) yield {
      (v,v_sindices(v)(i),v_genotypes(v)(i))
    }).groupBy({case (vindex,sindex,gt) => sindex})

    val vBuilder = new ArrayBuilder.ofInt
    vBuilder.sizeHint(variants.size+1) //TODO: Remove then updating to Scala 2.12+
    val gBuilder = new ArrayBuilder.ofByte
    gBuilder.sizeHint(variants.size+1) //TODO: Remove then updating to Scala 2.12+

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
    val v1_gts = variantsIndex.get(variantID1) match {
      case Some(vi) => v_sindices(vi).zip(v_genotypes(vi)).sortBy({case (s,gt) => s})
      case None => Array.empty[(Int,Byte)]
    }
    val v2_gts = variantsIndex.get(variantID2) match {
      case Some(vi) => v_sindices(vi).zip(v_genotypes(vi)).sortBy({case (s,gt) => s})
      case None => Array.empty[(Int,Byte)]
    }
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

  def getSingletons() : Set[String] = {

    v_genotypes.zipWithIndex.foldLeft(mutable.Set[String]())({
      case (res,(gts,vi)) =>
        if (gts.filter(_ == 1).length == 1)
          res += variants(vi)
        else
          res
    }).toSet

  }

}
