package org.broadinstitute.hail.driver

import org.apache.spark.{HashPartitioner}
import org.broadinstitute.hail.methods.{GeneBurden}
import org.broadinstitute.hail.variant.{VariantDataset, VariantSampleMatrix}
import org.kohsuke.args4j.{Option => Args4jOption}
import org.broadinstitute.hail.utils.SparseVariantSampleMatrix


/**
  * Created by laurent on 4/8/16.
  */
object RecessiveStatsCommand extends Command {

    def name = "recessive stats"

    def description = "Computes the a per-gene Poisson test on enrichment for rare functional variation give a set of cases and controls"
    override def supportsMultiallelic = true

    class Options extends BaseOptions {
      @Args4jOption(required = true, name = "-i", aliases = Array("--controls"), usage = "Input controls .vds file")
      var controls_input: String = _
      @Args4jOption(required = true, name = "-a", aliases = Array("--gene_annotation"), usage = "Annotation storing the gene information for aggregation")
      var gene_annotation: String = _
      @Args4jOption(required = false, name = "-cg", aliases = Array("--condition_genotypes"), usage = "Expression for filtering genotypes")
      var cg : String = ""
      @Args4jOption(required = false, name = "-cv", aliases = Array("--condition_variants"), usage = "Expression for filtering variants (non-filtered variants are considered functional)")
      var cv : String = ""
      @Args4jOption(required = false, name = "-ai", aliases = Array("--affected_intervals"), usage = "BED interval file for affected individuals to be considered in the test (i.e. well covered, etc.)")
      var ai : String = ""
      @Args4jOption(required = false, name = "-ui", aliases = Array("--unaffected"), usage = "BED interval for unaffected individuals to be considered in the test (i.e. well covered, etc.)")
      var ui : String = ""
    }

    def newOptions = new Options

    def filterVDS(state: State, options: Options) : VariantDataset = {

      var filteredState = state
      if(options.cg.nonEmpty) {
        filteredState = FilterGenotypes.run(state, Array("-c", options.cg))
      }

      if(options.cv.nonEmpty){
        filteredState = FilterVariants.run(state,Array("-c",options.cv))
      }

      return filteredState.vds
    }





    def run(state: State, options: Options): State = {

      /**val casesVDS = filterVDS(state,options)
      val controlsVDS = filterVDS(State(state.sc,state.sqlContext,VariantSampleMatrix.read(state.sqlContext, options.controls_input)),options)

      val casesGeneAnn = casesVDS.queryVA(options.gene_annotation)._2
      val controlsGeneAnn = controlsVDS.queryVA(options.gene_annotation)._2

      val partitioner = new HashPartitioner(200)

      val casesRDD = casesVDS.aggregateByAnnotation(partitioner,new SparseVariantSampleMatrix(casesVDS.sampleIds))({
        case(counter,v,va,s,sa,i,g) =>
          counter.addGenotype(v.toString(),i,g)},{
        case(c1,c2) => c1.merge(c2)},
        {case (v,va) => casesGeneAnn(va)}
      )

      val controlsRDD = controlsVDS.aggregateByAnnotation(partitioner,new SparseVariantSampleMatrix(controlsVDS.sampleIds))({
        case(counter,v,va,s,sa,i,g) =>
          counter.addGenotype(v.toString(),i,g)},{
        case(c1,c2) => c1.merge(c2)},
        {case (v,va) => controlsGeneAnn(va)}
      )

      val callsByGene = casesRDD.join(controlsRDD)

        callsByGene.map({case (gene, (cases,controls)) =>
          (gene,new GeneBurden(cases,controls))
        })
  **/
      state
    }
}
