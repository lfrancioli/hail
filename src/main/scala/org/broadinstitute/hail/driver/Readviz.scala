package org.broadinstitute.hail.driver

import org.broadinstitute.hail.utils._
import org.kohsuke.args4j.{Option => Args4jOption}
import org.broadinstitute.hail.variant.{Genotype, Variant}

import scala.collection.mutable.ArrayBuffer

object Readviz extends Command {

  def name = "readviz"

  def description = "Get readviz data"

  class Options extends BaseOptions {
      @Args4jOption(required = false, name = "--annotations",
      usage = "Annotations to output, e.g. bam=sa.meta.bam, gvcf=sa.meta.gvcf")
      var ann: String = _

    @Args4jOption(required = true, name = "--isMale-annotation",
      usage = "Annotation storing if sample is male [Boolean]")
    var isMaleAnn: String = _

    @Args4jOption(required = true, name = "-o", aliases = Array("--output"),
      usage = "path of output tsv")
    var output: String = _

  }

  def newOptions = new Options

  def supportsMultiallelic = true

  def requiresVDS = true

  def run(state: State, options: Options): State = {

    val vds = state.vds
    val sc = state.sc

    val annotations = options.ann.split(",").map(_.split("=").map(_.trim))
    val sampleannotations = sc.broadcast(
      vds.sampleAnnotations.map({ sa =>
        annotations.map({ a =>
          val querier = vds.querySA(a(1))._2
          querier(sa).getOrElse("NA").toString
        })
      })
    )

    val isMaleA = vds.querySA(options.isMaleAnn)._2
    val males = sc.broadcast(vds.sampleAnnotations.map({isMaleA(_)})
       .zipWithIndex
      .filter({case (x,i) => x.isDefined && x.get.asInstanceOf[Boolean]})
      .map(_._2).toSet)

    val sampleIDs = sc.broadcast(vds.sampleIds)

    def getHeader() : String = {
        "chrom\tpos\tref\talt\tgt\tsid\t" +
        annotations.map(_(0)).mkString("\t") + "\t" +
      "rank\tgq\tdp\tdp_ref\tdp_alt"
    }

    def getLines(v: Variant, altAlleleIndex: Int, gtString: String, genotypes: List[(Genotype, Int)]) : String = {

      val variantString = v.contig + "\t" + v.start + "\t" + v.ref + "\t" + v.altAlleles(altAlleleIndex).alt
      val x =genotypes.zipWithIndex.map({
        case(((g,i),n)) =>
        variantString + "\t" +
          gtString + "\t" +
          sampleIDs.value(i) + "\t" +
          sampleannotations.value(i).mkString("\t") + "\t" +
          + n + "\t" +
          g.gq.getOrElse(-1) + "\t" +
          g.dp.getOrElse(-1) + "\t" +
          (if(g.ad.isDefined){
            g.ad.get.zipWithIndex.filter(a => a._2 == 0 || a._2 == altAlleleIndex+1).map(_._1).mkString("\t")
          }else{
            "\t"
          })
      })

      x.mkString("\n")
    }

    vds.rdd.map({
      case (v,(va, gs)) =>

        val ab = new ArrayBuffer[String](10)

        //Get the non-ref genotypes sorted by GQ
        val gtest = gs.filter(_.isCalledNonRef).toList
        val genotypes = gs.zipWithIndex
          .filter({case (g,i) => g.isCalledNonRef && g.gq.getOrElse(0) > 19 && g.dp.getOrElse(0) > 9})
          .toList
          .sortBy({case(g,i) => g.gq}).reverse

        //Output each allele
        v.altAlleles.indices.filter(v.altAlleles(_).alt != "*").map({
          vi =>
            ab.clear()

            val genotypesa = genotypes.filter({
              case(g,i) =>
                val gtPair = Genotype.gtPair(g.gt.get)
                gtPair.j == vi+1 || gtPair.k == vi+1
            })

            //Print autosomes
            if (v.isAutosomalOrPseudoAutosomal) {
              //hets
              ab.append(
                getLines(v,
                  vi,
                  "het",
                  genotypesa.filter(_._1.isHet)
                    .take(10)
                ))
              //homs
              ab.append(
                getLines(v,
                  vi,
                  "hom",
                  genotypesa.filter(_._1.isHomVar)
                    .take(10)
                ))
            }
            //Sex chromosomes
            else {
              //hets and homs on female X non PAR
              if(v.inXNonPar) {
                ab.append(
                  getLines(v,
                    vi,
                    "het",
                    genotypesa.filter(x => x._1.isHet && !males.value.contains(x._2))
                      .take(10)
                  ))
                ab.append(
                  getLines(v,
                    vi,
                    "hom",
                    genotypesa.filter(x => x._1.isHomVar && !males.value.contains(x._2))
                      .take(10)
                  ))
              }
              //hemi on male X non PAR and Y non PAR
              ab.append(
                getLines(v,
                  vi,
                  "hemi",
                  genotypesa
                    .filter(x => (x._1.isHomVar) && males.value.contains(x._2))
                    .take(10)
                ))
            }
            ab.result().filter(!_.isEmpty).mkString("\n")
        }).filter(!_.isEmpty).mkString("\n")
    }).writeTable(options.output,header = Some(getHeader()))

    state
  }
}