package is.hail.io.fasta

import is.hail.HailContext
import is.hail.annotations._
import is.hail.expr.{Field, TString, TStruct}
import is.hail.utils._
import is.hail.variant.{AltAllele, Genotype, Variant, VariantMetadata, VariantSampleMatrix, _}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object LoadFasta {

  def apply(hc: HailContext,
    file: String,
    filter_Ns: Boolean = true,
    create_snv_alleles: Boolean = false,
    create_deletion_size: Int = 0,
    create_insertion_size : Int = 0,
    flanking_context: Int = 0) : VariantDataset = {

    if(create_insertion_size > 4)
      fatal("At the moment we capped the max deletion size to 4bp (i.e. 256 alleles per site)")

    val bases = Array("A","C","T","G")
    val hConf = hc.hadoopConf
    val sc = hc.sc

    def getInsertionsSeq(prev_bases : String, additional_length: Int) : Array[String] = {
      if(additional_length < 2)
        bases.map(b => prev_bases + b)
      else
        bases.flatMap(b =>
          getInsertionsSeq( prev_bases + b, additional_length -1) :+ prev_bases + b
        )
    }

    val insertion_alleles = getInsertionsSeq("",create_insertion_size)

    val nAltAlleles =  create_deletion_size +
      insertion_alleles.length +
      (if(create_snv_alleles) 4 else 0)

    def getVariant(contig:String, pos: Int, seq : String, base_pos: Int, max_del_size: Int) : Variant = {
      val base = seq(base_pos).toString

      if(base != 'N') {
        val altAlleles = new ArrayBuffer[AltAllele](initialSize = nAltAlleles)
        val delseq = seq.substring(base_pos + 1, base_pos + max_del_size + 1)
        val ref = base + delseq

        if (create_snv_alleles)
          bases.filter(_ != base)
            .foreach(b => altAlleles += AltAllele(ref, b + delseq))

        if (max_del_size > 0)
          Range(0, delseq.length)
            .foreach(i => altAlleles += AltAllele(ref, ref.substring(0, delseq.length - i)))

        if (create_insertion_size > 0)
          altAlleles ++= insertion_alleles.map(ins => AltAllele(ref, base + ins + delseq))
        Variant(contig, pos, ref, altAlleles.toArray)
      }
      else
        Variant(contig, pos, base, Array.empty[AltAllele])

    }

    info("Loading contigs\n")

    val contigs = hConf.readFile(file) { s =>
      var contig = ""
      var pos = 1
      var flank_size = Math.max(flanking_context, create_deletion_size)
      Source.fromInputStream(s)
        .getLines().foldLeft((new ArrayBuffer[Tuple5[String, Int ,String, String, String]]))({
        case (res, line) =>
          if (line.indexOf('>') == 0) {
            contig = line.split("[\\>\\s]")(1)
            pos = 1
            res
          }
          else {
            if(line.length < flanking_context)
              fatal("Import fasta does not support flanking context larger than the fasta sequence line length.")
            if(line.length < create_deletion_size)
              fatal("Import fasta does not support deletion sizes larger than the fasta sequence line length.")
            pos += line.length
            if (res.length > 0 && contig == res.last._1) {
              res(res.length - 1) = (res.last._1, res.last._2 ,res.last._3, res.last._4, line.take(flank_size))
              res.append((contig, pos, line, res.last._3.takeRight(flank_size), ""))
            }
            else
              res.append((contig, pos, line, "", ""))
            res
          }
      })
    }

    info("Creating VDS\n")

    val rdd = sc.parallelize(contigs)
      .flatMap({
        case (contig, pos, seq, left_flank, right_flank) =>
          val padded_seq = left_flank + seq + right_flank
          Range(left_flank.length, left_flank.length + seq.length)
            .filter(!filter_Ns || padded_seq(_) != 'N')
            .map({
              i =>
                val max_del_size = Math.min(padded_seq.length - i - 1, create_deletion_size)
                if (flanking_context < 1 || i < flanking_context || i > padded_seq.length - flanking_context -1)
                  (getVariant(contig, pos + i - left_flank.length, padded_seq,i, max_del_size ),
                    (Annotation(null), Iterable.empty[Genotype]))
                else
                  (getVariant(contig, pos + i - left_flank.length, padded_seq,i, max_del_size),
                    (Annotation(padded_seq.substring(i - flanking_context, i + flanking_context + 1)),
                      Iterable.empty[Genotype])
                    )
            })
      })

    val vaSignature = if(flanking_context > 0)
      TStruct(
        Array(
          Field("context", TString, 0)
        ))
    else
      TStruct.empty

    info(vaSignature.toString)

    VariantSampleMatrix(
      hc,
      VariantMetadata(
        sampleIds = Array.empty[String],
        sa = Array.empty[Annotation],
        globalAnnotation = Annotation.empty,
        sas = TStruct.empty,
        vas = vaSignature,
        globalSignature = TStruct.empty
      ),
      rdd.toOrderedRDD
    )
  }

}
