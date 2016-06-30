package org.broadinstitute.hail.methods

import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.check._
import org.broadinstitute.hail.check.Prop._
import org.testng.annotations.Test
import org.broadinstitute.hail.variant._
import org.broadinstitute.hail.driver._
import scala.io.Source
import sys.process._
import scala.language._

class SkatoSuite extends SparkSuite {

  def dichotomousPhenotype(nSamples: Int) = Gen.buildableOfN(nSamples, Gen.option(Gen.arbBoolean, 0.95))
  def quantitativePhenotype(nSamples: Int) = Gen.buildableOfN(nSamples, Gen.option(Gen.choose(-0.5, 0.5), 0.95))
  def covariateMatrix(nSamples: Int, numCovar: Int) = Gen.buildableOfN(numCovar, quantitativePhenotype(nSamples))

  object Spec extends Properties("SKAT-O") {
    val compGen = for (vds: VariantDataset <- VariantSampleMatrix.gen[Genotype](sc, Genotype.gen _);
                       blockSize: Int <- Gen.choose(1, 1000);
                       quantitative: Boolean <- Gen.arbBoolean;
                       nCovar: Int <- Gen.choose(0, 5);
                       nGroups: Int <- Gen.choose(1, 50);
                       y <- if (quantitative) quantitativePhenotype(vds.nSamples) else dichotomousPhenotype(vds.nSamples);
                       x <- covariateMatrix(vds.nSamples, nCovar)) yield (vds, blockSize, quantitative, nCovar, nGroups, y, x)


    property("Hail groups give same result as plink input") =
      forAll(compGen) { case (vds: VariantSampleMatrix[Genotype], blockSize: Int, quantitative: Boolean, nCovar: Int, nGroups: Int, y, x) =>

        // Export PLINK
        val plinkRoot = tmpDir.createTempFile("plinkOutput")
        var s = State(sc, sqlContext, vds.filterVariants((v, va, gs) => v.toString.length < 50)) //hack because SKAT-O won't accept snp ids with > 50 characters
        s = SplitMulti.run(s, Array.empty[String])
        s = ExportPlink.run(s, Array("-o", plinkRoot))

        if (s.vds.nSamples == 0 || s.vds.nVariants == 0) //skat-o from plink files will fail if 0 variants or samples
          true
        else {

          println(s"nVariants=${s.vds.nVariants} nSamples=${s.vds.nSamples}")
          // assign variants to groups
          val groups = s.vds.variants.zipWithIndex.map { case (v, i) =>
            val groupNum = i % nGroups
            (v, groupNum)
          }.collect()

          // Write set ID list
          val setID = tmpDir.createTempFile("setID", ".txt")
          writeTextFile(setID, sc.hadoopConfiguration) { w =>
            groups.foreach { case (v, i) => w.write(s"group_$i" + "\t" + v.toString + "\n")
            }
          }

          // Write covar file
          val covarFile = tmpDir.createTempFile("covar", ".txt")
          writeTextFile(covarFile, sc.hadoopConfiguration) { w =>
            s.vds.sampleIds.zipWithIndex.foreach { case (s, i) =>
              val sb = new StringBuilder()
              sb.append("0")
              sb.append("\t")
              sb.append(s)

              for (j <- 0 until nCovar) {
                sb.append("\t")
                sb.append(x(j)(i).getOrElse(-9))
              }
              sb.append("\n")
              w.write(sb.result())
            }
          }

          // Write phenotype into fam file
          writeTextFile(plinkRoot + ".fam", sc.hadoopConfiguration) { w =>
            s.vds.sampleIds.zipWithIndex.foreach { case (s, i) =>
              val sb = new StringBuilder()
              sb.append("0")
              sb.append("\t")
              sb.append(s)
              sb.append("\t")
              sb.append("0")
              sb.append("\t")
              sb.append("0")
              sb.append("\t")
              sb.append("0")
              sb.append("\t")
              sb.append(y(i).map(_.toString) match {
                case None => -9
                case Some("true") => 2
                case Some("false") => 1
                case Some(x) => x
              })
              sb.append("\n")
              w.write(sb.result())
            }
          }

          val ssd = plinkRoot + ".SSD"
          val info = plinkRoot + ".info"
          val resultsFile = plinkRoot + ".results"

          val yType = if (quantitative) "C" else "D"

          // Run SKAT-O from PLINK file
          val plinkSkatExitCode = s"Rscript src/test/resources/skatoPlinkFiles.R $plinkRoot $covarFile $setID $yType $ssd $info $resultsFile" !

          val plinkResults = sc.parallelize(readLines(resultsFile, sc.hadoopConfiguration) { lines =>
            lines.map { l => l.transform { line =>
              val Array(groupName, pValue, nMarkers, nMarkersTested) = line.value.split( """\s+""")

              (groupName, (pValue, nMarkers, nMarkersTested))
            }
            }.toArray
          })


          // Annotate samples with covariates and phenotype
          val sampleAnnotations = tmpDir.createTempFile("sampleAnnotations", ".tsv")
          writeTextFile(sampleAnnotations, sc.hadoopConfiguration) { w =>
            val sb = new StringBuilder()
            sb.append("Sample\tPhenotype")
            for (j <- 0 until nCovar) {
              sb.append(s"\tC$j")
            }
            sb.append("\n")
            w.write(sb.result())

            s.vds.sampleIds.zipWithIndex.foreach { case (s, i) =>
              val sb = new StringBuilder()
              sb.append(s)
              sb.append("\t")
              sb.append(y(i).getOrElse("NA"))

              for (j <- 0 until nCovar) {
                sb.append("\t")
                sb.append(x(j)(i).getOrElse("NA"))
              }
              sb.append("\n")
              w.write(sb.result())
            }
          }

          val sb = new StringBuilder()
          if (!quantitative)
            sb.append("Phenotype: Boolean")
          else
            sb.append("Phenotype: Double")

          for (j <- 0 until nCovar) {
            sb.append(s",C$j: Double")
          }
          val typeString = sb.result()

          s = AnnotateSamplesTable.run(s, Array("-i", sampleAnnotations, "-r", "sa.pheno", "-t", typeString))

          // Annotate variants with group names
          val variantAnnotations = tmpDir.createTempFile("variantAnnotations", ".tsv")
          writeTextFile(variantAnnotations, sc.hadoopConfiguration) { w =>
            w.write("Variant\tGroup\n")
            groups.foreach { case (v, i) => w.write(v.toString + "\t" + s"group_$i\n")
            }
          }

          s = AnnotateVariants.run(s, Array("table", variantAnnotations, "-r", "va.groups", "-v", "Variant"))

          // Run SKAT-O command
          val configSkato = tmpDir.createTempFile("configSkato")
          writeTextFile(configSkato, sc.hadoopConfiguration) { w =>
            w.write("hail.skato.R Rscript\n")
            w.write("hail.skato.script /Users/jigold/hail/src/test/resources/testSKATO.r\n")
          }

          val hailOutputFile = tmpDir.createTempFile("hailResults", ".tsv")

          if (!quantitative)
            s = GroupTestSKATO.run(s, Array("-k", "va.groups.Group", "--block-size", blockSize.toString, "-y", "sa.pheno.Phenotype", "--config", configSkato, "-o", hailOutputFile))
          else
            s = GroupTestSKATO.run(s, Array("-k", "va.groups.Group", "--block-size", blockSize.toString, "-y", "sa.pheno.Phenotype", "--config", configSkato, "-o", hailOutputFile, "-q"))


          val hailResults = sc.parallelize(readLines(hailOutputFile, sc.hadoopConfiguration) { lines =>
            lines.drop(1)
            lines.map { l => l.transform { line =>
              val Array(groupName, pValue, pValueResampling, nMarkers, nMarkersTested) = line.value.split( """\s+""")
              (groupName, (pValue, nMarkers, nMarkersTested))
            }
            }.toArray
          })

          val result = plinkResults.fullOuterJoin(hailResults).map { case (g, (p, h)) =>

            val p2 = p match {
              case None => (Double.NaN, 0, 0)
              case Some(x) => (if (x._1 == "null") Double.NaN else x._1.toDouble, x._2.toInt, x._3.toInt)
            }

            val h2 = h match {
              case None => (Double.NaN, 0, 0)
              case Some(x) => (if (x._1 == "null") Double.NaN else x._1.toDouble, x._2.toInt, x._3.toInt)
            }

            if (p2 == h2)
              true
            else if (math.abs(p2._1 - h2._1) < 1e-3 && p2._2 == h2._2 && p2._3 == h2._3)
                true
            else {
                println(s"PValue: plink=${p2._1} hail=${h2._1}")
                println(s"nMarkers: plink=${p2._2} hail=${h2._2}")
                println(s"nMarkersTested: plink=${p2._3} hail=${h2._3}")
                false
            }
          }.fold(true)(_ & _)

          result
        }
      }
  }

  @Test def testSKATO() {
    Spec.check()
  }

}
