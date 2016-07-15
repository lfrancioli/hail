package org.broadinstitute.hail.stats

import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.variant.VariantDataset
import org.broadinstitute.hail.variant._
import org.broadinstitute.hail.driver._
import org.testng.annotations.Test
import org.broadinstitute.hail.check.Gen._
import org.broadinstitute.hail.check.Prop._
import org.broadinstitute.hail.check.Properties
import scala.language.postfixOps
import scala.sys.process._
import org.broadinstitute.hail.Utils._
class FisherExactTestSuite extends SparkSuite {

  @Test def testPvalue() {
    val N = 200
    val K = 100
    val k = 10
    val n = 15
    val a = 5
    val b = 10
    val c = 95
    val d = 90

    val result = FisherExactTest(a, b, c, d).map(_.getOrElse(Double.NaN))

    assert(math.abs(result(0) - 0.2828) < 1e-4)
    assert(math.abs(result(1) - 0.4754059) < 1e-4)
    assert(math.abs(result(2) - 0.122593) < 1e-4)
    assert(math.abs(result(3) - 1.597972) < 1e-4)
  }

  object Spec extends Properties("FisherExactTest") {
    val twoBytwoMatrix = for (n: Int <- choose(10, 500); k: Int <- choose(1, n - 1); x: Int <- choose(1, n - 1)) yield (k, n - k, x, n - x)

    property("import generates same output as export") =
      forAll(twoBytwoMatrix) { case (a, b, c, d) =>

        val rResultTwoSided = s"Rscript src/test/resources/fisherExactTest.r two.sided $a $b $c $d" !!

        val rResultLess = s"Rscript src/test/resources/fisherExactTest.r less $a $b $c $d" !!

        val rResultGreater = s"Rscript src/test/resources/fisherExactTest.r greater $a $b $c $d" !!


        val rTwoSided = rResultTwoSided.split(" ").take(4)
          .map {s => if (s == "Inf") Double.PositiveInfinity else if (s == "NaN") Double.NaN else s.toDouble }
        val rLess = rResultLess.split(" ").take(4)
          .map {s => if (s == "Inf") Double.PositiveInfinity else if (s == "NaN") Double.NaN else s.toDouble }
        val rGreater = rResultGreater.split(" ").take(4)
          .map {s => if (s == "Inf") Double.PositiveInfinity else if (s == "NaN") Double.NaN else s.toDouble }

        val hailTwoSided = FisherExactTest(a, b, c, d, alternative = "two.sided")
        val hailLess = FisherExactTest(a, b, c, d, alternative = "less")
        val hailGreater = FisherExactTest(a, b, c, d, alternative = "greater")

        val hailResults = Array(hailTwoSided, hailLess, hailGreater).map {
          _.map {
            _.getOrElse(Double.NaN)
          }
        }
        val rResults = Array(rTwoSided, rLess, rGreater)

        hailResults.zip(rResults).forall { case (h, r) =>
          val res = D_==(h(0), r(0)) &&
            D_==(h(1), h(1)) &&
            D_==(h(2), r(2)) &&
            D_==(h(3), r(3))
          if (!res) {
            println(h(0), r(0), D_==(h(0), r(0)))
            println(h(1), r(1), D_==(h(1), h(1)))
            println(h(2), r(2), D_==(h(2), r(2)))
            println(h(3), r(3), D_==(h(3), r(3)))
          }
          res
        }
      }

    property("expr gives same result as class") =
      forAll(VariantSampleMatrix.gen[Genotype](sc, Genotype.gen _)) { (vds: VariantDataset) =>
        var s = State(sc, sqlContext, vds)
        val sampleIds = vds.sampleIds
        val phenotypes = sampleIds.zipWithIndex.map{case (sample, i) =>
          if (i % 3 == 0)
            (sample, "ADHD")
          else if (i % 3 == 1)
            (sample, "Control")
          else
            (sample, "NA")
        }

        val phenotypeFile = tmpDir.createTempFile("phenotypeAnnotation",".txt")
        writeTextFile(phenotypeFile, sc.hadoopConfiguration){case w =>
          w.write("Sample\tPheno1\n")
          phenotypes.foreach{case (sample, p) => w.write(s"$sample\t$p\n")}
        }

        s = AnnotateSamplesTable.run(s, Array("-i", phenotypeFile, "-r", "sa.pheno"))

        s = AnnotateVariantsExpr.run(s, Array("-c", """va.macCase = gs.count(sa.pheno.Pheno1 == "ADHD" && g.isHet) + 2 * gs.count(sa.pheno.Pheno1 == "ADHD" && g.isHomVar)"""))
        s = AnnotateVariantsExpr.run(s, Array("-c", """va.majCase = gs.count(sa.pheno.Pheno1 == "ADHD" && g.isHet) + 2 * gs.count(sa.pheno.Pheno1 == "ADHD" && g.isHomRef)"""))
        s = AnnotateVariantsExpr.run(s, Array("-c", """va.macControl = gs.count(sa.pheno.Pheno1 == "Control" && g.isHet) + 2 * gs.count(sa.pheno.Pheno1 == "ADHD" && g.isHomVar)"""))
        s = AnnotateVariantsExpr.run(s, Array("-c", """va.majControl = gs.count(sa.pheno.Pheno1 == "Control" && g.isHet) + 2 * gs.count(sa.pheno.Pheno1 == "ADHD" && g.isHomRef)"""))

        s = AnnotateVariantsExpr.run(s, Array("-c", """va.fet = fet(va.macCase.toInt, va.majCase.toInt, va.macControl.toInt, va.majControl.toInt)"""))


        val (_, q1) = s.vds.queryVA("va.macCase")
        val (_, q2) = s.vds.queryVA("va.majCase")
        val (_, q3) = s.vds.queryVA("va.macControl")
        val (_, q4) = s.vds.queryVA("va.majControl")
        val (_, q5) = s.vds.queryVA("va.fet.pValue")
        val (_, q6) = s.vds.queryVA("va.fet.oddsRatio")
        val (_, q7) = s.vds.queryVA("va.fet.ci95Lower")
        val (_, q8) = s.vds.queryVA("va.fet.ci95Upper")

        s.vds.variantsAndAnnotations.map{case (v, va) =>
          val result = FisherExactTest(q1(va).get.asInstanceOf[Long].toInt, q2(va).get.asInstanceOf[Long].toInt,
            q3(va).get.asInstanceOf[Long].toInt, q4(va).get.asInstanceOf[Long].toInt)
          val annotationResult = Array(q5(va).asInstanceOf[Option[Double]], q6(va).asInstanceOf[Option[Double]],
            q7(va).asInstanceOf[Option[Double]], q8(va).asInstanceOf[Option[Double]])

          if (result sameElements annotationResult)
            true
          else
            false
        }.fold(true)(_ && _)
      }
  }

  @Test def testFisherExactTest() {
    Spec.check(count = 100)
  }
}
