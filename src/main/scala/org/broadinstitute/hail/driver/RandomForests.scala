package org.broadinstitute.hail.driver

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.broadinstitute.hail.utils._
import org.broadinstitute.hail.annotations.Annotation
import org.broadinstitute.hail.expr.{BaseType, Parser, SparkAnnotationImpex, TArray, TBoolean, TDict, TDouble, TFloat, TInt, TLong, TNumeric, TString, TStruct, Type}
import org.broadinstitute.hail.variant.{Locus, Variant}
import org.kohsuke.args4j.{Option => Args4jOption}

import scala.collection.mutable

object RandomForests extends Command {

  class Options extends BaseOptions {

    @Args4jOption(name = "--training",
      usage = "variant annotation path to a Boolean value indicating if a variant should be used for training or not [Boolean]. " +
        "Missing values are not used for training.",
      required = true)
    var training: String = _


    @Args4jOption(name = "--label",
      usage = "variant annotation path to a String value containing the label for the training sites (e.g. TP / FP). " +
        "Any training site that doesn't have a label will be ignored.",
      required = true)
    var label: String = _
    //@Args4jOption(name = "--fp", usage = "false positive data for training [Boolean]", required = true)
    //var fp: String = _

    @Args4jOption(name = "--features",
      usage = "commma-separated list of variant annotations paths to features [List[String]]", required = true)
    var features: String = _

    @Args4jOption(required = true, name = "-r", aliases = Array("--root"),
      usage = "Period-delimited path starting with `va' where the prediction of the RF will be stored")
    var root: String = _

    @Args4jOption(required = false, name = "--rglobal",
      usage = "Period-delimited path starting with `global' where the labels will be stored. " +
        "Default is the wame as root but replacing `va' by `global'.")
    var groot: String = _

    @Args4jOption(required = false, name = "--percTraining",
      usage = "Percentage of training sites used for training (the rest is used for testing)")
    var pTrain: Double = 0.8

    @Args4jOption(required = false, name = "--numTrees",
      usage = "Number of trees to train")
    var numTrees: Int = 100


    @Args4jOption(required = false, name = "--maxDepth",
      usage = "Maximum tree depth")
    var maxDepth: Int = 5

    @Args4jOption(required = false, name = "-o", aliases = Array("--output"),
      usage = "outputs the model")
    var out: String = null

    @Args4jOption(required = false, name = "--missing-label",
      usage = "outputs the model")
    var missingLabel: String = "NA"

  }

  def newOptions = new Options

  def name = "randomForests"

  def description = "Runs random forests"

  def supportsMultiallelic = true

  def requiresVDS = true

  def run(state: State, options: Options): State = {

    val missingLabel = options.missingLabel
  
    val sqlContext = new org.apache.spark.sql.SQLContext(state.sc)

    //Get feature_queriers
    val feature_queriers = options.features.split(",")
      .map( {f =>
        val q = state.vds.queryVA(f)
        (f.replace(".","_"),q._1,q._2)
      })

    val training_querier = state.vds.queryVA(options.training)
    val label_querier = state.vds.queryVA(options.label)

    val schema = StructType(
      Variant.schema.fields ++
        Array(StructField("training", BooleanType, false),
          StructField("label",StringType,false)) ++
        feature_queriers.map({ case (f, t, q) => StructField(f, t.asInstanceOf[Type].schema, true) })
    )

    //Convert RDD records to Row
     val rowRDD = state.vds.rdd.map({
       case (v, (va, gs)) =>  Row.fromSeq(
         v.toRow.toSeq ++
           ((training_querier._2(va), label_querier._2(va)) match {
             case (Some(t), Some(l)) =>
               if (t.asInstanceOf[Boolean]) Array(true, l.toString)
               else Array(false, missingLabel)
             case _ => (Array(false, missingLabel))
           })
           ++ feature_queriers.map({
           case (f, t, q) => q(va) match {
             case Some(x) => t match {
               case TDouble => x.asInstanceOf[Double]
               case TInt => x.asInstanceOf[Int]
               case TString => x.asInstanceOf[String]
               case TFloat => x.asInstanceOf[Float]
               case TLong => x.asInstanceOf[Long]
               case TBoolean => x.asInstanceOf[Boolean]
               case _ => x.toString //TODO FIXME this whole thing is ugly!
             }
             case None => null
           }
         })
       )
     })

    //Apply schema to RDD and register as a table
    val vaSchemaDF = sqlContext.createDataFrame(rowRDD,schema).na.drop

    val Array(trainingData, evalData) = vaSchemaDF.filter("training").randomSplit(Array(options.pTrain,1-options.pTrain))

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(vaSchemaDF)

    val stringFeatures = feature_queriers
      .filter(_._2 == TString)
      .map(_._1)

    val stringFeaturesIndexer = stringFeatures
      .map(x => new(StringIndexer)
        .setInputCol(x)
        .setOutputCol(x + "index")
      )

    val assembler = new VectorAssembler()
      .setInputCols(feature_queriers
        .filter(_._2 != TString)
        .map(_._1) ++
        stringFeaturesIndexer.map(x => x.getOutputCol))
      .setOutputCol("features")

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(10)

    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(options.numTrees)
      .setMaxDepth(options.maxDepth)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer) ++
        stringFeaturesIndexer ++
        Array(assembler, featureIndexer, rf, labelConverter))

    info("Training RF model")

    val rfmodel = pipeline.fit(trainingData)

    val pipeline_length = rfmodel.stages.length

    val importances = rfmodel
      .stages(pipeline_length - 2)
      .asInstanceOf[RandomForestClassificationModel]
      .featureImportances
      .toArray

    val featurenames = feature_queriers
      .filter(_._2 != TString)
      .map(_._1) ++ feature_queriers
      .filter(_._2 == TString)
      .map(_._1)

    //Evaluate on test data
    info("Model evaluation")
    val eval = rfmodel.transform(evalData)
      //eval.take(5).map(x => info(x.mkString(",")))
      //eval.printSchema()
      eval
      .select("label","predictedLabel")
      .groupBy("label","predictedLabel").count().show()


    val labels = labelIndexer.labels

    //Apply on remaining data
    val predRDD = rfmodel
      .transform(vaSchemaDF)
      .rdd.map({
      row => (Variant.fromRow(row),
        Row(row.getAs[String]("predictedLabel"),
          labels.zip(row.getAs[org.apache.spark.mllib.linalg.DenseVector]("probability").toArray.toIndexedSeq).toMap)
          .asInstanceOf[Annotation])
    }).toOrderedRDD[Locus]

    val (vaFinalType, vaInserter) = state.vds.insertVA(
      TStruct(("prediction",TString),("probability",TDict(TDouble))),
      Parser.parseAnnotationRoot(options.root, Annotation.VARIANT_HEAD))

    val (gaFinalType, gaInserter) = state.vds.insertGlobal(
      TDict(TDouble),
      Parser.parseAnnotationRoot(Option(options.groot).getOrElse(options.root.replaceFirst("^va","global")),
        Annotation.GLOBAL_HEAD))

    state.copy(vds = state.vds
      .withGenotypeStream()
      .annotateVariants(predRDD, vaFinalType, vaInserter)
        .copy(
          //globalAnnotation = gaInserter(state.vds.globalAnnotation, Some(labelIndexer.labels.zipWithIndex.toMap)),
          globalAnnotation = gaInserter(state.vds.globalAnnotation, Some(featurenames.zip(importances).toMap)),
          globalSignature = gaFinalType
        )
    )
  }
}