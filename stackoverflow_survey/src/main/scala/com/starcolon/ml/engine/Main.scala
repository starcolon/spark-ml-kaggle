package com.starcolon.ml.engine

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.ArrayEncoder

import Console.{CYAN,GREEN,YELLOW,RED,MAGENTA,RESET}

import com.starcolon.ml.SparkBase
import com.starcolon.ml.process._
import com.starcolon.ml.process.Implicits._
import com.starcolon.types.TypeOps._
import com.starcolon.ml.domain.StackOverflowTypes
import com.starcolon.ml.domain.StackOverflowTypes._
import com.starcolon.ml.DatasetUtils._
import com.starcolon.ml.transformers._
import com.starcolon.ml.model.{Classifier, ModelColumns}

object SparkMain extends App with SparkBase with ModelColumns {
  import spark.implicits._

  // Read input
  val dsInput = ((new ReadCSV) <~ Const.stackOverflowSurveyCSV)
    .lowercaseColumns
    .convertToNone("NA")
    .castMany("respondent" :: "yearscodedjob" :: Nil, IntegerType)
    .where($"country".isNotNull and $"employmentstatus".isNotNull)
  
  // Test parsing the datasets
  PrintCollected(5) <~ dsInput.as[Bio]
  PrintCollected(5) <~ dsInput.as[Job]
  PrintCollected(5) <~ dsInput.as[Preference]

  // Feature encoding
  val nullStringImputer = new NullImputer()
    .setImputedValue("none")
    .setInputCols(Array("professional", "formaleducation", "university"))

  val stringSplitter = new StringSplitter()
    .setInputCols(Array("learningnewtech"))

  val stringArrayIndexer = new ArrayEncoder[String]()
    .setInputCol("learningnewtech")
    .setOutputCol("learningnewtech_array")

  val featureReport = new FeatureVsTargetReport()
    .setInputCols(Array("professional","formaleducation"))
    .setOutputCol("learningnewtech")

  val featureEncoder = new Pipeline().setStages(Array(
    nullStringImputer,
    stringArrayIndexer,
    featureReport))


  // Define classification models
  val models = Seq(Classifier.DecisionTree, Classifier.RandomForest)
    .map{clf => new Pipeline().setStages(Array(featureEncoder, clf))}

  // Evaluate each model separately
  //


  SparkSession.clearActiveSession()
  spark.stop()
}