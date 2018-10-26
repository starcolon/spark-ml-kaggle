package com.starcolon.ml.engine

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.ArrayEncoder
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.functions._

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
import com.starcolon.ml.process.Silo._
import com.starcolon.ml.process.OutputCol._
import com.starcolon.ml.process.Ops._
import com.starcolon.ml.process.KV

object SparkMain extends App with SparkBase with ModelColumns {
  import spark.implicits._

  // Read input
  val dsInput = ((new ReadCSV) <~ Const.stackOverflowSurveyCSV)
    .lowercaseColumns
    .convertToNone("NA")
    .castMany("respondent" :: "yearscodedjob" :: Nil, IntegerType)
    .where('country.isNotNull and 'employmentstatus.isNotNull)
    .na.fill("Somewhat agree", "salary" :: Nil)
    .na.fill("I don't know", "companysize" :: Nil)
    .na.fill(0, "careersatisfaction" :: Nil)

  println(CYAN)
  val featureColumns = List(
    "professional", "country", "formaleducation", "race", "majorundergrad",
    "employmentstatus", "companysize", "yearscodedjob", "salary", "expectedsalary"
  )
  val onehotEncodedColumns = featureColumns.filterNot(Seq("companysize","salary").indexOf(_) > 0)
  val labelColumn = "careersatisfaction"
  val stringValueCols = featureColumns :+ labelColumn
  dsInput.select("respondent", stringValueCols:_*).printLines(5)
  println(RESET)

  // +------------------------+-------+
  // |k                       |v      |
  // +------------------------+-------+
  // |I don't know            |13339.0|
  // |20 to 99 employees      |8587.0 |
  // |100 to 499 employees    |7274.0 |
  // |10,000 or more employees|5680.0 |
  // |10 to 19 employees      |4103.0 |
  // |1,000 to 4,999 employees|3831.0 |
  // |Fewer than 10 employees |3807.0 |
  // |500 to 999 employees    |2486.0 |
  // |5,000 to 9,999 employees|1604.0 |
  // |I prefer not to answer  |681.0  |
  // +------------------------+-------+
  val mapEncodeCompanySize = Map(
    "I don't know" -> 0,
    "I prefer not to answer" -> 0,
    "Fewer than 10 employees" -> 1,
    "10 to 19 employees" -> 10,
    "20 to 99 employees" -> 20,     
    "100 to 499 employees" -> 100,  
    "500 to 999 employees" -> 500,
    "1,000 to 4,999 employees" -> 100,
    "5,000 to 9,999 employees" -> 5000,
    "10,000 or more employees" -> 10000
  )

  // +-----------------+-------+
  // |k                |v      |
  // +-----------------+-------+
  // |Somewhat agree   |23046.0|
  // |Agree            |13671.0|
  // |Strongly agree   |13467.0|
  // |Disagree         |886.0  |
  // |Strongly disagree|322.0  |
  // +-----------------+-------+
  val mapEncodeSalary = Map(
    "Strongly disagree" -> -2,
    "disagree" -> -1,
    "Somewhat agree" -> 1,
    "Agree" -> 1,
    "Strongly agree" -> 2)

  // Data processing recipes
  val recipes: Seq[DataSiloT] = (
    ReplaceNumericalValue("salary", Inplace, value="Somewhat agree") ::
    ExploreValues("companysize" :: "careersatisfaction" :: "salary" :: Nil) ::
    onehotEncodedColumns.map{c => OneHotEncode(c, Inplace)}) :+
    PredefinedEncode("companysize", Inplace, mapEncodeCompanySize, defaultOutput=0) :+
    PredefinedEncode("salary", Inplace, mapEncodeSalary, defaultOutput=1) :+
    ComposeFeatureArray(featureColumns, As("feature")) :+
    OneHotEncode(labelColumn, As("label"))

  // Cook the data
  val dsPrepared = recipes $ dsInput

  println(GREEN)
  dsPrepared.select("respondent", (stringValueCols :+ "label"):_*).printLines(10)
  println(RESET)

  println(GREEN)
  dsPrepared.select("respondent", "feature", "label").show(20)
  println(RESET)

  val Array(training, test) = dsPrepared.randomSplit(Array(0.8, 0.2), seed = 55L)

  println(CYAN)
  println("Training models")
  println(RESET)
  val models = Classifier.DecisionTree :: Nil
  val fitted = models.map{m => 
    
    val mfit = m.fit(training)
    val dsVerify = mfit.transform(test)
    val mapVerify = dsVerify.withColumn("k", when('predict === 'label, lit("TRUE")).otherwise(lit("FALSE")))
      .withColumn("v", lit(1D))
      .as[KV]
      .rdd.keyBy(_.k)
      .reduceByKey(_ + _)
      .map{ case(k,kv) => (k,kv.v) }
      .collectAsMap

    val sum = mapVerify.values.sum.toDouble
    val pos = mapVerify("TRUE").toDouble
    val neg = mapVerify("FALSE").toDouble

    val accuary = pos/sum    

    println(GREEN + s"${Classifier.getName(m)} - accuary = $accuary" + RESET)
      
    mfit
  }

  // Bis später!
  SparkSession.clearActiveSession()
  spark.stop()
}