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
import com.starcolon.ml.process.Silo.{OneHotEncode, ReplaceNumericalValue}
import com.starcolon.ml.process.OutputCol._

object SparkMain extends App with SparkBase with ModelColumns {
  import spark.implicits._

  // Read input
  val dsInput = ((new ReadCSV) <~ Const.stackOverflowSurveyCSV)
    .lowercaseColumns
    .convertToNone("NA")
    .castMany("respondent" :: "yearscodedjob" :: Nil, IntegerType)
    .where('country.isNotNull and 'employmentstatus.isNotNull)

  println(CYAN)
  val stringValueCols = Seq(
    "professional", "country", "formaleducation", "race", "majorundergrad",
    "employmentstatus", "companysize", "yearscodedjob", "careersatisfaction", "salary", "expectedsalary"
  )
  dsInput.select(stringValueCols.head, stringValueCols.tail:_*).printLines(5)
  println(RESET)

  // Data processing recipes
  val recipes: Seq[DataSiloT] = 
    ReplaceNumericalValue("salary", Inplace, "Agree") +:
    stringValueCols.map{c => OneHotEncode(c, Inplace)}

  // Cook the data
  val dsPrepared: Dataset[_] = recipes.foldLeft(dsInput){ case(ds,r) => r $ ds }

  println(GREEN)
  dsPrepared.select(stringValueCols.head, stringValueCols.tail:_*).printLines(5)
  println(RESET)

  // Bis später!
  SparkSession.clearActiveSession()
  spark.stop()
}