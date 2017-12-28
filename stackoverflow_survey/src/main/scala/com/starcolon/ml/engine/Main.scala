package com.starcolon.ml.engine

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types._

import com.starcolon.ml.SparkBase
import com.starcolon.ml.process._
import com.starcolon.ml.process.Implicits._
import com.starcolon.types.TypeOps._
import com.starcolon.ml.domain.StackOverflowTypes
import com.starcolon.ml.domain.StackOverflowTypes._
import com.starcolon.ml.DatasetUtils._
import com.starcolon.ml.transformers._

object SparkMain extends App with SparkBase {
  import spark.implicits._

  // Read input
  val dsInput = (ReadCSV <~ Const.stackOverflowSurveyCSV)
    .lowercaseColumns
    .convertToNone("NA")
    .castMany("respondent" :: "yearscodedjob" :: Nil, IntegerType)
    .where($"country".isNotNull and $"employmentstatus".isNotNull)
  
  // DEBUG: Test parsing the datasets
  PrintCollected(5) <~ dsInput.as[Bio]
  PrintCollected(5) <~ dsInput.as[Job]
  PrintCollected(5) <~ dsInput.as[Preference]

  // Feature encoding
  val nullStringImputer = new NullImputer()
    .setImputedValue("none")
    .setInputCols(Array("professional", "formaleducation", "university"))

  val stringSplitter = new StringSplitter()
    .setInputCols(Array("learningnewtech"))

  val encoderModel = new Pipeline().setStages(Array(nullStringImputer)).fit(dsInput)


  // Classification Models


  // Evaluate
}