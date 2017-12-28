package com.starcolon.ml.engine

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types._
import Console.{CYAN, GREEN, MAGENTA, RESET}

import com.starcolon.ml.{IO, SparkBase}
import com.starcolon.types.TypeOps._
import com.starcolon.ml.domain.StackOverflowTypes
import com.starcolon.ml.domain.StackOverflowTypes._
import com.starcolon.ml.DatasetUtils._
import com.starcolon.ml.transformers._

object SparkMain extends App with SparkBase {
  import spark.implicits._

  // Read input
  val dsInput = (io <== Const.sourceFile)
    .lowercaseColumns
    .representAsNulls("NA")
    .castMany("respondent" :: "yearscodedjob" :: Nil, IntegerType)
    .where($"country".isNotNull and $"employmentstatus".isNotNull)
  
  // DEBUG: Test parsing the datasets
  val dfBio = dsInput.as[Bio]
  val dfJob = dsInput.as[Job]
  val dfPrf = dsInput.as[Preference]

  def printCyan(n: Any) { println(CYAN); println(n); print(RESET) }
  def printGreen(n: Any) { println(GREEN); println(n); print(RESET) }
  dfBio.rdd.take(5).foreach(printCyan)
  dfJob.rdd.take(5).foreach(printCyan)
  dfPrf.rdd.take(5).foreach(printCyan)

  // Feature encoding
  val nullStringImputer = new NullImputer()
    .setImputedValue("none")
    .setColumns(Seq("professional", "formaleducation", "university"))
  val stringSplitter = new StringSplitter()
    .setColumns(Seq("learningnewtech"))

  val encoderModel = new Pipeline().setStages(Array(nullStringImputer)).fit(dsInput)


  // Train the models


  // Evaluate
}