package com.starcolon.ml.engine

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.sql.types._
import Console.{CYAN, MAGENTA, RESET}

import com.starcolon.ml.{IO, SparkBase}
import com.starcolon.types.TypeOps._
import com.starcolon.ml.domain.StackOverflowTypes
import com.starcolon.ml.domain.StackOverflowTypes._
import com.starcolon.ml.DatasetUtils._

object SparkMain extends App with SparkBase {
  import spark.implicits._

  val trainPath = "/data/stackoverflow-survey-2017/survey_results_public.csv"
  val dsInput = (io <== trainPath)
    .lowercaseColumns
    .castMany("respondent" :: "yearscodedjob" :: Nil, IntegerType)
  
  val dfBio = dsInput.as[Bio]
  val dfJob = dsInput.as[Job]
  val dfPer = dsInput.as[Personality]

  def printCyan(n: Any) { println(CYAN); println(n); println(RESET) }
  dfBio.rdd.take(5).foreach(printCyan)
  dfJob.rdd.take(5).foreach(printCyan)
  dfPer.rdd.take(5).foreach(printCyan)
}