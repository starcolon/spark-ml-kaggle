package com.starcolon.ml.engine

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.sql.types._
import Console.{CYAN, MAGENTA, RESET}

import com.starcolon.ml.{IO, SparkBase}
import com.starcolon.types.TypeOps._
import com.starcolon.ml.domain.StackOverflowTypes
import com.starcolon.ml.domain.StackOverflowTypes._
import com.starcolon.ml.DatasetUtils._
import com.starcolon.ml.transformers._

object SparkMain extends App with SparkBase {
  import spark.implicits._

  val dsInput = (io <== Const.sourceFile)
    .lowercaseColumns
    .representAsNulls("NA")
    .castMany("respondent" :: "yearscodedjob" :: Nil, IntegerType)
  
  val dfBio = dsInput.as[Bio]
  val dfJob = dsInput.as[Job]
  val dfPrf = dsInput.as[Preference]

  def printCyan(n: Any) { println(CYAN); println(n); print(RESET) }
  dfBio.rdd.take(5).foreach(printCyan)
  dfJob.rdd.take(5).foreach(printCyan)
  dfPrf.rdd.take(5).foreach(printCyan)
}