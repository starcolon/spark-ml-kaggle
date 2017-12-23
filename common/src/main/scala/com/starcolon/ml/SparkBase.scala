package com.starcolon.ml

import org.apache.spark.sql.{SparkSession, Dataset, Row}

trait SparkBase {
  // Build a Spark local session
  implicit val sparkSession = SparkSession
    .builder.master("local")
    .appName("sequence")
    .config("spark.sql.shuffle.partitions", 3000)
    .getOrCreate()

  implicit val sparkContext = sparkSession.sparkContext
  import sparkSession.implicits._

  val io = new IO
}