package com.starcolon.ml

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class IO(implicit val spark: SparkSession) {
  // Read CSV file
  def <==(path: String): Dataset[Row] = spark.read.format("csv").option("header", "true").load(path)

  // Write text file
  def <~=(path: String, rdd: Dataset[_]) = ???
}