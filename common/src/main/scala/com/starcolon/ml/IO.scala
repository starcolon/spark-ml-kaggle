package com.starcolon.ml

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class IO(implicit val sparkContext: SparkContext) {
  // Read text file
  def <==(
    path: String, 
    delimiter: String,
    limit: Option[Int]): RDD[Array[String]] = {
    val rdd = sparkContext.textFile(path).map{_.split(delimiter)}
    limit match {
      case None => rdd 
      case Some(n) => sparkContext.parallelize(rdd.take(n))
    }
  }

  // Write text file
  def <~=(path: String, rdd: RDD[_]) = ???
}