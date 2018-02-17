package com.starcolon.ml.model

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.sql.functions._
import scala.reflect.runtime.universe._
import com.starcolon.ml.DatasetUtils._

trait InputResponse {
  val inputColumns: Seq[String]
  val output: String
}

abstract class InformationMetric extends InputResponse {
  def ~(df: Dataset[_]): Double = {
    require(df.columns.contains(output), s"Dataset must have the column $output")
    require(inputColumns.map{df.columns.contains}.reduce(_ && _), s"Dataset must have all of these columns : ${inputColumns.mkString(", ")}")
    0.0
  }
}

/**
 * Mutual information of discrete inputs and outputs
 * I = Sumx Sumy { p(x,y) * log(p(x,y) / p(x)*p(y))}
 */
class MutualInformation[T: Manifest](override val inputColumns: Seq[String], override val output: String) extends InformationMetric {
  
  private def plog(x: T, y: T, df: Dataset[_]): Double = ???

  override def ~(ds: Dataset[_]): Double = {
    super.~(ds)
    val df = ds.toDF.seqFromColumns(inputColumns, "x").cache
    // Find all distinct values of inputs and outputs
    val X = df.distinctValues[Seq[T]]("x")
    val Y = df.select(output).rdd.map(_.getAs[T](0)).distinct.collect

    val N = df.count.toDouble
    val pX = df.withColumn("n", lit(1D))
      .groupBy("x").agg(expr("sum(n) as n"))
      .withColumn("p", 'n/lit(N))
      .select("x","p")
      .cache

    val pY = df.withColumn("n", lit(1D))
      .groupBy(output).agg(expr("sum(n) as n"))
      .withColumn("p", 'n/lit(N))
      .select(output, "p")
      .cache

    val pXY = df.withColumn("n", lit(1D))
      .groupBy("x", output).agg(expr("sum(n) as n"))
      .withColumn("p", 'n/lit(N))
      .select("x", output, "p")
      .cache

    ???
  }
}



