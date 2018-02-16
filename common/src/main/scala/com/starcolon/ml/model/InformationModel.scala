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
class MutualInformation[T: TypeTag](override val inputColumns: Seq[String], override val output: String) extends InformationMetric {
  
  private def plog(x: T, y: T, df: Dataset[_]): Double = ???

  override def ~(ds: Dataset[_]): Double = {
    super.~(ds)
    val df = ds.toDF
    // Find all distinct values of inputs and outputs
    val X = df.seqFromColumns(inputColumns, "x").select("x").rdd.map(_.getAs[Seq[T]](0)).distinct.collect
    val Y = df.select(output).rdd.map(_.getAs[T](0)).distinct.collect

    ???
  }
}



