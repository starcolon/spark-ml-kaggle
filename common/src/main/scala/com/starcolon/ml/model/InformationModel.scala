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
class MutualInformation(override val inputColumns: Seq[String], override val output: String) extends InformationMetric {
  
  private val plog = udf{(px: Double, py: Double, pxy: Double) =>
    pxy * math.log(pxy / (px*py))
  }

  override def ~(ds: Dataset[_]): Double = {
    import ds.sparkSession.implicits._
    super.~(ds)
    val df = ds.toDF.seqFromColumns(inputColumns, "x").cache

    // TAODEBUG:
    println("------ test ------")
    df.peek("df")

    val N = df.count.toDouble
    val pX = df.withColumn("n", lit(1D))
      .groupBy("x").agg(expr("sum(n) as n"))
      .withColumn("pX", 'n/lit(N))
      .select("x","pX")
      .cache
      .peek("pX")

    val pY = df.withColumn("n", lit(1D))
      .groupBy(output).agg(expr("sum(n) as n"))
      .withColumn("pY", 'n/lit(N))
      .select(output, "pY")
      .cache
      .peek("pY")

    val pXY = df.withColumn("n", lit(1D))
      .groupBy("x", output).agg(expr("sum(n) as n"))
      .withColumn("p", 'n/lit(N))
      .select("x", output, "pXY")
      .cache
      .peek("pXY")

    pXY
      .join(pX, "x" :: Nil)
      .join(pY, output :: Nil)
      .withColumn("p", plog('pX, 'pY, 'pXY))
      .peek("p-all")
      .select("p")
      .rdd.map{_.getAs[Double](0)}
      .sum
  }
}

class JointEntropy(override val inputColumns: Seq[String], override val output: String) extends InformationMetric {
  
  override def ~(ds: Dataset[_]): Double = {
    import ds.sparkSession.implicits._
    super.~(ds)
    ???
  }
}


