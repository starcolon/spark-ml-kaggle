package com.starcolon.ml.process

import org.apache.spark.sql.{SparkSession, SQLContext, Dataset, Row}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types._

import Implicits.locationAsString

case object ReadCSV extends DataProvider {
  override def <~(from: Location = NoWhere)(implicit spark: SparkSession) = 
    spark.read.format("csv").option("header", "true").load(from)
}

case object ReadHive extends DataProvider {
  override def <~(from: Location = NoWhere)(implicit spark: SparkSession) = spark.sqlContext.table(from)
}

case class Print(num: Integer = 20) extends DataOutput {
  override def <~(data: Dataset[_])(implicit spark: SparkSession) = data.show(num)
}

class PrintWithSchema(num: Integer = 20) extends Print(num) {
  override def <~(data: Dataset[_])(implicit spark: SparkSession) = {
    data.printSchema
    super.<~(data)
  }
}

/**
 * Unwrap the row objects before printing
 * This is useful for printing out a typed RDD so we can examine the associated type.
 */
case class PrintCollected(num: Integer = 20, colour: String = Console.CYAN) extends DataOutput {
  private def printColoured(c: String)(rowTuple: (Any,Int)){
    print(colour)
    print(s"[${rowTuple._2}] ")
    println(rowTuple._1)
    print(Console.RESET)
    println("•" * 30)
  }
  override def <~(data: Dataset[_])(implicit spark: SparkSession) = {
    data.rdd.take(num).zipWithIndex.foreach(printColoured(colour))
  }
}