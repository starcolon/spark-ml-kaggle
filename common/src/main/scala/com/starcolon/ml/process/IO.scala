package com.starcolon.ml.process

import org.apache.spark.sql.{SparkSession, SQLContext, Dataset, Row}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types._

import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config._
import com.stratio.datasource.mongodb.config.MongodbConfig._

import Implicits.locationAsString

case class ReadCSV(implicit spark: SparkSession) extends DataProvider {
  override def <~(from: Location = NoWhere) = 
    spark.read.format("csv").option("header", "true").load(from)
}

case class ReadHive(implicit spark: SparkSession) extends DataProvider {
  override def <~(from: Location = NoWhere) = spark.sqlContext.table(from)
}

case class ReadCassandra(implicit spark: SparkSession) extends DataProvider {
  override def <~(from: Location = NoWhere) = ???
}

case class ReadMongo(database: String)(implicit spark: SparkSession) extends DataProvider {
  override def <~(from: Location = NoWhere) = {
    val collection = from
    val config = MongodbConfigBuilder(
      Map(
        Host -> List("localhost:27017"), 
        Database -> database, 
        Collection -> collection, 
        SamplingRatio -> 1.0, 
        WriteConcern -> "normal", 
        SplitSize -> 8, 
        SplitKey -> "_id")).build

    spark.sqlContext.fromMongoDB(config)
  }
}

case class Print(num: Integer = 20) extends DataOutput {
  override def <~(data: Dataset[_]) = data.show(num)
}

class PrintWithSchema(num: Integer = 20) extends Print(num) {
  override def <~(data: Dataset[_]) = {
    data.printSchema
    super.<~(data)
  }
}

case class WriteCassandra extends DataOutput {
  override def <~(data: Dataset[_]) = ???
}

case class WriteMongo(database: String, collection: String) extends DataOutput {
  override def <~(data: Dataset[_]) = {
    implicit val spark = data.sparkSession
    val config = MongodbConfigBuilder(
      Map(
        Host -> List("localhost:27017"), 
        Database -> database, 
        Collection -> collection, 
        SamplingRatio -> 1.0, 
        WriteConcern -> "normal", 
        SplitSize -> 8, 
        SplitKey -> "_id")).build
    data.toDF.saveToMongodb(config)
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
  override def <~(data: Dataset[_]) = {
    data.rdd.take(num).zipWithIndex.foreach(printColoured(colour))
  }
}