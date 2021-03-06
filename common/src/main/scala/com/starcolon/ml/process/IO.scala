package com.starcolon.ml.process

import org.apache.spark.sql.{SparkSession, SQLContext, Dataset, Row}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types._

import com.mongodb.spark._
import org.bson.Document
import com.mongodb.spark.config._

import org.apache.spark.sql.cassandra._

import Implicits.{locationAsString,locationAsStringPair,stringPairAsLocation}

case class ReadCSV(implicit spark: SparkSession) extends DataProvider {
  override def <~(from: Location = NoWhere) = 
    spark.read.format("csv").option("header", "true").load(from)
}

case class ReadHive(implicit spark: SparkSession) extends DataProvider {
  override def <~(from: Location = NoWhere) = spark.sqlContext.table(from)
}

case class ReadCassandra(implicit spark: SparkSession) extends DataProvider {
  import com.datastax.spark.connector._
  override def <~(from: Location = NoWhere) = {
    val DatabaseTable(database,table) = from
    spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map(
        "table" -> table,
        "keyspace" -> database)).load() // NOTE: reference to keyspace as a database
  }
}

case class ReadMongo(implicit spark: SparkSession) extends DataProvider {
  override def <~(from: Location = NoWhere) = {
    val DatabaseTable(database,collection) = from
    val readConfig = ReadConfig(Map(
      "uri" -> "mongodb://localhost:27017/",
      "database" -> database,
      "collection" -> collection,
      "partitioner" -> "MongoSplitVectorPartitioner"))
    MongoSpark.load(spark, readConfig).toDF
  }
}

case class Print(num: Integer = 20, colour: String = Console.RESET) extends DataOutput {
  override def <~(data: Dataset[_]) = {
    println(colour)
    data.show(num)
    println(Console.RESET)
  }
}

class PrintWithSchema(num: Integer = 20) extends Print(num) {
  override def <~(data: Dataset[_]) = {
    data.printSchema
    super.<~(data)
  }
}

case class WriteCassandra(keyspace: String, table: String) extends DataOutput {
  override def <~(data: Dataset[_]) = {
    import com.datastax.spark.connector._
    data.write.cassandraFormat(table, keyspace).save()
  }
}

case class WriteMongo(database: String, collection: String) extends DataOutput {
  override def <~(data: Dataset[_]) = {
    val writeConfig = WriteConfig(Map(
      "uri" -> "mongodb://localhost:27017/",
      "database" -> database,
      "collection" -> collection,
      "replaceDocument" -> "true"))
    MongoSpark.save(data, writeConfig)
  }
}

case class WriteCSV(file: String, withHeader: Boolean = false) extends DataOutput {
  override def <~(data: Dataset[_]) = {
    data.toDF
      .write
      .format("com.databricks.spark.csv")
      .option("header", withHeader.toString)
      .mode("overwrite")
      .save(file)
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

case class Tie(out: DataOutput*) extends DataOutput {
  override def <~ (data: Dataset[_]) = {
    out.foreach(_ <~ data)
  }
}