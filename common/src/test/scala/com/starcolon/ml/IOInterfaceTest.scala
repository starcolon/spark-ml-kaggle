package com.starcolon.ml

import org.scalatest._
import Matchers._

import com.starcolon.ml.process._
import com.starcolon.ml.transformers._
import com.starcolon.ml.DatasetUtils.litArray
import com.starcolon.ml.process.Implicits.stringAsLocation

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.ml.feature.ArrayEncoder
import org.apache.spark.sql.types._
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import scala.collection.mutable.WrappedArray

import com.datastax.spark.connector.cql.CassandraConnector

case class A(i: Integer, s: String)

class IOInterfaceTest extends SparkTestInstance with Matchers {

  import spark.implicits._
  import com.starcolon.ml.process.Implicits._

  // MongoDB
  val testDB = "test_ml"
  val testCollection = "cc"

  // Cassandra
  val testKeySpace = "test_ml"
  val testTable = "cc"

  override def beforeAll(){
    val conn = CassandraConnector(spark.sparkContext.getConf)
    conn.withSessionDo { session =>
      session.execute(s"""DROP KEYSPACE IF EXISTS $testKeySpace """)
    }
    Thread.sleep(3000)
    conn.withSessionDo { session =>
      session.execute(s"""CREATE KEYSPACE $testKeySpace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}; """)
      session.execute(s"""CREATE TABLE $testKeySpace.$testTable (i int PRIMARY KEY, s text)""")
    }
    Thread.sleep(3000)

    super.beforeAll()
  }

  describe("MongoDB interface"){
  
    val mongoReader = new ReadMongo
    val mongoWriter = new WriteMongo(testDB, testCollection)

    lazy val dfTest = Seq(A(1,"One"), A(2,"Two"), A(3,"Three")).toDF

    it("should write records to mongo"){
      mongoWriter <~ dfTest
    }

    // NOTE: This test is not idempotent
    //        Running the test multiple times will never clear the existing records.
    it("should read records from mongo"){
      val df = mongoReader <~ (testDB -> testCollection)
      
      Print(3, Console.CYAN) <~ df

      val output = df.as[A].rdd.collect
      dfTest.as[A].rdd.collect.foreach{ rec => 
        output should contain (rec)}
    }
  }

  describe("Cassandra interface"){

    val csReader = new ReadCassandra
    val csWriter = new WriteCassandra(testKeySpace, testTable)

    lazy val dfTest = Seq(A(5,"Five"), A(7,"Seven"), A(10,"Ten")).toDF

    it("should write records to canssandra"){
      csWriter <~ dfTest
    }

    it("should read records from canssandra"){
      val df = csReader <~ (testKeySpace -> testTable)

      Print(3, Console.CYAN) <~ df

      val output = df.as[A].rdd.collect
      dfTest.as[A].rdd.collect.foreach{ rec => 
        output should contain (rec)}
    }
  }

}