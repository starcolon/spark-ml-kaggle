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

case class A(i: Integer, s: String)

class IOInterfaceTest extends SparkTestInstance with Matchers {

  import spark.implicits._

  describe("MongoDB interface"){
    val testDB = "test_ml"
    val testCollection = (testDB -> "cc")
    val mongoReader = new ReadMongo
    val mongoWriter = new WriteMongo(testDB, testCollection)

    lazy val dfTest = Seq(A(1,"One"), A(2,"Two"), A(3,"Three")).toDF

    it("should write records to mongo"){
      mongoWriter <~ dfTest
    }

    // NOTE: This test is not idempotent
    //        Running the test multiple times will never clear the existing records.
    it("should read records from mongo"){
      val df = mongoReader <~ testCollection
      
      Print(3) <~ df
      
      df.count shouldBe dfTest.count
      df.as[A].rdd.collect should contain theSameElementsAs(dfTest.as[A].rdd.collect)
    }
  }

  describe("Cassandra interface"){

  }

}