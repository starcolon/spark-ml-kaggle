package com.starcolon.ml

import org.scalatest._
import Matchers._

import com.starcolon.ml.process._
import com.starcolon.ml.transformers._
import com.starcolon.ml.DatasetUtils._

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.ml.feature.ArrayEncoder
import org.apache.spark.sql.types._
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import scala.collection.mutable.WrappedArray

object DSTypes {
  case class A(i: Int, j: Int, k: Int)
  case class B(i: String, j: String, k: String)
  case class C(i: Double, j: Double, k: Double)
}

class DatasetUtilTest extends SparkTestInstance with Matchers {

  import spark.implicits._
  import DSTypes._

  describe("Basic dataset utility"){

    lazy val aa = A(1,2,3) :: A(0,1,1) :: A(3,4,5) :: A(0,-1,-1) :: Nil
    lazy val bb = B("a","bb","ccc") :: B("a","b","c") :: B("1","2","3") :: Nil
    lazy val cc = C(3.4, 4.5, 5.6) :: C(0,0,0) :: C(0,1.5,5.1) :: Nil

    lazy val dfA = aa.toDF
    lazy val dfB = bb.toDF
    lazy val dfC = cc.toDF

    it("should create a column of array (of Integer) from multiple columns"){
      val nn = dfA
        .seqFromColumns(Seq("i","j","k"), "x")
        .select("x")
        .rdd.map{_.getAs[Seq[Int]](0)}.collect

      nn should contain allOf(Seq(1,2,3), Seq(0,1,1), Seq(3,4,5), Seq(0,-1,-1))
    }

    it("should create a column of array (of String) from multiple columns"){
      val nn = dfB
        .seqFromColumns(Seq("i","j","k"), "x")
        .select("x")
        .rdd.map{_.getAs[Seq[String]](0)}.collect

      nn should contain allOf(Seq("a","bb","ccc"), Seq("a","b","c"), Seq("1","2","3"))
    }

    it("should create a column of array (of Double) from multiple columns"){
      val nn = dfC
        .seqFromColumns(Seq("i","j","k"), "x")
        .select("x")
        .rdd.map{_.getAs[Seq[Double]](0)}.collect

      nn should contain allOf(Seq(3.4, 4.5, 5.6), Seq(0,0,0), Seq(0,1.5,5.1))
    }
  }

}