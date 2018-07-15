package com.starcolon.ml

import org.scalatest._
import Matchers._

import com.starcolon.ml.process._
import com.starcolon.ml.process.Silo._
import com.starcolon.ml.process.OutputCol._
import com.starcolon.ml.transformers._
import com.starcolon.ml.DatasetUtils.litArray

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.ml.feature.ArrayEncoder
import org.apache.spark.sql.types._
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import scala.collection.mutable.WrappedArray

object SampleTypes {
  case class U(a: String)
  case class U2(a: Seq[Int], b: Seq[Double], c: Seq[Double])
}

class DataSiloTest extends SparkTestInstance with Matchers {

  import spark.implicits._
  import TestImplicits._
  import SampleTypes._

  lazy val uu = U("a,a,0") ::
                U("b,b,1") ::
                U("c,c,2") ::
                U("d,d") :: Nil

  lazy val u2 = U2(Seq(1,2,3), Seq(4.0,5.0), Nil) ::
                U2(Seq(3,2,1), Seq(0.0,-0.1), Seq(-0.2,-0.3)) :: Nil

  lazy val dfU = uu.toDF
  lazy val dfU2 = u2.toDF

  describe("Basic silo"){

    it("should split string"){
      val split = SplitString("a", ",", Inplace)
      val dfOut = split.f(dfU)
      dfOut.columns shouldBe(Seq("a"))
      dfOut.schema("a").dataType shouldBe(ArrayType(StringType, true))
      dfOut.select("a").rdd.map(_.getAs[Seq[String]](0)).collect shouldBe(Seq(
        Seq("a","a","0"),
        Seq("b","b","1"),
        Seq("c","c","2"),
        Seq("d","d")
      ))
    }

    it("should concatnate arrays - Int arrays"){
      val concat = ArrayConcat("w" :: "a" :: Nil, As("w"))
      val df = dfU2.withColumn("w", litArray(Seq(1,2,3)))
      val dfOut = concat.f(df)
      dfOut.columns shouldBe(Seq("a","b","c","w"))
      dfOut.schema("w").dataType shouldBe(ArrayType(IntegerType, false))
      dfOut.select("w").rdd.map(_.getAs[Seq[Int]](0)).collect shouldBe(Seq(
        Seq(1,2,3,1,2,3),
        Seq(3,2,1,1,2,3)
      ))
    }

    it("should concatnate arrays - Double arrays"){
      val concat = ArrayConcat("b" :: "c" :: Nil, As("w"))
      val dfOut = concat.f(dfU2)
      dfOut.columns shouldBe(Seq("a","b","c","w"))
      dfOut.schema("w").dataType shouldBe(ArrayType(DoubleType, false))
    }
  }
}