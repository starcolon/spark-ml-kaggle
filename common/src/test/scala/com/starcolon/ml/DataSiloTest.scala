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
}

class DataSiloTest extends SparkTestInstance with Matchers {

  import spark.implicits._
  import TestImplicits._
  import SampleTypes._

  lazy val uu = U("a,a,0") ::
                U("b,b,1") ::
                U("c,c,2") ::
                U("d,d") :: Nil

  lazy val dfU = uu.toDF

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

  }
}