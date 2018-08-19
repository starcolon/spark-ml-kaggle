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
  case class U3(a: Seq[Double], b: Seq[Double])
  case class U4(a: Seq[Double])
  case class U5(a: Option[Double])
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

  lazy val u3 = U3(Seq(0,0,0), Seq(25)) ::
                U3(Seq(-1,0,1),  Seq(10)) :: 
                U3(Nil, Seq(5,-10,5)) :: Nil

  lazy val u4 = U4(Seq(0,0,0)) ::
                U4(Seq(1,2,3)) :: 
                U4(Nil) :: 
                U4(Seq(-1,3,1)) :: Nil

  lazy val dfU = uu.toDF
  lazy val dfU2 = u2.toDF
  lazy val dfU3 = u3.toDF
  lazy val dfU4 = u4.toDF

  describe("Basic silo"){

    it("should split string"){
      val split = SplitString("a", ",", Inplace)
      val dfOut = split.$(dfU)
      dfOut.columns shouldBe(Seq("a"))
      dfOut.schema("a").dataType shouldBe(ArrayType(StringType, true))
      dfOut.select("a").rdd.map(_.getAs[Seq[String]](0)).collect shouldBe(Seq(
        Seq("a","a","0"),
        Seq("b","b","1"),
        Seq("c","c","2"),
        Seq("d","d")
      ))
    }

    it("should replace numerical values"){
      val df = Seq(
        ("135"),("233"),("Foo"),("1e3"),("Bar")
      ).toDF("a")

      val dfOut = ReplaceNumericalValue("a", Inplace, "Number").$(df)
      dfOut.select("a").rdd.map(_.getAs[String](0)).collect shouldBe(Seq(
        "Number", "Number", "Foo", "Number", "Bar"
      ))
    }

    it("should concatnate arrays - Int arrays"){
      val concat = ArrayConcat("w" :: "a" :: Nil, As("w"))
      val df = dfU2.withColumn("w", litArray(Seq(1,2,3)))
      val dfOut = concat.$(df)
      dfOut.columns shouldBe(Seq("a","b","c","w"))
      dfOut.schema("w").dataType shouldBe(ArrayType(IntegerType, false))
      dfOut.select("w").rdd.map(_.getAs[Seq[Int]](0)).collect shouldBe(Seq(
        Seq(1,2,3,1,2,3),
        Seq(1,2,3,3,2,1)
      ))
    }

    it("should concatnate arrays - Double arrays"){
      val concat = ArrayConcat("b" :: "c" :: Nil, As("w"))
      val dfOut = concat.$(dfU2)
      dfOut.columns shouldBe(Seq("a","b","c","w"))
      dfOut.schema("w").dataType shouldBe(ArrayType(DoubleType, false))
      dfOut.select("w").rdd.map(_.getAs[Seq[Double]](0)).collect shouldBe(Seq(
        Seq(4.0, 5.0),
        Seq(0.0, -0.1, -0.2, -0.3)
      ))
    }

    it("should scale arrays - Int Array"){
      val scale = ArrayScaler("a", Scaler.Ratio(50), Inplace)
      val dfOut = scale.$(dfU2)
      dfOut.columns shouldBe(Seq("a","b","c"))
      dfOut.schema("a").dataType shouldBe(ArrayType(DoubleType, false))
      dfOut.select("a").rdd.map(_.getAs[Seq[Double]](0)).collect shouldBe(Seq(
        Seq(50D,100D,150D),
        Seq(150D,100D,50D)
      ))
    }

    it("should scale arrays - Double Array"){
      val scale = ArrayScaler("c", Scaler.Ratio(-5), Inplace)
      val dfOut = scale.$(dfU2)
      dfOut.columns shouldBe(Seq("a","b","c"))
      dfOut.schema("c").dataType shouldBe(ArrayType(DoubleType, false))
      dfOut.select("c").rdd.map(_.getAs[Seq[Double]](0)).collect shouldBe(Seq(
        Nil,
        Seq(1,1.5)
      ))
    }

    it("should scale arrays - Norm"){
      val scale = ArrayScaler("b", Scaler.DivisionByNorm(2), Inplace)
      val dfOut = scale.$(dfU2)
      dfOut.columns shouldBe(Seq("a","b","c"))
      dfOut.schema("b").dataType shouldBe(ArrayType(DoubleType, false))
      dfOut.select("b").rdd.map(_.getAs[Seq[Double]](0)).collect shouldBe(Seq(
        Seq(0.62469504755442429, 0.78086880944303039),
        Seq(0,-1.0)
      ))
    }

    it("should min max cut arrays, open min"){
      val cut = ArrayScaler("b", Scaler.MinMaxCut(None, Some(4.5)), Inplace)
      val dfOut = cut.$(dfU2)
      dfOut.columns shouldBe(Seq("a","b","c"))
      dfOut.schema("b").dataType shouldBe(ArrayType(DoubleType, false))
      dfOut.select("b").rdd.map(_.getAs[Seq[Double]](0)).collect shouldBe(Seq(
        Seq(4D, 4.5D),
        Seq(0.0,-0.1)
      ))
    }

    it("should min max cut arrays, open max"){
      val cut = ArrayScaler("b", Scaler.MinMaxCut(Some(-0.5D), None), Inplace)
      val dfOut = cut.$(dfU2)
      dfOut.columns shouldBe(Seq("a","b","c"))
      dfOut.schema("b").dataType shouldBe(ArrayType(DoubleType, false))
      dfOut.select("b").rdd.map(_.getAs[Seq[Double]](0)).collect shouldBe(Seq(
        Seq(4D, 5D),
        Seq(0D, -0.1D)
      ))
    }

    it("should min max cut arrays"){
      val cut = ArrayScaler("b", Scaler.MinMaxCut(Some(0), Some(4D)), Inplace)
      val dfOut = cut.$(dfU2)
      dfOut.columns shouldBe(Seq("a","b","c"))
      dfOut.schema("b").dataType shouldBe(ArrayType(DoubleType, false))
      dfOut.select("b").rdd.map(_.getAs[Seq[Double]](0)).collect shouldBe(Seq(
        Seq(4D, 4D),
        Seq(0D, 0D)
      ))
    }

  // U4(Seq(0,0,0) ::
  // U4(Seq(1,2,3) :: 
  // U4(Nil) :: 
  // U4(Seq(-1,3,1)) :: Nil

    it("should avg arrays"){
      val agg = Aggregation("a", Aggregator.Avg, As("a"))
      val dfOut = agg.$(dfU4).as[U5]
      dfOut.rdd.collect shouldBe(Seq(
        U5(Some(0D)),
        U5(Some(2D)),
        U5(None),
        U5(Some(1D))
      ))
    }

    it("should find min/max of arrays"){
      val fmin = Aggregation("a", Aggregator.Min, As("a"))
      val fmax = Aggregation("a", Aggregator.Max, As("a"))
      val dfOutMin = fmin.$(dfU4).as[U5]
      val dfOutMax = fmax.$(dfU4).as[U5]
      dfOutMin.rdd.collect shouldBe(Seq(
        U5(Some(0D)),
        U5(Some(1D)),
        U5(None),
        U5(Some(-1D))
      ))
      dfOutMax.rdd.collect shouldBe(Seq(
        U5(Some(0D)),
        U5(Some(3D)),
        U5(None),
        U5(Some(3D))
      ))
    }

    it("should sum arrays"){
      val agg = Aggregation("a", Aggregator.Sum, As("a"))
      val dfOut = agg.$(dfU4).as[U5]
      dfOut.rdd.collect shouldBe(Seq(
        U5(Some(0D)),
        U5(Some(6D)),
        U5(None),
        U5(Some(3D))
      ))
    }

    it("should find std and variance of arrays"){
      val fstd = Aggregation("a", Aggregator.Std, As("a"))
      val fvar = Aggregation("a", Aggregator.Var, As("a"))
      val dfOutStd = fstd.$(dfU4).as[U5]
      val dfOutVar = fvar.$(dfU4).as[U5]
      dfOutStd.rdd.collect shouldBe(Seq(
        U5(Some(0D)),
        U5(Some(0.81649658092772603)),
        U5(None),
        U5(Some(1.6329931618554521))
      ))
      dfOutVar.rdd.collect shouldBe(Seq(
        U5(Some(0D)),
        U5(Some(0.66666666666666663)),
        U5(None),
        U5(Some(2.6666666666666665))
      ))
    }

    it("should find rms of arrays"){
      val fnorm = Aggregation("a", Aggregator.Rms, As("a"))
      val dfOut = fnorm.$(dfU4).as[U5]
      dfOut.rdd.collect shouldBe(Seq(
        U5(Some(0D)),
        U5(Some(2.1602468994692869)),
        U5(None),
        U5(Some(1.9148542155126762))
      ))
    }

    it("should find norm of arrays"){
      val fnorm = Aggregation("a", Aggregator.Norm(3), As("a"))
      val dfOut = fnorm.$(dfU4).as[U5]
      dfOut.rdd.collect shouldBe(Seq(
        U5(Some(0D)),
        U5(Some(3.3019272488946263)),
        U5(None),
        U5(Some(3.0723168256858471))
      ))
    }
  }

  describe("Chained operations"){

    import com.starcolon.ml.process.Ops._

    it("should chain the operations"){

      val scale  = ArrayScaler("b", Scaler.Ratio(0.2), Inplace)
      val cut    = ArrayScaler("a", Scaler.MinMaxCut(Some(0), None), Inplace)
      val concat = ArrayConcat(Seq("b","a"), As("w"))
      
      // val dfOut  = dfU3 $ scale $ cut $ concat
      val dfOut = Seq(scale, cut, concat) $ dfU3
      dfOut.schema("w").dataType shouldBe(ArrayType(DoubleType, false))
      dfOut.select("w").rdd.map(_.getAs[Seq[Double]](0)).collect shouldBe(Seq(
        Seq(5,0,0,0),
        Seq(2,0,0,1),
        Seq(1,-2,1)
      ))
    }

  }
}