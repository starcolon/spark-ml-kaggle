package com.starcolon.ml

import org.scalatest._
import Matchers._

import com.starcolon.ml.process._
import com.starcolon.ml.transformers._
import com.starcolon.ml.DatasetUtils.litArray

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.ml.feature.ArrayEncoder
import org.apache.spark.sql.types._
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import scala.collection.mutable.WrappedArray

object BasicTypes {
  case class B(i: Option[Long], j: Option[String], k: Option[Double])
  case class C(i: Option[Long], j: Option[Seq[String]], k: Option[Double])
  case class D(i: Option[Long], j: Option[String], k: Option[Seq[Double]])

  case class V(i: Option[Double], j: Option[Double], k:Option[Double])

  case class S(a: Seq[String], b: Seq[Double], c: Seq[Integer])
}

class BasicTest extends SparkTestInstance with Matchers {

  import spark.implicits._
  import BasicTypes._

  describe("Basic Transformers"){

    lazy val df = Seq(
        B(None, None, None),
        B(None, Some("foo,bar"), None),
        B(None, Some("bar"), Some(1.0)),
        B(Some(1), Some("foo,baz,foobar"), None),
        B(Some(2), None, Some(3.5)),
        B(Some(3), Some("bar,baz"), Some(0.0))
      ).toDS

    lazy val dfV = Seq(
        V(None, Some(4.5), Some(3.25)),
        V(Some(1.5), Some(2.5), Some(2.75)),
        V(Some(0.0), Some(0.0), None),
        V(Some(0.0), Some(0.0), Some(0.0))
      ).toDS

    lazy val dfS = Seq(
        S(Nil, Nil, Nil),
        S(Seq("a","b"),
          Seq(0.1, 0.2, 0.5),
          Seq(0,1)),
        S(Seq("b","c"),
          Seq(0.2),
          Seq(0,3,4,5)),
        S(Seq("c","c","c"),
          Seq(0,0.1,0),
          Seq(3,4,5))
        ).toDS


    it("should impute nulls (string column)"){
      val nullImputer = new NullImputer()
        .setImputedValue("not specified")
        .setInputCols(Array("j"))

      val df_ = new Pipeline().setStages(Array(nullImputer)).fit(df).transform(df).as[B]

      df_.where('j.isNull).count shouldBe 0
      df_.where('j.isNotNull and 'j === lit("not specified")).count shouldBe 2
    }

    it("should impute nulls (long column)"){
      val nullImputer = new NullImputer()
        .setImputedValue(10)
        .setInputCols(Array("i"))

      val df_ = new Pipeline().setStages(Array(nullImputer)).fit(df).transform(df).as[B]

      df_.where('i.isNull).count shouldBe 0
      df_.where('i.isNotNull and 'i === lit(10)).count shouldBe 3
    }

    it("should impute nulls (double column)"){
      val nullImputer = new NullImputer()
        .setImputedValue(0.1)
        .setInputCols(Array("k"))

      val df_ = new Pipeline().setStages(Array(nullImputer)).fit(df).transform(df).as[B]

      df_.where('k.isNull).count shouldBe 0
      df_.where('k.isNotNull and 'k === lit(0.1)).count shouldBe 3
    }

    it("should split string into array"){
      val splitter = new StringSplitter().setInputCols(Array("j"))
      val df_ = new Pipeline().setStages(Array(splitter)).fit(df).transform(df).as[C]

      df_.where('j.isNull).count shouldBe 0
      df_.where('j.isNotNull and 'j === litArray(Seq("foo","bar"))).count shouldBe 1
      df_.where('j.isNotNull and 'j === litArray(Seq("foo","baz","foobar"))).count shouldBe 1
      df_.where('j.isNotNull and 'j === litArray(Seq("bar","baz"))).count shouldBe 1
    }

    it("should lift a primitive type to array (string)"){
      val lifter = new TypeLiftToArrayLifter().setInputCols(Array("j"))
      val df_ = new Pipeline().setStages(Array(lifter)).fit(df).transform(df).as[C]

      df_.where('j.isNull).count shouldBe 0
      df_.where('j.isNotNull and 'j === litArray(Seq("foo,bar"))).count shouldBe 1
      df_.where('j.isNotNull and 'j === litArray(Seq("foo,baz,foobar"))).count shouldBe 1
      df_.where('j.isNotNull and 'j === litArray(Seq("bar,baz"))).count shouldBe 1
    }

    it("should lift a primitive type to array (double)"){
      val lifter = new TypeLiftToArrayLifter().setInputCols(Array("k"))
      val df_ = new Pipeline().setStages(Array(lifter)).fit(df).transform(df).as[D]

      df_.where('k.isNull).count shouldBe 3
      df_.where('k.isNotNull and 'k === litArray(Seq(1.0))).count shouldBe 1
      df_.where('k.isNotNull and 'k === litArray(Seq(3.5))).count shouldBe 1
      df_.where('k.isNotNull and 'k === litArray(Seq(0.0))).count shouldBe 1
    }

    it("should assembly features into vectors with null impute"){
      val assembler = new VectorAssemblerWithNullable()
        .setImputedValue(0.0)
        .setInputCols(Array("i","j","k"))
        .setOutputCol("v")

      val vecToArray = udf{ v: Vector => v.toArray }

      val dfV_ = new Pipeline().setStages(Array(assembler)).fit(dfV).transform(dfV)
        .withColumn("w", vecToArray('v))

      dfV_.where('v.isNull).count shouldBe 0
      val res = dfV_.select("w").collect.map(_.getAs[Seq[Double]](0))

      res(0) shouldBe Seq(0.0, 4.5, 3.25)
      res(1) shouldBe Seq(1.5, 2.5, 2.75)
      res(2) shouldBe Seq(0.0, 0.0, 0.0)
      res(3) shouldBe Seq(0.0, 0.0, 0.0)
    }

    it("should encode string array with indexer"){
      
      val encoder = new ArrayEncoder[String]().setInputCol("a").setOutputCol("v")
      val df_ = encoder.fit(dfS).transform(dfS)
      val res: Seq[Integer] = df_.select("v")
        .flatMap(_.getAs[Seq[Integer]](0))
        .collect
        .toList

      df_.show

      res should contain allOf(0,1,2)
      res should not contain (-1)
      res should not contain (3)
    }

    it("should encode double array with indexer"){

      val encoder = new ArrayEncoder[Double]().setInputCol("b").setOutputCol("v")
      val df_ = encoder.fit(dfS).transform(dfS)
      val res: Seq[Integer] = df_.select("v")
        .flatMap(_.getAs[Seq[Integer]](0))
        .collect
        .toList

      df_.show

      res should contain allOf(0,1,2,3)
      res should not contain (-1)
      res should not contain (4)
    }

    it("should encode long array with indexer"){

      val encoder = new ArrayEncoder[Integer]().setInputCol("c").setOutputCol("v")
      val df_ = encoder.fit(dfS).transform(dfS)
      val res: Seq[Integer] = df_.select("v")
        .flatMap(_.getAs[Seq[Integer]](0))
        .collect
        .toList

      df_.show

      res should contain allOf(0,1,2,3,4)
      res should not contain (-1)
      res should not contain (5)
    }

    it("should encode string array with array encoder"){
      
      val encoder = new StringArrayEncoder().setInputCol("a").setOutputCol("v")
      val df_ = new Pipeline().setStages(Array(encoder)).fit(dfS).transform(dfS)
      
      // +---------+---------------+------------+---------------+
      // |        a|              b|           c|              v|
      // +---------+---------------+------------+---------------+
      // |       []|             []|          []|[0.0, 0.0, 0.0]|
      // |   [a, b]|[0.1, 0.2, 0.5]|      [0, 1]|[1.0, 1.0, 0.0]|
      // |   [b, c]|          [0.2]|[0, 3, 4, 5]|[0.0, 1.0, 1.0]|
      // |[c, c, c]|[0.0, 0.1, 0.0]|   [3, 4, 5]|[0.0, 0.0, 3.0]|
      // +---------+---------------+------------+---------------+

      val res = df_
        .select("v")
        .rdd.map(_.getAs[Seq[Double]](0))
        .collect

      res(0) shouldBe (Seq(0D, 0D, 0D))
    }

  }

}