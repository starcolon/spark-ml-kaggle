package com.starcolon.ml

import org.scalatest._
import Matchers._

import com.starcolon.ml.process._
import com.starcolon.ml.transformers._
import com.starcolon.ml.DatasetUtils.litArray

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.sql.types._
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._

case class B(i: Option[Long], j: Option[String], k: Option[Double])
case class C(i: Option[Long], j: Option[Seq[String]], k: Option[Double])
case class D(i: Option[Long], j: Option[String], k: Option[Seq[Double]])

class BasicTest extends SparkTestInstance with Matchers {

  import spark.implicits._

  describe("Basic Transformers"){

    lazy val df = Seq(
        B(None, None, None),
        B(None, Some("foo,bar"), None),
        B(None, Some("bar"), Some(1.0)),
        B(Some(1), Some("foo,baz,foobar"), None),
        B(Some(2), None, Some(3.5)),
        B(Some(3), Some("bar,baz"), Some(0.0))
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

  }

}