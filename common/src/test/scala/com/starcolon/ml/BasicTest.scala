package com.starcolon.ml

import org.scalatest._
import Matchers._

import com.starcolon.ml.process._
import com.starcolon.ml.transformers._

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.sql.types._
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._

case class B(i: Option[Long], j: Option[String], k: Option[Double])

class BasicTest extends SparkTestInstance with Matchers {

  import spark.implicits._

  describe("Basic Transformers"){

    lazy val df = Seq(
        B(None, None, None),
        B(None, Some("foo"), None),
        B(None, Some("bar"), Some(1.0)),
        B(Some(1), Some("foo"), None),
        B(Some(2), None, Some(3.5)),
        B(Some(3), Some("bar"), Some(0.0))
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

  }

}