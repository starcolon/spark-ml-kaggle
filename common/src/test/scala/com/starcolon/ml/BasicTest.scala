package com.starcolon.ml

import org.scalatest._
import Matchers._

import com.starcolon.ml.process._
import com.starcolon.ml.transformers._
import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.sql.types._

case class B(i: Option[Int], j: Option[String], k: Option[Double])

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

    }

  }

}