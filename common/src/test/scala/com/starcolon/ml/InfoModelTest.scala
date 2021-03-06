package com.starcolon.ml

import org.scalatest._
import Matchers._

import com.starcolon.ml.process._
import com.starcolon.ml.transformers._
import com.starcolon.ml.DatasetUtils.litArray
import com.starcolon.ml.model.MutualInformation

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.ml.feature.ArrayEncoder
import org.apache.spark.sql.types._
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import scala.collection.mutable.WrappedArray

object InfoTypes {
  case class A(x1: String, x2: String, y: String)
}

class InfoModelTest extends SparkTestInstance with Matchers {

  import spark.implicits._
  import InfoTypes._
  import TestImplicits._

  lazy val aa = A("a","00","T") ::
              A("a","11","T") ::
              A("a","22","T") ::
              A("a","33","T") ::
              A("a","33","F") ::
              A("b","00","T") ::
              A("b","00","F") ::
              A("b","11","F") ::
              A("b","11","F") ::
              A("b","22","F") ::
              A("b","33","T") ::
              A("b","33","F") ::
              A("b","33","F") :: Nil
  lazy val dfA = aa.toDF            

  describe("Mutual information"){

    it("should compute mutual information of single column"){
      val mi = new MutualInformation("x1" :: Nil, "y")

      // +---+---+-------------------+-------------------+-------------------+--------------------+
      // |y  |x  |pXY                |pX                 |pY                 |p                   |
      // +---+---+-------------------+-------------------+-------------------+--------------------+
      // |F  |[b]|0.46153846153846156|0.6153846153846154 |0.5384615384615384 |0.1529340627482042  |
      // |F  |[a]|0.07692307692307693|0.38461538461538464|0.5384615384615384 |-0.07618451569445207|
      // |T  |[a]|0.3076923076923077 |0.38461538461538464|0.46153846153846156|0.16924502674439135 |
      // |T  |[b]|0.15384615384615385|0.6153846153846154 |0.46153846153846156|-0.09432376505944753|
      // +---+---+-------------------+-------------------+-------------------+--------------------+

      val d: Double = mi ~ dfA
      (d ~= 0.15) shouldBe true
    }

    ignore("should compute mutual information of multiple columns"){
      val mi = new MutualInformation("x1" :: "x2" :: Nil, "y", debug = true)

      // +---+-------+-------------------+-------------------+-------------------+---------------------+
      // |y  |x      |pXY                |pX                 |pY                 |p                    |
      // +---+-------+-------------------+-------------------+-------------------+---------------------+
      // |F  |[b, 00]|0.07692307692307693|0.15384615384615385|0.5384615384615384 |-0.005700613242593997|
      // |F  |[b, 11]|0.15384615384615385|0.15384615384615385|0.5384615384615384 |0.09523680129326514  |
      // |T  |[a, 22]|0.07692307692307693|0.07692307692307693|0.46153846153846156|0.059476145248729365 |
      // |T  |[a, 11]|0.07692307692307693|0.07692307692307693|0.46153846153846156|0.059476145248729365 |
      // |F  |[b, 33]|0.15384615384615385|0.23076923076923078|0.5384615384615384 |0.032857553892009096 |
      // |F  |[a, 33]|0.07692307692307693|0.15384615384615385|0.5384615384615384 |-0.005700613242593997|
      // |T  |[b, 33]|0.07692307692307693|0.23076923076923078|0.46153846153846156|-0.02503249234112524 |
      // |T  |[b, 00]|0.07692307692307693|0.15384615384615385|0.46153846153846156|0.006157131359502797 |
      // |T  |[a, 33]|0.07692307692307693|0.15384615384615385|0.46153846153846156|0.006157131359502797 |
      // |F  |[b, 22]|0.07692307692307693|0.07692307692307693|0.5384615384615384 |0.04761840064663257  |
      // |T  |[a, 00]|0.07692307692307693|0.07692307692307693|0.46153846153846156|0.059476145248729365 |
      // +---+-------+-------------------+-------------------+-------------------+---------------------+

      val d: Double = mi ~ dfA
      (d ~= 0.33) shouldBe true
    }
  }
}