package com.starcolon.ml

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.reflect.runtime.universe._

object NumberUtils {

  def scaleArray[T: Numeric](s: Double, arr: Seq[T]) = {
    arr.map{ a => implicitly[Numeric[T]].toDouble(a) * s }
  }

  def norm[T: Numeric](arr: Seq[T], n: Int) = {
    require(n > 0)
    val d = arr.foldLeft(0D){(a,b) =>
      a + math.abs(math.pow(implicitly[Numeric[T]].toDouble(b), n.toDouble))
    }
    math.pow(d, 1/n.toDouble)
  }

  def mean[T: Numeric](arr: Seq[T]) = {
    require(!arr.isEmpty)
    implicitly[Numeric[T]].toDouble(arr.sum)/arr.size.toDouble
  }

  def minCutArray[T: Numeric](t: Double, arr: Seq[T]) = {
    arr.map{ a => math.max(implicitly[Numeric[T]].toDouble(a), t) }
  }

  def maxCutArray[T: Numeric](t: Double, arr: Seq[T]) = {
    arr.map{ a => math.min(implicitly[Numeric[T]].toDouble(a), t) }
  }

  def minMaxCutArray[T: Numeric](t0: Double, t1: Double, arr: Seq[T]) = {
    require(t0 <= t1)
    arr.map{ a => 
      val d = implicitly[Numeric[T]].toDouble(a)
      math.min(math.max(d, t0), t1) 
    }
  }
}