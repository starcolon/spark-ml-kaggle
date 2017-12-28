package com.starcolon.ml

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, SQLContext, SQLImplicits}
import org.apache.spark.util.Utils

import org.scalatest._

trait SparkTestInstance extends FunSpec with BeforeAndAfterAll {
  implicit lazy val spark = {
    SparkSession.builder
      .master("local")
      .appName("MLTest")
      .getOrCreate()
  }

  override def afterAll() {
    try {
      SparkSession.clearActiveSession()
      if (spark != null) {
        spark.stop()
      }
    } finally {
      super.afterAll()
    }
  }
}