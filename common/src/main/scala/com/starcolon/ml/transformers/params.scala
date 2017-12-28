package com.starcolon.ml.transformers

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.ml.Transformer 
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._

// TAOTODO: Deprecate this, use Spark official params instead
trait InputOutputColumnParams extends Params {
  val inputColumn = new Param[String](this, "inputColumn", "Input column name")
  val outputColumn = new Param[String](this, "outputColumn", "Output column name")
  def setInputColumn(value: String): this.type = set(inputColumn, value)
  def setOutputColumn(value: String): this.type = set(outputColumn, value)
  def getInputColumn: String = $(inputColumn)
  def getOutputColumn: String = $(outputColumn)
}
