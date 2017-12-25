package com.starcolon.ml.transformers

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.ml.Transformer 
import org.apache.spark.ml.util._
import org.apache.spark.sql.types._
import org.apache.spark.ml.param._

trait FeatureTransformer
trait FeatureAssembler

class NullImputer[T](override val uid: String = Identifiable.randomUID("NullImputerTransformer")) 
extends Transformer
with ColumnListParams 
with DefaultParamsWritable {
  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
  override def transformSchema(schema: StructType): StructType = schema
  override def transform(df: Dataset[_]): Dataset[Row] = ???

  override def setColumns(value: Seq[String]): this.type = super.setColumns(value)

  final val newValue = new Param[T](this, "value", "Value to replace nulls")
  def setImputedValue(value: T): this.type = set(newValue, value)
  def getImputedValue: T = $(newValue)
}

class StringSplitter(override val uid: String = Identifiable.randomUID("StringSplitterTransformer"))
extends Transformer 
with ColumnListParams 
with DefaultParamsWritable {
  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
  
  override def transformSchema(schema: StructType): StructType = 
    StructType(schema.map{
      case c if (getColumns contains c.name) =>
        c.copy(dataType = ArrayType(StringType, c.nullable))
      case a => a
    })
  
  override def transform(df: Dataset[_]): Dataset[Row] = ???

  override def setColumns(value: Seq[String]): this.type = super.setColumns(value) 

  final val delimiter = new Param[String](this, "delimiter", "String delimiter")
  setDefault(delimiter, ",")
  def setValue(value: String): this.type = set(delimiter, value)
  def getValue: String = $(delimiter)
}