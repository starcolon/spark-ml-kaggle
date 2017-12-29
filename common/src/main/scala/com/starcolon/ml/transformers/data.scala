package com.starcolon.ml.transformers

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.ml.Transformer 
import org.apache.spark.ml.util._
import org.apache.spark.sql.types._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputColsExposed, HasInOutColExposed}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.functions._
import scala.reflect.runtime.universe._
import scala.util.Try

class NullImputer[T: TypeTag](override val uid: String = Identifiable.randomUID("NullImputerTransformer")) 
extends Transformer
with HasInputColsExposed 
with DefaultParamsWritable {
  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
  override def transformSchema(schema: StructType): StructType = schema
  override def transform(df: Dataset[_]): Dataset[Row] = {
    transformSchema(df.schema, logging = true)
    getImputedValue match {
      case v: Long => df.na.fill(v, $(inputCols))
      case v: Integer => df.na.fill(v.toLong, $(inputCols))
      case v: Double => df.na.fill(v, $(inputCols))
      case s: String => df.na.fill(s, $(inputCols))
      case _ => throw new java.lang.UnsupportedOperationException("Unsupported type to impute values")
    }
  }

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  final val newValue = new Param[T](this, "value", "Value to replace nulls")
  def setImputedValue(value: T): this.type = set(newValue, value)
  def getImputedValue: T = $(newValue)
}

class StringSplitter(override val uid: String = Identifiable.randomUID("StringSplitterTransformer"))
extends Transformer 
with HasInputColsExposed
with DefaultParamsWritable {
  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
  
  override def transformSchema(schema: StructType): StructType = 
    StructType(schema.map{
      case c if ($(inputCols) contains c.name) =>
        c.copy(dataType = ArrayType(StringType, c.nullable))
      case a => a
    })

  private val split = udf{ s: String => Try{ s.split($(delimiter)).toSeq } getOrElse(Seq.empty[String]) }
  
  override def transform(df: Dataset[_]): Dataset[Row] = {
    transformSchema(df.schema, logging = true)
    $(inputCols).foldLeft(df.toDF){ case(d,c) => d.withColumn(c, split(col(c))) }
  }

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  final val delimiter = new Param[String](this, "delimiter", "String delimiter")
  setDefault(delimiter, ",")
  def setValue(value: String): this.type = set(delimiter, value)
}

class TypeLiftToArrayLifter(override val uid: String = Identifiable.randomUID("TypeToArrayLifter"))
extends Transformer 
with HasInputColsExposed
with DefaultParamsWritable {
  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType) = 
    StructType(schema.map{
      case c if ($(inputCols) contains c.name) =>
        c.copy(dataType = ArrayType(c.dataType, c.nullable))
      case a => a
    })

  private final val liftString = udf((s: String) => Seq(s))
  private final val liftLong = udf((s: Long) => Seq(s))
  private final val liftDouble = udf((s: Double) => Seq(s))
  private final val mapLiftF = Map(
    "StringType" -> liftString,
    "LongType" -> liftLong,
    "DoubleType" -> liftDouble)

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  override def transform(df: Dataset[_]): Dataset[Row] = {
    transformSchema(df.schema, logging=true)
    val cols = $(inputCols)
    val schema = df.select(cols.head, cols.tail:_*).schema
    schema.foldLeft(df.toDF){ case(d,colSchema) => 
      val StructField(c,dataType,_,_) = colSchema
      d.withColumn(c, mapLiftF(dataType.toString)(col(c)))
    }
  }
}

class VectorAssemblerWithNullable[T: TypeTag](override val uid: String = Identifiable.randomUID("VectorAssemblerWithNullable"))
extends VectorAssembler {

  override def transform(df: Dataset[_]): Dataset[Row] = {
    // Impute missing values before passing through to the assembler
    transformSchema(df.schema, logging=true)
    val df_ = getImputedValue match {
      case v: Long => df.na.fill(v, $(inputCols))
      case v: Integer => df.na.fill(v.toLong, $(inputCols))
      case v: Double => df.na.fill(v, $(inputCols))
      case s: String => df.na.fill(s, $(inputCols))
      case _ => throw new java.lang.UnsupportedOperationException("Unsupported type to impute values")
    }
    super.transform(df_)
  }

  final val imputeValue = new Param[T](this, "imputeValue", "Value to replace nulls")
  def setImputedValue(value: T): this.type = set(imputeValue, value)
  def getImputedValue: T = $(imputeValue)
}