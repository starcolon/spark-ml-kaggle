package com.starcolon.ml.transformers

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.ml.Transformer 
import org.apache.spark.ml.util._
import org.apache.spark.sql.types._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
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

/**
 * A transformer which splits a string by a specified delimiter into an array of string
 */
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

// TAOTODO: Add string array encoder
// which maps [[Array[String]]] => [[Array[Int]]] at fixed length
// where the length of the final array is the number of distinct possible string values

class StringArrayEncoder(override val uid: String = Identifiable.randomUID("StringArrayEncoder"))
extends Transformer
with HasInputColExposed
with HasOutputColExposed
with DefaultParamsWritable {
  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType) = 
    schema.add($(outputCol), ArrayType(DoubleType, true), true)

  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(df: Dataset[_]): Dataset[Row] = {
    transformSchema(df.schema, logging=true)
    // Collect all possible values
    val sampleSet = df
      .select(explode(col($(inputCol))).as("v"))
      .dropDuplicates
      .orderBy("v")
      .rdd.map(_.getAs[String](0))
      .collect

    // TAODEBUG:
    println(Console.CYAN + 
      "All possible values : " + 
      sampleSet.mkString(", ") + Console.RESET)
    
    val encode = udf{ ns: Seq[String] => 
      if (ns == null) sampleSet.map(_ => 0D)
      else sampleSet.map(s => ns.count(_ == s).toDouble)
    }
    df.withColumn($(outputCol), encode(col($(inputCol))))
  }
}

/**
 * Lifting a column of type [T] to [Seq[T]]
 */
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

/**
 * A transformer which prints out an analysis of 
 * an interaction between features and the target values
 * without changing the data
 *
 * NOTE: currently only supports in/out types of [[double]] 
 */
class FeatureVsTargetReport(override val uid: String = Identifiable.randomUID("FeatureVsTargetMeasure"))
extends Transformer 
with HasInputColsExposed
with HasOutputColExposed {

  final def setInputCols(value: Array[String]): this.type = set(inputCols, value)
  final def setOutputCol(value: String): this.type = set(outputCol, value)

  // TAOTODO: Add strategy as a param

  private def printAnalysisOnInputs(inputs: Seq[String], df: Dataset[_]): Unit = {
    println
    println("*****************************************")
    println(s" Feature distribution : $inputs.mkString(", ")")
    println("*****************************************")
    val output = $(outputCol)
    val distinctOutputs = df.select($(outputCol))
      .distinct.collect
      .map(_.getAs[Double](0))
      .sorted

    val outputDists = distinctOutputs.map{ out =>
      df.where(col($(outputCol)) === out)
        .groupBy(inputs.head, inputs.tail:_*)
        .count
        .withColumnRenamed("count", s"$output = $out")
    }

    val joined = outputDists.reduce{ (a,b) =>
      a.join(b, inputs, "outer")
    }.orderBy(inputs.head, inputs.tail:_*).show(100)
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
  override def transformSchema(schema: StructType) = schema
  override def transform(df: Dataset[_]): Dataset[Row] = {
    val dfAnalys = df.select($(outputCol), $(inputCols).sorted:_*)

    val inputColsSorted = $(inputCols)
    
    // Analysis on each individual column
    for (c <- inputColsSorted) yield printAnalysisOnInputs(c :: Nil, dfAnalys)

    // Analysis on each pair of columns
    { for (a <- inputColsSorted; b <- inputColsSorted.filterNot(Set(a))) 
      yield Seq(a,b).sorted }
      .distinct
      .foreach{ cols => printAnalysisOnInputs(cols, dfAnalys) }

    df.toDF
  }
}

