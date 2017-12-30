package org.apache.spark.ml.feature

import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasHandleInvalid, HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.util.MLWriter
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

private[feature] trait ArrayEncoderBase
extends Params 
with HasInputCol with HasOutputCol {

  def transformAndValidate(schema: StructType): StructType = {
    val inputColumn = $(inputCol)
    val outputColumn = $(outputCol)
    require(schema.map(_.name) contains inputColumn, "Dataset has to contain the input column : $inputColumn}")
    require(schema.map(_.name) contains outputCol == false, "Dataset already has an output column : $outputColumn")
    schema.add(StructField(outputColumn, ArrayType(IntegerType, false), false))
  }

}

class ArrayEncoderModel[T: ClassTag](override val uid: String, labels: Array[T])
extends Model[ArrayEncoderModel[T]]
with ArrayEncoderBase
with MLWritable {

  def write: MLWriter = ???

  override def copy(extra: ParamMap): ArrayEncoderModel[T] = ???

  def transformSchema(schema: StructType): StructType = transformAndValidate(schema)

  def transform(dataset: Dataset[_]): DataFrame = ???

}


class ArrayEncoder[T: ClassTag] (override val uid: String = Identifiable.randomUID("ArrayEncoder"))
extends Estimator[ArrayEncoderModel[T]]
with ArrayEncoderBase
with DefaultParamsWritable {

  override def fit(dataset: Dataset[_]): ArrayEncoderModel[T] = {
    transformSchema(dataset.schema, logging = true)
    val counts = dataset.na.drop(Array($(inputCol))).select($(inputCol))
      .rdd
      .map(_.getAs[Seq[T]](0))
      .flatMap(identity)
      .countByValue()
    val labels = counts.toSeq.sortBy(-_._2).map(_._1).toArray
    copyValues(new ArrayEncoderModel[T](uid, labels).setParent(this))
  }

  override def transformSchema(schema: StructType) = transformAndValidate(schema)

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}