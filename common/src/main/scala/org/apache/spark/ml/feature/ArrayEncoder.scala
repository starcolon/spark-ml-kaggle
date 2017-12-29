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

class ArrayEncoderModel[T: ClassTag](override val uid: String, labels: Array[T])
extends Model[ArrayEncoderModel[T]]
with MLWritable {

  def write: MLWriter = ???

  override def copy(extra: ParamMap): ArrayEncoderModel[T] = ???

  def transformSchema(schema: StructType): StructType = ???

  def transform(dataset: Dataset[_]): DataFrame = ???

}


class ArrayEncoder[T: ClassTag] (override val uid: String = Identifiable.randomUID("ArrayEncoder"))
extends Estimator[ArrayEncoderModel[T]]
with HasInputCol with HasOutputCol
with DefaultParamsWritable {

  override def fit(dataset: Dataset[_]): ArrayEncoderModel[T] = {
    transformSchema(dataset.schema, logging = true)
    val counts = dataset.na.drop(Array($(inputCol))).select($(inputCol))
      .rdd
      .map(_.getAs[Seq[T]](0))
      .flatMap(identity)
      .countByValue()
    val labels = counts.toSeq.sortBy(-_._2).map(_._1).toArray
    // TAOTODO: Replace following logic of creating a model
    copyValues(new ArrayEncoderModel[T](uid, labels).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = ???

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}