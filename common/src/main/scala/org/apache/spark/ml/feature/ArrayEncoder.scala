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
import org.apache.spark.SparkException
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag
import org.apache.hadoop.fs.Path

private[feature] trait ArrayEncoderBase
extends Params 
with HasInputCol with HasOutputCol {

  def transformAndValidate(schema: StructType): StructType = {
    val inputColumn = $(inputCol)
    val outputColumn = $(outputCol)
    require(schema.map(_.name) contains inputColumn, s"Dataset has to contain the input column : $inputColumn")
    require(!(schema.map(_.name) contains outputCol), s"Dataset already has an output column : $outputColumn")
    schema.add(StructField(outputColumn, ArrayType(IntegerType, false), false))
  }  
}

private[feature] class ArrayEncoderWriter[T: ClassTag](instance: ArrayEncoderModel[T]) 
extends MLWriter {
  private case class Data(labels: Array[T])

  override protected def saveImpl(path: String): Unit = {
    DefaultParamsWriter.saveMetadata(instance, path, sc)
    val data = Data(instance.labels)
    val dataPath = new Path(path, "data").toString
    sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
  }
}

private[feature] object ArrayEncoderUtil {
  
  def toIntSeq(s: Array[_]) = s.toSeq.asInstanceOf[Seq[Integer]]
  def toLongSeq(s: Array[_]) = s.toSeq.asInstanceOf[Seq[Long]]
  def toDoubleSeq(s: Array[_]) = s.toSeq.asInstanceOf[Seq[Double]]
  def toStrSeq(s: Array[_]) = s.toSeq.asInstanceOf[Seq[String]]

}

class ArrayEncoderModel[T: ClassTag](override val uid: String, val labels: Array[T])
extends Model[ArrayEncoderModel[T]]
with ArrayEncoderBase
with MLWritable {

  lazy val stringLabels = ArrayEncoderUtil.toStrSeq(labels)
  lazy val intLabels = ArrayEncoderUtil.toIntSeq(labels)
  lazy val doubleLabels = ArrayEncoderUtil.toDoubleSeq(labels)
  lazy val longLabels = ArrayEncoderUtil.toLongSeq(labels)

  def write: MLWriter = new ArrayEncoderWriter[T](this)

  override def copy(extra: ParamMap): ArrayEncoderModel[T] = {
    val copied = new ArrayEncoderModel[T](uid, labels)
    copyValues(copied, extra).setParent(parent)
  }

  private val encodeStringArray = udf{ arr: Seq[String] => arr.map(stringLabels.indexOf(_)) }
  private val encodeIntArray = udf{ arr: Seq[Integer] => arr.map(intLabels.indexOf(_)) }
  private val encodeLongArray = udf{ arr: Seq[Long] => arr.map(longLabels.indexOf(_)) }
  private val encodeDoubleArray = udf{ arr: Seq[Double] => arr.map(doubleLabels.indexOf(_)) }

  def transformSchema(schema: StructType): StructType = transformAndValidate(schema)

  def transform(dataset: Dataset[_]): DataFrame = {
    transformAndValidate(dataset.schema)
    dataset.schema($(inputCol)).dataType match {
      case ArrayType(StringType,_) => dataset.withColumn($(outputCol), encodeStringArray(col($(inputCol))))
      case ArrayType(IntegerType,_) => dataset.withColumn($(outputCol), encodeIntArray(col($(inputCol))))
      case ArrayType(LongType,_) => dataset.withColumn($(outputCol), encodeLongArray(col($(inputCol))))
      case ArrayType(DoubleType,_) => dataset.withColumn($(outputCol), encodeDoubleArray(col($(inputCol))))
      case t => throw new SparkException(s"Unable to encode unsupported array type : $t")
    }
  }
}

private class ArrayEncoderModelReader[T: ClassTag] extends MLReader[ArrayEncoderModel[T]] {

  override def load(path: String): ArrayEncoderModel[T] = {
    val className = classOf[ArrayEncoderModel[T]].getName
    val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
    val dataPath = new Path(path, "data").toString
    val data = sparkSession.read.parquet(dataPath)
      .select("labels")
      .head()
    val labels = data.getAs[Seq[T]](0).toArray
    val model = new ArrayEncoderModel[T](metadata.uid, labels)
    DefaultParamsReader.getAndSetParams(model, metadata)
    model
  }

  def read: MLReader[ArrayEncoderModel[T]] = new ArrayEncoderModelReader[T]
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

  final def setInputCol(value: String) = set(inputCol, value)
  final def setOutputCol(value: String) = set(outputCol, value)
}