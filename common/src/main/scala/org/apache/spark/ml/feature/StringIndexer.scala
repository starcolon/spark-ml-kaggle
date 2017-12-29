package org.apache.spark.ml.feature

import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasHandleInvalid, HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


class StringArrayIndexer extends StringIndexer {

  /**
   * NOTE: Based on Spark 2.2.1 base code
   */
  override def fit(dataset: Dataset[_]): StringIndexerModel = {
    transformSchema(dataset.schema, logging = true)
    val counts = dataset.na.drop(Array($(inputCol))).select($(inputCol))
      .rdd
      .map(_.getAs[Seq[String]](0))
      .flatMap(identity)
      .countByValue()
    val labels = counts.toSeq.sortBy(-_._2).map(_._1).toArray
    copyValues(new StringIndexerModel(uid, labels).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = ???
}