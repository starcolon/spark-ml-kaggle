package com.starcolon.ml.model

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.ml.feature.ArrayEncoder
import org.apache.spark.sql.types._
import org.apache.spark.ml.{Pipeline,Estimator}
import org.apache.spark.ml.classification.DecisionTreeClassifier

object Classifier {
  val XGBoost: Pipeline = ???
  val DecisionTree: Pipeline = new Pipeline().setStages(Array(decisionTree))
  val RandomForest: Pipeline = ???
  val NaiveBayes: Pipeline = ???

  private val decisionTree = new DecisionTreeClassifier()
    .setFeaturesCol("features")
    .setImpurity("gini")
    .setMaxDepth(16)
    .setMinInstancesPerNode(60)
    .setMaxBins(8)
    .setPredictionCol("predict")
    .setProbabilityCol("predict_prob")

  def all = Seq(XGBoost, DecisionTree, RandomForest, NaiveBayes)
}