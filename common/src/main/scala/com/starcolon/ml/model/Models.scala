package com.starcolon.ml.model

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.ml.feature.ArrayEncoder
import org.apache.spark.sql.types._
import org.apache.spark.ml.{Pipeline,Estimator}
import org.apache.spark.ml.classification.{DecisionTreeClassifier,RandomForestClassifier}

object Classifier {
  val XGBoost: Pipeline = ???
  val DecisionTree: Pipeline = new Pipeline().setStages(Array(decisionTree))
  val RandomForest: Pipeline = new Pipeline().setStages(Array(randomForest))
  val NaiveBayes: Pipeline = ???

  private val predictionCol = "predict"
  private val featuresCol = "features"

  private val decisionTree = new DecisionTreeClassifier()
    .setFeaturesCol(featuresCol)
    .setImpurity("gini")
    .setMaxDepth(5)
    .setMinInstancesPerNode(500)
    .setMaxBins(3)
    .setPredictionCol(predictionCol)
    .setProbabilityCol(predictionCol + "_prob")

  private val randomForest = new RandomForestClassifier()
    .setFeaturesCol(featuresCol)
    .setImpurity("gini")
    .setMaxDepth(5)
    .setMaxBins(3)
    .setMinInstancesPerNode(500)
    .setPredictionCol(predictionCol)
    .setProbabilityCol(predictionCol + "_prob")

  def all = Seq(XGBoost, DecisionTree, RandomForest, NaiveBayes)
}