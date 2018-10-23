package com.starcolon.ml.model

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.ml.feature.ArrayEncoder
import org.apache.spark.sql.types._
import org.apache.spark.ml.{Pipeline,Estimator}
import org.apache.spark.ml.classification.{DecisionTreeClassifier,RandomForestClassifier}

import scala.util.Try

trait ModelColumns {
  val predictionCol = "predict"
  val featuresCol = "feature"
}

object Classifier extends ModelColumns {
  lazy val XGBoost: Pipeline = ???
  lazy val DecisionTree: Pipeline = new Pipeline().setStages(Array(decisionTree))
  lazy val RandomForest: Pipeline = new Pipeline().setStages(Array(randomForest))
  lazy val NaiveBayes: Pipeline = ???
  lazy val SVM: Pipeline = ???
  lazy val DNN: Pipeline = ???

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

  def getName(pl: Pipeline) = Try{ Some(pl.getStages.last.toString.split('@').head) } getOrElse(None)
  def all = Seq(XGBoost, DecisionTree, RandomForest, NaiveBayes)
  def allTreeBase = Seq(XGBoost, DecisionTree, RandomForest)
}