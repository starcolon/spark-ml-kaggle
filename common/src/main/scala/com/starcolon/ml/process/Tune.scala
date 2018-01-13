package com.starcolon.ml.process

import org.apache.spark.ml.{Pipeline,Estimator}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.classification.{DecisionTreeClassifier,RandomForestClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

trait Tuner {
  val numFolds: Int = 3
  val evaluator: Evaluator = new BinaryClassificationEvaluator

  val paramGrid = new ParamGridBuilder()
    // .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
    // .addGrid(lr.regParam, Array(0.1, 0.01))
    // .build()

  def crossValidate(pipe: Pipeline) = {
    val cv = new CrossValidator()
    .setEstimator(pipe)
    .setEvaluator(evaluator)
    .setEstimatorParamMaps(paramGrid.build)
    .setNumFolds(numFolds)
  }
}





  