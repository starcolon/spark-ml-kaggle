package com.starcolon.ml.process

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

trait DataSiloT extends Serializable {
  def f(input: Dataset[_]): Dataset[_]
}

trait OutputCol 

object OutputCol {
  case class As(c: String) extends OutputCol
  case object Inplace extends OutputCol
  def as(c: String) = As(c)
}

trait Scaler 

object Scaler {
  case class Ratio(c: Double) extends Scaler
  case class ElementWiseNorm(n: Int) extends Scaler
  case class MinMaxCut(min: Option[Double], max: Option[Double]) extends Scaler
}

object Silo {

  def getOutCol(inputCol: String, as: OutputCol) = as match {
    case OutputCol.Inplace => inputCol
    case OutputCol.As(c) => c
  }

  class DatasetOps(implicit df: Dataset[_]){
    def ->(silo: DataSiloT) = silo.f(df)
  }

  case class SplitString(inputCol: String, delimiter: String = ",", as: OutputCol = OutputCol.Inplace) extends DataSiloT {
    override def f(input: Dataset[_]) = {
      require(input.schema(inputCol).dataType == StringType)
      val out = getOutCol(inputCol, as)
      val split = udf{ s: String => s.split(delimiter) }
      input.withColumn(out, split(col(inputCol)))
    }
  }

  case class ArrayConcat(cols: Seq[String], as: OutputCol.As) extends DataSiloT {
    override def f(input: Dataset[_]) = {
      val types = cols.map(c => input.schema(c).dataType).toSet
      require(types.size == 1)
      val OutputCol.As(out) = as
      val concatInt = udf{(x: Seq[Int], y: Seq[Int]) => x ++ y}
      val concatLong = udf{(x: Seq[Long], y: Seq[Long]) => x ++ y}
      val concatDouble = udf{(x: Seq[Double], y: Seq[Double]) => x ++ y}
      val concatUdf = types.head match {
        case ArrayType(IntegerType,_) => concatInt
        case ArrayType(LongType,_) => concatLong
        case ArrayType(DoubleType,_) => concatDouble
      }
      cols.tail.foldLeft(input.withColumn(out, col(cols.head))){
        (d,c) => d.withColumn(out, concatUdf(col(out), col(c))) 
      }
    }
  }

  case class ArrayScaler(inputCol: String, scaler: Scaler, as: OutputCol = OutputCol.Inplace) extends DataSiloT {
    override def f(input: Dataset[_]) = ???
  }


}

