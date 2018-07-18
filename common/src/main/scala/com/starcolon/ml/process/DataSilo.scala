package com.starcolon.ml.process

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.starcolon.ml.NumberUtils._

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
  case class DivisionByNorm(n: Int) extends Scaler
  case class MinMaxCut(min: Option[Double], max: Option[Double]) extends Scaler
}

object Silo {

  def getOutCol(inputCol: String, as: OutputCol) = as match {
    case OutputCol.Inplace => inputCol
    case OutputCol.As(c) => c
  }

  implicit class DatasetOps(val df: Dataset[_]){
    def $(silo: DataSiloT) = silo.f(df)
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
    override def f(input: Dataset[_]) = {
      val output = getOutCol(inputCol, as)
      val ArrayType(dataType,_) = input.schema(inputCol).dataType
      
      val scaleInt = udf{(s: Double, in: Seq[Int]) => in.map(_ * s)}
      val scaleLong = udf{(s: Double, in: Seq[Long]) => in.map(_ * s)}
      val scaleDouble = udf{(s: Double, in: Seq[Double]) => in.map(_ * s)}

      val cutInt = udf{(a: Double, b: Double, in: Seq[Int]) => minMaxCutArray(a,b,in)}
      val cutLong = udf{(a: Double, b: Double, in: Seq[Long]) => minMaxCutArray(a,b,in)}
      val cutDouble = udf{(a: Double, b: Double, in: Seq[Double]) => minMaxCutArray(a,b,in)}

      val normInt = udf{(n: Int, in: Seq[Int]) => norm(in, n)}
      val normLong = udf{(n: Int, in: Seq[Long]) => norm(in, n)}
      val normDouble = udf{(n: Int, in: Seq[Double]) => norm(in, n)}

      scaler match {
        case Scaler.Ratio(r) => 
          val rl = lit(r)
          dataType match {
            case IntegerType => input.withColumn(output, scaleInt(rl, col(inputCol)))
            case LongType => input.withColumn(output, scaleLong(rl, col(inputCol)))
            case DoubleType => input.withColumn(output, scaleDouble(rl, col(inputCol)))
          }

        case Scaler.DivisionByNorm(n) => 
          val nl = lit(n)
          val TEMP = scala.util.Random.nextInt.toString
          (dataType match {
            case IntegerType => input
              .withColumn(TEMP, lit(1D)/normInt(nl, col(inputCol)))
              .withColumn(output, scaleInt(col(TEMP), col(inputCol)))
            case LongType => input
              .withColumn(TEMP, lit(1D)/normLong(nl, col(inputCol)))
              .withColumn(output, scaleLong(col(TEMP), col(inputCol)))
            case DoubleType => input
              .withColumn(TEMP, lit(1D)/normDouble(nl, col(inputCol)))
              .withColumn(output, scaleDouble(col(TEMP), col(inputCol)))
          }).drop(TEMP)
          
        case Scaler.MinMaxCut(a,b) => 
          val _min = lit(a.getOrElse(Double.NegativeInfinity))
          val _max = lit(b.getOrElse(Double.PositiveInfinity))
          dataType match {
            case IntegerType => input.withColumn(output, cutInt(_min, _max, col(inputCol)))
            case LongType => input.withColumn(output, cutLong(_min, _max, col(inputCol)))
            case DoubleType => input.withColumn(output, cutDouble(_min, _max, col(inputCol)))
          }
      }
    }
  }


}

