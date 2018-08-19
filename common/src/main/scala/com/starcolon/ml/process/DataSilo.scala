package com.starcolon.ml.process

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.starcolon.ml.{NumberUtils => NU}

import scala.reflect.runtime.universe._
import scala.reflect.ClassTag
import sys.process._

sealed trait DataSiloT extends Serializable {
  def $(input: Dataset[_]): Dataset[_]
}

sealed trait OutputCol 

object OutputCol {
  case class As(c: String) extends OutputCol
  case object Inplace extends OutputCol
  def as(c: String) = As(c)
}

sealed trait Scaler 

object Scaler {
  case class Ratio(c: Double) extends Scaler
  case class DivisionByNorm(n: Int) extends Scaler
  case class MinMaxCut(min: Option[Double], max: Option[Double]) extends Scaler
}

sealed trait Aggregator {
  def f[T: Numeric](arr: Seq[T]): Option[Double]

  lazy val udfInt = udf{(arr: Seq[Int]) => f(arr)}
  lazy val udfLong = udf{(arr: Seq[Long]) => f(arr)}
  lazy val udfDouble = udf{(arr: Seq[Double]) => f(arr)}
}

sealed trait StateMemory

object Aggregator {
  case object Sum extends Aggregator { override def f[T: Numeric](arr: Seq[T]) = if (arr.isEmpty) None else Some(implicitly[Numeric[T]].toDouble(arr.sum)) }
  case object Min extends Aggregator { override def f[T: Numeric](arr: Seq[T]) = if (arr.isEmpty) None else Some(implicitly[Numeric[T]].toDouble(arr.min)) }
  case object Max extends Aggregator { override def f[T: Numeric](arr: Seq[T]) = if (arr.isEmpty) None else Some(implicitly[Numeric[T]].toDouble(arr.max)) }
  case object Avg extends Aggregator { override def f[T: Numeric](arr: Seq[T]) = if (arr.isEmpty) None else Some(implicitly[Numeric[T]].toDouble(arr.sum)/arr.size.toDouble) }
  case object Std extends Aggregator { override def f[T: Numeric](arr: Seq[T]) = Var.f(arr).map(math.sqrt) }
  case object Var extends Aggregator { override def f[T: Numeric](arr: Seq[T]) = 
    arr match {
      case Nil => None
      case _ => 
        val m = NU.mean(arr)
        Some(arr.foldLeft(0D){(d,a) => d + math.pow(implicitly[Numeric[T]].toDouble(a)-m,2)}/arr.size.toDouble)
    } 
  }
  case object Rms extends Aggregator { override def f[T: Numeric](arr: Seq[T]) = 
    arr match {
      case Nil => None
      case _ =>
        Some(NU.rms(arr))
    }
  }
  case class Norm(n: Int) extends Aggregator { 
    require(n > 0) 
    override def f[T: Numeric](arr: Seq[T]) = 
      if (arr.isEmpty) None else Some(NU.norm(arr, n))
  }
}

object Silo {

  def getOutCol(inputCol: String, as: OutputCol) = as match {
    case OutputCol.Inplace => inputCol
    case OutputCol.As(c) => c
  }

  case class SplitString(inputCol: String, delimiter: String = ",", as: OutputCol = OutputCol.Inplace) extends DataSiloT {
    override def $(input: Dataset[_]) = {
      require(input.schema(inputCol).dataType == StringType)
      val out = getOutCol(inputCol, as)
      val split = udf{ s: String => s.split(delimiter) }
      input.withColumn(out, split(col(inputCol)))
    }
  }

  case class ArrayExplode(inputCol: String, as: OutputCol = OutputCol.Inplace) extends DataSiloT {
    override def $(input: Dataset[_]) = {
      val out = getOutCol(inputCol, as)
      input.withColumn(out, explode(col(inputCol)))
    }
  }

  case class ArrayEncode[T: ClassTag](
    inputCol: String, 
    as: OutputCol = OutputCol.Inplace, 
    valueFilePath: String = "tmp/spark-ml-ae-" + inputCol + "-" + java.util.UUID.randomUUID.toString) 
  extends DataSiloT {
    override def $(input: Dataset[_]) = {
      val out = getOutCol(inputCol, as)
      val distinctValDF = input.select(explode(col(inputCol)).as(inputCol))
        .dropDuplicates
        .coalesce(1)

      if (!valueFilePath.isEmpty)
        WriteCSV(valueFilePath, withHeader = false) <~ distinctValDF

      val distinctVals = distinctValDF
        .rdd
        .map(_.getAs[T](0))

      ???
    }
  }

  case class OneHotEncode[T: ClassTag](
    inputCol: String,
    as: OutputCol = OutputCol.Inplace,
    valueFilePath: String = "tmp/spark-ml-ohe-" + inputCol + "-" + java.util.UUID.randomUUID.toString)
  extends DataSiloT {
    // TAOTODO: Make value file either [ReadFrom] or [WriteTo]
    override def $(input: Dataset[_]) = {
      val out = getOutCol(inputCol, as)
      val distinctValDF = input.select(col(inputCol).cast(StringType)).dropDuplicates.coalesce(1)
      val valueMapp = distinctValDF.collect

      if (!valueFilePath.isEmpty)
        WriteCSV(valueFilePath, withHeader = false) <~ distinctValDF

      val mapToIndex = udf{v: String => valueMapp.indexOf(v)}
      input.withColumn(out, mapToIndex(col(inputCol).cast(StringType)))
    }
  }

  case class ArrayConcat(cols: Seq[String], as: OutputCol.As) extends DataSiloT {
    override def $(input: Dataset[_]) = {
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
    override def $(input: Dataset[_]) = {
      val output = getOutCol(inputCol, as)
      val ArrayType(dataType,_) = input.schema(inputCol).dataType
      
      val scaleInt = udf{(s: Double, in: Seq[Int]) => in.map(_ * s)}
      val scaleLong = udf{(s: Double, in: Seq[Long]) => in.map(_ * s)}
      val scaleDouble = udf{(s: Double, in: Seq[Double]) => in.map(_ * s)}

      val cutInt = udf{(a: Double, b: Double, in: Seq[Int]) => NU.minMaxCutArray(a,b,in)}
      val cutLong = udf{(a: Double, b: Double, in: Seq[Long]) => NU.minMaxCutArray(a,b,in)}
      val cutDouble = udf{(a: Double, b: Double, in: Seq[Double]) => NU.minMaxCutArray(a,b,in)}

      val normInt = udf{(n: Int, in: Seq[Int]) => NU.norm(in, n)}
      val normLong = udf{(n: Int, in: Seq[Long]) => NU.norm(in, n)}
      val normDouble = udf{(n: Int, in: Seq[Double]) => NU.norm(in, n)}

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

  case class Aggregation(inputCol: String, aggregator: Aggregator, as: OutputCol = OutputCol.Inplace) extends DataSiloT {
    override def $(input: Dataset[_]) = {
      val output = getOutCol(inputCol, as)
      val ArrayType(dataType,_) = input.schema(inputCol).dataType
      dataType match {
        case IntegerType => input.withColumn(output, aggregator.udfInt(col(inputCol)))
        case LongType => input.withColumn(output, aggregator.udfLong(col(inputCol)))
        case DoubleType => input.withColumn(output, aggregator.udfDouble(col(inputCol)))
      }
    }
  }

}

