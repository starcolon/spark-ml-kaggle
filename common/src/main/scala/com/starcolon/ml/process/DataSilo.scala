package com.starcolon.ml.process

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.Vectors
import com.starcolon.ml.{NumberUtils => NU}
import com.starcolon.ml.DatasetUtils.litArray

import scala.reflect.runtime.universe._
import scala.reflect.ClassTag
import sys.process._
import scala.util.{Try, Success, Failure}

private case class KV(k: String, v: Double){
  def + (another: KV) = KV(k, another.v + v)
}

sealed trait DataSiloT extends Serializable {
  def $(input: Dataset[_]): Dataset[_]
}

object Ops {
  implicit class SiloSeqOps(val seq: Seq[DataSiloT]) extends AnyVal {
    def $(input: Dataset[_]): Dataset[Row] = {
      seq.foldLeft(input.toDF){ case(d,s) => (s $ d).toDF}
    }
  }
}

sealed trait OutputCol 

object OutputCol {
  case class As(c: String) extends OutputCol
  case object Inplace extends OutputCol
  def as(c: String) = As(c)
}

sealed trait DataFile

case class SaveToDataFile(s: String) extends DataFile
case class LoadFromDataFile(s: String) extends DataFile

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
    valueFilePath: String = "tmp/spark-ml-ae-" + java.util.UUID.randomUUID.toString) 
  extends DataSiloT {
    override def $(input: Dataset[_]) = {
      val out = getOutCol(inputCol, as)
      val distinctValDF = input.select(explode(col(inputCol)).cast(StringType).as(inputCol))
        .dropDuplicates
        .coalesce(1)

      if (!valueFilePath.isEmpty)
        WriteCSV(valueFilePath, withHeader = false) <~ distinctValDF

      val distinctVals = distinctValDF
        .rdd
        .map(_.getAs[String](0))
        .collect

      val castArrayInt = udf{ arr: Seq[Int] => arr.map(_.toString)}
      val castArrayLong = udf{ arr: Seq[Long] => arr.map(_.toString)}
      val castArrayBool = udf{ arr: Seq[Boolean] => arr.map(_.toString)}
      val castArrayDouble = udf{ arr: Seq[Double] => arr.map(_.toString)}

      val mapToIndex = udf{vs: Seq[String] => vs.map(distinctVals.indexOf(_))}
      val inputCasted = input.schema(inputCol).dataType match {
        case ArrayType(IntegerType,_) => input.withColumn(inputCol, castArrayInt(col(inputCol)))
        case ArrayType(LongType,_) => input.withColumn(inputCol, castArrayLong(col(inputCol)))
        case ArrayType(BooleanType,_) => input.withColumn(inputCol, castArrayBool(col(inputCol)))
        case ArrayType(DoubleType,_) => input.withColumn(inputCol, castArrayDouble(col(inputCol)))
        case ArrayType(StringType,_) => input
      }
      inputCasted.withColumn(out, mapToIndex(col(inputCol)))
    }
  }

  case class OneHotEncode[T: ClassTag](
    inputCol: String,
    as: OutputCol = OutputCol.Inplace,
    dataFile: DataFile = SaveToDataFile("tmp/spark-ml-ohe-" + java.util.UUID.randomUUID.toString))
  extends DataSiloT {

    override def $(input: Dataset[_]) = {

      val spark = input.sparkSession
      val out = getOutCol(inputCol, as)

      val distinctValDF = dataFile match {
        case LoadFromDataFile(s) => spark.read.option("header", "false").csv(s)
        case SaveToDataFile(s) => 
          val dfV = input.select(col(inputCol).cast(StringType)).dropDuplicates.coalesce(1)
          WriteCSV(s, withHeader = false) <~ dfV
          dfV
      }

      val valueMapp = distinctValDF.rdd.map(_.getAs[String](0)).collect
      val mapToIndex = udf{v: String => valueMapp.indexOf(v)}
      input.withColumn(out, mapToIndex(col(inputCol).cast(StringType)))
    }
  }

  case class PredefinedEncode[T: ClassTag](
    inputCol: String,
    as: OutputCol = OutputCol.Inplace,
    map: Map[T, Int],
    defaultOutput: Int)
  extends DataSiloT {

    override def $(input: Dataset[_]) = {
      val out = getOutCol(inputCol, as)
      val strMap = map.map{ case(k,v) => (k.toString,v)}
      val mapper = udf{(v: String) => if (v==null) defaultOutput else strMap.getOrElse(v.trim(), defaultOutput)}
      input.withColumn(out, mapper(col(inputCol).cast(StringType)))
    }
  }

  /**
   * Convert integer values to percentile
   */
  case class PercentileInt(
    inputCol: String,
    as: OutputCol = OutputCol.Inplace,
    dataFile: DataFile = SaveToDataFile("tmp/spark-ml-sii-" + java.util.UUID.randomUUID.toString),
    digit: Int = 3)
  extends DataSiloT {

    override def $(input: Dataset[_]) = {
      val out = getOutCol(inputCol, as)
      val N = math.pow(10, digit).toInt 
      val round = udf{s: String => Try{ s.toDouble.round * N}.getOrElse(0L)}

      val mapPR = dataFile match {
        case LoadFromDataFile(s) => 
          ??? // TAOTODO: Load map file
        case SaveToDataFile(s) => 
          val df = input.withColumn(out, round(col(inputCol)))
          val pop = df.select(out).groupBy(out).count.rdd.map{ p =>
            (p.getAs[Long](0), p.getAs[Long](1))
          }.collectAsMap

          val sum = pop.values.sum.toDouble

          val map = pop.map{ case(k,v) => 
            (k, pop.filter{ case(a,_) => k > a}.values.sum / sum) 
          }.toMap

          // TAOTODO: Save map to file

          map
      }
      
      val toPR = udf{ d: Long => mapPR.getOrElse(d, 0D) }
      input.withColumn(out, toPR(round(col(inputCol))))
    }
  }

  /**
   * Replace the string value which is parsable to double with the specified value
   */
  case class ReplaceNumericalValue(inputCol: String, as: OutputCol, value: String) 
  extends DataSiloT {
    override def $(input: Dataset[_]) = {
      require(input.schema(inputCol).dataType == StringType)
      val out = getOutCol(inputCol, as)
      val replaceValue = udf{s: String =>
        Try { s.toDouble } match {
          case Success(_) => value
          case Failure(_) => s
        }
      }
      input.withColumn(out, replaceValue(col(inputCol)))
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

  // Add collector: exploring the possible values in each column
  case class ExploreValues(inputCols: Seq[String]) extends DataSiloT {
    override def $(input: Dataset[_]) = {

      import input.sqlContext.implicits._

      inputCols.foreach((c) => {
        println
        println("•" * 20)
        println(s" ${c}")
        println("•" * 20)

        input
          .select(col(c).cast(StringType).as("k"), lit(1D).as("v"))
          .as[KV]
          .rdd
          .keyBy(_.k)
          .reduceByKey(_ + _)
          .map{ case(k,v) => v }
          .sortBy(_.v, ascending=false)
          .toDS
          .show(25, false)
      })


      input
    }
  }

  /**
   * Output type is an array of double.
   * Supported input types: Primitive numeric, or array of numeric
   */
  case class ComposeFeatureArray(inputCols: Seq[String], as: OutputCol.As) extends DataSiloT {
    override def $(input: Dataset[_]) = {
      val OutputCol.As(output) = as
      val emptyDoubleArray: Seq[Double] = Nil

      val intToArray = udf{(i: Int) => Seq(i.toDouble)}
      val longToArray = udf{(i: Long) => Seq(i.toDouble)}
      val doubleToArray = udf{(i: Double) => Seq(i)}
      val intArrayToDoubleArray = udf{(arr: Seq[Int]) => arr.map(_.toDouble)}
      val longArrayToDoubleArray = udf{(arr: Seq[Long]) => arr.map(_.toDouble)}

      val dfOut = input.schema(inputCols.head).dataType match {
        case IntegerType => input.withColumn(output, intToArray(col(inputCols.head)))
        case LongType => input.withColumn(output, longToArray(col(inputCols.head)))
        case DoubleType => input.withColumn(output, doubleToArray(col(inputCols.head)))
        case ArrayType(IntegerType,_) => input.withColumn(output, intArrayToDoubleArray(col(inputCols.head)))
        case ArrayType(LongType,_) => input.withColumn(output, longArrayToDoubleArray(col(inputCols.head)))
        case ArrayType(DoubleType,_) => input.withColumn(output, col(inputCols.head))
      }

      val pushIntToArray = udf{(i: Int, arr: Seq[Double]) => arr :+ i.toDouble}
      val pushLongToArray = udf{(i: Long, arr: Seq[Double]) => arr :+ i.toDouble}
      val pushDoubleToArray = udf{(i: Double, arr: Seq[Double]) => arr :+ i}

      val concatIntArray = udf{(ns: Seq[Int], arr: Seq[Double]) => arr ++ ns.map(_.toDouble)}
      val concatLongArray = udf{(ns: Seq[Long], arr: Seq[Double]) => arr ++ ns.map(_.toDouble)}
      val concatDoubleArray = udf{(ns: Seq[Double], arr: Seq[Double]) => arr ++ ns}

      val toMLVector = udf{(ns: Seq[Double]) => Vectors.dense(ns.toArray)}

      inputCols.tail.foldLeft(dfOut){ case(d,c) =>
        input.schema(c).dataType match {
          case IntegerType => d.withColumn(output, pushIntToArray(col(c), col(output)))
          case LongType => d.withColumn(output, pushLongToArray(col(c), col(output)))
          case DoubleType => d.withColumn(output, pushDoubleToArray(col(c), col(output)))
          case ArrayType(IntegerType,_) => d.withColumn(output, concatIntArray(col(c), col(output)))
          case ArrayType(LongType,_) => d.withColumn(output, concatLongArray(col(c), col(output)))
          case ArrayType(DoubleType,_) => d.withColumn(output, concatDoubleArray(col(c), col(output)))
        }
      }.withColumn(output, toMLVector(col(output)))
    }
  }

}

