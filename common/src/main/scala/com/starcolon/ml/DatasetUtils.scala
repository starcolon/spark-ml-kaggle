package com.starcolon.ml

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.reflect.runtime.universe._

object DatasetUtils {

  private[ml] val initialIntArray = udf{() => Seq.empty[Int]}
  private[ml] val initialLongArray = udf{() => Seq.empty[Long]}
  private[ml] val initialDoubleArray = udf{() => Seq.empty[Double]}
  private[ml] val initialStringArray = udf{() => Seq.empty[String]}

  private[ml] val appendIntArray = udf{(i: Int, ns: Seq[Int]) => ns :+ i}
  private[ml] val appendLongArray = udf{(i: Long, ns: Seq[Long]) => ns :+ i}
  private[ml] val appendDoubleArray = udf{(i: Double, ns: Seq[Double]) => ns :+ i}
  private[ml] val appendStringArray = udf{(i: String, ns: Seq[String]) => ns :+ i}

  private val toOption = udf((v: String, _null: String) => {
    if (v==_null) None 
    else Some(v)
  })

  def litArray(ns: Seq[_]) = lit(array(ns.map(lit):_*))
  
  implicit class DatasetOps(val df: Dataset[Row]) extends AnyVal {
    def lowercaseColumns: Dataset[Row] = 
      df.columns.foldLeft(df){ case(d,c) => d.withColumnRenamed(c, c.toLowerCase)}

    def castMany(cols: Seq[String], as: DataType): Dataset[Row] = 
      cols.foldLeft(df){ case(d,c) => d.withColumn(c, col(c).cast(as))}

    def convertToNone(valueAsNull: String, cols: Seq[String] = df.columns): Dataset[Row] = 
      cols.foldLeft(df){ case(d,c) => d.withColumn(c, toOption(col(c), lit(valueAsNull)))}

    def dropMultiple(cols: Seq[String]): Dataset[Row] = 
      cols.foldLeft(df){case(d,c) => d.drop(c)}

    def seqFromColumns(cols: Seq[String], target: String): Dataset[Row] = {
      val t = df.schema(cols.head).dataType
      require(cols.tail.map(c => df.schema(c).dataType == t).reduce(_ && _),
        s"All columns (${cols.mkString(", ")}) have to be of the same type.")

      val df_ = t match {
        case IntegerType => df.withColumn(target, initialIntArray())
        case LongType => df.withColumn(target, initialLongArray())
        case DoubleType => df.withColumn(target, initialDoubleArray())
        case StringType => df.withColumn(target, initialStringArray())
        case _ => throw new java.lang.IllegalArgumentException(s"Unsupported type : $t")
      }

      cols.foldLeft(df_){ case(d,c) => 
        t match {
          case IntegerType => d.withColumn(target, appendIntArray(col(c), col(target)))
          case LongType => d.withColumn(target, appendLongArray(col(c), col(target)))
          case DoubleType => d.withColumn(target, appendDoubleArray(col(c), col(target)))
          case StringType => d.withColumn(target, appendStringArray(col(c), col(target)))
        }
      }
    }

    def bucket(bins: Seq[Double]): Dataset[Row] = ???
  }

}