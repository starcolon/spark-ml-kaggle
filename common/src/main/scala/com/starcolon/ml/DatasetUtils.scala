package com.starcolon.ml

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.reflect.runtime.universe._

object DatasetUtils {

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

    def seqFromColumns(cols: Seq[String], target: String): RDD[Seq[T]] = {
      val t = df.schema(cols.head).dataType
      require(cols.tail.map(c => df.schema(c).dataType == t).reduce(_ && _),
        s"All columns (${cols.mkString(", ")}) have to be of the same type.")
      
      // TAOTODO:
      val initialIntArray = udf{() => Seq.empty[Int]}
      val initialLongArray = udf{() => Seq.empty[Long]}
      val initialDoubleArray = udf{() => Seq.empty[Double]}
      val initialStringArray = udf{() => Seq.empty[String]}

      val appendIntArray = udf{(i: Int, ns: Seq[Int]) => ns :+ i}
      val appendLongArray = udf{(i: Long, ns: Seq[Long]) => ns :+ i}
      val appendDoubleArray = udf{(i: Double, ns: Seq[Double]) => ns :+ i}
      val appendStringArray = udf{(i: String, ns: Seq[String]) => ns :+ i}

      val df_ = t match {
        case IntType => df.schema df.withColumn(target, initialIntArray)
        case LongType => df.schema df.withColumn(target, initialLongArray)
        case DoubleType => df.schema df.withColumn(target, initialDoubleArray)
        case StringType => df.schema df.withColumn(target, initialStringArray)
        case _ => throw IllegalArgumentException(s"Unsupported type : $t")
      }

      cols.foldLeft(df_){ case(c,d) => 
        

      }
    }

    def bucket(bins: Seq[Double]): Dataset[Row] = ???
  }

}