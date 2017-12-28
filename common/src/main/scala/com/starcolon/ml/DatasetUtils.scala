package com.starcolon.ml

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object DatasetUtils {

  private val toOption = udf((v: String, _null: String) => {
    if (v==_null) None 
    else Some(v)
  })
  
  implicit class DatasetOps(val df: Dataset[Row]) extends AnyVal {
    def lowercaseColumns: Dataset[Row] = 
      df.columns.foldLeft(df){ case(d,c) => d.withColumnRenamed(c, c.toLowerCase)}

    def castMany(cols: Seq[String], as: DataType): Dataset[Row] = 
      cols.foldLeft(df){ case(d,c) => d.withColumn(c, col(c).cast(as))}

    def convertToNone(valueAsNull: String, cols: Seq[String] = df.columns): Dataset[Row] = 
      cols.foldLeft(df){ case(d,c) => d.withColumn(c, toOption(col(c), lit(valueAsNull)))}
  }

}