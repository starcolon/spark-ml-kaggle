package com.starcolon.ml.process

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types._

trait Location
case object NoWhere extends Location
case class PathLocation(path: String) extends Location

trait Qty
case class RowsQty(num: Integer) extends Qty
case object AnyQty extends Qty

object Implicits {
  implicit def stringAsLocation(str: String): Location = PathLocation(str)
  implicit def stringOptAsLocation(str: Option[String]): Location = str match {
    case None => NoWhere
    case Some(s) => PathLocation(s)
  }
  implicit def locationAsString(where: Location) = where match {
    case NoWhere => ""
    case PathLocation(p) => p
  }
}

trait DataProvider { def <~(from: Location = NoWhere)(implicit spark: SparkSession): Dataset[_] }
trait DataOutput { def <~(data: Dataset[_])(implicit spark: SparkSession): Unit }

trait Step 

trait BindableStep extends Step {
  val steps: Seq[Step] = Nil
  def ~> (next: BindableStep): BindableStep
}
