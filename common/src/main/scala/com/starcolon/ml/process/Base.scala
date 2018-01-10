package com.starcolon.ml.process

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

trait Location
case object NoWhere extends Location
case class PathLocation(path: String) extends Location
case class DatabaseTable(database: String, table: String) extends Location

trait Qty
case class RowsQty(num: Integer) extends Qty
case object AnyQty extends Qty

object Implicits {
  implicit def stringAsLocation(str: String): Location = PathLocation(str)
  implicit def stringOptAsLocation(str: Option[String]): Location = str match {
    case None => NoWhere
    case Some(s) => PathLocation(s)
  }
  implicit def stringPairAsLocation(pair: (String, String)): Location = DatabaseTable(pair._1, pair._2)
  implicit def locationAsString(where: Location) = where match {
    case NoWhere => ""
    case PathLocation(p) => p
    case DatabaseTable(a,b) => s"$a.$b"
  }
  implicit def locationAsStringPair(where: Location) = where match {
    case NoWhere => ("","")
    case PathLocation(p) => (p,"")
    case DatabaseTable(a,b) => (a,b)
  }
}

trait DataProvider { def <~(from: Location = NoWhere): Dataset[_] }
trait DataOutput { def <~(data: Dataset[_]): Unit }

trait Step 

trait BindableStep extends Step {
  val steps: Seq[Step] = Nil
  def ~> (next: BindableStep): BindableStep
}
