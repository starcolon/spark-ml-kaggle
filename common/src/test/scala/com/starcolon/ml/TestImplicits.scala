package com.starcolon.ml

object TestImplicits {

  implicit class DoubleOps(val d: Double) extends AnyVal {
    def ~= (v: Double): Boolean = {
      val nDigit = v.toString.split('.').last.size
      lazy val denom = math.pow(10, nDigit)
      def r(n: Double) = math.round(n * denom)/denom
      r(d) == v
    }
  }

}