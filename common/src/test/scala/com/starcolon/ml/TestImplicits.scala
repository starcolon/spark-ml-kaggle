package com.starcolon.ml

object TestImplicits {

  case class NumDigit(digit: Int = 1)

  class DoubleOps(val d: Double) extends AnyVal {
    def ~= (v: Double)(implicit g: NumDigit): Boolean = {
      lazy val denom = math.pow(10, g.digit)
      def r(n: Double) = math.round(n * denom)/denom
      val d_ = r(d)
      val v_ = r(v)
      math.abs(d_ - v_) == 0D
    }
  }

}