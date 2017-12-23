package com.starcolon.types

object TypeOps {

  implicit def IntAsOption(n: Int) = Some(n)

}