package com.yahoo.maha.core.query

import com.yahoo.maha.core.{Column, Grain}
import grizzled.slf4j.Logging

trait ResultSetTransformer extends Logging {

  def extractBigDecimal(field: Any) : BigDecimal = {
    field match  {
      case f : BigInt => BigDecimal(f.asInstanceOf[BigInt])
      case f : Double => BigDecimal(f.asInstanceOf[Double])
      case f : Long => BigDecimal(f.asInstanceOf[Long])
      case f : Float => BigDecimal(f.asInstanceOf[Float])
      case f : Int => BigDecimal(f.asInstanceOf[Int])
      case _ => BigDecimal(0.0)
    }
  }

  def transform(grain: Grain, resultAlias: String, column: Column, inputValue: Any) : Any
  def canTransform(resultAlias: String, column: Column): Boolean
}

object ResultSetTransformer {
  val DEFAULT_TRANSFORMS = List()
}
