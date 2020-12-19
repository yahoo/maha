// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import com.yahoo.maha.core.BaseBigqueryExpressionTest._
import com.yahoo.maha.core.BigqueryExpression.{BigqueryExp, UDFBigqueryExpression}

object TestBigqueryUDFRegistrationFactory extends UDFRegistrationFactory {
  register(TestUDF)
  register(DECODE_REG)
  register(GET_A_BY_B_REG)
  register(GET_A_BY_B_PLUS_C_REG)
  register(GET_CONDITIONAL_A_BY_B_REG)
  register(GET_UTC_TIME_FROM_EPOCH_REG)
}

object BaseBigqueryExpressionTest {
  implicit val uDFRegistrationFactory = TestBigqueryUDFRegistrationFactory

  case object TestUDF extends UDF {
    val statement: String = "CREATE TEMPORARY FUNCTION doSomething as 'com.yahoo.maha.test.udf.TestUDF';"
  }

  case class DO_SOMETHING(args: BigqueryExp*) extends UDFBigqueryExpression(TestUDF)(uDFRegistrationFactory) {
    val hasRollupExpression = true
    val hasNumericOperation = true
    val argStrs = args.map {
      arg=>
        arg.render(false)
    }.mkString(", ")

    def asString: String = s"doSomething($argStrs)"
  }

  case object FACT_BIGQUERY_EXPRESSION_REG extends UDF {
    val statement: String = "CREATE TEMPORARY FUNCTION fact_bigquery_expression as 'com.yahoo.maha.test.udf.TestUDF';"
  }

  case class FACT_BIGQUERY_EXPRESSION(args: BigqueryExp*) extends UDFBigqueryExpression(FACT_BIGQUERY_EXPRESSION_REG)(uDFRegistrationFactory) {
    val hasRollupExpression = true
    val hasNumericOperation = true
    val argStrs = args.map {
      arg=>
        arg.render(false)
    }.mkString(", ")

    def asString: String = s"fact_bigquery_expression($argStrs)"
  }

  case object DIM_BIGQUERY_EXPRESSION_REG extends UDF {
    val statement: String = "CREATE TEMPORARY FUNCTION dim_bigquery_expression as 'com.yahoo.maha.test.udf.TestUDF';"
  }

  case class DIM_BIGQUERY_EXPRESSION(args: BigqueryExp*) extends UDFBigqueryExpression(DIM_BIGQUERY_EXPRESSION_REG)(uDFRegistrationFactory) {
    val hasRollupExpression = true
    val hasNumericOperation = false
    val argStrs = args.map {
      arg=>
        arg.render(false)
    }.mkString(", ")

    def asString: String = s"dim_bigquery_expression($argStrs)"
  }

  case object DECODE_REG extends UDF {
    val statement: String = "CREATE TEMPORARY FUNCTION decodeUDF as 'com.yahoo.maha.test.udf.TestUDF';"
  }

  case object GET_A_BY_B_REG extends UDF {
    val statement: String = "CREATE TEMPORARY FUNCTION getAbyB as 'com.yahoo.maha.test.udf.TestUDF';"
  }

  case class GET_A_BY_B(args: BigqueryExp*) extends UDFBigqueryExpression(GET_A_BY_B_REG)(uDFRegistrationFactory) {
    val hasRollupExpression = true
    val hasNumericOperation = true
    val argStrs = args.map {
      arg=>
        arg.render(false)
    }.mkString(", ")

    def asString: String = s"getAbyB($argStrs)"
  }

  case object GET_A_BY_B_PLUS_C_REG extends UDF {
    val statement: String = "CREATE TEMPORARY FUNCTION getAbyBplusC as 'com.yahoo.maha.test.udf.TestUDF';"
  }

  case class GET_A_BY_B_PLUS_C(args: BigqueryExp*) extends UDFBigqueryExpression(GET_A_BY_B_PLUS_C_REG)(uDFRegistrationFactory) {
    val hasRollupExpression = true
    val hasNumericOperation = true
    val argStrs = args.map {
      arg=>
        arg.render(false)
    }.mkString(", ")

    def asString: String = s"getAbyBplusC($argStrs)"
  }

  case object GET_CONDITIONAL_A_BY_B_REG extends UDF {
    val statement: String = "CREATE TEMPORARY FUNCTION getConditionalAbyB as 'com.yahoo.maha.test.udf.TestUDF';"
  }

  case class GET_CONDITIONAL_A_BY_B(args: BigqueryExp*) extends UDFBigqueryExpression(GET_CONDITIONAL_A_BY_B_REG)(uDFRegistrationFactory) {
    val hasRollupExpression = true
    val hasNumericOperation = true
    val argStrs = args.map {
      arg=>
        arg.render(false)
    }.mkString(", ")

    def asString: String = s"getConditionalAbyB($argStrs)"
  }

  case object GET_UTC_TIME_FROM_EPOCH_REG extends UDF {
    val statement: String = "CREATE TEMPORARY FUNCTION getDateTimeFromEpoch as 'com.yahoo.maha.test.udf.TestUDF';"
  }

  case class GET_UTC_TIME_FROM_EPOCH(args: BigqueryExp*) extends UDFBigqueryExpression(GET_UTC_TIME_FROM_EPOCH_REG)(uDFRegistrationFactory) {
    val hasRollupExpression = args.exists(_.hasRollupExpression)
    val hasNumericOperation = args.exists(_.hasNumericOperation)
    val argStrs = args.map {
      arg=>
        arg.render(false)
    }.mkString(", ")

    def asString: String = s"getDateTimeFromEpoch($argStrs)"
  }

}
