// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import com.yahoo.maha.core.BaseExpressionTest._
import com.yahoo.maha.core.HiveExpression._

/**
 * Created by pranavbhole on 07/07/17.
 */
object TestUDFRegistrationFactory extends UDFRegistrationFactory {
  register(TestUDF)
  register(TIMESTAMP_TO_FORMATTED_DATE_REG)
  register(FACT_HIVE_EXPRESSION_REG)
  register(DIM_HIVE_EXPRESSION_REG)
  register(GET_INTERVAL_DATE_REG)
  register(DECODE_REG)
  register(GET_A_BY_B_REG)
  register(GET_A_BY_B_PLUS_C_REG)
  register(GET_CONDITIONAL_A_BY_B_REG)
  register(GET_UTC_TIME_FROM_EPOCH_REG)
}

object BaseExpressionTest {
  implicit val uDFRegistrationFactory = TestUDFRegistrationFactory

  case object TestUDF extends UDF {
    val statement: String = "CREATE TEMPORARY FUNCTION doSomething as 'com.yahoo.maha.test.udf.TestUDF';"
  }

  case class DO_SOMETHING(args: HiveExp*) extends UDFHiveExpression(TestUDF)(uDFRegistrationFactory) {
    val hasRollupExpression = true
    val hasNumericOperation = true
    val argStrs = args.map {
      arg=>
        arg.render(false)
    }.mkString(", ")

    def asString: String = s"doSomething($argStrs)"
  }

  case object FACT_HIVE_EXPRESSION_REG extends UDF {
    val statement: String = "CREATE TEMPORARY FUNCTION fact_hive_expression as 'com.yahoo.maha.test.udf.TestUDF';"
  }

  case class FACT_HIVE_EXPRESSION(args: HiveExp*) extends UDFHiveExpression(FACT_HIVE_EXPRESSION_REG)(uDFRegistrationFactory) {
    val hasRollupExpression = true
    val hasNumericOperation = true
    val argStrs = args.map {
      arg=>
        arg.render(false)
    }.mkString(", ")

    def asString: String = s"fact_hive_expression($argStrs)"
  }

  case object DIM_HIVE_EXPRESSION_REG extends UDF {
    val statement: String = "CREATE TEMPORARY FUNCTION dim_hive_expression as 'com.yahoo.maha.test.udf.TestUDF';"
  }

  case class DIM_HIVE_EXPRESSION(args: HiveExp*) extends UDFHiveExpression(DIM_HIVE_EXPRESSION_REG)(uDFRegistrationFactory) {
    val hasRollupExpression = true
    val hasNumericOperation = false
    val argStrs = args.map {
      arg=>
        arg.render(false)
    }.mkString(", ")

    def asString: String = s"dim_hive_expression($argStrs)"
  }


  case object GET_INTERVAL_DATE_REG extends UDF {
    val statement: String = "CREATE TEMPORARY FUNCTION fact_hive_expression as 'com.yahoo.maha.test.udf.TestUDF';"
  }

  case class GET_INTERVAL_DATE(args: HiveExp, fmt:String) extends UDFHiveExpression(GET_INTERVAL_DATE_REG)(uDFRegistrationFactory) {
    def hasRollupExpression = args.hasRollupExpression
    def hasNumericOperation = args.hasNumericOperation
    val argStrs = args.render(false)

    def asString: String = s"getIntervalDate($argStrs, '$fmt')"
  }

  case object DECODE_REG extends UDF {
    val statement: String = "CREATE TEMPORARY FUNCTION decodeUDF as 'com.yahoo.maha.test.udf.TestUDF';"
  }

  case class DECODE_DIM(args: HiveExp*) extends UDFHiveExpression(DECODE_REG)(uDFRegistrationFactory) {
    require(!args.exists(_.hasRollupExpression), s"DECODE_DIM cannot rely on expression with rollup ${args.mkString(", ")}")
    val hasRollupExpression = false
    val hasNumericOperation = false
    val argStrs = args.map {
      arg=>
        arg.render(false)
    }.mkString(", ")

    def asString: String = s"decodeUDF($argStrs)"
  }

  case class DECODE(args: HiveExp*) extends UDFHiveExpression(DECODE_REG)(uDFRegistrationFactory) {
    val hasRollupExpression = true
    val hasNumericOperation = false
    val argStrs = args.map {
      arg=>
        arg.render(false)
    }.mkString(", ")

    def asString: String = s"decodeUDF($argStrs)"
  }

  case object GET_A_BY_B_REG extends UDF {
    val statement: String = "CREATE TEMPORARY FUNCTION getAbyB as 'com.yahoo.maha.test.udf.TestUDF';"
  }

  case class GET_A_BY_B(args: HiveExp*) extends UDFHiveExpression(GET_A_BY_B_REG)(uDFRegistrationFactory) {
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

  case class GET_A_BY_B_PLUS_C(args: HiveExp*) extends UDFHiveExpression(GET_A_BY_B_PLUS_C_REG)(uDFRegistrationFactory) {
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

  case class GET_CONDITIONAL_A_BY_B(args: HiveExp*) extends UDFHiveExpression(GET_CONDITIONAL_A_BY_B_REG)(uDFRegistrationFactory) {
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

  case class GET_UTC_TIME_FROM_EPOCH(args: HiveExp*) extends UDFHiveExpression(GET_UTC_TIME_FROM_EPOCH_REG)(uDFRegistrationFactory) {
    val hasRollupExpression = args.exists(_.hasRollupExpression)
    val hasNumericOperation = args.exists(_.hasNumericOperation)
    val argStrs = args.map {
      arg=>
        arg.render(false)
    }.mkString(", ")

    def asString: String = s"getDateTimeFromEpoch($argStrs)"
  }

  case object TIMESTAMP_TO_FORMATTED_DATE_REG extends UDF {
    val statement: String = "CREATE TEMPORARY FUNCTION getDateFromEpoch as 'com.yahoo.maha.test.udf.TestUDF';"
  }

  case class TIMESTAMP_TO_FORMATTED_DATE(args: HiveExp , fmt: String) extends UDFHiveExpression(TIMESTAMP_TO_FORMATTED_DATE_REG)(uDFRegistrationFactory) {
    def hasRollupExpression = args.hasRollupExpression
    def hasNumericOperation = args.hasNumericOperation
    val argStrs = args.asString

    def asString: String = s"getDateFromEpoch(${argStrs}, '$fmt')"
  }
}
