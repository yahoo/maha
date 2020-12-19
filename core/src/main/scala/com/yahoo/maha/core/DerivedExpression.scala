// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

/**
 * Created by hiral on 10/2/15.
 */

import java.util
import java.util.concurrent.atomic.AtomicLong

import com.google.common.collect.Lists
import com.yahoo.maha.core.ThetaSketchSetOp.ThetaSketchSetOp
import com.yahoo.maha.core.fact.FactCol
import org.apache.druid.js.JavaScriptConfig
import org.apache.druid.query.aggregation.PostAggregator
import org.apache.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator
import org.apache.druid.query.aggregation.datasketches.theta.{SketchEstimatePostAggregator, SketchSetPostAggregator}
import org.apache.druid.query.aggregation.post.{ArithmeticPostAggregator, ConstantPostAggregator, FieldAccessPostAggregator, JavaScriptPostAggregator}

import scala.collection.mutable.ListBuffer
import org.json4s.JsonAST.{JArray, JNull, JObject, JValue}
import org.json4s.scalaz.JsonScalaz._

trait Expression[T] {
  def hasNumericOperation: Boolean
  def hasRollupExpression: Boolean
  def asString: String
  def render(insideDerived: Boolean) : T
  def *(that: Expression[T]) : Expression[T]
  def /(that: Expression[T]) : Expression[T]
  def ++(that: Expression[T]) : Expression[T] //++ instead of + due to conflict with standard + operator on strings
  def -(that: Expression[T]) : Expression[T]
  def /-(that: Expression[T]) : Expression[T]

  def asJSON: JObject =
    makeObj(
      List(
        ("expression" -> toJSON(this.getClass.getSimpleName))
        ,("hasNumericOperation" -> toJSON(hasNumericOperation))
        ,("hasRollupExpression" -> toJSON(hasRollupExpression))
      )
    )
}

sealed trait PostgresExpression extends Expression[String] {
  def render(insideDerived: Boolean) = insideDerived match {
    case true if hasNumericOperation =>
      s"($asString)"
    case _ =>
      asString
  }
}

object PostgresExpression {
  type PostgresExp = Expression[String]

  trait BasePostgresExpression extends PostgresExpression {
    def *(that: PostgresExp) : PostgresExp = {
      val expression = if(that.hasNumericOperation) {
        s"$asString * (${that.asString})"
      } else {
        s"$asString * ${that.asString}"
      }
      COL(expression, hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }

    def /(that: PostgresExp) : PostgresExp = {
      val expression = if(that.hasNumericOperation) {
        s"$asString / (${that.asString})"
      } else {
        s"$asString / ${that.asString}"
      }
      COL(expression, hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }

    def ++(that: PostgresExp) : PostgresExp = {
      COL(s"$asString + ${that.asString}", hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }

    def -(that: PostgresExp) : PostgresExp = {
      COL(s"$asString - ${that.asString}", hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }

    def /-(that: PostgresExp) : PostgresExp = {
      val safeExpression = if(that.hasNumericOperation) {
        s"CASE WHEN ${that.asString} = 0 THEN 0.0 ELSE $asString / (${that.asString}) END"
      } else {
        s"CASE WHEN ${that.asString} = 0 THEN 0.0 ELSE $asString / ${that.asString} END"
      }
      COL(safeExpression, hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }
  }

  case class COL(s: String, hasRollupExpression: Boolean = false, hasNumericOperation: Boolean = false) extends BasePostgresExpression {
    def asString : String = s
  }

  case class SUM(s: PostgresExp) extends BasePostgresExpression {
    val hasRollupExpression = true
    val hasNumericOperation = true
    def asString : String = s"SUM(${s.asString})"
  }

  case class ROUND(s: PostgresExp, format: Int) extends BasePostgresExpression {
    def hasRollupExpression = s.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation
    def asString : String = s"ROUND(${s.asString}, $format)"
  }

  case class MAX(s: PostgresExp) extends BasePostgresExpression {
    val hasRollupExpression = true
    val hasNumericOperation = true
    def asString : String = s"MAX(${s.asString})"
  }

  case class MIN(s: PostgresExp) extends BasePostgresExpression {
    val hasRollupExpression = true
    val hasNumericOperation = true
    def asString : String = s"MIN(${s.asString})"
  }

  case class TIMESTAMP_TO_FORMATTED_DATE(s: PostgresExp, fmt: String) extends BasePostgresExpression {
    def hasRollupExpression = s.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation
    // NVL(TO_CHAR(DATE '1970-01-01' + ( 1 / 24 / 60 / 60 / 1000)*CAST(MOD(start_time, 32503680000000) AS NUMBER) , 'YYYY-MM-DD'), 'NULL')
    def asString : String = s"COALESCE(TO_CHAR(TIMESTAMP '1970-01-01' + ( 1 / 24 / 60 / 60 / 1000)*CAST(MOD(${s.asString}, 32503680000000) AS NUMERIC) , '$fmt'), 'NULL')"
  }

  case class FORMAT_DATE(s: PostgresExp, fmt: String) extends BasePostgresExpression {
    def hasRollupExpression = s.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation
    def asString : String = s"COALESCE(TO_CHAR(${s.asString}, '$fmt'), 'NULL')"
  }

  case class FORMAT_DATE_WITH_LEAST(s: PostgresExp, fmt: String) extends BasePostgresExpression {
    def hasRollupExpression = s.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation
    def asString : String = s"COALESCE(TO_CHAR(LEAST(${s.asString},TO_DATE('01-Jan-3000','dd-Mon-YYYY')), '$fmt'), 'NULL')"
  }

  case class NVL(s: PostgresExp, default: String) extends BasePostgresExpression {
    def hasRollupExpression = s.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation
    def asString : String = s"COALESCE(${s.asString}, ${default.asString})"
  }

  case class TRUNC(s: PostgresExp) extends BasePostgresExpression {
    def hasRollupExpression = s.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation
    def asString : String = s"TRUNC(${s.asString})"
  }

  case class GET_INTERVAL_DATE(s: PostgresExp, fmt: String) extends BasePostgresExpression {

    // args should be stats_date and d|w|m
    GET_INTERVAL_DATE.checkFormat(fmt)

    def hasRollupExpression = s.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation
    def asString : String = GET_INTERVAL_DATE.getIntervalDate(s.asString, fmt)
  }

  object GET_INTERVAL_DATE {
    val regex = """[dwmDWM]|[dD][aA][yY]|[yY][rR]""".r

    def checkFormat(fmt: String):  Unit = {
      regex.findFirstIn(fmt) match {
        case Some(f) =>
        case _ => throw new IllegalArgumentException(s"Format for get_interval_date must be d|w|m|day|yr not $fmt")
      }
    }

    def getIntervalDate(exp: String, fmt: String):  String = {
      fmt.toLowerCase match {
        case "d" =>
          // TRUNC(DateString,"DD") does not work for Hive
          s"DATE_TRUNC('day', $exp)::DATE"
        case "w" =>
          //TRUNC(stats_date, 'IW') "Week"
          s"DATE_TRUNC('week', $exp)::DATE"
        case "m" =>
          //TRUNC(stats_date, 'MM') "Month"
          s"DATE_TRUNC('month', $exp)::DATE"
        case "day" =>
          s"TO_CHAR($exp, 'DAY')"
        case "yr" =>
          s"TO_CHAR($exp, 'YYYY')"
        case s => throw new IllegalArgumentException(s"Format for get_interval_date must be d|w|m|day|yr not $fmt")
      }
    }
  }

  case class DECODE_DIM(args: PostgresExp*) extends BasePostgresExpression {
    if (args.length < 3) throw new IllegalArgumentException("Usage: DECODE( expression , search , result [, search , result]... [, default] )")
    require(!args.exists(_.hasRollupExpression), s"DECODE_DIM cannot rely on expression with rollup ${args.mkString(", ")}")

    def buildExp(): String = {
      val exp = args(0).asString
      val default = if (args.length % 2 == 0) {
        s"ELSE ${args.last.asString} "
      } else {
        ""
      }
      var i = 1
      val end = if (args.length % 2 == 0) {
        args.length - 1
      } else {
        args.length
      }

      val builder = new StringBuilder
      builder.append("CASE ")
      while (i < end) {
        val search = args(i).asString
        val result = args(i+1).asString
        builder.append(s"WHEN $exp = $search THEN $result ")
        i += 2
      }
      builder.append(default)
      builder.append("END")
      builder.mkString
    }
    val caseExpression = buildExp()
    val hasRollupExpression = false
    val hasNumericOperation = args.exists(_.hasNumericOperation)
    def asString: String = caseExpression
  }

  case class DECODE(args: PostgresExp*) extends BasePostgresExpression {
    if (args.length < 3) throw new IllegalArgumentException("Usage: DECODE( expression , search , result [, search , result]... [, default] )")

    def buildExp(): String = {
      val exp = args(0).asString
      val default = if (args.length % 2 == 0) {
        s"ELSE ${args.last.asString} "
      } else {
        ""
      }
      var i = 1
      val end = if (args.length % 2 == 0) {
        args.length - 1
      } else {
        args.length
      }

      val builder = new StringBuilder
      builder.append("CASE ")
      while (i < end) {
        val search = args(i).asString
        val result = args(i+1).asString
        builder.append(s"WHEN $exp = $search THEN $result ")
        i += 2
      }
      builder.append(default)
      builder.append("END")
      builder.mkString
    }
    val caseExpression = buildExp()

    val hasRollupExpression = true
    val hasNumericOperation = args.exists(_.hasNumericOperation)
    //def asString: String = s"DECODE($strArgs)"
    def asString: String = caseExpression
  }

  case class COMPARE_PERCENTAGE(arg1: PostgresExp, arg2: PostgresExp, percentage: Int, value: String, nextExp: PostgresExpression) extends BasePostgresExpression {
    val hasRollupExpression = false
    val hasNumericOperation = false
    def asString: String = {
      val ELSE_END = if (nextExp.isInstanceOf[PostgresExpression.COL]) ("ELSE", "END") else ("", "") // Add ELSE if nextExp is last

      s"CASE WHEN ${arg1.asString} < ${percentage/100.0} * ${arg2.asString} THEN '${value}' ${ELSE_END._1} ${nextExp.render(false)} ${ELSE_END._2}"
        .replaceAll(" CASE", "") // replace all CASE other than the first
    }
  }

  case class COALESCE(s: PostgresExp, default: PostgresExp) extends BasePostgresExpression {
    def hasRollupExpression = s.hasRollupExpression || default.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation || default.hasNumericOperation
    def asString : String = s"COALESCE(${s.asString}, ${default.asString})"
  }

  case class TO_CHAR(s: PostgresExp, fmt: String) extends BasePostgresExpression {
    def hasRollupExpression = s.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation
    def asString : String = s"TO_CHAR(${s.asString}, '$fmt')"
  }

  implicit def from(s: String): PostgresExpression = {
    COL(s, false)
  }

  implicit def fromExpression(e: PostgresExp) : PostgresExpression = {
    e.asInstanceOf[PostgresExpression]
  }

  implicit class StringHelper(s: String) {
    def *(s2: String) : PostgresExpression = {
      COL(s, false) * COL(s2, false)
    }
    def ++(s2: String) : PostgresExpression = {
      COL(s, false) ++ COL(s2, false)
    }
    def -(s2: String) : PostgresExpression = {
      COL(s, false) - COL(s2, false)
    }
    def /(s2: String) : PostgresExpression = {
      COL(s, false) / COL(s2, false)
    }
    def /-(s2: String) : PostgresExpression = {
      COL(s, false) /- COL(s2, false)
    }
  }
}

trait PrestoExpression extends Expression[String] {
  def render(insideDerived: Boolean) = insideDerived match {
    case true if hasNumericOperation =>
      s"($asString)"
    case _ =>
      asString
  }
}

object PrestoExpression {
  type PrestoExp = Expression[String]

  trait BasePrestoExpression extends PrestoExpression {
    def *(that: PrestoExp) : PrestoExp = {
      val expression = if(that.hasNumericOperation) {
        s"$asString * (${that.asString})"
      } else {
        s"$asString * ${that.asString}"
      }
      COL(expression, hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }

    def /(that: PrestoExp) : PrestoExp = {
      val expression = if(that.hasNumericOperation) {
        s"$asString / (${that.asString})"
      } else {
        s"$asString / ${that.asString}"
      }
      COL(expression, hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }

    def ++(that: PrestoExp) : PrestoExp = {
      COL(s"$asString + ${that.asString}", hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }

    def -(that: PrestoExp) : PrestoExp = {
      COL(s"$asString - ${that.asString}", hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }

    def /-(that: PrestoExp) : PrestoExp = {
      val safeExpression = if(that.hasNumericOperation) {
        s"CASE WHEN ${that.asString} = 0 THEN 0.0 ELSE CAST($asString AS DOUBLE) / (${that.asString}) END"
      } else {
        s"CASE WHEN ${that.asString} = 0 THEN 0.0 ELSE CAST($asString AS DOUBLE) / ${that.asString} END"
      }
      COL(safeExpression, hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }

    def isUDF: Boolean = false
  }

  abstract class UDFPrestoExpression(val udfRegistration: UDFRegistration)(implicit uDFRegistrationFactory: UDFRegistrationFactory) extends BasePrestoExpression {
    override def isUDF: Boolean = true
    require(uDFRegistrationFactory.defaultUDFStatements.contains(udfRegistration), s"UDFPrestoExpression ${getClass} is not registered in the UDFRegistrationFactory")
  }

  case class COL(s: String, hasRollupExpression: Boolean = false, hasNumericOperation: Boolean = false) extends BasePrestoExpression {
    def asString : String = s
  }

  case class SUM(s: PrestoExp) extends BasePrestoExpression {
    val hasRollupExpression = true
    val hasNumericOperation = true
    def asString : String = s"SUM(${s.asString})"
  }

  case class ROUND(s: PrestoExp, format: Int) extends BasePrestoExpression {
    def hasRollupExpression = s.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation
    def asString : String = s"ROUND(${s.asString}, $format)"
  }

  case class MAX(s: PrestoExp) extends BasePrestoExpression {
    val hasRollupExpression = true
    val hasNumericOperation = true
    def asString : String = s"MAX(${s.asString})"
  }

  case class DAY_OF_WEEK(s: PrestoExp, fmt: String) extends BasePrestoExpression {
    val hasRollupExpression = s.hasRollupExpression
    val hasNumericOperation = s.hasNumericOperation
    def asString : String = s"date_format(date_parse(${s.asString}, '$fmt'), '%W')"
  }

  case class COALESCE(s: PrestoExp, default: PrestoExp) extends BasePrestoExpression {
    def hasRollupExpression = s.hasRollupExpression || default.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation || default.hasNumericOperation
    def asString : String = s"coalesce(${s.asString}, ${default.asString})"
  }

  case class NVL(s: PrestoExp, default: PrestoExp) extends BasePrestoExpression {
    def hasRollupExpression = s.hasRollupExpression || default.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation || default.hasNumericOperation
    def asString : String = s"nvl(${s.asString}, ${default.asString})"
  }

  case class TRIM(s: PrestoExp) extends BasePrestoExpression {
    val hasRollupExpression = false
    val hasNumericOperation = s.hasNumericOperation
    def asString: String = s"trim(${s.asString})"
  }

  case class REGEX_EXTRACT(s: PrestoExp, regex: String, index: Int, replaceMissingValue: Boolean, replaceMissingValueWith: String) extends BasePrestoExpression {
    val hasRollupExpression = s.hasRollupExpression
    val hasNumericOperation = s.hasNumericOperation
    def asString: String = if(replaceMissingValue) {
      s"CASE WHEN LENGTH(regexp_extract(${s.asString}, '$regex', $index)) > 0 THEN regexp_extract(${s.asString}, '$regex', $index) ELSE '$replaceMissingValueWith' END"
    } else {
      s"regexp_extract(${s.asString}, '$regex', $index)"
    }
  }

  implicit def from(s: String): PrestoExpression = {
    COL(s, false)
  }

  implicit def fromExpression(e: PrestoExp) : PrestoExpression = {
    e.asInstanceOf[PrestoExpression]
  }

  implicit class StringHelper(s: String) {
    def *(s2: String) : PrestoExpression = {
      COL(s, false) * COL(s2, false)
    }
    def ++(s2: String) : PrestoExpression = {
      COL(s, false) ++ COL(s2, false)
    }
    def -(s2: String) : PrestoExpression = {
      COL(s, false) - COL(s2, false)
    }
    def /(s2: String) : PrestoExpression = {
      COL(s, false) / COL(s2, false)
    }
    def /-(s2: String) : PrestoExpression = {
      COL(s, false) /- COL(s2, false)
    }
  }
}

trait HiveExpression extends Expression[String] {
  def render(insideDerived: Boolean) = insideDerived match {
    case true if hasNumericOperation =>
      s"($asString)"
    case _ =>
      asString
  }
}

object HiveExpression {
  type HiveExp = Expression[String]

  trait BaseHiveExpression extends HiveExpression {
    def *(that: HiveExp) : HiveExp = {
      val expression = if(that.hasNumericOperation) {
        s"$asString * (${that.asString})"
      } else {
        s"$asString * ${that.asString}"
      }
      COL(expression, hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }

    def /(that: HiveExp) : HiveExp = {
      val expression = if(that.hasNumericOperation) {
        s"$asString / (${that.asString})"
      } else {
        s"$asString / ${that.asString}"
      }
      COL(expression, hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }

    def ++(that: HiveExp) : HiveExp = {
      COL(s"$asString + ${that.asString}", hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }

    def -(that: HiveExp) : HiveExp = {
      COL(s"$asString - ${that.asString}", hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }

    def /-(that: HiveExp) : HiveExp = {
      val safeExpression = if(that.hasNumericOperation) {
        s"CASE WHEN ${that.asString} = 0 THEN 0.0 ELSE $asString / (${that.asString}) END"
      } else {
        s"CASE WHEN ${that.asString} = 0 THEN 0.0 ELSE $asString / ${that.asString} END"
      }
      COL(safeExpression, hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }

    def isUDF: Boolean = false
  }

  abstract class UDFHiveExpression(val udfRegistration: UDFRegistration)(implicit uDFRegistrationFactory: UDFRegistrationFactory) extends BaseHiveExpression {
    override def isUDF: Boolean = true
    require(uDFRegistrationFactory.defaultUDFStatements.contains(udfRegistration), s"UDFHiveExpression ${getClass} is not registered in the UDFRegistrationFactory")
  }

  case class COL(s: String, hasRollupExpression: Boolean = false, hasNumericOperation: Boolean = false) extends BaseHiveExpression {
    def asString : String = s
  }

  case class SUM(s: HiveExp) extends BaseHiveExpression {
    val hasRollupExpression = true
    val hasNumericOperation = true
    def asString : String = s"SUM(${s.asString})"
  }

  case class ROUND(s: HiveExp, format: Int) extends BaseHiveExpression {
    def hasRollupExpression = s.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation
    def asString : String = s"ROUND(${s.asString}, $format)"
  }

  case class MAX(s: HiveExp) extends BaseHiveExpression {
    val hasRollupExpression = true
    val hasNumericOperation = true
    def asString : String = s"MAX(${s.asString})"
  }

  case class MIN(s: HiveExp) extends BaseHiveExpression {
    val hasRollupExpression = true
    val hasNumericOperation = true
    def asString : String = s"MIN(${s.asString})"
  }

  case class COUNT(s: HiveExp) extends BaseHiveExpression {
    val hasRollupExpression = true
    val hasNumericOperation = true
    def asString : String = s"COUNT(${s.asString})"
  }

  case class COUNT_DISTINCT(s: HiveExp) extends BaseHiveExpression {
    val hasRollupExpression = true
    val hasNumericOperation = true
    def asString : String = s"COUNT(distinct ${s.asString})"
  }

  case class DAY_OF_WEEK(s: HiveExp, fmt: String) extends BaseHiveExpression {
    val hasRollupExpression = s.hasRollupExpression
    val hasNumericOperation = s.hasNumericOperation
    def asString : String = s"from_unixtime(unix_timestamp(${s.asString}, '$fmt'), 'EEEE')"
  }

  case class COALESCE(s: HiveExp, default: HiveExp) extends BaseHiveExpression {
    def hasRollupExpression = s.hasRollupExpression || default.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation || default.hasNumericOperation
    def asString : String = s"coalesce(${s.asString}, ${default.asString})"
  }

  case class NVL(s: HiveExp, default: HiveExp) extends BaseHiveExpression {
    def hasRollupExpression = s.hasRollupExpression || default.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation || default.hasNumericOperation
    def asString : String = s"nvl(${s.asString}, ${default.asString})"
  }

  case class TRIM(s: HiveExp) extends BaseHiveExpression {
    val hasRollupExpression = false
    val hasNumericOperation = s.hasNumericOperation
    def asString: String = s"trim(${s.asString})"
  }

  case class REGEX_EXTRACT(s: HiveExp, regex: String, index: Int, replaceMissingValue: Boolean, replaceMissingValueWith: String) extends BaseHiveExpression {
    val hasRollupExpression = s.hasRollupExpression
    val hasNumericOperation = s.hasNumericOperation
    def asString: String = if(replaceMissingValue) {
      s"CASE WHEN LENGTH(regexp_extract(${s.asString}, '$regex', $index)) > 0 THEN regexp_extract(${s.asString}, '$regex', $index) ELSE '$replaceMissingValueWith' END"
    } else {
      s"regexp_extract(${s.asString}, '$regex', $index)"
    }
  }

  implicit def from(s: String): HiveExpression = {
    COL(s, false)
  }

  implicit def fromExpression(e: HiveExp) : HiveExpression = {
    e.asInstanceOf[HiveExpression]
  }

  implicit class StringHelper(s: String) {
    def *(s2: String) : HiveExpression = {
      COL(s, false) * COL(s2, false)
    }
    def ++(s2: String) : HiveExpression = {
      COL(s, false) ++ COL(s2, false)
    }
    def -(s2: String) : HiveExpression = {
      COL(s, false) - COL(s2, false)
    }
    def /(s2: String) : HiveExpression = {
      COL(s, false) / COL(s2, false)
    }
    def /-(s2: String) : HiveExpression = {
      COL(s, false) /- COL(s2, false)
    }
  }
}

sealed trait OracleExpression extends Expression[String] {
  def render(insideDerived: Boolean) = insideDerived match {
    case true if hasNumericOperation =>
      s"($asString)"
    case _ =>
      asString
  }
}

object OracleExpression {
  type OracleExp = Expression[String]

  trait BaseOracleExpression extends OracleExpression {
    def *(that: OracleExp) : OracleExp = {
      val expression = if(that.hasNumericOperation) {
        s"$asString * (${that.asString})"
      } else {
        s"$asString * ${that.asString}"
      }
      COL(expression, hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }

    def /(that: OracleExp) : OracleExp = {
      val expression = if(that.hasNumericOperation) {
        s"$asString / (${that.asString})"
      } else {
        s"$asString / ${that.asString}"
      }
      COL(s"$asString / ${that.asString}", hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }

    def ++(that: OracleExp) : OracleExp = {
      COL(s"$asString + ${that.asString}", hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }

    def -(that: OracleExp) : OracleExp = {
      COL(s"$asString - ${that.asString}", hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }

    def /-(that: OracleExp) : OracleExp = {
      val safeExpression = if(that.hasNumericOperation) {
        s"CASE WHEN ${that.asString} = 0 THEN 0.0 ELSE $asString / (${that.asString}) END"
      } else {
        s"CASE WHEN ${that.asString} = 0 THEN 0.0 ELSE $asString / ${that.asString} END"
      }
      COL(safeExpression, hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }
  }

  case class COL(s: String, hasRollupExpression: Boolean = false, hasNumericOperation: Boolean = false) extends BaseOracleExpression {
    def asString : String = s
  }

  case class SUM(s: OracleExp) extends BaseOracleExpression {
    val hasRollupExpression = true
    val hasNumericOperation = true
    def asString : String = s"SUM(${s.asString})"
  }

  case class ROUND(s: OracleExp, format: Int) extends BaseOracleExpression {
    def hasRollupExpression = s.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation
    def asString : String = s"ROUND(${s.asString}, $format)"
  }

  case class MAX(s: OracleExp) extends BaseOracleExpression {
    val hasRollupExpression = true
    val hasNumericOperation = true
    def asString : String = s"MAX(${s.asString})"
  }

  case class MIN(s: OracleExp) extends BaseOracleExpression {
    val hasRollupExpression = true
    val hasNumericOperation = true
    def asString : String = s"MIN(${s.asString})"
  }

  case class TIMESTAMP_TO_FORMATTED_DATE(s: OracleExp, fmt: String) extends BaseOracleExpression {
    def hasRollupExpression = s.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation
    // NVL(TO_CHAR(DATE '1970-01-01' + ( 1 / 24 / 60 / 60 / 1000)*CAST(MOD(start_time, 32503680000000) AS NUMBER) , 'YYYY-MM-DD'), 'NULL')
    def asString : String = s"NVL(TO_CHAR(DATE '1970-01-01' + ( 1 / 24 / 60 / 60 / 1000)*CAST(MOD(${s.asString}}, 32503680000000) AS NUMBER) , '$fmt'), 'NULL')"
  }

  case class FORMAT_DATE(s: OracleExp, fmt: String) extends BaseOracleExpression {
    def hasRollupExpression = s.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation
    def asString : String = s"NVL(TO_CHAR(${s.asString}, '$fmt'), 'NULL')"
  }

  case class FORMAT_DATE_WITH_LEAST(s: OracleExp, fmt: String) extends BaseOracleExpression {
    def hasRollupExpression = s.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation
    def asString : String = s"NVL(TO_CHAR(LEAST(${s.asString},TO_DATE('01-Jan-3000','dd-Mon-YYYY')), '$fmt'), 'NULL')"
  }

  case class NVL(s: OracleExp, default: String) extends BaseOracleExpression {
    def hasRollupExpression = s.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation
    def asString : String = s"NVL(${s.asString}, ${default.asString})"
  }

  case class TRUNC(s: OracleExp) extends BaseOracleExpression {
    def hasRollupExpression = s.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation
    def asString : String = s"TRUNC(${s.asString})"
  }

  case class GET_INTERVAL_DATE(s: OracleExp, fmt: String) extends BaseOracleExpression {

    // args should be stats_date and d|w|m
    GET_INTERVAL_DATE.checkFormat(fmt)

    def hasRollupExpression = s.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation
    def asString : String = GET_INTERVAL_DATE.getIntervalDate(s.asString, fmt)
  }

  object GET_INTERVAL_DATE {
    val regex = """[dwmDWM]|[dD][aA][yY]|[yY][rR]""".r

    def checkFormat(fmt: String):  Unit = {
      regex.findFirstIn(fmt) match {
        case Some(f) =>
        case _ => throw new IllegalArgumentException(s"Format for get_interval_date must be d|w|m|day|yr not $fmt")
      }
    }

    def getIntervalDate(exp: String, fmt: String):  String = {
      fmt.toLowerCase match {
        case "d" =>
          // TRUNC(DateString,"DD") does not work for Hive
          s"TRUNC($exp)"
        case "w" =>
          //TRUNC(stats_date, 'IW') "Week"
          s"TRUNC($exp, 'IW')"
        case "m" =>
          //TRUNC(stats_date, 'MM') "Month"
          s"TRUNC($exp, 'MM')"
        case "day" =>
          s"TO_CHAR($exp, 'DAY')"
        case "yr" =>
          s"TO_CHAR($exp, 'yyyy')"
        case s => throw new IllegalArgumentException(s"Format for get_interval_date must be d|w|m|day|yr not $fmt")
      }
    }
  }

  case class DECODE_DIM(args: OracleExp*) extends BaseOracleExpression {
    if (args.length < 3) throw new IllegalArgumentException("Usage: DECODE( expression , search , result [, search , result]... [, default] )")
    require(!args.exists(_.hasRollupExpression), s"DECODE_DIM cannot rely on expression with rollup ${args.mkString(", ")}")

    val strArgs = args.map(_.asString).mkString(", ")
    val hasRollupExpression = false
    val hasNumericOperation = args.exists(_.hasNumericOperation)
    def asString: String = s"DECODE($strArgs)"
  }

  case class DECODE(args: OracleExp*) extends BaseOracleExpression {
    if (args.length < 3) throw new IllegalArgumentException("Usage: DECODE( expression , search , result [, search , result]... [, default] )")
    val strArgs = args.map(_.asString).mkString(", ")

    val hasRollupExpression = true
    val hasNumericOperation = args.exists(_.hasNumericOperation)
    def asString: String = s"DECODE($strArgs)"
  }

  case class COMPARE_PERCENTAGE(arg1: OracleExp, arg2: OracleExp, percentage: Int, value: String, nextExp: OracleExpression) extends BaseOracleExpression {
    val hasRollupExpression = false
    val hasNumericOperation = false
    def asString: String = {
      val ELSE_END = if (nextExp.isInstanceOf[OracleExpression.COL]) ("ELSE", "END") else ("", "") // Add ELSE if nextExp is last

      s"CASE WHEN ${arg1.asString} < ${percentage/100.0} * ${arg2.asString} THEN '${value}' ${ELSE_END._1} ${nextExp.render(false)} ${ELSE_END._2}"
        .replaceAll(" CASE", "") // replace all CASE other than the first
    }
  }

  case class COALESCE(s: OracleExp, default: OracleExp) extends BaseOracleExpression {
    def hasRollupExpression = s.hasRollupExpression || default.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation || default.hasNumericOperation
    def asString : String = s"COALESCE(${s.asString}, ${default.asString})"
  }

  case class TO_CHAR(s: OracleExp, fmt: String) extends BaseOracleExpression {
    def hasRollupExpression = s.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation
    def asString : String = s"TO_CHAR(${s.asString}, '$fmt')"
  }

  case class GET_WEIGHTED_VIDEO_SHOWN(video_25_complete: OracleExp, video_50_complete: OracleExp, video_75_complete: OracleExp, video_100_complete: OracleExp) extends BaseOracleExpression {
    val hasRollupExpression = false
    val hasNumericOperation = true
    def asString: String = s"(( 25 * ${video_25_complete.asString} ) + ( 50 * ${video_50_complete.asString} ) + ( 75 * ${video_75_complete.asString}) + (100 * ${video_100_complete.asString}))"
  }

  case class GET_VIDEO_SHOWN_SUM(video_25_complete: OracleExp, video_50_complete: OracleExp, video_75_complete: OracleExp, video_100_complete: OracleExp) extends BaseOracleExpression {
    val hasRollupExpression = false
    val hasNumericOperation = true
    def asString: String =  s"(${video_25_complete.asString} + ${video_50_complete.asString} + ${video_75_complete.asString} + ${video_100_complete.asString})"
  }

  implicit def from(s: String): OracleExpression = {
    COL(s, false)
  }

  implicit def fromExpression(e: OracleExp) : OracleExpression = {
    e.asInstanceOf[OracleExpression]
  }

  implicit class StringHelper(s: String) {
    def *(s2: String) : OracleExpression = {
      COL(s, false) * COL(s2, false)
    }
    def ++(s2: String) : OracleExpression = {
      COL(s, false) ++ COL(s2, false)
    }
    def -(s2: String) : OracleExpression = {
      COL(s, false) - COL(s2, false)
    }
    def /(s2: String) : OracleExpression = {
      COL(s, false) / COL(s2, false)
    }
    def /-(s2: String) : OracleExpression = {
      COL(s, false) /- COL(s2, false)
    }
  }
}

trait DerivedExpression[T] {

  type ConcreteType

  private[this] val columnRegex = """(\{[^}\\]+\})""".r

  /**
   * The expression with reference to source columns as {colA}
   * * e.g. "timestamp_to_formatted_date({colA}, 'YYYY-MM-DD')"
    *
    * @return
   */
  def expression: Expression[T]

  def columnContext: ColumnContext

  /**
   * Set of source columns if any, e.g. ("colA", "colB")
    *
    * @return
   */
  lazy val sourceColumns: Set[String] = {
    columnRegex.findAllIn(expression.asString).map(_.substring(1).replace("}","")).toSet
  }


  /**
   * A primitive column is defined as any column which exists in the underlying
   * table, not derived in any way.
   */
  lazy val sourcePrimitiveColumns: Set[String] = {
    getPrimitiveCols(List.empty[String].to[ListBuffer])
  }

  private def sourcePrimitivesWithInput(nameBuffer: ListBuffer[String]): Set[String] = {
    getPrimitiveCols(nameBuffer)
  }

  /**
   * Given a set of source columns for a DerivedExpression,
   * track its sources and the tree of all sources until
   * primitive columns are found, and return.
   * @return
   */
  private def getPrimitiveCols(nameBuffer: ListBuffer[String], passThroughSources: Set[String] = sourceColumns): Set[String] = {
    val cols: Set[Column] =
      (passThroughSources -- nameBuffer)
        .map(name => columnContext.getColumnByName(name))
        .filter(col => col.isDefined)
        .map(col => col.get)

    nameBuffer ++= cols.map(col => col.alias.getOrElse(col.name))
    cols.flatMap(col => {
      col match {
        case col1: DerivedColumn =>
          col1.derivedExpression.sourcePrimitivesWithInput(nameBuffer).to[ListBuffer]
        case col1: FactCol if col1.hasRollupWithEngineRequirement =>
          val (colIncludeSet, colExcludeSet) = col1.rollupExpression.sourceColumns.partition(_ != col1.alias.getOrElse(col1.name))
          getPrimitiveCols(colExcludeSet.to[ListBuffer] ++ nameBuffer, colIncludeSet)
        case _ =>
          Set(col.alias.getOrElse(col.name))
      }
    })
  }

  lazy val isDimensionDriven : Boolean = {
    sourceColumns.exists {
      sc =>
        columnContext.isDimensionDriven(sc)
    }
  }

  private[this] var rendered: Option[T] = None
  private[this] var renderedInsideDerived: Option[T] = None

  def render(columnName: String
             , renderedColumnAliasMap: scala.collection.Map[String, String] = Map.empty
             , renderedColExp: Option[String] = None
             , columnPrefix: Option[String] = None
             , expandDerivedExpression: Boolean = true
             , insideDerived: Boolean = false): T = {
    def renderNow(insideDerived: Boolean) : T = {
      var de: T = expression.render(insideDerived)
      for (c <- sourceColumns) {
        val renderC = {
          if (c == columnName && renderedColExp.isDefined) {
            renderedColExp.get
          } else {
            columnContext.render(c, renderedColumnAliasMap, columnPrefix = columnPrefix, parentColumn = Option(columnName), expandDerivedExpression)
          }
        }
        de = de match {
          case s if s.isInstanceOf[String] => s.asInstanceOf[String].replaceAllLiterally (s"{$c}", renderC).asInstanceOf[T]
          case _ => de
        }
      }
      postRenderHook(columnName, de)
    }

    if (columnPrefix.isDefined || !expandDerivedExpression) {
      renderNow(insideDerived)
    } else {
      if(insideDerived) {
        if (renderedInsideDerived.isEmpty) {
          renderedInsideDerived = Option(renderNow(insideDerived))
        }
        renderedInsideDerived.get
      } else {
        if (rendered.isEmpty) {
          rendered = Option(renderNow(insideDerived))
        }
        rendered.get
      }
    }
  }

  def copyWith(columnContext: ColumnContext) : ConcreteType

  def postRenderHook(columnName: String, rendered: T) : T = rendered

  private val jUtils = JsonUtils

  def asJSON: JObject =
    makeObj(
      List(
        ("expression" -> expression.asJSON)
        ,("sourcePrimitiveColumns" -> jUtils.asJSON(sourcePrimitiveColumns))
      )
    )
}

case class HiveDerivedExpression (columnContext: ColumnContext, expression: HiveExpression) extends DerivedExpression[String] with WithHiveEngine {
  type ConcreteType = HiveDerivedExpression
  def copyWith(columnContext: ColumnContext) : HiveDerivedExpression = {
    this.copy(columnContext = columnContext)
  }
}

object HiveDerivedExpression {
  import HiveExpression.HiveExp
  def apply(expression: HiveExpression)(implicit cc: ColumnContext) : HiveDerivedExpression = {
    HiveDerivedExpression(cc, expression)
  }

  implicit def fromHiveExpression(expression: HiveExpression)(implicit  cc: ColumnContext) : HiveDerivedExpression = {
    apply(expression)
  }

  implicit def fromExpression(expression: HiveExp)(implicit  cc: ColumnContext) : HiveDerivedExpression = {
    apply(expression)
  }

  implicit def fromString(s: String)(implicit  cc: ColumnContext) : HiveDerivedExpression = {
    apply(s)
  }
}

case class PrestoDerivedExpression (columnContext: ColumnContext, expression: PrestoExpression) extends DerivedExpression[String] with WithPrestoEngine {
  type ConcreteType = PrestoDerivedExpression
  def copyWith(columnContext: ColumnContext) : PrestoDerivedExpression = {
    this.copy(columnContext = columnContext)
  }
}

object PrestoDerivedExpression {
  import PrestoExpression.PrestoExp
  def apply(expression: PrestoExpression)(implicit cc: ColumnContext) : PrestoDerivedExpression = {
    PrestoDerivedExpression(cc, expression)
  }

  implicit def fromPrestoExpression(expression: PrestoExpression)(implicit  cc: ColumnContext) : PrestoDerivedExpression = {
    apply(expression)
  }

  implicit def fromExpression(expression: PrestoExp)(implicit  cc: ColumnContext) : PrestoDerivedExpression = {
    apply(expression)
  }

  implicit def fromString(s: String)(implicit  cc: ColumnContext) : PrestoDerivedExpression = {
    apply(s)
  }
}

case class OracleDerivedExpression (columnContext: ColumnContext, expression: OracleExpression) extends DerivedExpression[String] with WithOracleEngine {
  type ConcreteType = OracleDerivedExpression
  def copyWith(columnContext: ColumnContext) : OracleDerivedExpression = {
    this.copy(columnContext = columnContext)
  }
}

object OracleDerivedExpression {
  import OracleExpression.OracleExp

  def apply(expression: OracleExpression)(implicit cc: ColumnContext) : OracleDerivedExpression = {
    OracleDerivedExpression(cc, expression)
  }

  implicit def fromOracleExpression(expression: OracleExpression)(implicit  cc: ColumnContext) : OracleDerivedExpression = {
    apply(expression)
  }

  implicit def fromExpression(expression: OracleExp)(implicit  cc: ColumnContext) : OracleDerivedExpression = {
    apply(expression)
  }

  implicit def fromString(s: String)(implicit  cc: ColumnContext) : OracleDerivedExpression = {
    apply(s)
  }
}

case class PostgresDerivedExpression (columnContext: ColumnContext, expression: PostgresExpression) extends DerivedExpression[String] with WithPostgresEngine {
  type ConcreteType = PostgresDerivedExpression
  def copyWith(columnContext: ColumnContext) : PostgresDerivedExpression = {
    this.copy(columnContext = columnContext)
  }
}

object PostgresDerivedExpression {
  import PostgresExpression.PostgresExp

  def apply(expression: PostgresExpression)(implicit cc: ColumnContext) : PostgresDerivedExpression = {
    PostgresDerivedExpression(cc, expression)
  }

  implicit def fromPostgresExpression(expression: PostgresExpression)(implicit  cc: ColumnContext) : PostgresDerivedExpression = {
    apply(expression)
  }

  implicit def fromExpression(expression: PostgresExp)(implicit  cc: ColumnContext) : PostgresDerivedExpression = {
    apply(expression)
  }

  implicit def fromString(s: String)(implicit  cc: ColumnContext) : PostgresDerivedExpression = {
    apply(s)
  }
}

sealed trait DruidExpression extends Expression[(String, Map[String, String]) => PostAggregator] {
  protected def id: Long
  protected def fieldNamePlaceHolder: String

  def postRenderHook(columnName: String, aggregatorNameAliasMap: Map[String, String], rendered: (String, Map[String, String]) => PostAggregator) : PostAggregator = {
    rendered(columnName, aggregatorNameAliasMap)
  }
}

object DruidExpression {
  private[this] val CONSTANT_ID  = new AtomicLong(0)

  protected def getConstantId : Long = CONSTANT_ID.incrementAndGet()

  type DruidExp = Expression[(String, Map[String, String]) => PostAggregator]

  trait BaseDruidExpression extends DruidExpression {

    val id : Long = 1
    val fieldNamePlaceHolder = "_placeHolder"

    def *(that: DruidExp) : DruidExp = {
      Arithmetic("*", this, that)
    }

    def /(that: DruidExp) : DruidExp = {
      Arithmetic("/", this, that)
    }

    def ++(that: DruidExp) : DruidExp = {
      Arithmetic("+", this, that)
    }

    def -(that: DruidExp) : DruidExp = {
      Arithmetic("-", this, that)
    }

    def /-(that: DruidExp) : DruidExp = {
      Arithmetic("/", this, that)
    }

  }

  /* TODO: fix later
  case class JavaScriptArithmetic(fn: String, fields: Seq[String]) extends BaseDruidExpression {
    def asString = compact(render(makeObj(
      ("type" -> toJSON("javascript"))
      :: ("name" -> toJSON(fieldNamePlaceHolder))
      :: ("fieldNames" -> toJSON(fields.toList))
      :: ("function" -> toJSON(fn))
      :: Nil
    )))
  }*/

  case class JavaScript(fn: String, fields: List[String]) extends BaseDruidExpression {
    val hasNumericOperation: Boolean = false
    val hasRollupExpression: Boolean = false
    val aggregatorsDruidExpressions = fields.map(e => fromString(e))
    def asString = s"$aggregatorsDruidExpressions"

    override def render(insideDerived: Boolean) = {
      (s: String, aggregatorNameAliasMap: Map[String, String]) => {
        val listInJava = new util.ArrayList[String]()
        val filedNames = fields.map(_.replaceAll("[}{]",""))
        filedNames.foreach(f => listInJava.add(aggregatorNameAliasMap.getOrElse(f, f)))
        new JavaScriptPostAggregator(s, listInJava, fn, JavaScriptConfig.getEnabledInstance)
      }
    }
  }

  case class Arithmetic(fn: String, a: DruidExp, b: DruidExp) extends BaseDruidExpression {
    override val id : Long = Math.max(a.asInstanceOf[DruidExpression].id, b.asInstanceOf[DruidExpression].id) + 1
    override val fieldNamePlaceHolder = s"_placeHolder_$id"

    def render(insideDerived: Boolean) = {
      val _a = a.asInstanceOf[DruidExpression]
      val _b = b.asInstanceOf[DruidExpression]
      (s: String, aggregatorNameAliasMap: Map[String, String]) => new ArithmeticPostAggregator(
        s, fn, Lists.newArrayList(_a.render(insideDerived)(_a.fieldNamePlaceHolder,aggregatorNameAliasMap), _b.render(insideDerived)(_b.fieldNamePlaceHolder,aggregatorNameAliasMap)))
    }

    val hasRollupExpression = false
    val hasNumericOperation = true
    def asString = {
      s"${a.asString} $fn ${b.asString}"
    }

    /*
    def asString = compact(render(makeObj(
      ("type" -> toJSON("arithmetic"))
      :: ("name" -> toJSON(fieldNamePlaceHolder))
      :: ("fn" -> toJSON(fn))
      :: ("fields" -> toJSON(fields.toList))
      :: Nil
    )))*/
  }

  case class ThetaSketchEstimateWrapper(name: String) extends BaseDruidExpression {

    def render(insideDerived: Boolean) = {
      (s: String, aggregatorNameAliasMap: Map[String, String]) =>
        val fieldName = name.replaceAll("[}{]","")
        val e = fromString(name)
        new SketchEstimatePostAggregator(fieldName, e.render(insideDerived)(e.fieldNamePlaceHolder,aggregatorNameAliasMap), null)
    }

    val hasRollupExpression = false
    val hasNumericOperation = false

    def asString = s"${fromString(name)}"
  }

  case class HyperUniqueCardinalityWrapper(name: String) extends BaseDruidExpression {

    def render(insideDerived: Boolean) = {
      (s: String, aggregatorNameAliasMap: Map[String, String]) => {
        val fieldName = name.replaceAll("[}{]","")
        new HyperUniqueFinalizingPostAggregator(s, aggregatorNameAliasMap.getOrElse(fieldName, fieldName))
      }
    }

    val hasRollupExpression = false
    val hasNumericOperation = false

    def asString = s"${fromString(name)}"
  }

  case class ThetaSketchEstimator(fn: ThetaSketchSetOp, aggregators: List[String]) extends BaseDruidExpression {
    import scala.collection.JavaConverters._

    def render(insideDerived: Boolean) = {
      (s: String, aggregatorNameAliasMap: Map[String, String]) => {
        val postAggList = aggregatorsDruidExpressions.filter(de => aggregatorNameAliasMap.contains(de.fieldNamePlaceHolder)).map(e => e.render(insideDerived)(e.fieldNamePlaceHolder,aggregatorNameAliasMap))
        new SketchEstimatePostAggregator(s, new SketchSetPostAggregator(s, fn.toString, null, postAggList.asJava), null)
      }
    }

    val aggregatorsDruidExpressions = aggregators.map(e => fromString(e))
    val hasRollupExpression = false
    val hasNumericOperation = false
    def asString = {
      s"$aggregatorsDruidExpressions"
    }
  }

  case class Constant(value: BigDecimal) extends BaseDruidExpression {

    override val id : Long = getConstantId
    override val fieldNamePlaceHolder = s"_constant_$id"

    def render(insideDerived: Boolean) = {
      (s: String, aggregatorNameAliasMap: Map[String, String]) => new ConstantPostAggregator(s, value)
    }

    val hasRollupExpression = false
    val hasNumericOperation = false
    def asString: String = value.toString()
    /*
    def asString: String = compact(render(
      makeObj(
        ("type" -> toJSON("constant"))
        :: ("value" -> toJSON(s))
        :: Nil
      )
    ))*/
  }

  case class FieldAccess(name: String) extends BaseDruidExpression {

    override val fieldNamePlaceHolder = name.replaceAll("[}{]","")

    def render(insideDerived: Boolean) = {
      (s: String, aggregatorNameAliasMap: Map[String, String]) => {
        val fieldName = name.replaceAll("[}{]","")
        new FieldAccessPostAggregator(s, aggregatorNameAliasMap.getOrElse(fieldName, fieldName))
      }
    }

    val hasRollupExpression = false
    val hasNumericOperation = false
    def asString: String = name
    /*
    def asString : String = compact(render(
      makeObj(
        ("type" -> toJSON("fieldAccess"))
        :: ("name" -> toJSON(s))
        :: ("fieldName" -> toJSON(s))
        :: Nil
      )
    ))
    */
  }

  object FieldAccess {
    implicit def from(s: String) : FieldAccess = FieldAccess(s)
  }

  val regex = """(\{[^}\\]+\})""".r
  implicit def fromString(s: String) : DruidExpression = {
    regex.findFirstIn(s) match {
      case Some(p) => FieldAccess(s)
      case None => Constant(BigDecimal(s))
    }
  }

  implicit def fromExpression(e: DruidExp) : DruidExpression = {
    e.asInstanceOf[DruidExpression]
  }

  implicit class StringHelper(s: String) {
    def *(s2: String) : DruidExpression = {
      fromString(s) * fromString(s2)
    }
    def ++(s2: String) : DruidExpression = {
      fromString(s) ++ fromString(s2)
    }
    def -(s2: String) : DruidExpression = {
      fromString(s) - fromString(s2)
    }
    def /(s2: String) : DruidExpression = {
      fromString(s) / fromString(s2)
    }
    def /-(s2: String) : DruidExpression = {
      fromString(s) /- fromString(s2)
    }
  }

}

case class DruidDerivedExpression private(columnContext: ColumnContext, expression: DruidExpression) extends DerivedExpression[(String, Map[String,String]) => PostAggregator] with WithDruidEngine {
  type ConcreteType = DruidDerivedExpression

  def copyWith(columnContext: ColumnContext) : DruidDerivedExpression = {
    this.copy(columnContext = columnContext)
  }

  override def postRenderHook(columnName: String, rendered: (String, Map[String, String]) => PostAggregator) : (String, Map[String, String]) => PostAggregator = {
    (s: String, aggregatorNameAliasMap: Map[String, String]) => expression.postRenderHook(s, aggregatorNameAliasMap, rendered)
  }
}

object DruidDerivedExpression {
  import DruidExpression.DruidExp

  def apply(expression: DruidExpression)(implicit cc: ColumnContext) : DruidDerivedExpression = {
    DruidDerivedExpression(cc, expression)
  }

  implicit def fromDruidExpression(expression: DruidExpression)(implicit  cc: ColumnContext) : DruidDerivedExpression = {
    apply(expression)
  }

  implicit def fromExpression(expression: DruidExp)(implicit  cc: ColumnContext) : DruidDerivedExpression = {
    apply(expression)
  }

  implicit def fromString(s: String)(implicit  cc: ColumnContext) : DruidDerivedExpression = {
    apply(s)
  }
}

object ThetaSketchSetOp extends Enumeration {
  type ThetaSketchSetOp = Value
  val INTERSECT, UNION, NOT = Value
}

sealed trait BigqueryExpression extends Expression[String] {
  def render(insideDerived: Boolean) = insideDerived match {
    case true if hasNumericOperation =>
      s"($asString)"
    case _ =>
      asString
  }
}

object BigqueryExpression {
  type BigqueryExp = Expression[String]

  trait BaseBigqueryExpression extends BigqueryExpression {
    def *(that: BigqueryExp): BigqueryExp = {
      val expression = if (that.hasNumericOperation) {
        s"$asString * (${that.asString})"
      } else {
        s"$asString * ${that.asString}"
      }
      COL(expression, hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }

    def /(that: BigqueryExp): BigqueryExp = {
      val expression = if (that.hasNumericOperation) {
        s"$asString / (${that.asString})"
      } else {
        s"$asString / ${that.asString}"
      }
      COL(expression, hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }

    def ++(that: BigqueryExp): BigqueryExp = {
      COL(s"$asString + ${that.asString}", hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }

    def -(that: BigqueryExp): BigqueryExp = {
      COL(s"$asString - ${that.asString}", hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }

    def /-(that: BigqueryExp): BigqueryExp = {
      val safeExpression = if (that.hasNumericOperation) {
        s"CASE WHEN ${that.asString} = 0 THEN 0.0 ELSE $asString / (${that.asString}) END"
      } else {
        s"CASE WHEN ${that.asString} = 0 THEN 0.0 ELSE $asString / ${that.asString} END"
      }
      COL(safeExpression, hasRollupExpression || that.hasRollupExpression, hasNumericOperation = true)
    }

    def isUDF: Boolean = false
  }

  abstract class UDFBigqueryExpression(val udfRegistration: UDFRegistration)(implicit uDFRegistrationFactory: UDFRegistrationFactory) extends BaseBigqueryExpression {
    override def isUDF: Boolean = true
    require(uDFRegistrationFactory.defaultUDFStatements.contains(udfRegistration), s"UDFBigqueryExpression ${getClass} is not registered in the UDFRegistrationFactory")
  }

  case class COL(s: String, hasRollupExpression: Boolean = false, hasNumericOperation: Boolean = false) extends BaseBigqueryExpression {
    def asString: String = s
  }

  case class SUM(s: BigqueryExp) extends BaseBigqueryExpression {
    val hasRollupExpression = true
    val hasNumericOperation = true
    def asString: String = s"SUM(${s.asString})"
  }

  case class ROUND(s: BigqueryExp, format: Int) extends BaseBigqueryExpression {
    def hasRollupExpression = s.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation
    def asString: String = s"ROUND(${s.asString}, $format)"
  }

  case class MAX(s: BigqueryExp) extends BaseBigqueryExpression {
    val hasRollupExpression = true
    val hasNumericOperation = true
    def asString: String = s"MAX(${s.asString})"
  }

  case class MIN(s: BigqueryExp) extends BaseBigqueryExpression {
    val hasRollupExpression = true
    val hasNumericOperation = true
    def asString: String = s"MIN(${s.asString})"
  }

  case class TIMESTAMP_TO_FORMATTED_DATE(s: BigqueryExp, fmt: String) extends BaseBigqueryExpression {
    def hasRollupExpression = s.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation
    def asString: String = s"FORMAT_TIMESTAMP('$fmt', ${s.asString})"
  }

  case class FORMAT_DATE(s: BigqueryExp, fmt: String) extends BaseBigqueryExpression {
    def hasRollupExpression = s.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation
    def asString: String = s"FORMAT_DATE('$fmt', ${s.asString})"
  }

  case class NVL(s: BigqueryExp, default: String) extends BaseBigqueryExpression {
    def hasRollupExpression = s.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation
    def asString: String = s"COALESCE(${s.asString}, ${default.asString})"
  }

  case class TRUNC(s: BigqueryExp) extends BaseBigqueryExpression {
    def hasRollupExpression = s.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation
    def asString: String = s"TRUNC(${s.asString})"
  }

  object GET_INTERVAL_DATE {
    val regex = """[dwmDWM]|[dD][aA][yY]|[yY][rR]""".r

    def checkFormat(fmt: String): Unit = {
      regex.findFirstIn(fmt) match {
        case Some(f) =>
        case _ => throw new IllegalArgumentException(s"Format for get_interval_date must be d|w|m|day|yr not $fmt")
      }
    }

    def getIntervalDate(exp: String, fmt: String):  String = {
      fmt.toLowerCase match {
        case "d" =>
          s"DATE_TRUNC($exp, DAY)"
        case "w" =>
          s"DATE_TRUNC($exp, WEEK)"
        case "m" =>
          s"DATE_TRUNC($exp, MONTH)"
        case "day" =>
          s"EXTRACT(DAY FROM $exp)"
        case "yr" =>
          s"EXTRACT(YEAR FROM $exp)"
        case s => throw new IllegalArgumentException(s"Format for GET_INTERVAL_DATE must be d|w|m|day|yr not $fmt")
      }
    }
  }

  case class GET_INTERVAL_DATE(s: BigqueryExp, fmt: String) extends BaseBigqueryExpression {
    GET_INTERVAL_DATE.checkFormat(fmt)

    def hasRollupExpression = s.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation
    def asString: String = GET_INTERVAL_DATE.getIntervalDate(s.asString, fmt)
  }

  case class DECODE_DIM(args: BigqueryExp*) extends BaseBigqueryExpression {
    if (args.length < 3) throw new IllegalArgumentException("Usage: DECODE_DIM( expression , search , result [, search , result]... [, default] )")
    require(!args.exists(_.hasRollupExpression), s"DECODE_DIM cannot rely on expression with rollup ${args.mkString(", ")}")

    def buildExp(): String = {
      val exp = args(0).asString
      val default = if (args.length % 2 == 0) {
        s"ELSE ${args.last.asString} "
      } else {
        ""
      }
      var i = 1
      val end = if (args.length % 2 == 0) {
        args.length - 1
      } else {
        args.length
      }

      val builder = new StringBuilder
      builder.append("CASE ")
      while (i < end) {
        val search = args(i).asString
        val result = args(i+1).asString
        builder.append(s"WHEN $exp = $search THEN $result ")
        i += 2
      }
      builder.append(default)
      builder.append("END")
      builder.mkString
    }

    val caseExpression = buildExp()
    val hasRollupExpression = false
    val hasNumericOperation = args.exists(_.hasNumericOperation)
    def asString: String = caseExpression
  }

  case class DECODE(args: BigqueryExp*) extends BaseBigqueryExpression {
    if (args.length < 3) throw new IllegalArgumentException("Usage: DECODE( expression , search , result [, search , result]... [, default] )")

    def buildExp(): String = {
      val exp = args(0).asString
      val default = if (args.length % 2 == 0) {
        s"ELSE ${args.last.asString} "
      } else {
        ""
      }
      var i = 1
      val end = if (args.length % 2 == 0) {
        args.length - 1
      } else {
        args.length
      }

      val builder = new StringBuilder
      builder.append("CASE ")
      while (i < end) {
        val search = args(i).asString
        val result = args(i+1).asString
        builder.append(s"WHEN $exp = $search THEN $result ")
        i += 2
      }
      builder.append(default)
      builder.append("END")
      builder.mkString
    }

    val caseExpression = buildExp()
    val hasRollupExpression = true
    val hasNumericOperation = args.exists(_.hasNumericOperation)
    def asString: String = caseExpression
  }

  case class COMPARE_PERCENTAGE(arg1: BigqueryExp, arg2: BigqueryExp, percentage: Int, value: String, nextExp: BigqueryExpression) extends BaseBigqueryExpression {
    val hasRollupExpression = false
    val hasNumericOperation = false
    def asString: String = {
      val ELSE_END = if (nextExp.isInstanceOf[BigqueryExpression.COL]) ("ELSE", "END") else ("", "")

      s"CASE WHEN ${arg1.asString} < ${percentage/100.0} * ${arg2.asString} THEN '${value}' ${ELSE_END._1} ${nextExp.render(false)} ${ELSE_END._2}"
        .replaceAll(" CASE", "")
    }
  }

  case class COALESCE(s: BigqueryExp, default: BigqueryExp) extends BaseBigqueryExpression {
    def hasRollupExpression = s.hasRollupExpression || default.hasRollupExpression
    def hasNumericOperation = s.hasNumericOperation || default.hasNumericOperation
    def asString: String = s"COALESCE(${s.asString}, ${default.asString})"
  }

  implicit def from(s: String): BigqueryExpression = {
    COL(s, false)
  }

  implicit def fromExpression(e: BigqueryExp): BigqueryExpression = {
    e.asInstanceOf[BigqueryExpression]
  }

  implicit class StringHelper(s: String) {
    def *(s2: String): BigqueryExpression = {
      COL(s, false) * COL(s2, false)
    }
    def ++(s2: String): BigqueryExpression = {
      COL(s, false) ++ COL(s2, false)
    }
    def -(s2: String): BigqueryExpression = {
      COL(s, false) - COL(s2, false)
    }
    def /(s2: String): BigqueryExpression = {
      COL(s, false) / COL(s2, false)
    }
  }
}

case class BigqueryDerivedExpression (columnContext: ColumnContext, expression: BigqueryExpression) extends DerivedExpression[String] with WithBigqueryEngine {
  type ConcreteType = BigqueryDerivedExpression
  def copyWith(columnContext: ColumnContext): BigqueryDerivedExpression = {
    this.copy(columnContext = columnContext)
  }
}

object BigqueryDerivedExpression {
  import BigqueryExpression.BigqueryExp

  def apply(expression: BigqueryExpression)(implicit cc: ColumnContext): BigqueryDerivedExpression = {
    BigqueryDerivedExpression(cc, expression)
  }

  implicit def fromBigqueryExpression(expression: BigqueryExpression)(implicit  cc: ColumnContext): BigqueryDerivedExpression = {
    apply(expression)
  }

  implicit def fromExpression(expression: BigqueryExp)(implicit  cc: ColumnContext): BigqueryDerivedExpression = {
    apply(expression)
  }

  implicit def fromString(s: String)(implicit  cc: ColumnContext): BigqueryDerivedExpression = {
    apply(s)
  }
}
