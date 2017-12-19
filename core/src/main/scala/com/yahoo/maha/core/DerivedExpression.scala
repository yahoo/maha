// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

/**
 * Created by hiral on 10/2/15.
 */

import java.util.concurrent.atomic.AtomicLong

import com.google.common.collect.Lists
import io.druid.query.aggregation.PostAggregator
import io.druid.query.aggregation.post.{ArithmeticPostAggregator, ConstantPostAggregator, FieldAccessPostAggregator}

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
        s"CASE WHEN ${that.asString} = 0 THEN 0.0 ELSE $asString / (${that.asString}) END"
      } else {
        s"CASE WHEN ${that.asString} = 0 THEN 0.0 ELSE $asString / ${that.asString} END"
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
    def asString : String = s"from_unixtime(unix_timestamp(${s.asString}, '$fmt'), 'EEEE')"
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
      //println(s"\nExp:$exp\tFmt$fmt\n")
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
