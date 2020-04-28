// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import org.joda.time.DateTimeZone
import org.json4s._
import org.json4s.JsonAST.JObject
import org.json4s.scalaz.JsonScalaz._

/**
 * Created by hiral on 10/29/15.
 */
sealed trait DerivedFunction {
  def asJSON: JObject
}

sealed trait DruidDerivedFunction extends DerivedFunction

object DruidDerivedFunction {

  case class GET_INTERVAL_DATE(fieldName: String, format: String) extends DruidDerivedFunction {
    // args should be stats_date and d|w|m
    GET_INTERVAL_DATE.checkFormat(format)

    val dimColName = fieldName.replaceAll("[}{]", "")

    override def asJSON: JObject =
      makeObj(
        List(
          ("function_type" -> toJSON(this.getClass.getSimpleName))
          , ("fieldName" -> toJSON(fieldName))
          , ("format" -> toJSON(format))
        )
      )
  }

  object GET_INTERVAL_DATE {

    val regex = """[dwmDWM]|[dD][aA][yY]|[yY][rR]""".r

    def checkFormat(fmt: String): Unit = {
      regex.findFirstIn(fmt) match {
        case Some(f) =>
        case _ => throw new IllegalArgumentException(s"Format for get_interval_date must be d|w|m|day|yr not $fmt")
      }
    }
  }

  case class DAY_OF_WEEK(fieldName: String) extends DruidDerivedFunction {
    // args should be stats_date
    val dimColName = fieldName.replaceAll("[}{]", "")

    override def asJSON: JObject =
      makeObj(
        List(
          ("function_type" -> toJSON(this.getClass.getSimpleName))
          , ("fieldName" -> toJSON(fieldName))
        )
      )
  }

  object DAY_OF_WEEK

  case class DATETIME_FORMATTER(fieldName: String, index: Int, length: Int) extends DruidDerivedFunction {
    // args should be stats_date
    val dimColName = fieldName.replaceAll("[}{]", "")

    override def asJSON: JObject =
      makeObj(
        List(
          ("function_type" -> toJSON(this.getClass.getSimpleName))
          , ("fieldName" -> toJSON(fieldName))
          , ("index" -> toJSON(index))
          , ("length" -> toJSON(length))
        )
      )
  }

  object DATETIME_FORMATTER

  case class DECODE_DIM(fieldName: String, args: String*) extends DruidDerivedFunction {
    if (args.length < 2) throw new IllegalArgumentException("Usage: DECODE( expression , search , result [, search , result]... [, default] )")
    val dimColName = fieldName.replaceAll("[}{]", "")
    val map: Map[String, String] = args.grouped(2).filter(_.size == 2).map(seq => (seq.head, seq(1))).toMap
    val default: Option[String] = args.length % 2 match {
      case 0 => None
      case _ => Option.apply(args(args.length - 1))
    }
    val apply = new PartialFunction[String, Option[String]] {
      override def isDefinedAt(v: String): Boolean = {
        true
      }

      override def apply(v: String): Option[String] = {
        map.get(v) match {
          case Some(s) => Some(s)
          case None if default.isDefined => default
          case _ => None
        }
      }
    }

    override def asJSON(): JObject =
      makeObj(
        List(
          ("function_type" -> toJSON(this.getClass.getSimpleName))
          , ("fieldName" -> toJSON(fieldName))
          , ("args" -> toJSON(args.mkString(",")))
        )
      )
  }

  case class JAVASCRIPT(fieldName: String, function: String) extends DruidDerivedFunction {
    val dimColName = fieldName.replaceAll("[}{]", "")

    override def asJSON(): JObject =
      makeObj(
        List(
          ("function_type" -> toJSON(this.getClass.getSimpleName))
          , ("fieldName" -> toJSON(fieldName))
          , ("function" -> toJSON(function))
        )
      )
  }

  case class REGEX(fieldName: String, expr: String, index: Int, replaceMissingValue: Boolean, replaceMissingValueWith: String) extends DruidDerivedFunction {
    val dimColName = fieldName.replaceAll("[}{]", "")

    override def asJSON(): JObject =
      makeObj(
        List(
          ("function_type" -> toJSON(this.getClass.getSimpleName))
          , ("fieldName" -> toJSON(fieldName))
          , ("expr" -> toJSON(expr))
          , ("index" -> toJSON(index))
          , ("replaceMissingValue" -> toJSON(replaceMissingValue))
          , ("replaceMissingValueWith" -> toJSON(replaceMissingValueWith))
        )
      )
  }

  case class LOOKUP(lookupNamespace: String, valueColumn: String, dimensionOverrideMap: Map[String, String] = Map.empty) extends DruidDerivedFunction {
    override def asJSON(): JObject =
      makeObj(
        List(
          ("function_type" -> toJSON(this.getClass.getSimpleName))
          , ("lookupNamespace" -> toJSON(lookupNamespace))
          , ("valueColumn" -> toJSON(valueColumn))
          , ("dimensionOverrideMap" -> toJSON(dimensionOverrideMap))
        )
      )
  }

  case class LOOKUP_WITH_EMPTY_VALUE_OVERRIDE(lookupNamespace: String, valueColumn: String, overrideValue: String, dimensionOverrideMap: Map[String, String] = Map.empty) extends DruidDerivedFunction {
    override def asJSON(): JObject =
      makeObj(
        List(
          ("function_type" -> toJSON(this.getClass.getSimpleName))
          , ("lookupNamespace" -> toJSON(lookupNamespace))
          , ("valueColumn" -> toJSON(valueColumn))
          , ("overrideValue" -> toJSON(overrideValue))
          , ("dimensionOverrideMap" -> toJSON(dimensionOverrideMap))
        )
      )
  }

  case class LOOKUP_WITH_DECODE_ON_OTHER_COLUMN(lookupNamespace: String,
                                                columnToCheck: String,
                                                valueToCheck: String,
                                                columnIfValueMatched: String,
                                                columnIfValueNotMatched: String,
                                                dimensionOverrideMap: Map[String, String] = Map.empty) extends DruidDerivedFunction {
    override def asJSON(): JObject =
      makeObj(
        List(
          ("function_type" -> toJSON(this.getClass.getSimpleName))
          , ("lookupNamespace" -> toJSON(lookupNamespace))
          , ("columnToCheck" -> toJSON(columnToCheck))
          , ("valueToCheck" -> toJSON(valueToCheck))
          , ("columnIfValueMatched" -> toJSON(columnIfValueMatched))
          , ("columnIfValueNotMatched" -> toJSON(columnIfValueNotMatched))
          , ("dimensionOverrideMap" -> toJSON(dimensionOverrideMap))
        )
      )
  }

  case class LOOKUP_WITH_TIMEFORMATTER(lookupNameSpace: String, valueColumn: String, inputFormat: String, resultFormat: String, dimensionOverrideMap: Map[String, String] = Map.empty, overrideValue: Option[String] = None) extends DruidDerivedFunction {
    override def asJSON(): JObject =
      makeObj(
        List(
          ("function_type" -> toJSON(this.getClass.getSimpleName))
          , ("lookupNamespace" -> toJSON(lookupNameSpace))
          , ("valueColumn" -> toJSON(valueColumn))
          , ("inputFormat" -> toJSON(inputFormat))
          , ("resultFormat" -> toJSON(resultFormat))
          , ("dimensionOverrideMap" -> toJSON(dimensionOverrideMap))
        )
      )
  }

  case class LOOKUP_WITH_DECODE(lookupNamespace: String,
                                valueColumn: String,
                                dimensionOverrideMap: Map[String, String],
                                args: String*) extends DruidDerivedFunction {
    if (args.length < 2) throw new IllegalArgumentException("Usage: DECODE( expression , search , result [, search , result]... [, default] )")

    val map: Map[String, String] = args.grouped(2).filter(_.size == 2).map(seq => (seq.head, seq(1))).toMap
    val default: Option[String] = args.length % 2 match {
      case 0 => None
      case _ => Option.apply(args(args.length - 1))
    }

    override def asJSON(): JObject =
      makeObj(
        List(
          ("function_type" -> toJSON(this.getClass.getSimpleName))
          , ("lookupNamespace" -> toJSON(lookupNamespace))
          , ("valueColumn" -> toJSON(valueColumn))
          , ("dimensionOverrideMap" -> toJSON(dimensionOverrideMap))
          , ("args" -> toJSON(args.mkString(",")))
        )
      )
  }

  case class LOOKUP_WITH_DECODE_RETAIN_MISSING_VALUE(lookupNamespace: String,
                                                     valueColumn: String,
                                                     retainMissingValue: Boolean,
                                                     injective: Boolean,
                                                     dimensionOverrideMap: Map[String, String],
                                                     args: String*) extends DruidDerivedFunction {
    val lookupWithDecode: LOOKUP_WITH_DECODE = LOOKUP_WITH_DECODE(lookupNamespace, valueColumn, dimensionOverrideMap, args: _*)

    override def asJSON(): JObject =
      makeObj(
        List(
          ("function_type" -> toJSON(this.getClass.getSimpleName))
          , ("lookupNamespace" -> toJSON(lookupNamespace))
          , ("valueColumn" -> toJSON(valueColumn))
          , ("retainMissingValue" -> toJSON(retainMissingValue))
          , ("injective" -> toJSON(injective))
          , ("dimensionOverrideMap" -> toJSON(dimensionOverrideMap))
          , ("args" -> toJSON(args.mkString(",")))
        )
      )
  }

  case class DRUID_TIME_FORMAT(format: String, zone: DateTimeZone = DateTimeZone.UTC) extends DruidDerivedFunction {
    override def asJSON(): JObject =
      makeObj(
        List(
          ("function_type" -> toJSON(this.getClass.getSimpleName))
          , ("format" -> toJSON(format))
          , ("zone" -> toJSON(zone.toString))
        )
      )
  }

  object DRUID_TIME_FORMAT {
    val sourceDimColName = "__time"
  }

  case class DRUID_TIME_FORMAT_WITH_PERIOD_GRANULARITY(
    format: String,
    period: String,
    zone: DateTimeZone = DateTimeZone.UTC
  ) extends DruidDerivedFunction {
    override def asJSON(): JObject =
      makeObj(
        List(
          ("function_type" -> toJSON(this.getClass.getSimpleName))
          , ("format" -> toJSON(format))
          , ("period" -> toJSON(period))
          , ("zone" -> toJSON(zone.toString))
        )
      )
  }

  object DRUID_TIME_FORMAT_WITH_PERIOD_GRANULARITY {
    val sourceDimColName = "__time"
  }

  /* Information like timezone is passed in the request context. */
  case class TIME_FORMAT_WITH_REQUEST_CONTEXT(format: String) extends DruidDerivedFunction {
    override def asJSON(): JObject =
      makeObj(
        List(
          ("function_type" -> toJSON(this.getClass.getSimpleName))
          , ("format" -> toJSON(format))
        )
      )
  }

  object TIME_FORMAT_WITH_REQUEST_CONTEXT {
    val sourceDimColName = "__time"

  }

  case class LOOKUP_WITH_TIMESTAMP(lookupNamespace: String, valueColumn: String, resultFormat: String, dimensionOverrideMap: Map[String, String] = Map.empty, overrideValue: Option[String] = None, asMillis: Boolean = true) extends DruidDerivedFunction {
    override def asJSON(): JObject =
      makeObj(
        List(
          ("function_type" -> toJSON(this.getClass.getSimpleName))
          , ("lookupNamespace" -> toJSON(lookupNamespace))
          , ("valueColumn" -> toJSON(valueColumn))
          , ("resultFormat" -> toJSON(resultFormat))
          , ("dimensionOverrideMap" -> toJSON(dimensionOverrideMap))
          , ("overrideValue" -> toJSON(overrideValue.getOrElse("None")))
          , ("asMillis" -> toJSON(asMillis))
        )
      )

  }

}

