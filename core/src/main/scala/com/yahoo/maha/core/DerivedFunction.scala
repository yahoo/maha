// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import org.joda.time.DateTimeZone

/**
 * Created by hiral on 10/29/15.
 */
sealed trait DerivedFunction

sealed trait DruidDerivedFunction extends DerivedFunction

object DruidDerivedFunction {
  case class GET_INTERVAL_DATE(fieldName: String, format: String) extends DruidDerivedFunction {
    // args should be stats_date and d|w|m
    GET_INTERVAL_DATE.checkFormat(format)

    val dimColName = fieldName.replaceAll("[}{]","")
  }

  object GET_INTERVAL_DATE {

    val regex = """[dwmDWM]|[dD][aA][yY]|[yY][rR]""".r

    def checkFormat(fmt: String):  Unit = {
      regex.findFirstIn(fmt) match {
        case Some(f) =>
        case _ => throw new IllegalArgumentException(s"Format for get_interval_date must be d|w|m|day|yr not $fmt")
      }
    }
  }

  case class DAY_OF_WEEK(fieldName: String) extends DruidDerivedFunction {
    // args should be stats_date
    val dimColName = fieldName.replaceAll("[}{]","")
  }

  object DAY_OF_WEEK

  case class DATETIME_FORMATTER(fieldName: String, index: Int, length: Int) extends DruidDerivedFunction {
    // args should be stats_date
    val dimColName = fieldName.replaceAll("[}{]","")
  }

  object DATETIME_FORMATTER

  case class DECODE_DIM(fieldName: String, args: String*) extends DruidDerivedFunction {
    if (args.length < 2) throw new IllegalArgumentException("Usage: DECODE( expression , search , result [, search , result]... [, default] )")
    val dimColName = fieldName.replaceAll("[}{]","")
    val map : Map[String, String] = args.grouped(2).filter(_.size == 2).map(seq => (seq.head, seq(1))).toMap
    val default: Option[String] = args.length % 2 match {
      case 0 => None
      case _ => Option.apply(args(args.length - 1))
    }
    val apply = new PartialFunction[String, Option[String]] {
      override def isDefinedAt(v: String): Boolean = {
        true
      }

      override def apply(v: String): Option[String] = {
        map.get(v) match  {
          case Some(s) => Some(s)
          case None if default.isDefined => default
          case _ => None
        }
      }
    }
  }
  case class JAVASCRIPT(fieldName: String, function: String) extends DruidDerivedFunction {
    val dimColName = fieldName.replaceAll("[}{]","")
  }

  case class LOOKUP(lookupNamespace: String, valueColumn: String, dimensionOverrideMap: Map[String, String] = Map.empty) extends DruidDerivedFunction

  case class LOOKUP_WITH_DECODE_ON_OTHER_COLUMN(lookupNamespace: String,
                                                columnToCheck: String, valueToCheck: String,
                                                columnIfValueMatched: String,
                                                columnIfValueNotMatched: String,
                                                dimensionOverrideMap: Map[String, String] = Map.empty) extends DruidDerivedFunction

  case class LOOKUP_WITH_TIMEFORMATTER(lookupNameSpace:String, valueColumn:String, inputFormat:String,resultFormat:String, dimensionOverrideMap: Map[String, String] = Map.empty) extends DruidDerivedFunction

  case class LOOKUP_WITH_DECODE(lookupNamespace: String,
                                valueColumn: String,
                                dimensionOverrideMap: Map[String, String],
                                args: String*) extends DruidDerivedFunction {
    if (args.length < 2) throw new IllegalArgumentException("Usage: DECODE( expression , search , result [, search , result]... [, default] )")

    val map : Map[String, String] = args.grouped(2).filter(_.size == 2).map(seq => (seq.head, seq(1))).toMap
    val default: Option[String] = args.length % 2 match {
      case 0 => None
      case _ => Option.apply(args(args.length - 1))
    }
  }

  case class LOOKUP_WITH_DECODE_RETAIN_MISSING_VALUE(lookupNamespace: String,
                                valueColumn: String,
                                retainMissingValue: Boolean,
                                injective: Boolean,
                                dimensionOverrideMap: Map[String, String],
                                args: String*) extends DruidDerivedFunction {
    val lookupWithDecode: LOOKUP_WITH_DECODE = LOOKUP_WITH_DECODE(lookupNamespace, valueColumn, dimensionOverrideMap, args: _*)
  }

  case class DRUID_TIME_FORMAT(format: String, zone: DateTimeZone = DateTimeZone.UTC) extends DruidDerivedFunction
  object DRUID_TIME_FORMAT {
    val sourceDimColName = "__time"
  }
}

