// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.owasp.esapi.ESAPI
import org.owasp.esapi.codecs.OracleCodec

/**
 * Created by hiral on 11/12/15.
 */

trait LiteralMapper {
  val ORACLE_CODEC = new OracleCodec
  protected def getEscapedSqlString(s: String): String = {
    ESAPI.encoder().encodeForSQL(ORACLE_CODEC, s)
  }
  def toLiteral(column: Column, value: String, grainOption: Option[Grain] = None) : String
}
trait SqlLiteralMapper extends LiteralMapper {
  def getDateFormatFromGrain(grain: Grain): String
}
class OracleLiteralMapper extends SqlLiteralMapper {
  def getDateFormatFromGrain(grain: Grain): String = {
    grain match {
      case DailyGrain => "YYYY-MM-DD"
      case HourlyGrain => "HH24"
      case _ => "YYYY-MM-DD"
    }
  }

  private def renderDate(value: String, fmt: Option[String], grainOption: Option[Grain]): String = {
    val grainFormat: String = grainOption.map(getDateFormatFromGrain).getOrElse("YYYY-MM-DD")
    s"to_date('${getEscapedSqlString(value)}', '${fmt.getOrElse(grainFormat)}')"
  }

  def toLiteral(column: Column, value: String, grainOption: Option[Grain] = None): String = {
    column.dataType match {
      case DateType(fmt) =>
        //TODO: validate format
        renderDate(value, fmt, grainOption)
      case TimestampType(fmt) =>
        //TODO: validate format
        s"'${getEscapedSqlString(value)}'"
      case _ if column.annotations.contains(DayColumn.instance) =>
        //it's a date column, treat same as date type
        renderDate(value, None, grainOption)
      case StrType(_, _, _) =>
        s"'${getEscapedSqlString(value)}'"
      case IntType(_, _, _, _, _) => BigInt(value).toString
      case DecType(_, _, _, _, _, _) => BigDecimal(value).toString
    }
  }
}

class PostgresLiteralMapper extends SqlLiteralMapper {
  def getDateFormatFromGrain(grain: Grain): String = {
    grain match {
      case DailyGrain => "YYYY-MM-DD"
      case HourlyGrain => "HH24"
      case _ => "YYYY-MM-DD"
    }
  }

  private def renderDate(value: String, fmt: Option[String], grainOption: Option[Grain]): String = {
    val grainFormat: String = grainOption.map(getDateFormatFromGrain).getOrElse("YYYY-MM-DD")
    s"to_date('${getEscapedSqlString(value)}', '${fmt.getOrElse(grainFormat)}')"
  }

  def toLiteral(column: Column, value: String, grainOption: Option[Grain] = None): String = {
    column.dataType match {
      case DateType(fmt) =>
        //TODO: validate format
        renderDate(value, fmt, grainOption)
      case TimestampType(fmt) =>
        //TODO: validate format
        s"'${getEscapedSqlString(value)}'"
      case _ if column.annotations.contains(DayColumn.instance) =>
        //it's a date column, treat same as date type
        renderDate(value, None, grainOption)
      case StrType(_, _, _) =>
        s"'${getEscapedSqlString(value)}'"
      case IntType(_, _, _, _, _) => BigInt(value).toString
      case DecType(_, _, _, _, _, _) => BigDecimal(value).toString
    }
  }
}

class HiveLiteralMapper extends SqlLiteralMapper {
  def getDateFormatFromGrain(grain: Grain): String = {
    grain match {
      case DailyGrain => "YYYYMMdd"
      case HourlyGrain => "YYYYMMddHH"
      case _ => "YYYYMMdd"
    }
  }
  def toLiteral(column: Column, value: String, grainOption: Option[Grain] = None) : String = {
    val grain = grainOption.getOrElse(DailyGrain)
    column.dataType match {
      case StrType(_, _, _) =>
        s"'${getEscapedSqlString(value)}'"
      case DateType(fmt) =>
        //TODO: validate format
        val formatted = if(fmt.isDefined) {
          val dtf = DateTimeFormat.forPattern(fmt.get)
          dtf.print(grain.fromFormattedString(value))
        } else {
          val fmt = getDateFormatFromGrain(grain)
          val dtf = DateTimeFormat.forPattern(fmt)
          val grainDate = grain.fromFormattedString(value)
          dtf.print(grainDate)
        }
        s"'${getEscapedSqlString(formatted)}'"
      case TimestampType(fmt) =>
        //TODO: validate format
        s"'${getEscapedSqlString(value)}'"
      case IntType(_, _, _, _, _) => BigInt(value).toString
      case DecType(_, _, _, _, _, _) => BigDecimal(value).toString
    }

  }
}

class DruidLiteralMapper extends LiteralMapper {
  def toDateTime(column: Column, value: String, grainOption: Option[Grain]) : DateTime = {
    val grain = grainOption.getOrElse(DailyGrain)
    column.dataType match {
      case DateType(fmt) =>
        //TODO: validate format
        if(fmt.isDefined) {
          val dtf = DateTimeFormat.forPattern(fmt.get)
          dtf.withZone(DateTimeZone.UTC).parseDateTime(dtf.print(grain.fromFormattedString(value)))
        } else {
          grain.fromFormattedString(value)
        }
      case TimestampType(fmt) =>
        //TODO: validate format
        if(fmt.isDefined) {
          val dtf = DateTimeFormat.forPattern(fmt.get)
          dtf.withZone(DateTimeZone.UTC).parseDateTime(dtf.print(grain.fromFormattedString(value)))
        } else {
          grain.fromFormattedString(value)
        }
      case _ => grain.fromFormattedString(value)
    }
  }
  
  def toLiteral(column: Column, value: String, grainOption: Option[Grain]) : String = {
    val grain = grainOption.getOrElse(DailyGrain)
    column.dataType match {
      case DateType(fmt) =>
        //TODO: validate format
        if(fmt.isDefined) {
          val dtf = DateTimeFormat.forPattern(fmt.get)
          dtf.print(grain.fromFormattedString(value))
        } else {
          grain.toFormattedString(grain.fromFormattedString(value))
        }
      case TimestampType(fmt) =>
        if(fmt.isDefined) {
          val dtf = DateTimeFormat.forPattern(fmt.get)
          dtf.print(grain.fromFullFormattedString(value))
        } else {
          grain.toFullFormattedString(grain.fromFullFormattedString(value))
        }
      case _ => value
    }

  }
  
  def toNumber(column: Column, value: String) : Number = {
    column.dataType match {
      case t: DecType =>
        BigDecimal(value)
      case t: IntType =>
        BigDecimal(value)
      case any =>
        throw new UnsupportedOperationException(s"Cannot convert data type to number : dataType=$any , column=${column.name}")

    }
  }
}

class BigqueryLiteralMapper extends SqlLiteralMapper {
  def getDateFormatFromGrain(grain: Grain): String = {
    grain match {
      case DailyGrain => "YYYY-MM-DD"
      case HourlyGrain => "YYYY-MM-DD-HH"
      case _ => "YYYY-MM-DD"
    }
  }

  def toLiteral(column: Column, value: String, grainOption: Option[Grain] = None): String = {
    val grain = grainOption.getOrElse(DailyGrain)
    val escapedValue = getEscapedSqlString(value)
    column.dataType match {
      case StrType(_, _, _) =>
        s"'$escapedValue'"
      case DateType(fmt) =>
        s"DATE('$escapedValue')"
      case TimestampType(fmt) =>
        s"TIMESTAMP('$escapedValue')"
      case IntType(_, _, _, _, _) => BigInt(value).toString
      case DecType(_, _, _, _, _, _) => BigDecimal(value).toString
    }
  }
}
