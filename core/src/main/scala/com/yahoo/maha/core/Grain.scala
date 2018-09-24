// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import grizzled.slf4j.Logging
import org.joda.time.format._
import org.joda.time.{DateTime, DateTimeZone, Days}

/**
 * Created by jians on 10/21/15.
 */
sealed trait Grain {
  //value between 0 and 9
  def level: Int
  def formatString: String
  def toFormattedString(dt: DateTime) : String
  def fromFormattedString(date: String) : DateTime
  def incrementOne(dt: DateTime): DateTime
}

object Grain {
  val grainFields : Set[String] = Set(DailyGrain.DAY_FILTER_FIELD, HourlyGrain.HOUR_FILTER_FIELD, MinuteGrain.MINUTE_FILTER_FIELD, "Month", "Week", "Year")
  val fieldToGrainMap : Map[String, Grain] = Map(DailyGrain.DAY_FILTER_FIELD -> DailyGrain, HourlyGrain.HOUR_FILTER_FIELD -> HourlyGrain, MinuteGrain.MINUTE_FILTER_FIELD -> MinuteGrain)
  def getGrainByField(field: String) : Option[Grain] = fieldToGrainMap.get(field)
}

case object DailyGrain extends Grain with Logging {
  
  val level = 0
  
  val DAY_FILTER_FIELD: String = "Day"

  override val formatString = "YYYY-MM-dd"
  private[this] val datetimeFormat = DateTimeFormat.forPattern(formatString).withZoneUTC()
  
  def toFormattedString(dt: DateTime) : String = datetimeFormat.print(dt)
  def fromFormattedString(date: String) : DateTime = datetimeFormat.parseDateTime(date)
  def fromFormattedStringAndZone(date: String, timezone: String) : DateTime = DateTimeFormat.forPattern(formatString).withZone(DateTimeZone.forID(timezone)).parseDateTime(date)
  def incrementOne(dt: DateTime): DateTime = dt.plusDays(1)

  def validateFilterAndGetNumDays(filter: Filter): Int = {
    filter match {
      case BetweenFilter(field, from, to) =>
        validateFormat(field, from)
        validateFormat(field, to)
        getDaysBetween(from, to)
      case InFilter(field, values, _, _) =>
        values.foreach(f => DailyGrain.validateFormat(field, f))
        values.size
      case EqualityFilter(field, value, _, _) =>
        validateFormat(field, value)
        1
      case _ =>
        throw new IllegalArgumentException(s"Unsupported filter operation on daily grain filter : ${filter.operator}")
    }
  }

  def validateFormat(fieldName: String, dateToParse: String) = {
    try {
      datetimeFormat.parseDateTime(dateToParse)
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(s"Failed to parse field=$fieldName, with value=$dateToParse, expected format=$formatString")
    }
  }

  def getDaysBetween(from: String, to: String) : Int = {
    try {
      val fromDate = datetimeFormat.parseDateTime(from)
      val toDate = datetimeFormat.parseDateTime(to)
      require(fromDate.isBefore(toDate) || fromDate.isEqual(toDate), s"From date must be before or equal to To date : from=$from, to=$to")
      val daysBetween = Days.daysBetween(fromDate, toDate).getDays
      daysBetween
    } catch {
      case e: Exception =>
        logger.error(s"Unchecked input to function from=$from, to=$to", e)
        throw e
    }
  }

  def getDaysFromNow(from: String): Int = {
    val fromDate = datetimeFormat.parseDateTime(from)
    val currentDate = DateTime.now(DateTimeZone.UTC)
    try {
      Days.daysBetween(fromDate, currentDate).getDays
    } catch {
      case e: Exception =>
        1
    }

  }
}

case object HourlyGrain extends Grain {

  val level = 1
  
  val HOUR_FILTER_FIELD: String = "Hour"
  
  override val formatString = "HH"
  private[this] val datetimeFormat = DateTimeFormat.forPattern(formatString)

  def toFormattedString(dt: DateTime) : String = datetimeFormat.print(dt)
  def fromFormattedString(date: String) : DateTime = datetimeFormat.parseDateTime(date)
  def incrementOne(dt: DateTime): DateTime = dt.plusHours(1)
  def getAsInt(date: String): Int = Integer.parseInt(date)

  def validateFilter(filter: Filter): Unit = {
    filter match {
      case BetweenFilter(field, from, to) =>
        validateFormat(field, from)
        validateFormat(field, to)
      case EqualityFilter(field, value, _, _) =>
        validateFormat(field, value)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported filter operation on hourly grain filter : ${filter.operator}")
    }
  }

  def validateFormat(fieldName: String, dateToParse: String) = {
    try {
      datetimeFormat.parseDateTime(dateToParse)
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(s"Failed to parse field=$fieldName, with value=$dateToParse, expected format=$formatString")
    }
  }
}

case object MinuteGrain extends Grain {

  val level = 1

  val MINUTE_FILTER_FIELD: String = "Minute"

  override val formatString = "mm"
  private[this] val datetimeFormat = DateTimeFormat.forPattern(formatString)

  def toFormattedString(dt: DateTime) : String = datetimeFormat.print(dt)
  def fromFormattedString(date: String) : DateTime = datetimeFormat.parseDateTime(date)
  def incrementOne(dt: DateTime): DateTime = dt.plusMinutes(1)
  def getAsInt(date: String): Int = Integer.parseInt(date)

  def validateFilter(filter: Filter): Unit = {
    filter match {
      case BetweenFilter(field, from, to) =>
        validateFormat(field, from)
        validateFormat(field, to)
      case EqualityFilter(field, value, _, _) =>
        validateFormat(field, value)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported filter operation on minute grain filter : ${filter.operator}")
    }
  }

  def validateFormat(fieldName: String, dateToParse: String) = {
    try {
      datetimeFormat.parseDateTime(dateToParse)
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(s"Failed to parse field=$fieldName, with value=$dateToParse, expected format=$formatString")
    }
  }
}
