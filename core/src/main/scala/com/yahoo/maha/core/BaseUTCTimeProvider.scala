// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import com.yahoo.maha.core.HourlyGrain.{HOUR_FILTER_FIELD, _}
import grizzled.slf4j.Logging
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.SortedSet

/**
  * Created by surabhip on 3/9/16.
  */
class BaseUTCTimeProvider extends UTCTimeProvider with Logging {

  override def getUTCDayHourMinuteFilter(localTimeDayFilter: Filter
                                         , localTimeHourFilter: Option[Filter]
                                         , localTimeMinuteFilter: Option[Filter]
                                         , timezone: Option[String]
                                         , isDebugEnabled: Boolean): (Filter, Option[Filter], Option[Filter]) = {

    if (localTimeDayFilter.isInstanceOf[DateTimeFilter]) {
      require(localTimeHourFilter.isEmpty && localTimeMinuteFilter.isEmpty
        , "A DateTimeBetweenFilter on Day cannot be used with a filter on Hour and Minute")
      var utcDateTimeFilter = localTimeDayFilter.asInstanceOf[DateTimeFilter]

      if (!timezone.isDefined) {
        utcDateTimeFilter = extendDateTimeBackwardsByOneDay(utcDateTimeFilter)
        utcDateTimeFilter = extendDateTimeForwardByOneDay(utcDateTimeFilter)
      }

      if (timezone.contains(DateTimeZone.UTC.toString))
        return (utcDateTimeFilter, None, None)

      if (timezone.isDefined) {
        val dateTimeZone = DateTimeZone.forID(timezone.get)
        val requestDTF = localTimeDayFilter.asInstanceOf[DateTimeFilter]
        utcDateTimeFilter = requestDTF.convertToUTCFromLocalZone(dateTimeZone)
      }
      (utcDateTimeFilter, None, None)
    } else {
      handleLegacyDayHourMinuteFilter(localTimeDayFilter, localTimeHourFilter, localTimeMinuteFilter, timezone, isDebugEnabled)
    }
  }

  private def handleLegacyDayHourMinuteFilter(localTimeDayFilter: Filter
                                      , localTimeHourFilter: Option[Filter]
                                      , localTimeMinuteFilter: Option[Filter]
                                      , timezone: Option[String]
                                      , isDebugEnabled: Boolean): (Filter, Option[Filter], Option[Filter]) = {

    var utcDayFilter = localTimeDayFilter
    var utcHourFilter = localTimeHourFilter
    var utcMinuteFilter = localTimeMinuteFilter
    if (isDebugEnabled) info(s"Timezone: $timezone")
    if (!validateFilters(localTimeDayFilter, localTimeHourFilter, localTimeMinuteFilter) || !timezone.isDefined) {
      if (utcHourFilter.isEmpty) {
        warn(s"Failed to validate day/hour filters, or timezone cannot be fetched. Extending day filter by one day")
        utcDayFilter = extendDaysBackwardsByOneDay(utcDayFilter)
        utcDayFilter = extendDaysForwardByOneDay(utcDayFilter)
      }
    } else {
      //if timezone is UTC, pass through
      if (timezone.contains(DateTimeZone.UTC.toString)) return (localTimeDayFilter, localTimeHourFilter, localTimeMinuteFilter)

      val dateTimeZone = DateTimeZone.forID(timezone.get)

      val offsetMinutes = getOffsetMinutes(dateTimeZone)
      val offsetHours = offsetMinutes / 60
      if (isDebugEnabled) {
        info(s"OffsetMinutes: $offsetMinutes")
        info(s"OffsetHours: $offsetHours")
      }
      val inferredLocalHourFilter: Filter = if (!localTimeHourFilter.isDefined) {
        new BetweenFilter(HOUR_FILTER_FIELD, "00", "23")
      } else {
        localTimeHourFilter.get
      }

      val (minLocalHour, maxLocalHour) = getMinAndMaxHours(inferredLocalHourFilter)
      val minUtcHour = minLocalHour + offsetHours
      val maxUtcHour = maxLocalHour + offsetHours
      if (isDebugEnabled) info(s"minUtcHour: $minUtcHour maxUtcHour: $maxUtcHour")
      if (minUtcHour < 0 && maxUtcHour < 0) {
        utcDayFilter = shiftDaysBackwardsByOneDay(localTimeDayFilter)
      } else if (minUtcHour < 0) {
        utcDayFilter = extendDaysBackwardsByOneDay(localTimeDayFilter)
      }

      if (minUtcHour > 23 && maxUtcHour > 23) {
        utcDayFilter = shiftDaysForwardByOneDay(localTimeDayFilter)
      } else if (maxUtcHour > 23) {
        utcDayFilter = extendDaysForwardByOneDay(localTimeDayFilter)
      }

      if (localTimeHourFilter.isDefined) {
        utcHourFilter = Some(updateHours(localTimeHourFilter.get, offsetHours)) // Don't set hourFilter if inputHour filter isn't specified
      }

      if (localTimeHourFilter.isDefined && localTimeMinuteFilter.isDefined) {
        //this means we can only have between or equality filters
        localTimeDayFilter.operator match {
          case BetweenFilterOperation =>
            val fromDay = DailyGrain.fromFormattedString(localTimeDayFilter.asInstanceOf[BetweenFilter].from)
            val toDay = DailyGrain.fromFormattedString(localTimeDayFilter.asInstanceOf[BetweenFilter].to)
            val fromHour = HourlyGrain.fromFormattedString(localTimeHourFilter.get.asInstanceOf[BetweenFilter].from)
            val toHour = HourlyGrain.fromFormattedString(localTimeHourFilter.get.asInstanceOf[BetweenFilter].to)
            val fromMinute = MinuteGrain.fromFormattedString(localTimeMinuteFilter.get.asInstanceOf[BetweenFilter].from)
            val toMinute = MinuteGrain.fromFormattedString(localTimeMinuteFilter.get.asInstanceOf[BetweenFilter].to)
            val fromDate = new DateTime(fromDay.getYear, fromDay.getMonthOfYear, fromDay.getDayOfMonth, fromHour.getHourOfDay, fromMinute.getMinuteOfHour, dateTimeZone).withZone(DateTimeZone.UTC)
            val toDate = new DateTime(toDay.getYear, toDay.getMonthOfYear, toDay.getDayOfMonth, toHour.getHourOfDay, toMinute.getMinuteOfHour, dateTimeZone).withZone(DateTimeZone.UTC)
            utcMinuteFilter = Option(new BetweenFilter(localTimeMinuteFilter.get.field, MinuteGrain.toFormattedString(fromDate), MinuteGrain.toFormattedString(toDate)))
          case EqualityFilterOperation =>
            val valueDay = DailyGrain.fromFormattedString(localTimeDayFilter.asInstanceOf[EqualityFilter].value)
            val valueHour = HourlyGrain.fromFormattedString(localTimeHourFilter.get.asInstanceOf[EqualityFilter].value)
            val valueMinute = MinuteGrain.fromFormattedString(localTimeMinuteFilter.get.asInstanceOf[EqualityFilter].value)
            val valueDate = new DateTime(valueDay.getYear, valueDay.getMonthOfYear, valueDay.getDayOfMonth, valueHour.getHourOfDay, valueMinute.getMinuteOfHour, dateTimeZone).withZone(DateTimeZone.UTC)
            utcMinuteFilter = Option(new EqualityFilter(localTimeMinuteFilter.get.field, MinuteGrain.toFormattedString(valueDate)))
          case any => throw new UnsupportedOperationException(s"Unsupported filter type when hour and minute filters present : $localTimeDayFilter")
        }
      }
    }
    (utcDayFilter, utcHourFilter, utcMinuteFilter)
  }

  def shiftDaysBackwardsByOneDay(dayFilter: Filter): Filter = {
    dayFilter match {
      case BetweenFilter(field, from, to) =>
        new BetweenFilter(field, oneDayBefore(from), oneDayBefore(to))
      case InFilter(field, days, _, _) =>
        val prevDays = new collection.mutable.TreeSet[String]()
        val daysSorted = days.to[SortedSet]
        days.foreach { day => {
          val nextDay = oneDayBefore(day)
          if (!daysSorted.contains(nextDay)) {
            prevDays += nextDay
          }
        }
        }
        prevDays ++= days
        new InFilter(field, prevDays.toList)
      case EqualityFilter(field, day, _, _) =>
        new EqualityFilter(field, oneDayBefore(day))
      case a =>
        throw new IllegalArgumentException(s"Filter operation not supported. Day filter can be 'between', 'in' or 'equality' : $a")
    }
  }

  def extendDaysBackwardsByOneDay(dayFilter: Filter): Filter = {
    dayFilter match {
      case BetweenFilter(field, from, to) =>
        new BetweenFilter(field, oneDayBefore(from), to)
      case InFilter(field, days, _, _) =>
        val prevDays = new collection.mutable.TreeSet[String]()
        val daysSorted = days.to[SortedSet]
        daysSorted.foreach { day => {
          val nextDay = oneDayBefore(day)
          if (!daysSorted.contains(nextDay)) {
            prevDays += nextDay
          }
        }
        }
        prevDays ++= days
        new InFilter(field, prevDays.toList)
      case EqualityFilter(field, day, _, _) =>
        new BetweenFilter(field, oneDayBefore(day), day)
      case a =>
        throw new IllegalArgumentException(s"Filter operation not supported. Day filter can be 'between', 'in' or 'equality' : $a")
    }
  }

  def extendDateTimeBackwardsByOneDay(dtf: DateTimeFilter): DateTimeFilter = {
    dtf match {
      case dtbf: DateTimeBetweenFilter =>
        DateTimeBetweenFilter(
          dtbf.field, dtbf.dateTimeFormatter.print(dtbf.fromDateTime.minusDays(1)), dtbf.to, dtbf.format
        )
      case a =>
        throw new IllegalArgumentException(s"Filter operation not supported : $a")
    }
  }

  def shiftDaysForwardByOneDay(dayFilter: Filter): Filter = {
    dayFilter match {
      case BetweenFilter(field, from, to) =>
        new BetweenFilter(field, oneDayAfter(from), oneDayAfter(to))
      case InFilter(field, days, _, _) =>
        val nextDays = new collection.mutable.TreeSet[String]()
        val daysSorted = days.to[SortedSet]
        days.foreach { day => {
          val nextDay = oneDayAfter(day)
          if (!daysSorted.contains(nextDay)) {
            nextDays += nextDay
          }
        }
        }
        nextDays ++= days
        new InFilter(field, nextDays.toList)
      case EqualityFilter(field, day, _, _) =>
        new EqualityFilter(field, oneDayAfter(day))
      case a =>
        throw new IllegalArgumentException(s"Filter operation not supported. Day filter can be 'between', 'in' or 'equality' : $a")
    }
  }

  def extendDaysForwardByOneDay(dayFilter: Filter): Filter = {
    dayFilter match {
      case BetweenFilter(field, from, to) =>
        new BetweenFilter(field, from, oneDayAfter(to))
      case InFilter(field, days, _, _) =>
        val nextDays = new collection.mutable.TreeSet[String]()
        val daysSorted = days.to[SortedSet]
        days.foreach { day => {
          val nextDay = oneDayAfter(day)
          if (!daysSorted.contains(nextDay)) {
            nextDays += nextDay
          }
        }
        }
        nextDays ++= days
        new InFilter(field, nextDays.toList)
      case EqualityFilter(field, day, _, _) =>
        new BetweenFilter(field, day, oneDayAfter(day))
      case a =>
        throw new IllegalArgumentException(s"Filter operation not supported. Day filter can be 'between', 'in' or 'equality' : $a")
    }
  }

  def extendDateTimeForwardByOneDay(dtf: DateTimeFilter): DateTimeFilter = {
    dtf match {
      case dtbf: DateTimeBetweenFilter =>
        DateTimeBetweenFilter(
          dtbf.field, dtbf.from, dtbf.dateTimeFormatter.print(dtbf.toDateTime.plusDays(1)), dtbf.format
        )
      case a =>
        throw new IllegalArgumentException(s"Filter operation not supported : $a")
    }
  }

  def getMinAndMaxHours(localHourFilter: Filter): Tuple2[Int, Int] = {
    localHourFilter match {
      case BetweenFilter(_, from, to) =>
        (getAsInt(from), getAsInt(to))
      case InFilter(_, hours, _, _) =>
        val sortedHours = hours.sortWith((x, y) => HourlyGrain.getAsInt(x) < HourlyGrain.getAsInt(y))
        (getAsInt(sortedHours.head), getAsInt(sortedHours.last))
      case EqualityFilter(_, hour, _, _) =>
        (getAsInt(hour), getAsInt(hour))
      case a =>
        throw new IllegalArgumentException(s"Filter operation not supported. Hour filter can be 'between', 'in' or 'equality' : $a")
    }
  }

  def updateHours(localHourFilter: Filter, offsetHours: Int): Filter = {
    localHourFilter match {
      case BetweenFilter(field, from, to) =>
        new BetweenFilter(field, applyOffset(from, offsetHours), applyOffset(to, offsetHours))
      case InFilter(field, hours, _, _) =>
        val sortedHours = hours.sortWith((x, y) => HourlyGrain.getAsInt(x) < HourlyGrain.getAsInt(y))
        new InFilter(field, hours.map { hour => applyOffset(hour, offsetHours) })
      case EqualityFilter(field, hour, _, _) =>
        new EqualityFilter(field, applyOffset(hour, offsetHours))
      case a =>
        throw new IllegalArgumentException(s"Filter operation not supported. Hour filter can be 'between', 'in' or 'equality' : $a")
    }
  }

  def applyOffset(hour: String, offsetHours: Int): String = {
    val updatedHour = getAsInt(hour) + offsetHours
    if (updatedHour < 0) {
      HourlyGrain.toFormattedString(DateTime.now().withHourOfDay(updatedHour + 24))
    } else if (updatedHour > 23) {
      HourlyGrain.toFormattedString(DateTime.now().withHourOfDay(updatedHour - 24))
    } else {
      HourlyGrain.toFormattedString(DateTime.now().withHourOfDay(updatedHour)) // "00"
    }
  }

  def oneDayBefore(day: String): String = {
    DailyGrain.toFormattedString(DailyGrain.fromFormattedString(day).minusDays(1))
  }

  def oneDayAfter(day: String): String = {
    DailyGrain.toFormattedString(DailyGrain.fromFormattedString(day).plusDays(1))
  }

  def validateFilters(dayFilter: Filter, hourFilter: Option[Filter], minuteFilter: Option[Filter]): Boolean = {
    if (!hourFilter.isDefined && !minuteFilter.isDefined) return true
    if (!hourFilter.isDefined && minuteFilter.isDefined) return false
    if (hourFilter.isDefined && dayFilter.operator != hourFilter.get.operator) return false
    if (hourFilter.isDefined && minuteFilter.isDefined && hourFilter.get.operator != minuteFilter.get.operator) return false
    true
  }

  private def getOffsetMinutes(timezone: DateTimeZone): Int = {
    Math.ceil(timezone.getOffset(null) / (1000 * 60)).toInt * (-1) // offset in minutes to be added to local time to get UTC
  }
}
