// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.utils

import com.yahoo.maha.core.DailyGrain._
import com.yahoo.maha.core._
import grizzled.slf4j.Logging
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone, Days}

/**
 * Created by pranavbhole on 19/04/16.
 */
case object DaysUtils extends Logging {
  val hourNumberFormatString = "YYYYMMddHH"
  val hourFormatString = "YYYY-MM-dd HH"
  val dayNumberFormat = "YYYYMMdd"

  def getDaysBetweenIntoLongList(from: String, to: String) : List[Long] = {
    val days = new scala.collection.mutable.LinkedHashSet[Long]
    val datetimeFormat = DateTimeFormat.forPattern(formatString)
    try {
      var fromDate = datetimeFormat.parseDateTime(from)
      val toDate = datetimeFormat.parseDateTime(to)
      if (fromDate.isAfter(toDate)) {
        throw new IllegalArgumentException(s"From date ($from) cannot be after To date ($to)")
      }
      while(fromDate!=toDate) {
        val longStr =  fromDate.toString(dayNumberFormat)
        days.add(longStr.toLong)
        fromDate = fromDate.plusDays(1)
      }
      days.add(toDate.toString(dayNumberFormat).toLong)
      days.toList
    } catch {
      case e: Exception =>
        logger.error(s"Unchecked input to function from=$from, to=$to", e)
        throw e
    }
  }

  def getDaysIntoLongList(dates:List[String]) : List[Long] = {
    val days = new scala.collection.mutable.LinkedHashSet[Long]
    val numberFormatString = "YYYYMMdd"
    val datetimeFormat = DateTimeFormat.forPattern(formatString)
    try {
      dates.foreach{
        date =>
        val toDate = datetimeFormat.parseDateTime(date)
        val longStr =  toDate.toString(numberFormatString)
        days.add(longStr.toLong)
      }
      days.toList
    } catch {
      case e: Exception =>
        logger.error(s"Unchecked input to function getDaysIntoLongList", e)
        throw e
    }
  }

  def getDayBetweenFilters(colName:String, days:List[Long], engine:Engine): List[Filter] = {
    val betweenFilterSet = new scala.collection.mutable.LinkedHashSet[Filter]
    val dateNumberFormatString = "YYYYMMdd"
    val datetimeFormat = DateTimeFormat.forPattern(dateNumberFormatString)
    getDisjointSetTuplesForDays(days).foreach {
      pair =>
        engine match {
          case OracleEngine=>
            val from = datetimeFormat.parseDateTime(pair._1.toString).toString(formatString)
            val to = datetimeFormat.parseDateTime(pair._2.toString).toString(formatString)
            betweenFilterSet.add(new BetweenFilter(colName,from,to))
          case PostgresEngine=>
            val from = datetimeFormat.parseDateTime(pair._1.toString).toString(formatString)
            val to = datetimeFormat.parseDateTime(pair._2.toString).toString(formatString)
            betweenFilterSet.add(new BetweenFilter(colName,from,to))
          case HiveEngine | PrestoEngine =>
            val from = datetimeFormat.parseDateTime(pair._1.toString).toString(dateNumberFormatString)
            val to = datetimeFormat.parseDateTime(pair._2.toString).toString(dateNumberFormatString)
            betweenFilterSet.add(new BetweenFilter(colName,from,to))
          case BigqueryEngine =>
            val from = datetimeFormat.parseDateTime(pair._1.toString).toString(formatString)
            val to = datetimeFormat.parseDateTime(pair._2.toString).toString(formatString)
            betweenFilterSet.add(new BetweenFilter(colName,from,to))
          case _ =>
            throw new IllegalArgumentException(s"Need to define case for this engine Engine $engine")
        }
    }
    betweenFilterSet.toList
  }

  // List fed to this is originally LinkedHashSet thus it wont contain duplicates
  def getDisjointSetTuplesForHours(ts:List[Long]):List[(Long,Long)] = {
    if(ts.isEmpty) {
      return List.empty
    }
    val min:Long = ts.head
    val max:Long = ts.takeRight(1).head
    if (max - min + 1 == ts.size) {
      return List((min,max))
    }
    val disjointSets = new scala.collection.mutable.LinkedHashSet[(Long,Long)]
    var low = min
    var high = min
      ts.sliding(2).foreach {
        pair =>
          if(pair(0)+1 == pair(1)) {
            high = pair(1)
          } else {
            disjointSets.add((low,high))
            low = pair(1)
            high = pair(1)
          }
      }
    if(low<=high) {
      disjointSets.add((low,high))
    }
    return disjointSets.toList
  }

  def longToDate(day:Long) : DateTime = {
    val dateNumberFormatString = "YYYYMMdd"
    val datetimeFormat = DateTimeFormat.forPattern(dateNumberFormatString)
     try {
         return datetimeFormat.parseDateTime(day.toString)
     } catch {
       case e: Exception =>
       logger.error(s"Unchecked input to function $day", e)
       throw e
     }
  }

  def truncateHourFromGivenHourString(hourString:Long) : String = {
    val hourNumberFormatString = "YYYYMMddHH"
    val dayNumberFormatString = "YYYYMMdd"
    val datetimeFormat = DateTimeFormat.forPattern(hourNumberFormatString)
    try {
      val dateTime = datetimeFormat.parseDateTime(hourString.toString)
      dateTime.toString(dayNumberFormatString)
    } catch {
      case e: Exception =>
        logger.error(s"Unchecked input to function $hourString", e)
        throw e
    }
  }

  // List fed to this is originally LinkedHashSet thus it wont contain duplicates
  def getDisjointSetTuplesForDays(ts:List[Long]):List[(Long,Long)] = {
    if(ts.isEmpty) {
      return List.empty
    }
    if(ts.size == 1) {
      return List((ts.head,ts.head))
    }
    val min:Long = ts.head
    val max:Long = ts.takeRight(1).head

    val disjointSets = new scala.collection.mutable.LinkedHashSet[(Long,Long)]
    var low = min
    var high = min
    ts.sliding(2).foreach {
      pair =>
        if(longToDate(pair(0)).plusDays(1) == longToDate(pair(1))) {
          high = pair(1)
        } else {
          disjointSets.add((low,high))
          low = pair(1)
          high = pair(1)
        }
    }
    if(low<=high) {
      disjointSets.add((low,high))
    }
    return disjointSets.toList
  }


  def getHourBetweenFilters(colName:String, days:List[Long], engine:Engine): List[Filter] = {
    val betweenFilterSet = new scala.collection.mutable.LinkedHashSet[Filter]
    val dateNumberFormatString = "YYYYMMddHH"
    val datetimeFormat = DateTimeFormat.forPattern(dateNumberFormatString)

    getDisjointSetTuplesForHours(days).foreach {
      pair =>
        engine match {
          case OracleEngine=>
            val from = datetimeFormat.parseDateTime(pair._1.toString).toString(hourFormatString)
            val to = datetimeFormat.parseDateTime(pair._2.toString).toString(hourFormatString)
            betweenFilterSet.add(new BetweenFilter(colName,from,to))
          case PostgresEngine=>
            val from = datetimeFormat.parseDateTime(pair._1.toString).toString(hourFormatString)
            val to = datetimeFormat.parseDateTime(pair._2.toString).toString(hourFormatString)
            betweenFilterSet.add(new BetweenFilter(colName,from,to))
          case HiveEngine | PrestoEngine =>
            val from = datetimeFormat.parseDateTime(pair._1.toString).toString(dateNumberFormatString)
            val to = datetimeFormat.parseDateTime(pair._2.toString).toString(dateNumberFormatString)
            betweenFilterSet.add(new BetweenFilter(colName,from,to))
          case BigqueryEngine =>
            val from = datetimeFormat.parseDateTime(pair._1.toString).toString(hourFormatString)
            val to = datetimeFormat.parseDateTime(pair._2.toString).toString(hourFormatString)
            betweenFilterSet.add(new BetweenFilter(colName,from,to))
          case _ =>
            throw new IllegalArgumentException(s"Need to define case for this engine Engine $engine")
        }
    }
    betweenFilterSet.toList
  }

  def refactorFilter(dayFilter:Filter):Filter =  {
    val datetimeFormat = DateTimeFormat.forPattern(formatString)
    dayFilter match {
      case BetweenFilter(field, from, to) =>
        try {
          val fromDate = datetimeFormat.parseDateTime(from)
          val toDate = datetimeFormat.parseDateTime(to)
          new BetweenFilter(field,fromDate.toString(dayNumberFormat),toDate.toString(dayNumberFormat))
        } catch {
          case e: Exception =>
            logger.error(s"Unchecked input to function from=$from, to=$to", e)
            throw e
        }
      case InFilter(field, dates, _, _) =>
        val newDates = new scala.collection.mutable.LinkedHashSet[String]
        try {
          dates.foreach {
            date => newDates.add(datetimeFormat.parseDateTime(date).toString(dayNumberFormat))
          }
          new InFilter(field, newDates.toList)
        } catch {
          case e: Exception =>
            logger.error(s"Unchecked input to function $dates", e)
            throw e
        }
      case EqualityFilter(field, date, _, _) =>
        try {
          val newDate = datetimeFormat.parseDateTime(date)
          new EqualityFilter(field,newDate.toString(dayNumberFormat))
        } catch {
          case e: Exception =>
            logger.error(s"Unchecked input to function $date", e)
            throw e
        }
      case a =>
        throw new IllegalArgumentException(s"Filter operation not supported. Day filter can be between, in, equality filter : $a")
    }
  }


  def getDayIntoLongList(day:String) : List[Long] = {
  return getDaysBetweenIntoLongList(day,day)
  }

  def getUTCToday() : Long = {
    val currentDate = DateTime.now(DateTimeZone.UTC)
    currentDate.toString(dayNumberFormat).toLong
  }

  def getUTCTodayMinusLookBackWindow(lookBackWindow: Int) : Long = {
    val currentDate = DateTime.now(DateTimeZone.UTC)
    currentDate.minusDays(lookBackWindow).toString(dayNumberFormat).toLong
  }

  def getNumberOfDaysFromToday(pastDate: String): Int = {
    val currentDate = DateTime.now(DateTimeZone.UTC)
    try {
      val datetimeFormat = DateTimeFormat.forPattern(formatString)
      val fromDate = datetimeFormat.parseDateTime(pastDate)
      Days.daysBetween(fromDate, currentDate).getDays
    } catch {
      case e: Exception =>
        logger.error(s"Unchecked input to function $pastDate", e)
        throw e
    }
  }

  def isFutureDate(date: String): Boolean = {
    try {
      val datetimeFormat = DateTimeFormat.forPattern(formatString)
      val givenDate = datetimeFormat.withZoneUTC().parseDateTime(date)
      givenDate.isAfterNow()
    } catch {
      case e: Exception =>
        logger.error(s"Unchecked input to function $date", e)
        throw e
    }
  }

  def getAllHoursOfDay(days:List[Long]): List[Long] = {
    val hours = new scala.collection.mutable.LinkedHashSet[Long]
    val dateNumberFormatString = "YYYYMMdd"
    val datetimeFormat = DateTimeFormat.forPattern(dateNumberFormatString).withZoneUTC()
    days.foreach {
      day=>
        val date = datetimeFormat.parseDateTime(day.toString)
        var tmp = date
        while(tmp!=date.plusDays(1)) {
          val longStr =  tmp.toString(hourNumberFormatString)
          hours.add(longStr.toLong)
          tmp = tmp.plusHours(1)
        }
    }
  hours.toList
  }
}
