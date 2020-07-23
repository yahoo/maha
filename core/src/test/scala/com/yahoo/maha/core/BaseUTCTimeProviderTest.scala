// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import com.yahoo.maha.jdbc._
import org.joda.time.{DateTime, DateTimeZone, Instant}
import org.junit.Assert._
import org.scalatest.FunSuite

/**
 * Created by hiral on 4/1/16.
 */
class BaseUTCTimeProviderTest extends FunSuite {
  val baseUTCTimeProvider = new BaseUTCTimeProvider {}

  test("Case: Timezone: UTC - datetime between should pass through") {
    val timezone = Option("UTC")
    val localDayFilter = new DateTimeBetweenFilter("Day", "2016-03-07T00:00:00.000Z", "2016-03-10T00:00:00.000Z", DateTimeBetweenFilterHelper.iso8601FormatString)
    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, None, None, timezone, true).asInstanceOf[Tuple3[DateTimeBetweenFilter, Option[Filter], Option[Filter]]]

    assertEquals("2016-03-07T00:00:00.000Z", DateTimeBetweenFilterHelper.iso8601FormattedString(utcDayFilter.fromDateTime))
    assertEquals("2016-03-10T00:00:00.000Z", DateTimeBetweenFilterHelper.iso8601FormattedString(utcDayFilter.toDateTime))
    assertFalse(utcHourFilter.isDefined)
    assertFalse(utcMinuteFilter.isDefined)
  }
  test("Case: Timezone: not provided - datetime between filter - one day before and one day after") {
    val timezone = None
    val localDayFilter = new DateTimeBetweenFilter("Day", "2016-03-07T00:00:00.000Z", "2016-03-10T00:00:00.000Z", DateTimeBetweenFilterHelper.iso8601FormatString)
    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, None, None, timezone, true).asInstanceOf[Tuple3[DateTimeBetweenFilter, Option[Filter], Option[Filter]]]

    assertEquals("2016-03-06T00:00:00.000Z", DateTimeBetweenFilterHelper.iso8601FormattedString(utcDayFilter.fromDateTime))
    assertEquals("2016-03-11T00:00:00.000Z", DateTimeBetweenFilterHelper.iso8601FormattedString(utcDayFilter.toDateTime))
    assertFalse(utcHourFilter.isDefined)
  }
  test("Case: Timezone: not provided - datetime between with hour provided should give one day before and one day after") {
    val timezone = None
    val localDayFilter = new DateTimeBetweenFilter("Day", "2016-03-07T06:00:00.000Z", "2016-03-10T20:00:00.000Z", DateTimeBetweenFilterHelper.iso8601FormatString)
    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, None, None, timezone, true).asInstanceOf[Tuple3[DateTimeBetweenFilter, Option[BetweenFilter], Option[Filter]]]

    assertEquals("2016-03-06T06:00:00.000Z", DateTimeBetweenFilterHelper.iso8601FormattedString(utcDayFilter.fromDateTime))
    assertEquals("2016-03-11T20:00:00.000Z", DateTimeBetweenFilterHelper.iso8601FormattedString(utcDayFilter.toDateTime))
    assertFalse(utcHourFilter.isDefined)
    assertFalse(utcMinuteFilter.isDefined)
  }
  test("Case: Timezone: not provided - datetime between with hour and minute provided should give one day before and one day after") {
    val timezone = None
    val localDayFilter = new DateTimeBetweenFilter("Day", "2016-03-07T06:30:00.000Z", "2016-03-10T20:15:00.000Z", DateTimeBetweenFilterHelper.iso8601FormatString)
    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, None, None, timezone, true).asInstanceOf[Tuple3[DateTimeBetweenFilter, Option[BetweenFilter], Option[BetweenFilter]]]

    assertEquals("2016-03-06T06:30:00.000Z", DateTimeBetweenFilterHelper.iso8601FormattedString(utcDayFilter.fromDateTime))
    assertEquals("2016-03-11T20:15:00.000Z", DateTimeBetweenFilterHelper.iso8601FormattedString(utcDayFilter.toDateTime))
    assertFalse(utcHourFilter.isDefined)
    assertFalse(utcMinuteFilter.isDefined)
  }
  test("Case: Timezone: UTC - should pass through") {
    val timezone = Option("UTC")
    val localDayFilter = new BetweenFilter("Day", "2016-03-07", "2016-03-10")
    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, None, None, timezone, true).asInstanceOf[Tuple3[BetweenFilter, Option[Filter], Option[Filter]]]

    assertEquals("2016-03-07", utcDayFilter.from)
    assertEquals("2016-03-10", utcDayFilter.to)
    assertFalse(utcHourFilter.isDefined)
    assertFalse(utcMinuteFilter.isDefined)
  }
  test("Case: Timezone: not provided - between filter - one day before and one day after") {
    val timezone = None
    val localDayFilter = new BetweenFilter("Day", "2016-03-07", "2016-03-10")
    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, None, None, timezone, true).asInstanceOf[Tuple3[BetweenFilter, Option[Filter], Option[Filter]]]

    assertEquals("2016-03-06", utcDayFilter.from)
    assertEquals("2016-03-11", utcDayFilter.to)
    assertFalse(utcHourFilter.isDefined)
  }
  test("Case: Timezone: not provided - between with hour filter should not give one day before and one day after") {
    val timezone = None
    val localDayFilter = new BetweenFilter("Day", "2016-03-07", "2016-03-10")
    val localHourFilter = new BetweenFilter("Hour", "01", "05")
    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, Some(localHourFilter), None, timezone, true).asInstanceOf[Tuple3[BetweenFilter, Option[BetweenFilter], Option[Filter]]]

    assertEquals("2016-03-07", utcDayFilter.from)
    assertEquals("2016-03-10", utcDayFilter.to)
    assertEquals("01", utcHourFilter.get.from)
    assertEquals("05", utcHourFilter.get.to)
    assertFalse(utcMinuteFilter.isDefined)
  }
  test("Case: Timezone: not provided - between with hour and minute filter should not give one day before and one day after") {
    val timezone = None
    val localDayFilter = new BetweenFilter("Day", "2016-03-07", "2016-03-10")
    val localHourFilter = new BetweenFilter("Hour", "01", "05")
    val localMinuteFilter = new BetweenFilter("Minute", "10", "20")
    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, Some(localHourFilter), Some(localMinuteFilter), timezone, true).asInstanceOf[Tuple3[BetweenFilter, Option[BetweenFilter], Option[BetweenFilter]]]

    assertEquals("2016-03-07", utcDayFilter.from)
    assertEquals("2016-03-10", utcDayFilter.to)
    assertEquals("01", utcHourFilter.get.from)
    assertEquals("05", utcHourFilter.get.to)
    assertEquals("10", utcMinuteFilter.get.from)
    assertEquals("20", utcMinuteFilter.get.to)
  }
  test("Case: Timezone: not provided - in filter - one day before and one day after") {
    val timezone = None
    val localDayFilter = new InFilter("Day", List("2016-03-07", "2016-03-10", "2016-03-12"))
    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, None, None,  timezone, true).asInstanceOf[Tuple3[InFilter, Option[Filter], Option[Filter]]]

    assert(utcDayFilter.values.contains("2016-03-06"))
    assert(utcDayFilter.values.contains("2016-03-07"))
    assert(utcDayFilter.values.contains("2016-03-08"))
    assert(utcDayFilter.values.contains("2016-03-09"))
    assert(utcDayFilter.values.contains("2016-03-10"))
    assert(utcDayFilter.values.contains("2016-03-11"))
    assert(utcDayFilter.values.contains("2016-03-12"))
    assert(utcDayFilter.values.contains("2016-03-13"))
    assertFalse(utcHourFilter.isDefined)
  }
  test("Case: Timezone: AU, Day - equal, Hour - non-zero") {
    val timezone = Option("Australia/Melbourne")
    val localDayFilter = new EqualityFilter("Day", "2016-03-07")
    val localHourFilter = new EqualityFilter("Hour", "05")

    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, Some(localHourFilter), None,  timezone, true).asInstanceOf[Tuple3[EqualityFilter, Option[EqualityFilter], Option[EqualityFilter]]]

    assertEquals("2016-03-06", utcDayFilter.value)
    val expected = if(getOffsetHours("Australia/Melbourne") == 10) "19" else "18";
    assertEquals(expected, utcHourFilter.get.value)
  }

  test("Case: Timezone: AU, Day - In, Hour - non-zero") {
    val timezone = Option("Australia/Melbourne")
    val localDayFilter = new InFilter("Day", List("2016-03-07", "2016-03-08"))
    val localHourFilter = new InFilter("Hour", List("05", "05"))

    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, Some(localHourFilter), None,  timezone, true).asInstanceOf[Tuple3[InFilter, Option[InFilter], Option[InFilter]]]

    assertEquals("2016-03-06,2016-03-07,2016-03-08", utcDayFilter.asValues)
    val expected = if(getOffsetHours("Australia/Melbourne") == 10) "19,19" else "18,18";
    assertEquals(expected, utcHourFilter.get.asValues)
  }

  test("Case: Timezone: AU, Day - NotIn, Hour - non-zero (intentional exception)") {
    val thrown = intercept[IllegalArgumentException] {
      val timezone = Option("Australia/Melbourne")
      val localDayFilter = new NotInFilter("Day", List("2016-03-07", "2016-03-08"))
      //val localHourFilter = new NotInFilter("Hour", List("05", "05"))

      val (utcDayFilter, utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, None, None, timezone, true).asInstanceOf[Tuple3[EqualityFilter, Option[EqualityFilter], Option[EqualityFilter]]]

    }
    assert(thrown.getMessage.startsWith("Filter operation not supported. Day filter can be"), "Unsupported operations should throw properly.")
  }

  test("Case: Timezone: AU, Day - equal, Hour - non-zero, Minute - non-zero") {
    val timezone = Option("Australia/Melbourne")
    val localDayFilter = new EqualityFilter("Day", "2016-03-07")
    val localHourFilter = new EqualityFilter("Hour", "05")
    val localMinuteFilter = new EqualityFilter("Minute", "15")

    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, Some(localHourFilter), Some(localMinuteFilter), timezone, true).asInstanceOf[Tuple3[EqualityFilter, Option[EqualityFilter], Option[EqualityFilter]]]

    assertEquals("2016-03-06", utcDayFilter.value)
    val expected = if(getOffsetHours("Australia/Melbourne") == 10) "19" else "18";
    assertEquals(expected, utcHourFilter.get.value)
    assertEquals("15", utcMinuteFilter.get.value)
  }

  test("Case: Timezone: AU, Day - between, Hour - between, Minute - between") {
    val timezone = Option("Australia/Melbourne")
    val localDayFilter = new BetweenFilter("Day", "2016-03-07", "2016-03-10")
    val localHourFilter = new BetweenFilter("Hour", "01", "05")
    val localMinuteFilter = new BetweenFilter("Minute", "00", "20")

    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, Some(localHourFilter), Some(localMinuteFilter), timezone, true).asInstanceOf[Tuple3[BetweenFilter, Option[BetweenFilter], Option[BetweenFilter]]]
    val (from, to) = if (!isNotDST(timezone.get)) ("15", "19") else ("14", "18")

    assertEquals("2016-03-06", utcDayFilter.from)
    assertEquals("2016-03-09", utcDayFilter.to)
    assertEquals(from, utcHourFilter.get.from)
    assertEquals(to, utcHourFilter.get.to)
    assertEquals("00", utcMinuteFilter.get.from)
    assertEquals("20", utcMinuteFilter.get.to)
  }

  test("Case: Timezone: AU, datetime between with day, hour, minute with UTC input") {
    val timezone = Option("Australia/Melbourne")
    val localDayFilter = new DateTimeBetweenFilter("Day", "2016-03-07T01:00:00.000Z", "2016-03-10T05:20:00.000Z", DateTimeBetweenFilterHelper.iso8601FormatString)

    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider
      .getUTCDayHourMinuteFilter(localDayFilter, None, None, timezone, true)
      .asInstanceOf[Tuple3[DateTimeBetweenFilter, Option[DateTimeBetweenFilter], Option[DateTimeBetweenFilter]]]

    assertEquals("2016-03-06T14:00:00.000Z", DateTimeBetweenFilterHelper.iso8601FormattedString(utcDayFilter.fromDateTime))
    assertEquals("2016-03-09T18:20:00.000Z", DateTimeBetweenFilterHelper.iso8601FormattedString(utcDayFilter.toDateTime))
  }

  test("Case: Timezone: AU, datetime between with day, hour, minute with non-UTC input") {
    val timezone = Option("Australia/Melbourne")
    //the input is in +1100 which gets converted to UTC
    val localDayFilter = new DateTimeBetweenFilter("Day", "2016-03-07T01:00:00.000+1100", "2016-03-10T05:20:00.000+1100", DateTimeBetweenFilterHelper.iso8601FormatString)

    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider
      .getUTCDayHourMinuteFilter(localDayFilter, None, None, timezone, true)
      .asInstanceOf[Tuple3[DateTimeBetweenFilter, Option[DateTimeBetweenFilter], Option[DateTimeBetweenFilter]]]

    //Since input is +1100 we convert to UTC then since we pass customer timezone as Australia/Melbourne, it takes the UTC time
    //and applies that timezone without conversion, then then converts that to UTC, the result is -22 hours (double applied)
    assertEquals("2016-03-06T03:00:00.000Z", DateTimeBetweenFilterHelper.iso8601FormattedString(utcDayFilter.fromDateTime))
    assertEquals("2016-03-09T07:20:00.000Z", DateTimeBetweenFilterHelper.iso8601FormattedString(utcDayFilter.toDateTime))
  }

  test("Case: Timezone: UTC, Day - between, Hour - between, Minute - between") {
    val timezone = Option("UTC")
    val localDayFilter = new BetweenFilter("Day", "2016-03-07", "2016-03-10")
    val localHourFilter = new BetweenFilter("Hour", "01", "05")
    val localMinuteFilter = new BetweenFilter("Minute", "00", "20")

    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, Some(localHourFilter), Some(localMinuteFilter), timezone, true).asInstanceOf[Tuple3[BetweenFilter, Option[BetweenFilter], Option[BetweenFilter]]]

    assertEquals("2016-03-07", utcDayFilter.from)
    assertEquals("2016-03-10", utcDayFilter.to)
    assertEquals("01", utcHourFilter.get.from)
    assertEquals("05", utcHourFilter.get.to)
    assertEquals("00", utcMinuteFilter.get.from)
    assertEquals("20", utcMinuteFilter.get.to)
  }

  test("Case: Timezone: UTC, datetime between with day, hour, minute with UTC input") {
    val timezone = Option("UTC")
    val localDayFilter = new DateTimeBetweenFilter("Day", "2016-03-07T01:00:00.000Z", "2016-03-10T05:20:00.000Z", DateTimeBetweenFilterHelper.iso8601FormatString)

    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider
      .getUTCDayHourMinuteFilter(localDayFilter, None, None, timezone, true)
      .asInstanceOf[Tuple3[DateTimeBetweenFilter, Option[DateTimeBetweenFilter], Option[DateTimeBetweenFilter]]]

    assertEquals("2016-03-07T01:00:00.000Z", DateTimeBetweenFilterHelper.iso8601FormattedString(utcDayFilter.fromDateTime))
    assertEquals("2016-03-10T05:20:00.000Z", DateTimeBetweenFilterHelper.iso8601FormattedString(utcDayFilter.toDateTime))
  }

  test("Case: Timezone: UTC, datetime between with day, hour, minute with non-UTC input") {
    val timezone = Option("UTC")
    //the input is in +1100 which gets converted to UTC
    val localDayFilter = new DateTimeBetweenFilter("Day", "2016-03-07T01:00:00.000+1100", "2016-03-10T05:20:00.000+1100", DateTimeBetweenFilterHelper.iso8601FormatString)

    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider
      .getUTCDayHourMinuteFilter(localDayFilter, None, None, timezone, true)
      .asInstanceOf[Tuple3[DateTimeBetweenFilter, Option[DateTimeBetweenFilter], Option[DateTimeBetweenFilter]]]

    //Since input is +1100 we convert to UTC then since we pass customer timezone as UTC, it takes the UTC time
    //and applies that timezone without conversion, then then converts that to UTC, the result is -11 hours
    assertEquals("2016-03-06T14:00:00.000Z", DateTimeBetweenFilterHelper.iso8601FormattedString(utcDayFilter.fromDateTime))
    assertEquals("2016-03-09T18:20:00.000Z", DateTimeBetweenFilterHelper.iso8601FormattedString(utcDayFilter.toDateTime))
  }

  test("Case: Timezone: not provided, Day - between, Hour - between, Minute - between") {
    val timezone = Option("UTC")
    val localDayFilter = new BetweenFilter("Day", "2016-03-07", "2016-03-10")
    val localHourFilter = new BetweenFilter("Hour", "01", "05")
    val localMinuteFilter = new BetweenFilter("Minute", "00", "20")

    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, Some(localHourFilter), Some(localMinuteFilter), timezone, true).asInstanceOf[Tuple3[BetweenFilter, Option[BetweenFilter], Option[BetweenFilter]]]

    assertEquals("2016-03-07", utcDayFilter.from)
    assertEquals("2016-03-10", utcDayFilter.to)
    assertEquals("01", utcHourFilter.get.from)
    assertEquals("05", utcHourFilter.get.to)
    assertEquals("00", utcMinuteFilter.get.from)
    assertEquals("20", utcMinuteFilter.get.to)
  }

  test("Case: Timezone: PST, Day - between, Hour - not-specified") {
    val timezone = Option("America/Los_Angeles")
    val localDayFilter = new BetweenFilter("Day", "2016-03-07", "2016-03-10")

    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, None, None,  timezone, true).asInstanceOf[Tuple3[BetweenFilter, Option[Filter], Option[Filter]]]

    assertEquals("2016-03-07", utcDayFilter.from)
    assertEquals("2016-03-11", utcDayFilter.to)
    assertFalse(utcHourFilter.isDefined)
  }

  test("Case: Timezone: PST, Day - between, Hour - not-specified # 2") {
    val timezone = Option("America/Los_Angeles")
    val localDayFilter = new BetweenFilter("Day", "2016-06-18", "2016-06-20")

    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, None, None,  timezone, true).asInstanceOf[Tuple3[BetweenFilter, Option[Filter], Option[Filter]]]

    assertEquals("2016-06-18", utcDayFilter.from)
    assertEquals("2016-06-21", utcDayFilter.to)
    assertFalse(utcHourFilter.isDefined)
  }

  test("Case: Timezone: AU, Day - between, Hour - not-specified") {
    val timezone = Option("Australia/Melbourne")
    val localDayFilter = new BetweenFilter("Day", "2016-03-07", "2016-03-10")

    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, None, None, timezone, true).asInstanceOf[Tuple3[BetweenFilter, Option[Filter], Option[Filter]]]

    assertEquals("2016-03-06", utcDayFilter.from)
    assertEquals("2016-03-10", utcDayFilter.to)
    assertFalse(utcHourFilter.isDefined)
  }

  test("Case: Timezone: PST, Day - between, Hour - zero") {
    val timezone = Option("America/Los_Angeles")
    val localDayFilter = new BetweenFilter("Day", "2016-03-07", "2016-03-10")
    val localHourFilter = new EqualityFilter("Hour", "00")

    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, Some(localHourFilter), None,  timezone, true).asInstanceOf[Tuple3[BetweenFilter, Option[EqualityFilter], Option[EqualityFilter]]]

    assertEquals("2016-03-07", utcDayFilter.from)
    assertEquals("2016-03-10", utcDayFilter.to)

    assertEquals("00", utcHourFilter.get.value)
  }


  test("Case: Timezone: PST, Day - between, Hour - non-zero") {
    val timezone = Option("America/Los_Angeles")
    val localDayFilter = new BetweenFilter("Day", "2016-03-07", "2016-03-10")
    val localHourFilter = new EqualityFilter("Hour", "20")

    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, Some(localHourFilter), None, timezone, true).asInstanceOf[Tuple3[BetweenFilter, Option[EqualityFilter], Option[EqualityFilter]]]

    assertEquals("2016-03-07", utcDayFilter.from)
    assertEquals("2016-03-10", utcDayFilter.to)

    assertEquals("20", utcHourFilter.get.value)
  }

  test("Case: Timezone: India, Day - between, Hour - non-zero") {
    val timezone = Option("Asia/Calcutta")
    val localDayFilter = new BetweenFilter("Day", "2016-03-07", "2016-03-10")
    val localHourFilter = new EqualityFilter("Hour", "02")

    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, Some(localHourFilter), None, timezone, true).asInstanceOf[Tuple3[BetweenFilter, Option[EqualityFilter], Option[EqualityFilter]]]

    assertEquals("2016-03-07", utcDayFilter.from)
    assertEquals("2016-03-10", utcDayFilter.to)
    assertEquals("02", utcHourFilter.get.value)
  }

  test("Case: Timezone: AU, Day - between, Hour - non-zero") {
    val timezone = Option("Australia/Melbourne")
    val localDayFilter = new BetweenFilter("Day", "2016-03-07", "2016-03-10")
    val localHourFilter = new EqualityFilter("Hour", "03")

    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, Some(localHourFilter), None,  timezone, true).asInstanceOf[Tuple3[BetweenFilter, Option[EqualityFilter], Option[EqualityFilter]]]

    assertEquals("2016-03-07", utcDayFilter.from)
    assertEquals("2016-03-10", utcDayFilter.to)
    assertEquals("03", utcHourFilter.get.value)
  }


  test("Case: Timezone: AU, Day - equalTo, Hour - between, Result - prev-and-same day") {
    val timezone = Option("Australia/Melbourne")
    val localDayFilter = new EqualityFilter("Day", "2016-03-07")
    val fromHour = "05"
    val toHour = "20"
    val localHourFilter = new BetweenFilter("Hour", fromHour, toHour)

    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, Some(localHourFilter), None, timezone, true).asInstanceOf[Tuple3[EqualityFilter, Option[BetweenFilter], Option[BetweenFilter]]]

    assertEquals("2016-03-07", utcDayFilter.value)
    //assertEquals("2016-03-08", utcDayFilter.to)

    assertEquals("05", utcHourFilter.get.from)
    assertEquals("20", utcHourFilter.get.to)
  }


  test("Case: Timezone: PST, Day - equalTo, Hour - between, Result - same-and-next day") {
    val timezone = Option("America/Los_Angeles")
    val localDayFilter = new EqualityFilter("Day", "2016-03-07")
    val fromHour = "05"
    val toHour = "20"
    val localHourFilter = new BetweenFilter("Hour", fromHour, toHour)

    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, Some(localHourFilter), None,  timezone, true).asInstanceOf[Tuple3[EqualityFilter, Option[BetweenFilter], Option[BetweenFilter]]]

    assertEquals("2016-03-07", utcDayFilter.value)
    //assertEquals("2016-03-08", utcDayFilter.to)

    assertEquals("05", utcHourFilter.get.from)
    assertEquals("20", utcHourFilter.get.to)
  }


  test("Case: Timezone: AU, Day - equalTo, Hour - between, Result - prev day") {
    val timezone = Option("Australia/Melbourne")
    val localDayFilter = new EqualityFilter("Day", "2016-03-07")
    val fromHour = "03"
    val toHour = "05"
    val localHourFilter = new BetweenFilter("Hour", fromHour, toHour)

    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, Some(localHourFilter), None, timezone, true).asInstanceOf[Tuple3[EqualityFilter, Option[BetweenFilter], Option[BetweenFilter]]]

    assertEquals("2016-03-07", utcDayFilter.value)

    assertEquals("03", utcHourFilter.get.from)
    assertEquals("05", utcHourFilter.get.to)
  }


  test("Case: Timezone: PST, Day - equalTo, Hour - between, Result - next day") {
    val timezone = Option("America/Los_Angeles")
    val localDayFilter = new EqualityFilter("Day", "2016-03-07")
    val fromHour = "19"
    val toHour = "20"
    val localHourFilter = new BetweenFilter("Hour", fromHour, toHour)

    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, Some(localHourFilter), None,  timezone, true).asInstanceOf[Tuple3[EqualityFilter, Option[BetweenFilter], Option[BetweenFilter]]]

    assertEquals("2016-03-07", utcDayFilter.value)

    assertEquals("19", utcHourFilter.get.from)
    assertEquals("20", utcHourFilter.get.to)
  }


  test("Case: Timezone: PST, Day - equalTo, Hour - between, Result - next day zero hour") {
    val timezone = Option("America/Los_Angeles")
    val localDayFilter = new EqualityFilter("Day", "2016-03-07")
    val fromHour = "17"
    val toHour = "20"
    val localHourFilter = new BetweenFilter("Hour", fromHour, toHour)

    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, Some(localHourFilter), None, timezone, true).asInstanceOf[Tuple3[EqualityFilter, Option[BetweenFilter], Option[BetweenFilter]]]

    assertEquals("2016-03-07", utcDayFilter.value)

    assertEquals("17", utcHourFilter.get.from)
    assertEquals("20", utcHourFilter.get.to)
  }


  test("Case: Timezone: AU, Day - between, Hour - in") {
    val timezone = Option("Australia/Melbourne")
    val curOffsetHours = if (!isNotDST(timezone.get)) 10 else 11
    val localDayFilter = new BetweenFilter("Day", "2016-03-07", "2016-03-10")
    val localHourFilter = new InFilter("Hour", List("02", "06", "09"))
    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, Some(localHourFilter), None, timezone, true).asInstanceOf[Tuple3[BetweenFilter, Option[InFilter], Option[BetweenFilter]]]

    assertEquals("2016-03-07", utcDayFilter.from)
    assertEquals("2016-03-10", utcDayFilter.to)

    val offsetHours = getOffsetHours("Australia/Melbourne")
    
    val expectedHours = if (offsetHours == curOffsetHours) List("02", "06", "09") else List("15", "19", "22");
    expectedHours.foreach { hour => assertTrue(utcHourFilter.get.values.contains(hour)) }
  }


  test("Case: Timezone: PST, Day - in, Hour - between") {
    val timezone = Option("America/Los_Angeles")
    val localDayFilter = new InFilter("Day", List("2016-03-07", "2016-03-10", "2016-03-12"))
    val localHourFilter = new BetweenFilter("Hour", "02", "15")
    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, Some(localHourFilter),  None, timezone, true).asInstanceOf[Tuple3[InFilter, Option[BetweenFilter], Option[BetweenFilter]]]
    val expectedDates = List("2016-03-07", "2016-03-10", "2016-03-12")
    
    expectedDates.foreach { date => assertTrue(utcDayFilter.values.contains(date)) }
  }

  test("Case: Publisher Timezone: AU, Day - between, Hour - non-zero") {
    val timezone = Option("Australia/Melbourne")
    val localDayFilter = new BetweenFilter("Day", "2016-03-07", "2016-03-10")
    val localHourFilter = new EqualityFilter("Hour", "03")

    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, Some(localHourFilter), None, timezone, true).asInstanceOf[Tuple3[BetweenFilter, Option[EqualityFilter], Option[BetweenFilter]]]

    assertEquals("2016-03-07", utcDayFilter.from)
    assertEquals("2016-03-10", utcDayFilter.to)

    val expected = if (getOffsetHours("Australia/Melbourne") == 10) "17" else "16";
    assertEquals("03", utcHourFilter.get.value)
  }

  test("Case: Shift UtcDay forward by 1 day") {
    val timezone = Option("America/Los_Angeles")
    val localDayFilter = new BetweenFilter("Day", "2016-03-07", "2016-03-08")
    val hourStr = {
      if (DateTimeZone.forID(timezone.get).isStandardOffset(Instant.now().getMillis))
        "16"
      else
        "17"
    } ;
    val localHourFilter = new BetweenFilter("Hour", hourStr, hourStr)
    val (utcDayFilter,utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, Some(localHourFilter),  None, timezone, true).asInstanceOf[Tuple3[BetweenFilter, Option[BetweenFilter], Option[BetweenFilter]]]
    assertEquals("2016-03-08-2016-03-09", utcDayFilter.asValues)

    assertEquals("00-00", utcHourFilter.get.asValues)
  }

  test("Case: Shift backward by invalid filter") {
    val localDayFilter = new NotInFilter("Day", List("2018-02-02"))
    val thrown = intercept[IllegalArgumentException] {
      baseUTCTimeProvider.shiftDaysBackwardsByOneDay(localDayFilter)
    }
    assert(thrown.getMessage.contains("Filter operation not supported. Day filter can be 'between', 'in' or 'equality' :"))
  }

  test("Case: Extend backwards with Equality filter") {
    val localDayFilter = new EqualityFilter("Day", "2018-02-02")
    val filterVal = baseUTCTimeProvider.extendDaysBackwardsByOneDay(localDayFilter)
    assert(filterVal.operator == BetweenFilterOperation)
  }

  test("Case: Shift forward with Equality filter") {
    val localDayFilter = new EqualityFilter("Day", "2018-02-02")
    val filterVal = baseUTCTimeProvider.shiftDaysForwardByOneDay(localDayFilter)
    assert(filterVal.operator == EqualityFilterOperation)
  }

  test("Case: Shift forward by invalid filter") {
    val localDayFilter = new NotInFilter("Day", List("2018-02-02"))
    val thrown = intercept[IllegalArgumentException] {
      baseUTCTimeProvider.shiftDaysForwardByOneDay(localDayFilter)
    }
    assert(thrown.getMessage.contains("Filter operation not supported. Day filter can be 'between', 'in' or 'equality' :"))
  }

  test("Case: Extend forward with Equality filter") {
    val localDayFilter = new EqualityFilter("Day", "2018-02-02")
    val filterVal = baseUTCTimeProvider.extendDaysForwardByOneDay(localDayFilter)
    assert(filterVal.operator == BetweenFilterOperation)
  }

  test("Case: Extend forward by invalid filter") {
    val localDayFilter = new NotInFilter("Day", List("2018-02-02"))
    val thrown = intercept[IllegalArgumentException] {
      baseUTCTimeProvider.extendDaysForwardByOneDay(localDayFilter)
    }
    assert(thrown.getMessage.contains("Filter operation not supported. Day filter can be 'between', 'in' or 'equality' :"))
  }

  test("Case: getMinAndMaxHours invalid filter") {
    val localDayFilter = new NotInFilter("Day", List("2018-02-02"))
    val thrown = intercept[IllegalArgumentException] {
      baseUTCTimeProvider.getMinAndMaxHours(localDayFilter)
    }
    assert(thrown.getMessage.contains("Filter operation not supported. Hour filter can be 'between', 'in' or 'equality' :"))
  }

  test("Case: updateHours invalid filter") {
    val localDayFilter = new NotInFilter("Day", List("2018-02-02"))
    val thrown = intercept[IllegalArgumentException] {
      baseUTCTimeProvider.updateHours(localDayFilter, 24)
    }
    assert(thrown.getMessage.contains("Filter operation not supported. Hour filter can be 'between', 'in' or 'equality' :"))
  }

  test("Case: applyOffset zero offset") {
    baseUTCTimeProvider.applyOffset("19", 0)
  }

  test("Case: validateFilters minuteFilter test") {
    val localDayFilter = new BetweenFilter("Day", "2016-03-07", "2016-03-10")
    val localMinuteFilter = new BetweenFilter("Minute", "00", "20")
    assert(!baseUTCTimeProvider.validateFilters(localDayFilter, Option.empty, Option(localMinuteFilter)))
  }

  test("Case: validateFilters different hour and minute types test") {
    val localDayFilter = new BetweenFilter("Day", "2016-03-07", "2016-03-10")
    val localHourFilter = new InFilter("Hour", List("16", "16"))
    val localMinuteFilter = new BetweenFilter("Minute", "00", "20")
    assert(!baseUTCTimeProvider.validateFilters(localDayFilter, Option(localHourFilter), Option(localMinuteFilter)))
  }

  test("Case: Invalid Filter Operation (InFilter)") {
    val throwable = intercept[UnsupportedOperationException] {
      val timezone = Option("America/Los_Angeles")
      val localDayFilter = new InFilter("Day", List("2016-03-07", "2016-03-08"))
      val localHourFilter = new InFilter("Hour", List("16", "16"))
      val localMinuteFilter = new InFilter("Minute", List("00", "20"))
      val (utcDayFilter, utcHourFilter, utcMinuteFilter) = baseUTCTimeProvider.getUTCDayHourMinuteFilter(localDayFilter, Some(localHourFilter), Some(localMinuteFilter), timezone, true).asInstanceOf[Tuple3[BetweenFilter, Option[BetweenFilter], Option[BetweenFilter]]]
    }
    assert(throwable.getMessage.startsWith("Unsupported filter type when hour and minute filters present :"), "Must throw exception on unsupported filter op")
  }

  private def getOffsetHours(timezone: String): Int = {
    Math.abs(Math.ceil(DateTimeZone.forID(timezone).getOffset(null) / (1000 * 60 * 60d)).toInt) // offset in hours to be added to local time to get UTC
  }

  private def isNotDST(area: String): Boolean = {
    val zone = DateTimeZone.forID(area)
    val nowDT = new DateTime(zone)
    val isNowDST = !zone.isStandardOffset(nowDT.getMillis)
    isNowDST
  }

}
