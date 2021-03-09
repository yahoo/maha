// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.utils

import com.yahoo.maha.core._
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Created by pranavbhole on 18/04/16.
 */
class DaysUtilsTest extends AnyFunSuite with Matchers {
  test("fake truncated hour, followed by real hour") {
    val catcher = intercept[IllegalArgumentException] {
      val truncated = DaysUtils.truncateHourFromGivenHourString(20171101)
    }
    assert(catcher.getMessage.contains("is too short"), "Illegal dates not caught")
    val new_trunc = DaysUtils.truncateHourFromGivenHourString(2017110123)
    assert(new_trunc == "20171101", "Incorrect truncated date returned")
  }

  test("Attempt to refactor a filter success cases") {
    val betweenFilter = BetweenFilter("Day", "2017-12-01", "2017-12-02")
    val betweenList = DaysUtils.refactorFilter(betweenFilter)
    val inFilter = InFilter("Day", List("2017-12-01", "2017-12-02"))
    val inList = DaysUtils.refactorFilter(inFilter)
    val eqFilter = EqualityFilter("Day", "2017-12-01")
    val eqList = DaysUtils.refactorFilter(eqFilter)
    assert(betweenList == BetweenFilter("Day","20171201","20171202"), "Between Filter refactor failure")
    assert(inList == InFilter("Day", List("20171201", "20171202")), "In Filter refactor failure")
    assert(eqList == EqualityFilter("Day", "20171201"), "Equality Filter refactor failure")
  }

  test("Attempt to refactor a filter failure cases") {
    val betweenCatcher = intercept[Exception] {
      val betweenFilter = BetweenFilter("Day", "2017-12-01a", "2017-12-02")
      val betweenList = DaysUtils.refactorFilter(betweenFilter)
    }
    assert(betweenCatcher.getMessage.contains("is malformed at"), "Illegal dates not caught")

    val inCatcher = intercept[Exception] {
      val inFilter = InFilter("Day", List("2017-12-01a", "2017-12-02"))
      val inList = DaysUtils.refactorFilter(inFilter)
    }
    assert(inCatcher.getMessage.contains("is malformed at"), "Illegal dates not caught")

    val equalityCatcher = intercept[Exception] {
      val equalityFilter = EqualityFilter("Day", "2017-12-01a")
      val equalityList = DaysUtils.refactorFilter(equalityFilter)
    }
    assert(equalityCatcher.getMessage.contains("is malformed at"), "Illegal dates not caught")

    val otherCatcher = intercept[Exception] {
      val notEqFilter = NotEqualToFilter("Day", "2017-12-01a")
      val equalityList = DaysUtils.refactorFilter(notEqFilter)
    }
    assert(otherCatcher.getMessage.contains("Filter operation not supported. Day filter can be"), "Illegal dates not caught")
  }

  test("Invalid from, to") {
    val catcher = intercept[IllegalArgumentException] {
      val dayFilter = BetweenFilter("Day", "2017-12-02", "2017-12-01")
      val list = DaysUtils.getDaysBetweenIntoLongList(dayFilter.from, dayFilter.to)
    }
    assert(catcher.getMessage.contains("cannot be after To date"), "Illegal dates not caught")
  }

  test("Test long list for days") {
    val dayFiltter = BetweenFilter("Day", "2015-12-24", "2016-01-04")
    val list = DaysUtils.getDaysBetweenIntoLongList(dayFiltter.from, dayFiltter.to)
    val expectedList = List(20151224, 20151225, 20151226, 20151227, 20151228, 20151229, 20151230, 20151231, 20160101, 20160102, 20160103, 20160104)
    
    list should be equals expectedList
  }
  test("Get all hours for a day") {
    val list = DaysUtils.getAllHoursOfDay(List(20151224))
    val expectedList = List(2015122400, 2015122401, 2015122402, 2015122403, 2015122404, 2015122405, 2015122406, 2015122407, 2015122408, 2015122409, 2015122410, 2015122411, 2015122412, 2015122413, 2015122414, 2015122415, 2015122416, 2015122417, 2015122418, 2015122419, 2015122420, 2015122421, 2015122422, 2015122423)
    
    expectedList.filter(list.contains(_)) should be equals List.empty
  }

  test("Joda should not skip one hour (2nd hour) as daylight saving starts") {
    val list = DaysUtils.getAllHoursOfDay(List(20170312))
    val expectedList = List(2017031200, 2017031201, 2017031202, 2017031203, 2017031204, 2017031205, 2017031206, 2017031207, 2017031208, 2017031209, 2017031210, 2017031211, 2017031212, 2017031213, 2017031214, 2017031215, 2017031216, 2017031217, 2017031218, 2017031219, 2017031220, 2017031221, 2017031222, 2017031223)
    
    list.foreach {
      l=>
        assert(expectedList.contains(l))
    }
  }

  test("Test long list for days IN Filter") {
    val dayFiltter = InFilter("Day", List("2015-12-24", "2016-01-04"))
    val list = DaysUtils.getDaysIntoLongList(dayFiltter.values)
    val expectedList = List(20151224, 20160104)
    
    list should be equals expectedList
  }

  test("Test long list getDayBetweenFilters") {
    val list = DaysUtils.getDayBetweenFilters("utc_time", List(20151224, 20151225, 20151226, 20151227),OracleEngine)
    
    list should be equals List(BetweenFilter("utc_time","2015-12-24","2015-12-27"))
  }

  test("Test long list getHourBetweenFilters") {
    val list = DaysUtils.getHourBetweenFilters("utc_time", List(2015122401, 2015122402, 2015122405, 2015122406),OracleEngine)
    
    list should be equals List(
      BetweenFilter("utc_time","2015-12-24 01","2015-12-24 02"),
      BetweenFilter("utc_time","2015-12-24 05","2015-12-24 06"))
  }

  test("Test long list getDayBetweenFilters In Hive") {
    val list = DaysUtils.getDayBetweenFilters("utc_time", List(20151224, 20151225, 20151226, 20151227),HiveEngine)
    
    list should be equals List(BetweenFilter("utc_time","2015-12-24","2015-12-27"))
  }

  test("Test long list getDayBetweenFilters In Presto") {
    val list = DaysUtils.getDayBetweenFilters("utc_time", List(20151224, 20151225, 20151226, 20151227),PrestoEngine)
    
    list should be equals List(BetweenFilter("utc_time","2015-12-24","2015-12-27"))
  }

  test("Test long list getDayBetweenFilters In Bigquery") {
    val list = DaysUtils.getDayBetweenFilters("utc_time", List(20151224, 20151225, 20151226, 20151227), BigqueryEngine)

    list should be equals List(BetweenFilter("utc_time","2015-12-24","2015-12-27"))
  }

  test("Test long list getHourBetweenFilters In Hive") {
    val list = DaysUtils.getHourBetweenFilters("utc_time", List(2015122401, 2015122402, 2015122405, 2015122406),HiveEngine)
    
    list should be equals List(
      BetweenFilter("utc_time","2015-12-24 01","2015-12-24 02"),
      BetweenFilter("utc_time","2015-12-24 05","2015-12-24 06"))
  }

  test("Test long list getHourBetweenFilters In Presto") {
    val list = DaysUtils.getHourBetweenFilters("utc_time", List(2015122401, 2015122402, 2015122405, 2015122406),PrestoEngine)
    
    list should be equals List(
      BetweenFilter("utc_time","2015-12-24 01","2015-12-24 02"),
      BetweenFilter("utc_time","2015-12-24 05","2015-12-24 06"))
  }

  test("Test long list getHourBetweenFilters In Bigquery") {
    val list = DaysUtils.getHourBetweenFilters("utc_time", List(2015122401, 2015122402, 2015122405, 2015122406), BigqueryEngine)

    list should be equals List(
      BetweenFilter("utc_time","2015-12-24 01","2015-12-24 02"),
      BetweenFilter("utc_time","2015-12-24 05","2015-12-24 06"))
  }

  test("Test Today UTC") {
    val longUTC = DaysUtils.getUTCToday()
    
    val currentDate = DateTime.now(DateTimeZone.UTC)
    longUTC should be equals currentDate.toString("YYYYMMDD").toLong
  }
  test("Test for Disjoint Set Tuples, IN to BetWeen Filters") {
      val hourList:List[Long] = List(2015122400, 2015122401, 2015122402, 2015122403, 2015122404, 2015122405, 2015122406, 2015122407, 2015122408, 2015122409, 2015122410, 2015122411, 2015122412, 2015122413, 2015122414, 2015122415, 2015122416, 2015122417, 2015122418, 2015122419, 2015122420, 2015122421, 2015122422, 2015122423)
      val tuples = DaysUtils.getDisjointSetTuplesForHours(hourList)
      
      tuples should be equals List((2015122400,2015122423))
  }
  test("getDisjointSetTuplesForDays 3 slots"){
    val dayList:List[Long] = List(20151201,20151202,20151204,20151205,20151206,20151209)
    val dayTuples = DaysUtils.getDisjointSetTuplesForDays(dayList)
    
    dayTuples should be equals List((20151201,20151202), (20151204,20151206), (20151209,20151209))
  }
  test("getDisjointSetTuplesForDays 3 slots with first and last repetition") {
    val dayList:List[Long] = List(20151201,20151201,20151204,20151205,20151206,20151209,20151209)
    val dayTuples = DaysUtils.getDisjointSetTuplesForDays(dayList)
    
    dayTuples should be equals List((20151201,20151201), (20151204,20151206), (20151209,20151209))
  }
  test("getDisjointSetTuplesForDays 4 slots"){
    val dayList:List[Long] = List(20151201,20151204,20151205,20151206,20151209,20151209,20151210)
    val dayTuples = DaysUtils.getDisjointSetTuplesForDays(dayList)
    
    dayTuples should be equals List((20151201,20151201), (20151204,20151206), (20151209,20151209), (20151209,20151210))
  }
  test("getDisjointSetTuplesForDays 1 slots over different months") {
    val dayList:List[Long] = List(20151129,20151130,20151201,20151202,20151203,20151204,20151205)
    val dayTuples = DaysUtils.getDisjointSetTuplesForDays(dayList)
    
    dayTuples should be equals List((20151129,20151205))
  }
  test("getDisjointSetTuplesForDays 1 slots over different years") {
    val dayList:List[Long] = List(20151229,20151230,20151231,20160101,20160102,20160103,20160104)
    val dayTuples = DaysUtils.getDisjointSetTuplesForDays(dayList)
    
    dayTuples should be equals List((20151229,20160104))
  }
  test("getDisjointSetTuplesForDays 2 slots over different years") {
    val dayList:List[Long] = List(20151229,20151230,20151231,20160101,20160103,20160104,20160105)
    val dayTuples = DaysUtils.getDisjointSetTuplesForDays(dayList)
    
    dayTuples should be equals List((20151229,20160101), (20160103,20160105))
  }
  test("getDisjointSetTuplesForDays 1 slots 1 entry") {
    val dayList:List[Long] = List(20160419)
    val dayTuples = DaysUtils.getDisjointSetTuplesForDays(dayList)
    
    dayTuples should be equals List((20160419,20160420))
  }
  test("Get number of days from today") {
    assert(DaysUtils.getNumberOfDaysFromToday("2016-06-15") > 5)
    assert(DaysUtils.getNumberOfDaysFromToday("2015-06-20") > 365)
    assert(DaysUtils.getNumberOfDaysFromToday("2050-06-20") < 0)
  }

  test("Test invalid long list for days IN Filter") {
    intercept[IllegalArgumentException] {
      val dayFilter = InFilter("Day", List("2015-12-24", "20160104abc"))
      DaysUtils.getDaysIntoLongList(dayFilter.values)
    }
  }

  test("Test invalid date string for conversion to long list") {
    intercept[IllegalArgumentException] {
      DaysUtils.getDayIntoLongList("")
    }
  }

  test("getDisjointSetTuples for empty timestamps") {
    assert(DaysUtils.getDisjointSetTuplesForDays(List.empty).equals(List.empty))
    assert(DaysUtils.getDisjointSetTuplesForHours(List.empty).equals(List.empty))
  }

}
