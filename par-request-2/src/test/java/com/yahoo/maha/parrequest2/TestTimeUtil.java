// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Created by hiral on 3/4/14.
 */
public class TestTimeUtil {

    @Test
    public void testCurrentStartOfMonth() {
        DateTime today = TimeUtil.currentTimeUTC();
        DateTime dt = TimeUtil.currentStartOfMonth(DateTimeZone.UTC);
        assertTrue(dt.getMonthOfYear() == today.getMonthOfYear(), dt.toString());
        assertTrue(dt.getDayOfMonth() == 1, dt.toString());
        assertTrue(dt.getHourOfDay() == 0, dt.toString());
        assertTrue(dt.getMinuteOfHour() == 0, dt.toString());
        assertTrue(dt.getSecondOfDay() == 0, dt.toString());
    }

    @Test
    public void testExtractMethods() {
        assertEquals(TimeUtil.getSqlDateString(20140425L), "2014-04-25");
        assertEquals(TimeUtil.getHourFromLongTime(2014042516L), "16");
        assertEquals(TimeUtil.getSqlDateFromLongTime(2014042516L), "2014-04-25");

        DateTime today = DateTime.now();
        String
            expectedDateStr =
            String.format("%04d-%02d-%02d", today.getYear(), today.getMonthOfYear(), today.getDayOfMonth());
        String
            expectedTimeStr =
            String.format("%02d:%02d:%02d", today.getHourOfDay(), today.getMinuteOfHour(), today.getSecondOfMinute());
        assertEquals(TimeUtil.getSqlDate(today), expectedDateStr);
        assertEquals(TimeUtil.getSqlTimestamp(today), expectedDateStr + " " + expectedTimeStr);
        assertEquals(TimeUtil.getDateFromSqlTimestamp(expectedDateStr + " " + expectedTimeStr),
                     today.withMillisOfSecond(0));
    }

    @Test
    public void testDateOperations() {
        DateTime day1 = TimeUtil.getDateFromSqlTimestamp("2014-04-25 00:00:00");
        DateTime day2 = TimeUtil.getDateFromSqlTimestamp("2014-04-24 00:00:00");
        assertEquals(TimeUtil.oneDayAgo(day1), day2);
        assertEquals(TimeUtil.manyDaysAgo(day1, 1), day2);

        assertEquals(TimeUtil.today().getDayOfMonth(), TimeUtil.currentTimeUTC().getDayOfMonth());
        assertEquals(TimeUtil.today().getMonthOfYear(), TimeUtil.currentTimeUTC().getMonthOfYear());
        assertEquals(TimeUtil.today().getYear(), TimeUtil.currentTimeUTC().getYear());
        assertEquals(TimeUtil.yesterday(), TimeUtil.oneDayAgo(TimeUtil.currentTimeUTC().withTimeAtStartOfDay()));
    }

    @Test
    public void testGetTimestampFromMilliseconds() {
        Long value = Long.valueOf(1410037023000L);
        assertEquals(TimeUtil.getTimestampFromMilliseconds(value), "2014-09-06T16:57:03-04:00");
    }
}
