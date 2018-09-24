// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2;

import com.google.common.base.Preconditions;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.Either;
import scala.util.Left;
import scala.util.Right;

import java.util.regex.Pattern;

public class TimeUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimeUtil.class);

    public static final int DELAY_HOURS = 6;
    public static final long MILLISECONDS_IN_A_DAY = 24 * 60 * 60 * 1000;
    public static final DateTimeZone EASTERN = DateTimeZone.forID("EST5EDT");

    public static class Formatters {

        public static DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("YYYYMMdd");
        public static DateTimeFormatter HOUR_DATE_FORMATTER = DateTimeFormat.forPattern("YYYYMMddHH");
        public static DateTimeFormatter SQL_DATE_FORMATTER = DateTimeFormat.forPattern("YYYY-MM-dd");
        public static DateTimeFormatter SQL_TIMESTAMP_FORMATTER = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss");
        public static DateTimeFormatter
            TIMESTAMP_WITH_ZONE_FORMATTER =
            DateTimeFormat.forPattern("YYYY-MM-dd'T'HH:mm:ssZZ");
        public static DateTimeFormatter
            TIMESTAMP_WITHOUT_ZONE_FORMATTER =
            DateTimeFormat.forPattern("YYYY-MM-dd'T'HH:mm:ss");
        public static DateTimeFormatter ISO_DATE_TIME_FORMATTER = ISODateTimeFormat.dateTimeNoMillis();
    }

    public static String ISO_TIMESTAMP = "yyyy-MM-dd'T'HH:mm:ssXXX";
    public static Pattern ISO_TIMESTAMP_HMSTZ_PATTERN = Pattern.compile(".*([\\-\\+]\\d\\d:\\d\\d)");
    public static String SQL_FORMAT = "yyyy-MM-dd HH:mm:ss.S";
    public static String DATE_FORMAT = "YYYY-MM-dd";
    public static String TIMESTAMP_WITH_ZONE_FORMAT = "YYYY-MM-dd'T'HH:mm:ss-ZZ";
    public static String TIMESTAMP_WITH_EASTERN_ZONE_FORMATTER = "YYYY-MM-dd'T'HH:mm:ss.000-04:00";

    /**
     * Convert Long of yyyymmdd to String of yyyy-mm-dd
     *
     * @param date date in yyyymmdd format as long
     * @return date in yyyy-mm-dd format as string
     */
    public static String getSqlDateString(Long date) {
        String yyyymmdd = String.format("%08d", date);
        String yyyy = yyyymmdd.substring(0, 4);
        String mm = yyyymmdd.substring(4, 6);
        String dd = yyyymmdd.substring(6, 8);
        return new StringBuilder().append(yyyy).append("-").append(mm).append("-")
            .append(dd).toString();
    }

    /**
     * Time zone formatter with specific eastern timezone input
     */

    public static DateTime getESTTimeStampfromString(String dateString) {

        DateTimeFormatter
            dateFormatter =
            DateTimeFormat.forPattern(TIMESTAMP_WITH_EASTERN_ZONE_FORMATTER).withZone(EASTERN);
        return dateFormatter.parseDateTime(dateString);

    }

    /**
     * @param time long of yyyymmddhh
     * @return hh as string
     */
    public static String getHourFromLongTime(Long time) {
        String stringTime = time.toString();
        Preconditions.checkArgument(stringTime.length() == 10, "Invalid format %s, expected yyyymmddhh", stringTime);
        String hour = stringTime.substring(8);
        Preconditions.checkArgument(Integer.parseInt(hour) < 24, "Invalid hour %s, expected range 0-23", hour);
        return hour;
    }

    /**
     * @param time long of yyyymmddhh
     * @return yyyy-mm-dd string
     */
    public static String getSqlDateFromLongTime(Long time) {
        String stringTime = time.toString();
        Preconditions.checkArgument(stringTime.length() == 10, "Invalid format %s, expected yyyymmddhh", stringTime);
        String yyyy = stringTime.substring(0, 4);
        String mm = stringTime.substring(4, 6);
        String dd = stringTime.substring(6, 8);
        return new StringBuilder().append(yyyy).append("-").append(mm).append("-").append(dd).toString();
    }

    /**
     * Returns date minus 1 day
     */
    public static DateTime oneDayAgo(DateTime dt) {
        return dt.minusDays(1);
    }

    /**
     * Returns date minus X days but at start of that day
     */
    public static DateTime manyDaysAgo(DateTime dt, int days) {
        return dt.minusDays(days).withTimeAtStartOfDay();
    }

    public static DateTime currentTimeUTC() {
        return (new DateTime(DateTimeZone.UTC));
    }

    public static DateTime currentTime(DateTimeZone tz) {
        return (new DateTime(tz));
    }

    /**
     * Returns date minus 6 hours to indicate the high latency pipeline timestamp
     */
    public static DateTime currentHighLatencyTime(DateTimeZone zone) {
        return new DateTime(zone).minusHours(DELAY_HOURS).minuteOfHour()
            .withMinimumValue().secondOfMinute().withMinimumValue().millisOfSecond()
            .withMinimumValue();
    }

    /**
     * Returns SQL date string YYYY-mm-dd
     */
    public static String getSqlDate(DateTime dt) {
        return Formatters.SQL_DATE_FORMATTER.print(dt);
    }

    /**
     * Returns SQL timestamp string YYYY-mm-dd HH:mm:ss
     */
    public static String getSqlTimestamp(DateTime dt) {
        return Formatters.SQL_TIMESTAMP_FORMATTER.print(dt);
    }

    public static DateTime getDateFromSqlTimestamp(String s) {
        return Formatters.SQL_TIMESTAMP_FORMATTER.parseDateTime(s);
    }

    public static DateTime getDateFromSqlTimestampEST(String s) {
        return Formatters.SQL_TIMESTAMP_FORMATTER.withZone(EASTERN).parseDateTime(s);
    }

    /**
     * Returns timestamp with zone string e.g. 2014-01-01T01:21:30+00:00
     */
    public static String getTimestampWithZone(DateTime dt) {
        if (dt == null) {
            return null;
        }
        return Formatters.TIMESTAMP_WITH_ZONE_FORMATTER.print(dt);
    }

    /**
     * Returns timestamp with zone string e.g. 2014-01-01T01:21:30+00:00
     */
    public static String getTimestampWithZone(String dt) {
        if (dt == null) {
            return null;
        }
        String formattedDate = Formatters.TIMESTAMP_WITH_ZONE_FORMATTER.withZone(EASTERN)
            .print(ISODateTimeFormat.dateTime().parseDateTime(dt));
        return formattedDate;
    }

    public static DateTime currentStartOfMonth(DateTimeZone tz) {
        return getStartOfMonth(currentTime(tz));
    }

    public static DateTime getStartOfMonth(DateTime dt) {
        return dt.dayOfMonth().withMinimumValue().withTimeAtStartOfDay();
    }

    public static int getHours(DateTime dt) {
        return (int) ((dt.getMillis() / 1000) / 3600);
    }

    public static int getDays(DateTime dt) {
        return (getHours(dt) / 24);
    }

    public static DateTime today() {
        return DateTime.now().withZone(DateTimeZone.UTC).withTimeAtStartOfDay();
    }

    public static DateTime yesterday() {
        return DateTime.now().withZone(DateTimeZone.UTC).minusDays(1).withTimeAtStartOfDay();
    }

    public static String getMySqlDateFromTimestamp(String timestamp, DateTimeZone zone) {
        return "date_format(convert_tz(from_unixtime(" + timestamp + "), 'UTC', '" + zone.getID() + "'), '%Y-%m-%d')";
    }

    public static DateTime getESTDate(String dateStr) {
        return Formatters.SQL_DATE_FORMATTER.withZone(EASTERN).parseDateTime(dateStr);
    }

    public static DateTime getESTDateTimestamp(String dateTimestampStr) {
        return Formatters.TIMESTAMP_WITH_ZONE_FORMATTER.withZone(EASTERN).parseDateTime(dateTimestampStr);
    }

    public static boolean isSameDate(DateTime date1, DateTime date2) {
        if (date1 != null && date2 != null
            && date1.getDayOfYear() == date2.getDayOfYear()
            && date1.getYear() == date2.getYear()) {
            return true;
        } else {
            return false;
        }
    }

    public static int getPreviousMonthStartHourId(int currentHourId) {
        return getHours(
            new DateTime((long) currentHourId * 3600 * 1000, TimeUtil.EASTERN).minusMonths(1).withDayOfMonth(1)
                .withTimeAtStartOfDay());
    }

    public static int getCurrentMonthStartHourId(int currentHourId) {
        return getHours(getStartOfMonth(new DateTime((long) currentHourId * 3600 * 1000, TimeUtil.EASTERN)));
    }

    public static DateTime getDateFromHourId(int hourId, DateTimeZone zone) {
        return new DateTime((long) hourId * 3600 * 1000, zone);
    }

    public static long getNextHourTimestampInSec() {
        return ((currentTimeUTC().getMillis() / 3600000L) + 1L) * 3600L;
    }

    public static long getNextMinuteTimestampInSec() {
        return ((currentTimeUTC().getMillis() / 60000L) + 2L) * 60L;
    }

    public static DateTime getBeginingOfDay(DateTime dateTime) {
        return dateTime.withTimeAtStartOfDay();
    }

    public static DateTime getBeginingOfHour(DateTime dateTime) {
        return getBeginingOfMinute(dateTime).withMinuteOfHour(0);
    }

    public static DateTime getBeginingOfMinute(DateTime dateTime) {
        return dateTime.withSecondOfMinute(0).withMillisOfSecond(0);
    }

    /* DATETIME PARSING METHOD */
    public static Either<GeneralError, DateTime> parseDateTime(String input, DateTimeFormatter[] inputFormatters) {
        DateTimeFormatter[] formatters;
        if (inputFormatters != null && inputFormatters.length > 0) {
            formatters = inputFormatters;
            for (DateTimeFormatter formatter : formatters) {
                try {
                    return new Right<>(formatter.parseDateTime(input));
                } catch (Exception e) {
                    // do nothing and try to parse with the next formatter
                }
            }
        }
        return new Left<>(GeneralError.from("validateDateTimeFormat", "Invalid datetime format"));
    }

    public static Either<GeneralError, DateTime> parseDateTimeWithZone(String input, DateTimeZone zone,
                                                                       DateTimeFormatter[] dateTimeFormatter) {

        Either<GeneralError, DateTime> parsedResult = parseDateTime(input, dateTimeFormatter);
        if (parsedResult.isLeft()) {
            return EitherUtils.castLeft(parsedResult);
        } else {
            return new Right<>(new DateTime(parsedResult.right().get().getMillis(), zone));
        }
    }

    public static String getTimestampFromMilliseconds(Long milliseconds) {
        if (milliseconds == null) {
            return null;
        }
        DateTime time = new DateTime(milliseconds);
        return Formatters.TIMESTAMP_WITH_ZONE_FORMATTER.withZone(EASTERN)
            .print(time);
    }
}
