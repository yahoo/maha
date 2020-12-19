// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import com.yahoo.maha.core.dimension.DimCol
import com.yahoo.maha.core.fact.ForceFilter
import org.apache.druid.query.filter.{NotDimFilter, SearchQueryDimFilter}
import org.apache.druid.query.search.InsensitiveContainsSearchQuerySpec
import org.apache.druid.segment.filter.BoundFilter
import org.joda.time.DateTime
import org.json4s.JsonAST.{JArray, JObject, JString, JValue}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.TreeSet

/**
 * Created by jians on 11/10/15.
 */

class FilterTest extends AnyFunSuite with Matchers {

  System.setProperty("user.timezone", "GMT")

  val oracleLiteralMapper = new OracleLiteralMapper
  val postgresLiteralMapper = new PostgresLiteralMapper
  val hiveLiteralMapper = new HiveLiteralMapper
  val bigqueryLiteralMapper = new BigqueryLiteralMapper

  def render[T, O](renderer: FilterRenderer[T, O], filter: T, literalMapper: SqlLiteralMapper, engine: Engine, column: Column, aliasToRenderedSqlMap: Map[String, (String, String)]) : O = {
    renderer.render(aliasToRenderedSqlMap, filter, literalMapper, column, engine, None)
  }

  def renderWithGrain[T, O](renderer: FilterRenderer[T, O], filter: T, literalMapper: SqlLiteralMapper, engine: Engine, column: Column, grain : Grain, aliasToRenderedSqlMap: Map[String, (String, String)]) : O = {
    renderer.render(aliasToRenderedSqlMap, filter, literalMapper, column, engine, Some(grain))
  }
  
  implicit val columnContext: ColumnContext = new ColumnContext
  val col = DimCol("field1", StrType())
  val dateCol = DimCol("stats_date", DateType())
  val timestampCol = DimCol("timestamp_date", TimestampType())
  val intDateCol = DimCol("date_sid", IntType(), annotations = Set(DayColumn("YYYYMMDD")))
  val strDateCol = DimCol("date_sid2", StrType(), annotations = Set(DayColumn("YYYYMMDD")))
  val insensitiveCol = DimCol("insensitive", StrType(), annotations = Set(CaseInsensitive))
  val escapedCol = DimCol("%sfield__1", StrType())
  val escapedIntCol = DimCol("%sfield__2", IntType())

  object EqualityObj {
    def compare(a : Int, b : String) = EqualityObj.baseEq.compare(a, b)
    val baseEq : BaseEquality[Int] = BaseEquality.from[Int] {
      (a,b) =>
        a.compare(b)
    }
  }

  test("BetweenFilter with a defined grain should modify the output result to include it.") {
    val filter = BetweenFilter("stats_date", "2018-01-01", "2018-01-07")
    val dailyResult = renderWithGrain(SqlBetweenFilterRenderer, filter, oracleLiteralMapper, OracleEngine, dateCol, DailyGrain, Map("stats_date" -> ("stats_date", "stats_date"))).filter
    dailyResult shouldBe "stats_date >= trunc(to_date('2018-01-01', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('2018-01-07', 'YYYY-MM-DD'))"
    val hourlyResult = renderWithGrain(SqlBetweenFilterRenderer, filter, oracleLiteralMapper, OracleEngine, dateCol, HourlyGrain, Map("stats_date" -> ("stats_date", "stats_date"))).filter
    hourlyResult shouldBe "stats_date >= to_date('2018-01-01', 'HH24') AND stats_date <= to_date('2018-01-07', 'HH24')"

    val intBetweenfilter = BetweenFilter("date_sid", "2018-01-01", "2018-01-02")
    val intHourlyResult = renderWithGrain(SqlBetweenFilterRenderer, intBetweenfilter, oracleLiteralMapper, OracleEngine, intDateCol, HourlyGrain, Map("date_sid" -> ("date_sid", "date_sid"))).filter
    intHourlyResult shouldBe "date_sid >= to_date('2018-01-01', 'HH24') AND date_sid <= to_date('2018-01-02', 'HH24')"

    val strBetweenfilter = BetweenFilter("date_sid2", "2018-01-01", "2018-01-02")
    val strHourlyResult = renderWithGrain(SqlBetweenFilterRenderer, strBetweenfilter, oracleLiteralMapper, OracleEngine, strDateCol, HourlyGrain, Map("date_sid2" -> ("date_sid2", "date_sid2"))).filter
    strHourlyResult shouldBe "date_sid2 >= to_date('2018-01-01', 'HH24') AND date_sid2 <= to_date('2018-01-02', 'HH24')"
  }

  test("successfully render DateTimeBetweenFilter with daily and hourly grain and date type on oracle engine") {
    val filter = DateTimeBetweenFilter(dateCol.name, "2018-01-01T01:10:50", "2018-01-06T10:20:30", "yyyy-MM-dd'T'HH:mm:ss")
    val dailyResult = renderWithGrain(SqlDateTimeBetweenFilterRenderer, filter, oracleLiteralMapper, OracleEngine, dateCol, DailyGrain, Map("stats_date" -> ("stats_date", "stats_date"))).filter
    dailyResult shouldBe "stats_date >= to_date('2018-01-01', 'YYYY-MM-DD') AND stats_date <= to_date('2018-01-06', 'YYYY-MM-DD')"
    val hourlyResult = renderWithGrain(SqlDateTimeBetweenFilterRenderer, filter, oracleLiteralMapper, OracleEngine, dateCol, HourlyGrain, Map("stats_date" -> ("stats_date", "stats_date"))).filter
    hourlyResult shouldBe "stats_date >= to_date('2018-01-01', 'YYYY-MM-DD') AND stats_date <= to_date('2018-01-06', 'YYYY-MM-DD')"
  }

  test("successfully render DateTimeBetweenFilter with daily and hourly grain and timestamp type on oracle engine") {
    val filter = DateTimeBetweenFilter(timestampCol.name, "2018-01-01T01:10:50", "2018-01-06T10:20:30", "yyyy-MM-dd'T'HH:mm:ss")
    val dailyResult = renderWithGrain(SqlDateTimeBetweenFilterRenderer, filter, oracleLiteralMapper, OracleEngine, timestampCol, DailyGrain, Map("timestamp_date" -> ("timestamp_date", "timestamp_date"))).filter
    dailyResult shouldBe "timestamp_date >= TO_UTC_TIMESTAMP_TZ('2018-01-01T01:10:50.000Z') AND timestamp_date <= TO_UTC_TIMESTAMP_TZ('2018-01-06T10:20:30.000Z')"
    val hourlyResult = renderWithGrain(SqlDateTimeBetweenFilterRenderer, filter, oracleLiteralMapper, OracleEngine, timestampCol, HourlyGrain, Map("timestamp_date" -> ("timestamp_date", "timestamp_date"))).filter
    hourlyResult shouldBe "timestamp_date >= TO_UTC_TIMESTAMP_TZ('2018-01-01T01:10:50.000Z') AND timestamp_date <= TO_UTC_TIMESTAMP_TZ('2018-01-06T10:20:30.000Z')"
  }

  test("successfully render DateTimeBetweenFilter with daily and hourly grain and date type on postgres engine") {
    val filter = DateTimeBetweenFilter(dateCol.name, "2018-01-01T01:10:50", "2018-01-06T10:20:30", "yyyy-MM-dd'T'HH:mm:ss")
    val dailyResult = renderWithGrain(SqlDateTimeBetweenFilterRenderer, filter, postgresLiteralMapper, PostgresEngine, dateCol, DailyGrain, Map("stats_date" -> ("stats_date", "stats_date"))).filter
    dailyResult shouldBe "stats_date >= to_date('2018-01-01', 'YYYY-MM-DD') AND stats_date <= to_date('2018-01-06', 'YYYY-MM-DD')"
    val hourlyResult = renderWithGrain(SqlDateTimeBetweenFilterRenderer, filter, postgresLiteralMapper, PostgresEngine, dateCol, HourlyGrain, Map("stats_date" -> ("stats_date", "stats_date"))).filter
    hourlyResult shouldBe "stats_date >= to_date('2018-01-01', 'YYYY-MM-DD') AND stats_date <= to_date('2018-01-06', 'YYYY-MM-DD')"
  }

  test("successfully render DateTimeBetweenFilter with daily and hourly grain and timestamp type on postgres engine") {
    val filter = DateTimeBetweenFilter(timestampCol.name, "2018-01-01T01:10:50", "2018-01-06T10:20:30", "yyyy-MM-dd'T'HH:mm:ss")
    val dailyResult = renderWithGrain(SqlDateTimeBetweenFilterRenderer, filter, postgresLiteralMapper, PostgresEngine, timestampCol, DailyGrain, Map("timestamp_date" -> ("timestamp_date", "timestamp_date"))).filter
    dailyResult shouldBe "timestamp_date >= '2018-01-01T01:10:50.000Z'::timestamptz AND timestamp_date <= '2018-01-06T10:20:30.000Z'::timestamptz"
    val hourlyResult = renderWithGrain(SqlDateTimeBetweenFilterRenderer, filter, postgresLiteralMapper, PostgresEngine, timestampCol, HourlyGrain, Map("timestamp_date" -> ("timestamp_date", "timestamp_date"))).filter
    hourlyResult shouldBe "timestamp_date >= '2018-01-01T01:10:50.000Z'::timestamptz AND timestamp_date <= '2018-01-06T10:20:30.000Z'::timestamptz"
  }

  test("successfully render DateTimeBetweenFilter with daily and hourly grain and date type on hive engine") {
    val filter = DateTimeBetweenFilter(dateCol.name, "2018-01-01T01:10:50", "2018-01-06T10:20:30", "yyyy-MM-dd'T'HH:mm:ss")
    val dailyResult = renderWithGrain(SqlDateTimeBetweenFilterRenderer, filter, hiveLiteralMapper, HiveEngine, dateCol, DailyGrain, Map("stats_date" -> ("stats_date", "stats_date"))).filter
    dailyResult shouldBe "stats_date >= cast('2018-01-01' as date) AND stats_date <= cast('2018-01-06' as date)"
    val hourlyResult = renderWithGrain(SqlDateTimeBetweenFilterRenderer, filter, hiveLiteralMapper, HiveEngine, dateCol, HourlyGrain, Map("stats_date" -> ("stats_date", "stats_date"))).filter
    hourlyResult shouldBe "stats_date >= cast('2018-01-01' as date) AND stats_date <= cast('2018-01-06' as date)"
  }

  test("successfully render DateTimeBetweenFilter with daily and hourly grain and timestamp type on hive engine") {
    val filter = DateTimeBetweenFilter(timestampCol.name, "2018-01-01T01:10:50", "2018-01-06T10:20:30", "yyyy-MM-dd'T'HH:mm:ss")
    val dailyResult = renderWithGrain(SqlDateTimeBetweenFilterRenderer, filter, hiveLiteralMapper, HiveEngine, timestampCol, DailyGrain, Map("timestamp_date" -> ("timestamp_date", "timestamp_date"))).filter
    dailyResult shouldBe "timestamp_date >= cast('2018-01-01T01:10:50.000' as timestamp) AND timestamp_date <= cast('2018-01-06T10:20:30.000' as timestamp)"
    val hourlyResult = renderWithGrain(SqlDateTimeBetweenFilterRenderer, filter, hiveLiteralMapper, HiveEngine, timestampCol, HourlyGrain, Map("timestamp_date" -> ("timestamp_date", "timestamp_date"))).filter
    hourlyResult shouldBe "timestamp_date >= cast('2018-01-01T01:10:50.000' as timestamp) AND timestamp_date <= cast('2018-01-06T10:20:30.000' as timestamp)"
  }

  test("successfully render DateTimeBetweenFilter with daily and hourly grain and date type on presto engine") {
    val filter = DateTimeBetweenFilter(dateCol.name, "2018-01-01T01:10:50", "2018-01-06T10:20:30", "yyyy-MM-dd'T'HH:mm:ss")
    val dailyResult = renderWithGrain(SqlDateTimeBetweenFilterRenderer, filter, hiveLiteralMapper, PrestoEngine, dateCol, DailyGrain, Map("stats_date" -> ("stats_date", "stats_date"))).filter
    dailyResult shouldBe "stats_date >= date('2018-01-01') AND stats_date <= date('2018-01-06')"
    val hourlyResult = renderWithGrain(SqlDateTimeBetweenFilterRenderer, filter, hiveLiteralMapper, PrestoEngine, dateCol, HourlyGrain, Map("stats_date" -> ("stats_date", "stats_date"))).filter
    hourlyResult shouldBe "stats_date >= date('2018-01-01') AND stats_date <= date('2018-01-06')"
  }

  test("successfully render DateTimeBetweenFilter with daily and hourly grain and timestamp type on presto engine") {
    val filter = DateTimeBetweenFilter(timestampCol.name, "2018-01-01T01:10:50", "2018-01-06T10:20:30", "yyyy-MM-dd'T'HH:mm:ss")
    val dailyResult = renderWithGrain(SqlDateTimeBetweenFilterRenderer, filter, hiveLiteralMapper, PrestoEngine, timestampCol, DailyGrain, Map("timestamp_date" -> ("timestamp_date", "timestamp_date"))).filter
    dailyResult shouldBe "timestamp_date >= from_iso8601_timestamp('2018-01-01T01:10:50.000Z') AND timestamp_date <= from_iso8601_timestamp('2018-01-06T10:20:30.000Z')"
    val hourlyResult = renderWithGrain(SqlDateTimeBetweenFilterRenderer, filter, hiveLiteralMapper, PrestoEngine, timestampCol, HourlyGrain, Map("timestamp_date" -> ("timestamp_date", "timestamp_date"))).filter
    hourlyResult shouldBe "timestamp_date >= from_iso8601_timestamp('2018-01-01T01:10:50.000Z') AND timestamp_date <= from_iso8601_timestamp('2018-01-06T10:20:30.000Z')"
  }

  //we only need to test one engine here since only diff is the timezone input
  test("successfully render DateTimeBetweenFilter with input zone offset, daily and hourly grain and date type on oracle engine") {
    val filter = DateTimeBetweenFilter(dateCol.name, "2018-01-01T01:10:50+08:00", "2018-01-06T10:20:30+08:00", "yyyy-MM-dd'T'HH:mm:ssZZ")
    val dailyResult = renderWithGrain(SqlDateTimeBetweenFilterRenderer, filter, oracleLiteralMapper, OracleEngine, dateCol, DailyGrain, Map("stats_date" -> ("stats_date", "stats_date"))).filter
    dailyResult shouldBe "stats_date >= to_date('2017-12-31', 'YYYY-MM-DD') AND stats_date <= to_date('2018-01-06', 'YYYY-MM-DD')"
    val hourlyResult = renderWithGrain(SqlDateTimeBetweenFilterRenderer, filter, oracleLiteralMapper, OracleEngine, dateCol, HourlyGrain, Map("stats_date" -> ("stats_date", "stats_date"))).filter
    hourlyResult shouldBe "stats_date >= to_date('2017-12-31', 'YYYY-MM-DD') AND stats_date <= to_date('2018-01-06', 'YYYY-MM-DD')"
  }

  //we only need to test one engine here since only diff is the timezone input
  test("successfully render DateTimeBetweenFilter with input zone offset, daily and hourly grain and timestamp type on oracle engine") {
    val filter = DateTimeBetweenFilter(timestampCol.name, "2018-01-01T01:10:50+08:00", "2018-01-06T10:20:30+08:00", "yyyy-MM-dd'T'HH:mm:ssZZ")
    val dailyResult = renderWithGrain(SqlDateTimeBetweenFilterRenderer, filter, oracleLiteralMapper, OracleEngine, timestampCol, DailyGrain, Map("timestamp_date" -> ("timestamp_date", "timestamp_date"))).filter
    dailyResult shouldBe "timestamp_date >= TO_UTC_TIMESTAMP_TZ('2017-12-31T17:10:50.000Z') AND timestamp_date <= TO_UTC_TIMESTAMP_TZ('2018-01-06T02:20:30.000Z')"
    val hourlyResult = renderWithGrain(SqlDateTimeBetweenFilterRenderer, filter, oracleLiteralMapper, OracleEngine, timestampCol, HourlyGrain, Map("timestamp_date" -> ("timestamp_date", "timestamp_date"))).filter
    hourlyResult shouldBe "timestamp_date >= TO_UTC_TIMESTAMP_TZ('2017-12-31T17:10:50.000Z') AND timestamp_date <= TO_UTC_TIMESTAMP_TZ('2018-01-06T02:20:30.000Z')"
  }

  test("successfully render DateTimeBetweenFilter with daily and hourly grain and date type on Bigquery engine") {
    val filter = DateTimeBetweenFilter(dateCol.name, "2018-01-01T01:10:50", "2018-01-06T10:20:30", "yyyy-MM-dd'T'HH:mm:ss")
    val dailyResult = renderWithGrain(SqlDateTimeBetweenFilterRenderer, filter, bigqueryLiteralMapper, BigqueryEngine, dateCol, DailyGrain, Map("stats_date" -> ("stats_date", "stats_date"))).filter
    dailyResult shouldBe "stats_date >= DATE('2018-01-01') AND stats_date <= DATE('2018-01-06')"
    val hourlyResult = renderWithGrain(SqlDateTimeBetweenFilterRenderer, filter, bigqueryLiteralMapper, BigqueryEngine, dateCol, HourlyGrain, Map("stats_date" -> ("stats_date", "stats_date"))).filter
    hourlyResult shouldBe "stats_date >= DATE('2018-01-01') AND stats_date <= DATE('2018-01-06')"
  }

  test("successfully render DateTimeBetweenFilter with daily and hourly grain and timestamp type on Bigquery engine") {
    val filter = DateTimeBetweenFilter(timestampCol.name, "2018-01-01T01:10:50", "2018-01-06T10:20:30", "yyyy-MM-dd'T'HH:mm:ss")
    val dailyResult = renderWithGrain(SqlDateTimeBetweenFilterRenderer, filter, bigqueryLiteralMapper, BigqueryEngine, timestampCol, DailyGrain, Map("timestamp_date" -> ("timestamp_date", "timestamp_date"))).filter
    dailyResult shouldBe "timestamp_date >= TIMESTAMP('2018-01-01T01:10:50.000') AND timestamp_date <= TIMESTAMP('2018-01-06T10:20:30.000')"
    val hourlyResult = renderWithGrain(SqlDateTimeBetweenFilterRenderer, filter, bigqueryLiteralMapper, BigqueryEngine, timestampCol, HourlyGrain, Map("timestamp_date" -> ("timestamp_date", "timestamp_date"))).filter
    hourlyResult shouldBe "timestamp_date >= TIMESTAMP('2018-01-01T01:10:50.000') AND timestamp_date <= TIMESTAMP('2018-01-06T10:20:30.000')"
  }

  test("Filter Types in Druid should return valid MaxDate") {
    val filter = BetweenFilter("stats_date", "2018-01-01", "2018-01-07")
    val eqFilter = EqualityFilter("stats_date", "2018-01-01")
    val inFilter = InFilter("stats_date", List("2018-01-01", "2018-01-07"))
    val maxDate = FilterDruid.getMaxDate(filter, DailyGrain)
    assert(maxDate.getClass == classOf[DateTime])
    val maxIntDate = FilterDruid.getMaxDate(inFilter, DailyGrain)
    assert(maxIntDate.getClass == classOf[DateTime])
    val maxEqDate = FilterDruid.getMaxDate(eqFilter, DailyGrain)
    assert(maxEqDate.getClass == classOf[DateTime])
  }

  test("InFilter should render correct string for Oracle") {
    val filter = InFilter("field1", List("abc", "def", "ghi"))
    render(SqlInFilterRenderer, filter, oracleLiteralMapper, OracleEngine, col, Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 IN ('abc','def','ghi')")
    val insensitiveFilter = InFilter("insensitive", List("abc", "def", "ghi"))
    render(SqlInFilterRenderer, insensitiveFilter, oracleLiteralMapper, OracleEngine, insensitiveCol, Map("insensitive" -> ("insensitive", "insensitive"))) shouldBe DefaultResult("lower(insensitive) IN (lower('abc'),lower('def'),lower('ghi'))")
  }

  test("NotInFilter should render correct string for Oracle") {
    val filter = NotInFilter("field1", List("abc", "def", "ghi"))
    render(SqlNotInFilterRenderer, filter, oracleLiteralMapper, OracleEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 NOT IN ('abc','def','ghi')")
  }

  test("BetweenFilter should render correct string for Oracle") {
    val filter = BetweenFilter("field1", "abc", "def")
    render(SqlBetweenFilterRenderer, filter, oracleLiteralMapper, OracleEngine, col,  Map("field1" -> ("field1", "field1"))).filter shouldBe "field1 >= 'abc' AND field1 <= 'def'"
  }

  test("EqualityFilter should render correct string for Oracle") {
    val filter = EqualityFilter("field1", "ghi")
    render(SqlEqualityFilterRenderer, filter, oracleLiteralMapper, OracleEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 = \'ghi\'")
  }

  test("LikeFilter should render correct string for Oracle") {
    val filter = LikeFilter("field1", "ghi")
    render(SqlLikeFilterRenderer, filter, oracleLiteralMapper, OracleEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 LIKE \'%ghi%\'")
  }

  test("LikeFilter should render correct string for Presto") {
    val filter = LikeFilter("field1", "ghi")
    render(SqlLikeFilterRenderer, filter, hiveLiteralMapper, PrestoEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 LIKE \'%ghi%\'")
  }

  test("NotEqualToFilter should render correct string for Presto") {
    val filter = NotEqualToFilter("field1", "ghi")
    render(SqlNotEqualToFilterRenderer, filter, hiveLiteralMapper, PrestoEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 <> \'ghi\'")
  }

  test("NotEqualToFilter should render correct string for Oracle") {
    val filter = NotEqualToFilter("field1", "ghi")
    render(SqlNotEqualToFilterRenderer, filter, oracleLiteralMapper, OracleEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 <> \'ghi\'")
  }

  test("IsNullFilter should render correct string for Oracle") {
    val filter = IsNullFilter("field1")
    render(SqlIsNullFilterRenderer, filter, oracleLiteralMapper, OracleEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 IS NULL")
  }

  test("IsNotNullFilter should render correct string for Oracle") {
    val filter = IsNotNullFilter("field1")
    render(SqlIsNotNullFilterRenderer, filter, oracleLiteralMapper, OracleEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 IS NOT NULL")
  }

  test("IsNullFilter should render correct string for Presto") {
    val filter = IsNullFilter("field1")
    render(SqlIsNullFilterRenderer, filter, hiveLiteralMapper, PrestoEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 IS NULL")
  }

  test("IsNotNullFilter should render correct string for Presto") {
    val filter = IsNotNullFilter("field1")
    render(SqlIsNotNullFilterRenderer, filter, hiveLiteralMapper, PrestoEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 IS NOT NULL")
  }

  test("InFilter should render correct string for Hive") {
    val filter = InFilter("field1", List("abc", "def", "ghi"))
    render(SqlInFilterRenderer, filter, hiveLiteralMapper, HiveEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 IN ('abc','def','ghi')")
    val insensitiveFilter = InFilter("insensitive", List("abc", "def", "ghi"))
    render(SqlInFilterRenderer, insensitiveFilter, oracleLiteralMapper, HiveEngine, insensitiveCol,  Map("insensitive" -> ("insensitive", "insensitive"))) shouldBe DefaultResult("lower(insensitive) IN (lower('abc'),lower('def'),lower('ghi'))")
  }

  test("NotInFilter should render correct string for Hive") {
    val filter = NotInFilter("field1", List("abc", "def", "ghi"))
    render(SqlNotInFilterRenderer, filter, hiveLiteralMapper, HiveEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 NOT IN ('abc','def','ghi')")
  }

  test("BetweenFilter should render correct string for Hive") {
    val filter = BetweenFilter("field1", "abc", "def")
    render(SqlBetweenFilterRenderer, filter, hiveLiteralMapper, HiveEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 >= 'abc' AND field1 <= 'def'")
  }

  test("EqualityFilter should render correct string for Hive") {
    val filter = EqualityFilter("field1", "ghi")
    render(SqlEqualityFilterRenderer, filter, hiveLiteralMapper, HiveEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 = \'ghi\'")
  }

  test("LikeFilter should render correct string for Hive") {
    val filter = LikeFilter("field1", "ghi")
    render(SqlLikeFilterRenderer, filter, hiveLiteralMapper, HiveEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 LIKE \'%ghi%\'")
  }

  test("NotEqualToFilter should render correct string for Hive") {
    val filter = NotEqualToFilter("field1", "ghi")
    render(SqlNotEqualToFilterRenderer, filter, hiveLiteralMapper, HiveEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 <> \'ghi\'")
  }

  test("IsNullFilter should render correct string for Hive") {
    val filter = IsNullFilter("field1")
    render(SqlIsNullFilterRenderer, filter, hiveLiteralMapper, HiveEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 IS NULL")
  }

  test("IsNotNullFilter should render correct string for Hive") {
    val filter = IsNotNullFilter("field1")
    render(SqlIsNotNullFilterRenderer, filter, hiveLiteralMapper, HiveEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 IS NOT NULL")
  }

  test("InFilter should render correct string for Bigquery") {
    val filter = InFilter("field1", List("abc", "def", "ghi"))
    render(SqlInFilterRenderer, filter, bigqueryLiteralMapper, BigqueryEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 IN ('abc','def','ghi')")
    val insensitiveFilter = InFilter("insensitive", List("abc", "def", "ghi"))
    render(SqlInFilterRenderer, insensitiveFilter, bigqueryLiteralMapper, BigqueryEngine, insensitiveCol,  Map("insensitive" -> ("insensitive", "insensitive"))) shouldBe DefaultResult("lower(insensitive) IN (lower('abc'),lower('def'),lower('ghi'))")
  }

  test("NotInFilter should render correct string for Bigquery") {
    val filter = NotInFilter("field1", List("abc", "def", "ghi"))
    render(SqlNotInFilterRenderer, filter, bigqueryLiteralMapper, BigqueryEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 NOT IN ('abc','def','ghi')")
  }

  test("BetweenFilter should render correct string for Bigquery") {
    val filter = BetweenFilter("field1", "abc", "def")
    render(SqlBetweenFilterRenderer, filter, bigqueryLiteralMapper, BigqueryEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 >= 'abc' AND field1 <= 'def'")
  }

  test("EqualityFilter should render correct string for Bigquery") {
    val filter = EqualityFilter("field1", "ghi")
    render(SqlEqualityFilterRenderer, filter, bigqueryLiteralMapper, BigqueryEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 = \'ghi\'")
  }

  test("LikeFilter should render correct string for Bigquery") {
    val filter = LikeFilter("field1", "ghi")
    render(SqlLikeFilterRenderer, filter, bigqueryLiteralMapper, BigqueryEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 LIKE \'%ghi%\'")
  }

  test("NotLikeFilter should render correct string for Bigquery") {
    val filter = NotLikeFilter("field1", "ghi")
    render(SqlNotLikeFilterRenderer, filter, bigqueryLiteralMapper, BigqueryEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 NOT LIKE \'%ghi%\'")
  }

  test("NotEqualToFilter should render correct string for Bigquery") {
    val filter = NotEqualToFilter("field1", "ghi")
    render(SqlNotEqualToFilterRenderer, filter, bigqueryLiteralMapper, BigqueryEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 <> \'ghi\'")
  }

  test("IsNullFilter should render correct string for Bigquery") {
    val filter = IsNullFilter("field1")
    render(SqlIsNullFilterRenderer, filter, bigqueryLiteralMapper, BigqueryEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 IS NULL")
  }

  test("IsNotNullFilter should render correct string for Bigquery") {
    val filter = IsNotNullFilter("field1")
    render(SqlIsNotNullFilterRenderer, filter, bigqueryLiteralMapper, BigqueryEngine, col,  Map("field1" -> ("field1", "field1"))) shouldBe DefaultResult("field1 IS NOT NULL")
  }

  test("AndFilter should render combined filters with AND") {
    val andFilter = AndFilter(
      List(
        InFilter("field1", List("abc", "def", "ghi"))
        , BetweenFilter("field2", "def", "ghi")
        , EqualityFilter("field3", "ghi")
    ))
    andFilter.isEmpty shouldBe false
    andFilter.field shouldEqual "and"
    andFilter.toString shouldBe "AndFilter(List(InFilter(field1,List(abc, def, ghi),false,false), BetweenFilter(field2,def,ghi), EqualityFilter(field3,ghi,false,false)))"
  }

  test("OrFilter should render combined filters with OR") {
    val orFilter = OrFilter(List(
      InFilter("field1", List("abc", "def", "ghi"))
      , BetweenFilter("field2", "def", "ghi")
      , EqualityFilter("field3", "ghi"))
    )
    orFilter.toString shouldBe "OrFilter(List(InFilter(field1,List(abc, def, ghi),false,false), BetweenFilter(field2,def,ghi), EqualityFilter(field3,ghi,false,false)))"
    orFilter.isEmpty shouldBe false
    orFilter.field shouldEqual "or"
    orFilter.filters.isEmpty shouldBe false
  }

  test("PreRenderedAndFilter should render combined filters with AND") {
    val andFilter = RenderedAndFilter(
      List(
        "field1 IN ('abc', 'def', 'ghi')"
        , "field2 BETWEEN 'abc' AND 'def'"
        , "field3 =  'ghi'"
      ))
    andFilter.isEmpty shouldBe false
    andFilter.toString shouldBe "(field1 IN ('abc', 'def', 'ghi')) AND (field2 BETWEEN 'abc' AND 'def') AND (field3 =  'ghi')"
  }

  test("PreRenderedOrFilter should render combined filters with OR") {
    val orFilter = RenderedOrFilter(
      List(
        "field1 IN ('abc', 'def', 'ghi')"
        , "field2 BETWEEN 'abc' AND 'def'"
        , "field3 =  'ghi'"
      ))
    orFilter.isEmpty shouldBe false
    orFilter.toString shouldBe "(field1 IN ('abc', 'def', 'ghi')) OR (field2 BETWEEN 'abc' AND 'def') OR (field3 =  'ghi')"
  }

  test("OrFilter should be able to successfully render with an alias.") {
    val orFilter : Filter = OrFilter(List(
      InFilter("field1", List("abc", "def", "ghi"))
      , BetweenFilter("field2", "def", "ghi")
      , EqualityFilter("field3", "ghi"))
    )
    val orCol : Column = col
    val aliasToRenderedSqlMap: Map[String, (String, String)] = Map(
      "field1" -> (col.alias.getOrElse("field1"), "field1")
      , "field2" -> ("stats_date", "field2")
      , "field3" -> ("date_sid", "field3"))
    val engine : Engine = OracleEngine
    val mapper : SqlLiteralMapper = oracleLiteralMapper
    val grainOption : Option[Grain] = Some(DailyGrain)
    val retVal = FilterSql.renderFilterWithAlias(orFilter, aliasToRenderedSqlMap, orCol, engine, mapper, grainOption)
    val expectedRenderedString : String = "(field1 IN ('abc','def','ghi')) OR (field2 >= trunc(to_date('def', 'YYYY-MM-DD')) AND field2 <= trunc(to_date('ghi', 'YYYY-MM-DD'))) OR (field3 = to_date('ghi', 'YYYY-MM-DD'))"
    retVal shouldEqual DefaultResult(expectedRenderedString, false)

    val renderedFilter = Filter.filterJSONW.write(orFilter)
    val  expectedRenderedJson : JObject =
      new JObject(
        List[(String, JValue)]
          (("operator", JString("Or"))
            , ("filterExpressions", JArray(
            List[JObject](
              new JObject(
                List[(String, JValue)](
                  ("field", JString("field1"))
                  , ("operator", JString("In"))
                  , ("values", JArray(List[JValue](JString("abc"), JString("def"), JString("ghi"))))))
              , new JObject(
                List[(String, JValue)](
                  ("field", JString("field2"))
                  , ("operator", JString("Between"))
                  , ("from", JString("def"))
                  , ("to", JString("ghi"))
                )
              )
              , new JObject(
                List[(String, JValue)](
                  ("field", JString("field3"))
                  , ("operator", JString("="))
                  , ("value", JString("ghi"))
                )
              ))))))

    renderedFilter shouldEqual expectedRenderedJson

  }

  test("AndFilter should be able to successfully render with an alias.") {
    val andFilter : Filter = AndFilter(List(
      InFilter("field1", List("abc", "def", "ghi"))
      , BetweenFilter("field2", "def", "ghi")
      , EqualityFilter("field3", "ghi"))
    )
    val andCol : Column = col
    val aliasToRenderedSqlMap: Map[String, (String, String)] = Map(
      "field1" -> (col.alias.getOrElse("field1"), "field1")
      , "field2" -> ("stats_date", "field2")
      , "field3" -> ("date_sid", "field3"))
    val engine : Engine = OracleEngine
    val mapper : SqlLiteralMapper = oracleLiteralMapper
    val grainOption : Option[Grain] = Some(DailyGrain)
    val retVal = FilterSql.renderFilterWithAlias(andFilter, aliasToRenderedSqlMap, andCol, engine, mapper, grainOption)
    val expectedRenderedString : String = "(field1 IN ('abc','def','ghi')) AND (field2 >= trunc(to_date('def', 'YYYY-MM-DD')) AND field2 <= trunc(to_date('ghi', 'YYYY-MM-DD'))) AND (field3 = to_date('ghi', 'YYYY-MM-DD'))"
    retVal shouldEqual DefaultResult(expectedRenderedString, false)

    val renderedFilter = Filter.filterJSONW.write(andFilter)
    val  expectedRenderedJson : JObject =
      new JObject(
        List[(String, JValue)]
          (("operator", JString("And"))
            , ("filterExpressions", JArray(
            List[JObject](
              new JObject(
            List[(String, JValue)](
              ("field", JString("field1"))
            , ("operator", JString("In"))
            , ("values", JArray(List[JValue](JString("abc"), JString("def"), JString("ghi"))))))
            , new JObject(
                List[(String, JValue)](
                  ("field", JString("field2"))
                  , ("operator", JString("Between"))
                  , ("from", JString("def"))
                  , ("to", JString("ghi"))
                )
              )
            , new JObject(
                List[(String, JValue)](
                  ("field", JString("field3"))
                  , ("operator", JString("="))
                  , ("value", JString("ghi"))
                )
              ))))))

    renderedFilter shouldEqual expectedRenderedJson

    val readJsonFilter = Filter.filterJSONR.read(renderedFilter)

    readJsonFilter.isSuccess shouldBe true

  }

  test("BetweenFilter should fail for Druid engine") {
    val filter = BetweenFilter("filed1", "abc", "def")
    val thrown = intercept[IllegalArgumentException] {
      render(SqlBetweenFilterRenderer, filter, oracleLiteralMapper, DruidEngine, col,  Map("filed1" -> ("filed1", "filed1")))
    }
    thrown.getMessage should startWith ("Unsupported engine for BetweenFilterRenderer Druid")
  }

  test("OuterFilter should render combined filters with AND") {
    val outerFilter = OuterFilter(List(EqualityFilter("Field One", "abc"), IsNullFilter("Field Two")))
    outerFilter.toString shouldBe "OuterFilter(List(EqualityFilter(Field One,abc,false,false), IsNullFilter(Field Two,false,false)))"
  }

  test("InFilter should fail for Druid engine") {
    val filter = InFilter("filed1", List("abc", "ads"))
    val thrown = intercept[IllegalArgumentException] {
      render(SqlInFilterRenderer, filter, oracleLiteralMapper, DruidEngine, col,  Map("filed1" -> ("filed1", "filed1")))
    }
    thrown.getMessage should startWith ("Unsupported engine for InFilterRenderer Druid")
  }

  test("NotInFilter should fail for Druid engine") {
    val filter = NotInFilter("filed1", List("abc", "ads"))
    val thrown = intercept[IllegalArgumentException] {
      render(SqlNotInFilterRenderer, filter, oracleLiteralMapper, DruidEngine, col,  Map("filed1" -> ("filed1", "filed1")))
    }
    thrown.getMessage should startWith ("Unsupported engine for NotInFilterRenderer Druid")
  }

  test("EqualityFilter should fail for Druid engine") {
    val filter = EqualityFilter("filed1", "abc")
    val thrown = intercept[IllegalArgumentException] {
      render(SqlEqualityFilterRenderer, filter, oracleLiteralMapper, DruidEngine, col,  Map("filed1" -> ("filed1", "filed1")))
    }
    thrown.getMessage should startWith ("Unsupported engine for EqualityFilterRenderer Druid")
  }

  test("LikeFilter should fail for Druid engine") {
    val filter = LikeFilter("filed1", "ads")
    val thrown = intercept[IllegalArgumentException] {
      render(SqlLikeFilterRenderer, filter, oracleLiteralMapper, DruidEngine, col,  Map("filed1" -> ("filed1", "filed1")))
    }
    thrown.getMessage should startWith ("Unsupported engine for LikeFilterRenderer Druid")
  }

  test("SqlLikeFilterRenderer edit strings") {
    val filter = LikeFilter("%sfield__1", "ad%s")
    val normalFilter = LikeFilter("field1", "ads")
    val rendered = render(SqlLikeFilterRenderer, filter, oracleLiteralMapper, OracleEngine, escapedCol,  Map("%sfield__1" -> ("%sfield__1", "%sfield__1")))
    val renderedNonEscaped = render(SqlLikeFilterRenderer, normalFilter, oracleLiteralMapper, OracleEngine, col,  Map("field1" -> ("field1", "field1")))
    assert(rendered.isInstanceOf[DefaultResult])
    assert(renderedNonEscaped.isInstanceOf[DefaultResult])
  }

  test("NotEqualToFilter should fail for Druid engine") {
    val filter = NotEqualToFilter("filed1", "ads")
    val thrown = intercept[IllegalArgumentException] {
      render(SqlNotEqualToFilterRenderer, filter, oracleLiteralMapper, DruidEngine, col,  Map("filed1" -> ("filed1", "filed1")))
    }
    thrown.getMessage should startWith ("Unsupported engine for NotEqualToFilterRenderer Druid")
  }

  test("InFilter Test") {
    val filter = InFilter("filed1", List("one"))
    render(SqlInFilterRenderer, filter, oracleLiteralMapper, OracleEngine, col,  Map("filed1" -> ("filed1", "filed1")))
  }
  
  test("IsNullFilter should fail for Druid engine") {
    val filter = IsNullFilter("filed1")
    val thrown = intercept[IllegalArgumentException] {
      render(SqlIsNullFilterRenderer, filter, oracleLiteralMapper, DruidEngine, col,  Map("filed1" -> ("filed1", "filed1")))
    }
    thrown.getMessage should startWith ("Unsupported engine for IsNullFilterRenderer Druid")
  }
  
  test("IsNotNullFilter should fail for Druid engine") {
    val filter = IsNotNullFilter("filed1")
    val thrown = intercept[IllegalArgumentException] {
      render(SqlIsNotNullFilterRenderer, filter, oracleLiteralMapper, DruidEngine, col,  Map("filed1" -> ("filed1", "filed1")))
    }
    thrown.getMessage should startWith ("Unsupported engine for IsNotNullFilterRenderer Druid")
  }

  test("Filters with different forcefilter flags are same using the baseFilterOrdering") {
    val filter1 = IsNotNullFilter("field1", isForceFilter = true)
    val filter2 = IsNotNullFilter("field1", isForceFilter = false)
    val filter3 = IsNotNullFilter("field1", isForceFilter = false)
    val filter4 = IsNotNullFilter("field1", isForceFilter = true)
    val filter5 = IsNotNullFilter("field2", isForceFilter = true)

    val thrown = intercept[IllegalArgumentException] {
      ForceFilter(EqualityFilter("field1", "1", isForceFilter = false))
    }

    assert(thrown.getMessage.contains("Filter must be declared with isForceFilter = true"))

    assert(filter1 === filter4)
    assert(filter2 === filter3)
    assert(Filter.baseFilterOrdering.compare(filter1, filter2) === 0)
    assert(Filter.baseFilterOrdering.compare(filter3, filter4) === 0)

    val s: Set[Filter] = Set(filter1, filter2, filter3, filter4, filter5)
    assert(s.size === 3)

    var ts: TreeSet[Filter] = TreeSet.empty(Filter.baseFilterOrdering)
    ts ++= s
    assert(ts.size === 2)

    assert(ts.contains(filter1) === true)
    assert(ts.contains(filter2) === true)

  }

  test("Filters with different overridable flags are same using the baseFilterOrdering") {
    val filter1 = InFilter("field1", List("a","b","c"), isOverridable = true)
    val filter2 = InFilter("field1", List("a","b","c"), isOverridable = false)
    val filter3 = InFilter("field1", List("a","b","c"), isOverridable = false)
    val filter4 = InFilter("field1", List("a","b","c"), isOverridable = true)
    val filter5 = InFilter("field1", List("a","b","c","d"), isOverridable = true)
    val filter6 = filter5.renameField("new_field_name")

    assert(filter1 === filter4)
    assert(filter2 === filter3)
    assert(Filter.baseFilterOrdering.compare(filter1, filter2) === 0)
    assert(Filter.baseFilterOrdering.compare(filter3, filter4) === 0)

    val s: Set[Filter] = Set(filter1, filter2, filter3, filter4, filter5)
    assert(s.size === 3)

    var ts: TreeSet[Filter] = TreeSet.empty(Filter.baseFilterOrdering)
    ts ++= s
    assert(ts.size === 2)

    assert(ts.contains(filter1) === true)
    assert(ts.contains(filter2) === true)
    assert(filter6.field === "new_field_name")
  }

  test("Filters with different operations are different using the baseFilterOrdering") {
    val filter1 = InFilter("field1", List("a","b","c"))
    val filter2 = NotInFilter("field1", List("a","b","c"))
    val filter3 = BetweenFilter("field1", "a", "b")
    val filter4 = EqualityFilter("field1", "a")
    val filter5 = IsNotNullFilter("field1")
    val filter6 = IsNullFilter("field1")
    val filter7 = LikeFilter("field1", "a")
    val filter8 = NotEqualToFilter("field1", "a")
    val filter9 = PushDownFilter(IsNotNullFilter("field1"))
    val filter10 = OuterFilter(List(IsNotNullFilter("field1")))
    val filter11 = filter2.renameField("new_field_name")
    val filter12 = FieldEqualityFilter("field1", "field2")

    val s: Set[Filter] = Set(filter1, filter2, filter3, filter4, filter5, filter6, filter7, filter8, filter9, filter10)
    assert(s.size === 10)

    var ts: TreeSet[Filter] = TreeSet.empty(Filter.baseFilterOrdering)
    ts ++= s
    assert(ts.size === 10)
    assert(filter11.field === "new_field_name")
    assert(filter1.canBeHighCardinalityFilter
      && filter2.canBeHighCardinalityFilter
      && !filter3.canBeHighCardinalityFilter
      && filter4.canBeHighCardinalityFilter
      && !filter5.canBeHighCardinalityFilter
      && !filter6.canBeHighCardinalityFilter
      && filter7.canBeHighCardinalityFilter
      && filter8.canBeHighCardinalityFilter
      && !filter9.canBeHighCardinalityFilter
      && !filter10.canBeHighCardinalityFilter
      && filter11.canBeHighCardinalityFilter
      && filter12.canBeHighCardinalityFilter, "All known filters and ability to be high cardinality is asserted here.")
  }

  test("Attempt to compare incomparable types") {
    assert(EqualityObj.compare(1, "one") == -10, "Incomparable objects should have a sensible returnable value.")
  }

  test("failing forced filter") {
    val thrown = intercept[UnsupportedOperationException] {
      FilterSql.renderFilterWithAlias(null, null, null, null, null)
    }
    assert(thrown.getMessage.contains("Unhandled filter operation"))
  }

  test("forced filter test") {
    val forceFilter:ForcedFilter = EqualityFilter("FieldName", "10", true).asInstanceOf[ForcedFilter]
    assert(forceFilter.isForceFilter)
    assert(forceFilter.asValues === "10")
  }

  test("Druid Filter Dim should be valid") {
    val pdFilter = {
      val pdFilter = PushDownFilter(BetweenFilter("field1", "1", "2"))
      FilterDruid.renderFilterDim(pdFilter, Map("field1" -> "field1"), Map("field1" -> col), Option(DailyGrain))
    }
    assert(pdFilter.toFilter.isInstanceOf[BoundFilter])
  }

  test("Should return the expected filter sets") {
    val equalityFilter = EqualityFilter("field1", "a")
    val outerFilter = OuterFilter(List(EqualityFilter("field2", "a")))
    val orFilter = OrFilter(List(EqualityFilter("field3", "a")))
    val andFilter = AndFilter(List(EqualityFilter("field17", "a")))
    val betweenFilter = BetweenFilter("field4", "1", "3")
    val inFilter = InFilter("field5", List("a", "b", "c"))
    val notInFilter = NotInFilter("field6", List("a", "b", "c"))
    val notEqualToFilter = NotEqualToFilter("field7", "1")
    val greaterThanFilter = GreaterThanFilter("field8", "1")
    val lessThanFilter = LessThanFilter("field9", "1")
    val isNotNullFilter = IsNotNullFilter("field10")
    val likeFilter = LikeFilter("field11", "a")
    val isNullFilter = IsNullFilter("field12")
    val pushDownFilter = PushDownFilter(EqualityFilter("field13", "a"))
    val fieldEqualityFilter = FieldEqualityFilter("field15", "field16")
    val jsFilter = JavaScriptFilter("field14", "this filter will fail.") //This rendering fn is not defined, so being used as fallthrough to test failure in field set rendering.
    val notLikeFilter = NotLikeFilter("field18", "a")

    val returnedFields: Set[String] = Filter.returnFieldSetOnMultipleFiltersWithoutValidation(
      Set(equalityFilter
        , outerFilter
        , orFilter
        , andFilter
        , betweenFilter
        , inFilter
        , notInFilter
        , notEqualToFilter
        , greaterThanFilter
        , lessThanFilter
        , isNotNullFilter
        , likeFilter
        , isNullFilter
        , pushDownFilter
        , fieldEqualityFilter
        , notLikeFilter)
    )

    val expectedReturnedFields : Set[String] =
      Set("field1"
        //, "field2"      OuterFilter returns no fields.
        //, "field3"      OrFilter returns no fields.
        , "field4"
        , "field5"
        , "field6"
        , "field7"
        , "field8"
        , "field9"
        , "field10"
        , "field11"
        , "field12"
        , "field13"
        , "field15"
        , "field16"
        //, "field17"     AndFilter returns no fields.
        , "field18"
      )

    assert(returnedFields == expectedReturnedFields, "Should return all expected fields!")

    val thrown = intercept[IllegalArgumentException] {
      Filter.returnFieldSetWithoutValidation(jsFilter)
    }

    assert(thrown.getMessage.contains("The field set for the input filter is undefined. "))

  }

  test("Should compare two filters & return t/f correctly.") {
    val eqFilter1 = EqualityFilter("field1", "1")
    val eqFilter2 = EqualityFilter("field2", "2")
    val inEqFilter = NotEqualToFilter("field1", "1")
    val fieldEqFilter = FieldEqualityFilter("field2", "field3")
    val orFilter = OrFilter(List(eqFilter1, eqFilter2))

    assert(Filter.compareForcedFilters(eqFilter1, eqFilter1), "Both filters are the same!")
    assert(!Filter.compareForcedFilters(eqFilter1, eqFilter2), "Unique fields implies unique forced filters.")
    assert(Filter.compareForcedFilters(inEqFilter, eqFilter1), "Only the fields need to be equivalent in forced filter comparison.")
    assert(Filter.compareForcedFilters(fieldEqFilter, eqFilter2), "At least one field has already been mentioned!")
    assert(!Filter.compareForcedFilters(fieldEqFilter, eqFilter1), "There is no field overlap!")
    assert(!Filter.compareForcedFilters(fieldEqFilter, orFilter), "OrFilter is never equivalent to anything.")
    assert(!Filter.compareForcedFilters(orFilter, fieldEqFilter), "OrFilter is never equivalent to anything.")
  }

  test("Should map the correct field to operation.") {
    val equalityFilter = EqualityFilter("field1", "a")
    val outerFilter = OuterFilter(List(EqualityFilter("field2", "a")))
    val orFilter = OrFilter(List(EqualityFilter("field3", "a")))
    val andFilter = AndFilter(List(EqualityFilter("field17", "a")))
    val betweenFilter = BetweenFilter("field4", "1", "3")
    val inFilter = InFilter("field5", List("a", "b", "c"))
    val notInFilter = NotInFilter("field6", List("a", "b", "c"))
    val notEqualToFilter = NotEqualToFilter("field7", "1")
    val greaterThanFilter = GreaterThanFilter("field8", "1")
    val lessThanFilter = LessThanFilter("field9", "1")
    val isNotNullFilter = IsNotNullFilter("field10")
    val likeFilter = LikeFilter("field11", "a")
    val isNullFilter = IsNullFilter("field12")
    val pushDownFilter = PushDownFilter(EqualityFilter("field13", "a"))
    val fieldEqualityFilter = FieldEqualityFilter("field15", "field16")
    val jsFilter = JavaScriptFilter("field14", "this filter will fail.") //This rendering fn is not defined, so being used as fallthrough to test failure in field set rendering.

    val returnedMap: Map[String, FilterOperation] = Filter.returnFieldAndOperationMapOnMultipleFiltersWithoutValidation(
      Set(equalityFilter
        , outerFilter
        , orFilter
        , andFilter
        , betweenFilter
        , inFilter
        , notInFilter
        , notEqualToFilter
        , greaterThanFilter
        , lessThanFilter
        , isNotNullFilter
        , likeFilter
        , isNullFilter
        , pushDownFilter
        , fieldEqualityFilter)
    )

    val expectedReturnedMap: Map[String, FilterOperation] = Map(
      "field1" -> EqualityFilterOperation,
      "field4" -> BetweenFilterOperation,
      "field5" -> InFilterOperation,
      "field6" -> NotInFilterOperation,
      "field7" -> NotEqualToFilterOperation,
      "field8" -> GreaterThanFilterOperation,
      "field9" -> LessThanFilterOperation,
      "field10" -> IsNotNullFilterOperation,
      "field11" -> LikeFilterOperation,
      "field12" -> IsNullFilterOperation,
      "field13" -> EqualityFilterOperation,
      "field15" -> FieldEqualityFilterOperation,
      "field16" -> FieldEqualityFilterOperation
    )

    assert(returnedMap == expectedReturnedMap)

    val thrown = intercept[IllegalArgumentException] {
      Filter.returnFieldAndOperationMapWithoutValidation(jsFilter)
    }

    assert(thrown.getMessage.contains("The filter map for the input filter is undefined. "))
  }

  test("Should generate proper Spec for FilterDruid") {
    val druidLikeFilter = LikeFilter("field", "value")
    val druidLikeFilterResult = FilterDruid.renderFilterDim(druidLikeFilter, Map("field" -> "field"), Map("field" -> DimCol("field", StrType())), Option(DailyGrain))
    assert(druidLikeFilterResult.isInstanceOf[SearchQueryDimFilter], "Should generate a proper Druid LikeFilter")
    assert(druidLikeFilterResult.asInstanceOf[SearchQueryDimFilter].getQuery.isInstanceOf[InsensitiveContainsSearchQuerySpec])

    val druidNotEqualToFilter = NotEqualToFilter("not", "value")
    val druidNotEqualToFilterResult = FilterDruid.renderFilterDim(druidNotEqualToFilter, Map("not" -> "not"), Map("not" -> DimCol("not", StrType())), Option(DailyGrain))
    assert(druidNotEqualToFilterResult.isInstanceOf[NotDimFilter], "Should generate a proper Druid NotDimFilter.")
  }

  test("Should return the expected filter sets with Pk fields") {
    val equalityFilter = EqualityFilter("field1", "a")
    val outerFilter = OuterFilter(List(EqualityFilter("field2", "a")))
    val orFilter = OrFilter(List(EqualityFilter("field3", "a")))
    val andFilter = AndFilter(List(EqualityFilter("field17", "a")))
    val betweenFilter = BetweenFilter("field4", "1", "3")
    val inFilter = InFilter("field5", List("a", "b", "c"))
    val notInFilter = NotInFilter("field6", List("a", "b", "c"))
    val notEqualToFilter = NotEqualToFilter("field7", "1")
    val greaterThanFilter = GreaterThanFilter("field8", "1")
    val lessThanFilter = LessThanFilter("field9", "1")
    val isNotNullFilter = IsNotNullFilter("field10")
    val likeFilter = LikeFilter("field11", "a")
    val isNullFilter = IsNullFilter("field12")
    val pushDownFilter = PushDownFilter(EqualityFilter("field13", "a"))
    val fieldEqualityFilter = FieldEqualityFilter("field15", "field16")
    val jsFilter = JavaScriptFilter("field14", "this filter will fail.") //This rendering fn is not defined, so being used as fallthrough to test failure in field set rendering.

    val returnedFields: Set[String] = Filter.returnFillFieldSetOnMultipleFiltersForPkAliases(
      Set(equalityFilter
        , outerFilter
        , orFilter
        , andFilter
        , betweenFilter
        , inFilter
        , notInFilter
        , notEqualToFilter
        , greaterThanFilter
        , lessThanFilter
        , isNotNullFilter
        , likeFilter
        , isNullFilter
        , pushDownFilter
        , fieldEqualityFilter)
    )

    val expectedReturnedFields : Set[String] =
      Set("field1"
        //, "field2"      //OuterFilter returns no fields.
        , "field3"        //OrFilter returns its fields.
        , "field4"
        , "field5"
        , "field6"
        , "field7"
        , "field8"
        , "field9"
        , "field10"
        , "field11"
        , "field12"
        , "field13"
        , "field15"
        , "field16"
        , "field17"       //AndFilter returns its fields.
      )

    assert(returnedFields == expectedReturnedFields, "Should return all expected fields!")

    val thrown = intercept[IllegalArgumentException] {
      Filter.returnFullFieldSetForPkAliases(jsFilter)
    }

    assert(thrown.getMessage.contains("The field alias set for the input filter is undefined. "))

  }

  test("Test serialization/deserialization for Not like filter") {
    val notLikeFilter: Filter = NotLikeFilter("testField", "testValue")
    val renderedFilter = Filter.filterJSONW.write(notLikeFilter)
    val  expectedRenderedJson : JObject =
      new JObject(List[(String, JValue)](
                  ("field", JString("testField"))
                  , ("operator", JString("Not Like"))
                  , ("value", JString("testValue"))
                )
              )
    renderedFilter shouldEqual expectedRenderedJson
    val readJsonFilter = Filter.filterJSONR.read(renderedFilter)
    readJsonFilter.isSuccess shouldBe true
  }
}
