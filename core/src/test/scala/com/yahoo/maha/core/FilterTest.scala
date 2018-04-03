// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import com.yahoo.maha.core.dimension.DimCol
import com.yahoo.maha.core.fact.ForceFilter
import org.joda.time.DateTime
import org.scalatest.{FunSuite, Matchers}

import scala.collection.immutable.TreeSet

/**
 * Created by jians on 11/10/15.
 */

class FilterTest extends FunSuite with Matchers {

  val oracleLiteralMapper = new OracleLiteralMapper
  val hiveLiteralMapper = new HiveLiteralMapper
  val druidLiteralMapper = new DruidLiteralMapper


  def render[T, O](renderer: FilterRenderer[T, O], filter: T, literalMapper: LiteralMapper, engine: Engine, column: Column) : O = {
    renderer.render(column.name, filter, literalMapper, column, engine, None)
  }

  def renderWithGrain[T, O](renderer: FilterRenderer[T, O], filter: T, literalMapper: LiteralMapper, engine: Engine, column: Column, grain : Grain) : O = {
    renderer.render(column.name, filter, literalMapper, column, engine, Some(grain))
  }
  
  implicit val columnContext: ColumnContext = new ColumnContext
  val col = DimCol("field1", StrType())
  val dateCol = DimCol("stats_date", DateType())
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
    val dailyResult = renderWithGrain(SqlBetweenFilterRenderer, filter, oracleLiteralMapper, OracleEngine, dateCol, DailyGrain).filter
    dailyResult shouldBe "stats_date >= trunc(to_date('2018-01-01', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('2018-01-07', 'YYYY-MM-DD'))"
    val hourlyResult = renderWithGrain(SqlBetweenFilterRenderer, filter, oracleLiteralMapper, OracleEngine, dateCol, HourlyGrain).filter
    hourlyResult shouldBe "stats_date >= to_date('2018-01-01', 'HH24') AND stats_date <= to_date('2018-01-07', 'HH24')"

    val intBetweenfilter = BetweenFilter("date_sid", "2018-01-01", "2018-01-02")
    val intHourlyResult = renderWithGrain(SqlBetweenFilterRenderer, intBetweenfilter, oracleLiteralMapper, OracleEngine, intDateCol, HourlyGrain).filter
    intHourlyResult shouldBe "date_sid >= to_date('2018-01-01', 'HH24') AND date_sid <= to_date('2018-01-02', 'HH24')"

    val strBetweenfilter = BetweenFilter("date_sid2", "2018-01-01", "2018-01-02")
    val strHourlyResult = renderWithGrain(SqlBetweenFilterRenderer, strBetweenfilter, oracleLiteralMapper, OracleEngine, strDateCol, HourlyGrain).filter
    strHourlyResult shouldBe "date_sid2 >= to_date('2018-01-01', 'HH24') AND date_sid2 <= to_date('2018-01-02', 'HH24')"
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
    render(SqlInFilterRenderer, filter, oracleLiteralMapper, OracleEngine, col) shouldBe DefaultResult("field1 IN ('abc','def','ghi')")
    val insensitiveFilter = InFilter("insensitive", List("abc", "def", "ghi"))
    render(SqlInFilterRenderer, filter, oracleLiteralMapper, OracleEngine, insensitiveCol) shouldBe DefaultResult("lower(insensitive) IN (lower('abc'),lower('def'),lower('ghi'))")
  }

  test("NotInFilter should render correct string for Oracle") {
    val filter = NotInFilter("field1", List("abc", "def", "ghi"))
    render(SqlNotInFilterRenderer, filter, oracleLiteralMapper, OracleEngine, col) shouldBe DefaultResult("field1 NOT IN ('abc','def','ghi')")
  }

  test("BetweenFilter should render correct string for Oracle") {
    val filter = BetweenFilter("field1", "abc", "def")
    render(SqlBetweenFilterRenderer, filter, oracleLiteralMapper, OracleEngine, col).filter shouldBe "field1 >= 'abc' AND field1 <= 'def'"
  }

  test("EqualityFilter should render correct string for Oracle") {
    val filter = EqualityFilter("field1", "ghi")
    render(SqlEqualityFilterRenderer, filter, oracleLiteralMapper, OracleEngine, col) shouldBe DefaultResult("field1 = \'ghi\'")
  }

  test("LikeFilter should render correct string for Oracle") {
    val filter = LikeFilter("field1", "ghi")
    render(SqlLikeFilterRenderer, filter, oracleLiteralMapper, OracleEngine, col) shouldBe DefaultResult("field1 LIKE \'%ghi%\'")
  }

  test("NotEqualToFilter should render correct string for Oracle") {
    val filter = NotEqualToFilter("field1", "ghi")
    render(SqlNotEqualToFilterRenderer, filter, oracleLiteralMapper, OracleEngine, col) shouldBe DefaultResult("field1 <> \'ghi\'")
  }

  test("IsNullFilter should render correct string for Oracle") {
    val filter = IsNullFilter("field1")
    render(SqlIsNullFilterRenderer, filter, oracleLiteralMapper, OracleEngine, col) shouldBe DefaultResult("field1 IS NULL")
  }

  test("IsNotNullFilter should render correct string for Oracle") {
    val filter = IsNotNullFilter("field1")
    render(SqlIsNotNullFilterRenderer, filter, oracleLiteralMapper, OracleEngine, col) shouldBe DefaultResult("field1 IS NOT NULL")
  }

  test("InFilter should render correct string for Hive") {
    val filter = InFilter("field1", List("abc", "def", "ghi"))
    render(SqlInFilterRenderer, filter, hiveLiteralMapper, HiveEngine, col) shouldBe DefaultResult("field1 IN ('abc','def','ghi')")
    val insensitiveFilter = InFilter("insensitive", List("abc", "def", "ghi"))
    render(SqlInFilterRenderer, filter, oracleLiteralMapper, HiveEngine, insensitiveCol) shouldBe DefaultResult("lower(insensitive) IN (lower('abc'),lower('def'),lower('ghi'))")
  }

  test("NotInFilter should render correct string for Hive") {
    val filter = NotInFilter("field1", List("abc", "def", "ghi"))
    render(SqlNotInFilterRenderer, filter, hiveLiteralMapper, HiveEngine, col) shouldBe DefaultResult("field1 NOT IN ('abc','def','ghi')")
  }

  test("BetweenFilter should render correct string for Hive") {
    val filter = BetweenFilter("field1", "abc", "def")
    render(SqlBetweenFilterRenderer, filter, hiveLiteralMapper, HiveEngine, col) shouldBe DefaultResult("field1 >= 'abc' AND field1 <= 'def'")
  }

  test("EqualityFilter should render correct string for Hive") {
    val filter = EqualityFilter("field1", "ghi")
    render(SqlEqualityFilterRenderer, filter, hiveLiteralMapper, HiveEngine, col) shouldBe DefaultResult("field1 = \'ghi\'")
  }

  test("LikeFilter should render correct string for Hive") {
    val filter = LikeFilter("field1", "ghi")
    render(SqlLikeFilterRenderer, filter, hiveLiteralMapper, HiveEngine, col) shouldBe DefaultResult("field1 LIKE \'%ghi%\'")
  }

  test("NotEqualToFilter should render correct string for Hive") {
    val filter = NotEqualToFilter("field1", "ghi")
    render(SqlNotEqualToFilterRenderer, filter, hiveLiteralMapper, HiveEngine, col) shouldBe DefaultResult("field1 <> \'ghi\'")
  }

  test("IsNullFilter should render correct string for Hive") {
    val filter = IsNullFilter("field1")
    render(SqlIsNullFilterRenderer, filter, hiveLiteralMapper, HiveEngine, col) shouldBe DefaultResult("field1 IS NULL")
  }

  test("IsNotNullFilter should render correct string for Hive") {
    val filter = IsNotNullFilter("field1")
    render(SqlIsNotNullFilterRenderer, filter, hiveLiteralMapper, HiveEngine, col) shouldBe DefaultResult("field1 IS NOT NULL")
  }

  test("AndFilter should render combined filters with AND") {
    val andFilter = AndFilter(List(
      "field1 IN (\'abc\', \'def\', \'ghi\')",
      "field2 BETWEEN \'abc\' AND \'def\'",
      "field3 =  \'ghi\'"
    ))
    andFilter.toString shouldBe "(field1 IN ('abc', 'def', 'ghi')) AND (field2 BETWEEN 'abc' AND 'def') AND (field3 =  'ghi')"
  }

  test("OrFilter should render combined filters with OR") {
    val orFilter = OrFilter(List(
    "field1 IN (\'abc\', \'def\', \'ghi\')",
      "field2 BETWEEN \'abc\' AND \'def\'",
      "field3 =  \'ghi\'"
    ))
    orFilter.toString shouldBe "(field1 IN ('abc', 'def', 'ghi')) OR (field2 BETWEEN 'abc' AND 'def') OR (field3 =  'ghi')"
    orFilter.isEmpty shouldBe false
  }

  test("BetweenFilter should fail for Druid engine") {
    val filter = BetweenFilter("filed1", "abc", "def")
    val thrown = intercept[IllegalArgumentException] {
      render(SqlBetweenFilterRenderer, filter, druidLiteralMapper, DruidEngine, col)
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
      render(SqlInFilterRenderer, filter, druidLiteralMapper, DruidEngine, col)
    }
    thrown.getMessage should startWith ("Unsupported engine for InFilterRenderer Druid")
  }

  test("NotInFilter should fail for Druid engine") {
    val filter = NotInFilter("filed1", List("abc", "ads"))
    val thrown = intercept[IllegalArgumentException] {
      render(SqlNotInFilterRenderer, filter, druidLiteralMapper, DruidEngine, col)
    }
    thrown.getMessage should startWith ("Unsupported engine for NotInFilterRenderer Druid")
  }

  test("EqualityFilter should fail for Druid engine") {
    val filter = EqualityFilter("filed1", "abc")
    val thrown = intercept[IllegalArgumentException] {
      render(SqlEqualityFilterRenderer, filter, druidLiteralMapper, DruidEngine, col)
    }
    thrown.getMessage should startWith ("Unsupported engine for EqualityFilterRenderer Druid")
  }

  test("LikeFilter should fail for Druid engine") {
    val filter = LikeFilter("filed1", "ads")
    val thrown = intercept[IllegalArgumentException] {
      render(SqlLikeFilterRenderer, filter, druidLiteralMapper, DruidEngine, col)
    }
    thrown.getMessage should startWith ("Unsupported engine for LikeFilterRenderer Druid")
  }

  test("SqlLikeFilterRenderer edit strings") {
    val filter = LikeFilter("%sfield__1", "ad%s")
    val intFilter = LikeFilter("%sfield__2", "ads")
    val normalFilter = LikeFilter("field1", "ads")
    val rendered = render(SqlLikeFilterRenderer, filter, druidLiteralMapper, OracleEngine, escapedCol)
    val renderedInt = render(SqlLikeFilterRenderer, intFilter, druidLiteralMapper, OracleEngine, escapedIntCol)
    val renderedNonEscaped = render(SqlLikeFilterRenderer, normalFilter, druidLiteralMapper, OracleEngine, col)
    assert(rendered.isInstanceOf[DefaultResult])
    assert(renderedInt.isInstanceOf[DefaultResult])
    assert(renderedNonEscaped.isInstanceOf[DefaultResult])
  }

  test("NotEqualToFilter should fail for Druid engine") {
    val filter = NotEqualToFilter("filed1", "ads")
    val thrown = intercept[IllegalArgumentException] {
      render(SqlNotEqualToFilterRenderer, filter, druidLiteralMapper, DruidEngine, col)
    }
    thrown.getMessage should startWith ("Unsupported engine for NotEqualToFilterRenderer Druid")
  }

  test("InFilter Test") {
    val filter = InFilter("filed1", List("one"))
    render(SqlInFilterRenderer, filter, oracleLiteralMapper, OracleEngine, col)
  }
  
  test("IsNullFilter should fail for Druid engine") {
    val filter = IsNullFilter("filed1")
    val thrown = intercept[IllegalArgumentException] {
      render(SqlIsNullFilterRenderer, filter, druidLiteralMapper, DruidEngine, col)
    }
    thrown.getMessage should startWith ("Unsupported engine for IsNullFilterRenderer Druid")
  }
  
  test("IsNotNullFilter should fail for Druid engine") {
    val filter = IsNotNullFilter("filed1")
    val thrown = intercept[IllegalArgumentException] {
      render(SqlIsNotNullFilterRenderer, filter, druidLiteralMapper, DruidEngine, col)
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

    val s: Set[Filter] = Set(filter1, filter2, filter3, filter4, filter5, filter6, filter7, filter8, filter9, filter10)
    assert(s.size === 10)

    var ts: TreeSet[Filter] = TreeSet.empty(Filter.baseFilterOrdering)
    ts ++= s
    assert(ts.size === 10)
    assert(filter11.field === "new_field_name")
  }

  test("Attempt to compare incomparable types") {
    assert(EqualityObj.compare(1, "one") == -10, "Incomparable objects should have a sensible returnable value.")
  }

  test("failing forced filter") {
    val thrown = intercept[UnsupportedOperationException] {
      FilterSql.renderFilterWithAlias(null, null, null, null)
    }
    assert(thrown.getMessage.contains("Unhandled filter operation"))
  }

  test("Druid Filter Dim should be valid") {
    val thrown = intercept[UnsupportedOperationException] {
      FilterDruid.renderFilterDim(IsNotNullFilter("field1"), Map("field1"->"field1"), Map("field1"->col), Some(DailyGrain))
    }
    assert(thrown.getMessage.contains("Unhandled filter operation"))

    val pdThrown = intercept[UnsupportedOperationException] {
      val pdFilter = PushDownFilter(BetweenFilter("field1", "1", "2"))
      FilterDruid.renderFilterDim(pdFilter, Map("field1" -> "field1"), Map("field1" -> col), Option(DailyGrain))
    }
    assert(pdThrown.getMessage.contains("Between filter not supported on Druid dimension fields :"))
  }
}
