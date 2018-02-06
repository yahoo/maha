// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import com.yahoo.maha.core.dimension.DimCol
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

  test("BetweenFilter with a defined grain should modify the output result to include it.") {
    val filter = BetweenFilter("stats_date", "2018-01-01", "2018-01-07")
    val dailyResult = renderWithGrain(SqlBetweenFilterRenderer, filter, oracleLiteralMapper, OracleEngine, dateCol, DailyGrain).filter
    dailyResult shouldBe "stats_date >= trunc(to_date('2018-01-01', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('2018-01-07', 'YYYY-MM-DD'))"
    val hourlyResult = renderWithGrain(SqlBetweenFilterRenderer, filter, oracleLiteralMapper, OracleEngine, dateCol, HourlyGrain).filter
    hourlyResult shouldBe "stats_date >= to_date('2018-01-01', 'HH24') AND stats_date <= to_date('2018-01-07', 'HH24')"

  }

  test("InFilter should render correct string for Oracle") {
    val filter = InFilter("field1", List("abc", "def", "ghi"))
    render(SqlInFilterRenderer, filter, oracleLiteralMapper, OracleEngine, col) shouldBe DefaultResult("field1 IN ('abc','def','ghi')")
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

    val s: Set[Filter] = Set(filter1, filter2, filter3, filter4, filter5, filter6, filter7, filter8, filter9, filter10)
    assert(s.size === 10)

    var ts: TreeSet[Filter] = TreeSet.empty(Filter.baseFilterOrdering)
    ts ++= s
    assert(ts.size === 10)
  }
}
