package com.yahoo.maha.core

import com.yahoo.maha.core.dimension.DimCol
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class LiteralMapperTest extends AnyFunSuite with Matchers {
  test("Test DruidLiteralMapper ") {
    val druidMapper = new DruidLiteralMapper

    implicit val cc: ColumnContext = new ColumnContext
    val col = DimCol("field1", IntType())
    val colDateNoFormat = DimCol("field1_date", DateType())
    val colDateWithFormat = DimCol("field1_date_fmt", DateType("YYYY-MM-dd"))
    val colTSNoFmt = DimCol("field1_ts", TimestampType())
    val colTSWithFmt = DimCol("field1_ts_fmt", TimestampType("YYYY-MM-dd"))

    assert(druidMapper.toDateTime(col, "2018-01-01", Option(DailyGrain)).toString == "2018-01-01T00:00:00.000Z")
    assert(druidMapper.toDateTime(colDateNoFormat, "2018-01-01", Option(DailyGrain)).toString == "2018-01-01T00:00:00.000Z")
    assert(druidMapper.toDateTime(colDateWithFormat, "2018-01-01", Option(DailyGrain)).toString == "2018-01-01T00:00:00.000Z")
    assert(druidMapper.toDateTime(colTSNoFmt, "2018-01-01", Option(DailyGrain)).toString == "2018-01-01T00:00:00.000Z")
    assert(druidMapper.toDateTime(colTSWithFmt, "2018-01-01", Option(DailyGrain)).toString == "2018-01-01T00:00:00.000Z")
    assert(druidMapper.toLiteral(col, "2018-01-01", Option(DailyGrain)).toString == "2018-01-01")
    assert(druidMapper.toLiteral(colTSNoFmt, "2018-01-01", Option(DailyGrain)).toString == "2018-01-01")
    assert(druidMapper.toLiteral(colTSWithFmt, "2018-01-01", Option(DailyGrain)).toString == "2018-01-01")

    assert(druidMapper.toNumber(col, "1") == 1)
    assertThrows[UnsupportedOperationException](druidMapper.toNumber(colTSNoFmt, "1"), "Expected UnsupportedOperationException, but didn't get it.")
  }
}
