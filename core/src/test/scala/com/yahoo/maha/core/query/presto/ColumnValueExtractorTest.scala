// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import java.sql.{Date, ResultSet, Timestamp}
import java.time.Instant

import com.yahoo.maha.core.{Column, ColumnAnnotation, ColumnContext, DataType, DateType, DecType, FilterOperation, IntType, TimestampType}
import org.joda.time.DateTime
import java.math.BigDecimal

import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}


class ColumnValueExtractorTest extends FunSuite with Matchers with BeforeAndAfterAll {

  val columnValueExtractor = new ColumnValueExtractor
  test("test getBigDecimalSafely") {
    val rs = mock(classOf[ResultSet])

    doReturn(BigDecimal.valueOf(1.23)).when(rs).getBigDecimal(0)
    doReturn(BigDecimal.valueOf(1.0)).when(rs).getBigDecimal(1)
    doReturn(BigDecimal.valueOf(0.2398374857887346875637538579)).when(rs).getBigDecimal(2)
    doReturn(null).when(rs).getBigDecimal(3)

    assert(columnValueExtractor.getBigDecimalSafely(rs, 0) == 1.23)
    assert(columnValueExtractor.getBigDecimalSafely(rs, 1) == 1)
    assert(columnValueExtractor.getBigDecimalSafely(rs, 2) == 0.2398374857887346875637538579)
    assert(columnValueExtractor.getBigDecimalSafely(rs, 3) == null)
  }

  test("test getLongSafely") {
    val rs = mock(classOf[ResultSet])

    doReturn(BigDecimal.valueOf(91823981932131923L)).when(rs).getBigDecimal(0)
    doReturn(BigDecimal.valueOf(9.83742)).when(rs).getBigDecimal(1)
    doReturn(null).when(rs).getBigDecimal(2)

    assert(columnValueExtractor.getLongSafely(rs, 0) == 91823981932131923L)
    assert(columnValueExtractor.getLongSafely(rs, 1) == 9)
    assert(columnValueExtractor.getLongSafely(rs, 2) == 0)
  }

  test("test getColumnValue") {
    val rs = mock(classOf[ResultSet])
    doReturn(BigDecimal.valueOf(91823981932131923L)).when(rs).getBigDecimal(0)
    doReturn("Native").when(rs).getString(1)
    doReturn(BigDecimal.valueOf(1.234)).when(rs).getBigDecimal(2)
    doReturn("20180503").when(rs).getString(3)
    doReturn(Date.valueOf("2018-05-04")).when(rs).getDate(4)

    val dt = new DateTime("2018-05-04T21:39:45.618+00:00")
    doReturn(Timestamp.from(Instant.ofEpochMilli(dt.getMillis))).when(rs).getTimestamp(5)

    val intCol1 = new TestCol {
      override def dataType : DataType = IntType()
    }
    assert(columnValueExtractor.getColumnValue(0, intCol1, rs) == 91823981932131923L)

    val intCol2 = new TestCol {
      override def dataType : DataType = IntType(10, (Map(1 -> "Search", 2 -> "Native"), "Unknown"))
    }
    assert(columnValueExtractor.getColumnValue(1, intCol2, rs) == "Native")

    val decCol = new TestCol {
      override def dataType : DataType = DecType(10, 2)
    }
    assert(columnValueExtractor.getColumnValue(2, decCol, rs) == 1.23)

    val dateCol1 = new TestCol {
      override def dataType : DataType = DateType("YYYYMMDD")
    }
    assert(columnValueExtractor.getColumnValue(3, dateCol1, rs) == "20180503")

    val dateCol2 = new TestCol {
      override def dataType : DataType = DateType()
    }
    assert(columnValueExtractor.getColumnValue(4, dateCol2, rs) == "2018-05-04")

    val timestampCol = new TestCol {
      override def dataType : DataType = TimestampType()
    }

    assert(columnValueExtractor.getColumnValue(5, timestampCol, rs).asInstanceOf[String] == "2018-05-04 21:39:45")
  }

  abstract class TestCol extends Column {
    override def alias: Option[String] = None
    override def filterOperationOverrides: Set[FilterOperation] = Set.empty
    override def isDerivedColumn: Boolean = false
    override def name: String = "test"
    override def annotations: Set[ColumnAnnotation] = Set.empty
    override def columnContext: ColumnContext = null
    override def dataType: DataType = ???
  }
}