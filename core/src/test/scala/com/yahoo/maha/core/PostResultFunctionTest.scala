// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import com.yahoo.maha.core.DruidPostResultFunction.START_OF_THE_WEEK
import com.yahoo.maha.core.query.{PostResultRowData, Row}
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable.ArrayBuffer

class PostResultFunctionTest extends FunSuite with Matchers {

  test("START_OF_THE_WEEK postresult column test") {
    ColumnContext.withColumnContext {implicit cc : ColumnContext =>
      val col = START_OF_THE_WEEK("{stats_date}")
      val alias: Map[String, Int] = Map("A" -> 1, "B" -> 2)

      val arrayBuffer = new ArrayBuffer[Any]()
      arrayBuffer += "2018-52"
      arrayBuffer += "2019-01"
      val newRow = new Row(alias, arrayBuffer)
      val rowData = PostResultRowData(newRow , columnAlias = "A")
      col.resultApply(rowData)
      rowData.r.getValue("A") should equal("2018-12-31")
    }
  }

  test("START_OF_THE_WEEK postresult should not error out due to invalid format") {
    ColumnContext.withColumnContext {implicit cc : ColumnContext =>
      val col = START_OF_THE_WEEK("{stats_date}")
      val alias: Map[String, Int] = Map("A" -> 1, "B" -> 2)

      val arrayBuffer = new ArrayBuffer[Any]()
      arrayBuffer += "2017-22"
      arrayBuffer += "2017-06-21"
      val newRow = new Row(alias, arrayBuffer)
      val rowData = PostResultRowData(newRow , columnAlias = "A")
      col.resultApply(rowData)
      rowData.r.getValue("A") should equal("2017-06-21")
    }
  }

}
