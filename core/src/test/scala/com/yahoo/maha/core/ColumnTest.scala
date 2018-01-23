// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import com.yahoo.maha.core.fact.ConstFactCol
import org.scalatest.{FunSuite, Matchers}
import com.yahoo.maha.core.dimension.{ConstDimCol, DimCol}

/**
 * Created by shengyao on 2/9/16.
 */
class ColumnTest extends FunSuite with Matchers {
  test("register duplicate column in one column context should fail") {
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext {implicit cc : ColumnContext =>
        val col = DimCol("dimcol1", IntType())
        cc.register(col)
        cc.register(col)
      }
    }
    thrown.getMessage should startWith ("requirement failed: Column already exists : dimcol1")
  }

  test("render column that does not exist in column context should fail") {
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext {implicit cc : ColumnContext =>
        val col = DimCol("dimcol1", IntType())
        cc.render("dimcol2", Map.empty)
      }
    }
    thrown.getMessage should startWith ("requirement failed: Column doesn't exist: dimcol2")
  }

  test("Const Cols Tests") {
    ColumnContext.withColumnContext {implicit cc : ColumnContext =>
      val col = ConstFactCol("field", IntType(), "0")
      require(col.constantValue == "0")
      val dimCol = ConstDimCol("field1", StrType(), "Y")
      require(dimCol.constantValue == "Y")
    }
  }

}
