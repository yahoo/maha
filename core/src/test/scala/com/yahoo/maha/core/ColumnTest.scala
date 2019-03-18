// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import com.yahoo.maha.core.fact.{ConstFactCol, DruidConstDerFactCol, HiveDerFactCol, NoopRollup}
import org.scalatest.{FunSuite, Matchers}
import com.yahoo.maha.core.dimension.{ConstDimCol, DimCol}

/**
 * Created by shengyao on 2/9/16.
 */
class ColumnTest extends FunSuite with Matchers {
  test("register duplicate column in one column context should fail") {
    val thrown = intercept[IllegalArgumentException] {
      ColumnContext.withColumnContext {implicit cc : ColumnContext =>
        import HiveExpression._
        val col = DimCol("dimcol1", IntType())
        val col2 = HiveDerFactCol("noop_rollup_spend", DecType(0, "0.0"), HiveDerivedExpression("{spend}" * "{forecasted_clicks}" / "{actual_clicks}" * "{recommended_bid}" / "{modified_bid}"), rollupExpression = NoopRollup, annotations = Set(EscapingRequired))
        cc.register(col)
        cc.register(col)
        ColumnContext.validateColumnContext(Set(col), "no prefix")
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
      val colname = cc.getColumnByName("field")
      assert(colname.get.dataType == IntType(0,None,None,None,None))
    }
  }

  test("Attempt to validate columns in the CC") {
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      import HiveExpression._
      val col = DimCol("dimcol100", IntType())
      val col2 = HiveDerFactCol("noop_rollup_spend", DecType(0, "0.0"), HiveDerivedExpression("{spend}" * "{forecasted_clicks}" / "{actual_clicks}" * "{recommended_bid}" / "{modified_bid}"), rollupExpression = NoopRollup)
      ColumnContext.validateColumnContext(Set(col, col2), "no prefix")
    }
  }

  test("DruidConstDerFactCol test") {
    ColumnContext.withColumnContext { implicit cc: ColumnContext =>
      DimCol("dimCol", IntType())
     val col =  DruidConstDerFactCol("druidConst",None,  DecType(), "val", cc, "{dimCol}" * 100, Set(EscapingRequired, CaseInsensitive), NoopRollup, Set.empty)
      ColumnContext.withColumnContext {
        implicit cc:ColumnContext=>
          col.copyWith(cc, Map.empty, true)
      }
    }
  }
}
