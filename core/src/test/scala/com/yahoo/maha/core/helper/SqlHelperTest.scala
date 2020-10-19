package com.yahoo.maha.core.helper

import com.yahoo.maha.core.query.{InnerJoin, LeftOuterJoin, RightOuterJoin}
import com.yahoo.maha.core.{DruidEngine, HiveEngine, OracleEngine, PrestoEngine}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SqlHelperTest extends AnyFunSuite with Matchers{
  test("Attempt to use each join-able engine") {
    assert(SqlHelper.getJoinString(InnerJoin, HiveEngine).contains("INNER JOIN"))
    assert(SqlHelper.getJoinString(LeftOuterJoin, OracleEngine).contains("LEFT OUTER JOIN"))
    assert(SqlHelper.getJoinString(RightOuterJoin, PrestoEngine).contains("RIGHT OUTER JOIN"))
  }

  test("Should fail with DruidEngine") {
    val thrown = intercept[IllegalArgumentException] {
      SqlHelper.getJoinString(InnerJoin, DruidEngine)
    }
    assert(thrown.getMessage.contains("Unexpected Engine"))
  }

  test("Should fail with invalid JoinType") {
    val thrown = intercept[IllegalArgumentException] {
      SqlHelper.getJoinString(null, OracleEngine)
    }
    assert(thrown.getMessage.contains("Unexpected Join Type"))
  }
}
