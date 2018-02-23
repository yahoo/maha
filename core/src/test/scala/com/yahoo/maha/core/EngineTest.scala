package com.yahoo.maha.core

import org.scalatest.{FunSuiteLike, Matchers}

class EngineTest extends FunSuiteLike with Matchers{
  test("Ensure a valid return in Engine.from(String)") {
    val p = Engine.from("presto")
    assert(p.get.equals(PrestoEngine))
    val o = Engine.from("oracle")
    assert(o.get.equals(OracleEngine))
    val d = Engine.from("druid")
    assert(d.get.equals(DruidEngine))
    val h = Engine.from("hive")
    assert(h.get.equals(HiveEngine))
    val fail = Engine.from("fail")
    assert(fail.equals(None))
  }
}
