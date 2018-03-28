package com.yahoo.maha.core

import com.yahoo.maha.core.DruidDerivedFunction.{DECODE_DIM, GET_INTERVAL_DATE, LOOKUP_WITH_DECODE}
import org.scalatest.{FunSuiteLike, Matchers}

class DerivedFunctionTest extends FunSuiteLike with Matchers {
  test("Create a DECODE_DIM failure cases") {
    val minLengthCatch = intercept[IllegalArgumentException] {
      new DECODE_DIM("fieldName", "tooFewArgs")
    }
    assert(minLengthCatch.getMessage.contains("Usage: DECODE( expression , search , result [, search , result]... [, default] )"))
  }

  test("Create value DECODE_DIM") {
    val newDecode = new DECODE_DIM("fieldName", "arg1", "decodeVal1", "arg2", "decodeVal2", "default")
    val newDecodeWithoutDefault = new DECODE_DIM("fieldName", "arg1", "decodeVal1", "arg2", "decodeVal2")
    assert(newDecode.apply("arg1") == Some("decodeVal1"))
    assert(newDecode.apply("arg3") == Some("default"))
    assert(newDecodeWithoutDefault.apply("arg3") == None)
    assert(newDecode.apply.isDefinedAt("arg20"))
  }

  test("Attempt LOOKUP_WITH_DECODE fail") {
    val minLengthCatch = intercept[IllegalArgumentException] {
      new LOOKUP_WITH_DECODE("fieldNameSpace", "valueCol", dimensionOverrideMap = Map.empty, "tooFewArgs")
    }
    assert(minLengthCatch.getMessage.contains("Usage: DECODE( expression , search , result [, search , result]... [, default] )"))
  }

  test("Failure to get interval date with blank format") {
    val thrown = intercept[IllegalArgumentException]{
      GET_INTERVAL_DATE.checkFormat("")
    }
    assert(thrown.getMessage.contains("Format for get_interval_date must be d|w|m|day|yr not"))
  }
}
