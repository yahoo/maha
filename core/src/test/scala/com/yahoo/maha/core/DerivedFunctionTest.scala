package com.yahoo.maha.core

import java.util.TimeZone

import com.yahoo.maha.core.DruidDerivedFunction._
import org.joda.time.DateTimeZone
import org.json4s.JObject
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

  test("All Derived Functions should generate proper JSON Strings.") {
    val gid = GET_INTERVAL_DATE("fieldName", "yyyyMMdd")
    val dow = DAY_OF_WEEK("fieldName")
    val dtf = DATETIME_FORMATTER("fieldName", 0, 10)
    val dd = DECODE_DIM("fieldName", "arg1", "decodeVal1", "arg2", "decodeVal2", "default")
    val js = JAVASCRIPT("fieldName", "function(x) { return x > 0; }")
    val rgx = REGEX("fieldName", "blah", 0, true, "t")
    val lu = LOOKUP("namespace", "val", Map("a" -> "b"))
    val lwd = LOOKUP_WITH_DECODE("namespace", "valCol", Map("b" -> "a"), "arg1", "decodeVal1", "arg2", "decodeVal2", "default")
    val lwe = LOOKUP_WITH_EMPTY_VALUE_OVERRIDE("namespace", "valCol", "ovr", Map("c" -> "d"))
    val lwo = LOOKUP_WITH_DECODE_ON_OTHER_COLUMN("namespace", "valCol", "valToCheck", "valIfMatched", "valIfNot", Map("2" -> "4", "b" -> "a"))
    val ltf = LOOKUP_WITH_TIMEFORMATTER("namespace", "valCol", "yyyyMMdd", "yyyy", Map("do" -> "dont"), Some("override"))
    val ldr = LOOKUP_WITH_DECODE_RETAIN_MISSING_VALUE("namespace", "valCol", true, true, Map("rtn" -> "not"), "arg1", "decodeVal1", "arg2", "decodeVal2", "default")
    val dtz = DRUID_TIME_FORMAT("format", DateTimeZone.forID("Asia/Jakarta"))
    val dpg = DRUID_TIME_FORMAT_WITH_PERIOD_GRANULARITY("format", "P1D", DateTimeZone.forID("Asia/Jakarta"))
    val rc = TIME_FORMAT_WITH_REQUEST_CONTEXT("yyyy")
    val lwt = LOOKUP_WITH_TIMESTAMP("namespace", "val", "fmt", Map.empty, Some("ovrVal"), asMillis = false)

    val resultArray = List(gid, dow, dtf, dd, js, rgx, lu, lwd, lwe, lwo, ltf, ldr, dtz, dpg, rc, lwt)

    val expectedJSONs = List(
      """{"function_type":"GET_INTERVAL_DATE","fieldName":"fieldName","format":"yyyyMMdd"}""",
      """{"function_type":"DAY_OF_WEEK","fieldName":"fieldName"}""",
      """{"function_type":"DATETIME_FORMATTER","fieldName":"fieldName","index":0,"length":10}""",
      """{"function_type":"DECODE_DIM","fieldName":"fieldName","args":"arg1,decodeVal1,arg2,decodeVal2,default"}""",
      """{"function_type":"JAVASCRIPT","fieldName":"fieldName","function":"function(x) { return x > 0; }"}""",
      """{"function_type":"REGEX","fieldName":"fieldName","expr":"blah","index":0,"replaceMissingValue":true,"replaceMissingValueWith":"t"}""",
      """{"function_type":"LOOKUP","lookupNamespace":"namespace","valueColumn":"val","dimensionOverrideMap":{"a":"b"}}""",
      """{"function_type":"LOOKUP_WITH_DECODE","lookupNamespace":"namespace","valueColumn":"valCol","dimensionOverrideMap":{"b":"a"},"args":"arg1,decodeVal1,arg2,decodeVal2,default"}""",
      """{"function_type":"LOOKUP_WITH_EMPTY_VALUE_OVERRIDE","lookupNamespace":"namespace","valueColumn":"valCol","overrideValue":"ovr","dimensionOverrideMap":{"c":"d"}}""",
      """{"function_type":"LOOKUP_WITH_DECODE_ON_OTHER_COLUMN","lookupNamespace":"namespace","columnToCheck":"valCol","valueToCheck":"valToCheck","columnIfValueMatched":"valIfMatched","columnIfValueNotMatched":"valIfNot","dimensionOverrideMap":{"2":"4","b":"a"}}""",
      """{"function_type":"LOOKUP_WITH_TIMEFORMATTER","lookupNamespace":"namespace","valueColumn":"valCol","inputFormat":"yyyyMMdd","resultFormat":"yyyy","dimensionOverrideMap":{"do":"dont"}}""",
      """{"function_type":"LOOKUP_WITH_DECODE_RETAIN_MISSING_VALUE","lookupNamespace":"namespace","valueColumn":"valCol","retainMissingValue":true,"injective":true,"dimensionOverrideMap":{"rtn":"not"},"args":"arg1,decodeVal1,arg2,decodeVal2,default"}""",
      """{"function_type":"DRUID_TIME_FORMAT","format":"format","zone":"Asia/Jakarta"}""",
      """{"function_type":"DRUID_TIME_FORMAT_WITH_PERIOD_GRANULARITY","format":"format","period":"P1D","zone":"Asia/Jakarta"}""",
      """{"function_type":"TIME_FORMAT_WITH_REQUEST_CONTEXT","format":"yyyy"}""",
      """{"function_type":"LOOKUP_WITH_TIMESTAMP","lookupNamespace":"namespace","valueColumn":"val","resultFormat":"fmt","dimensionOverrideMap":{},"overrideValue":"ovrVal","asMillis":false}"""
    )

    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    implicit val formats = DefaultFormats

    val allJSONs: List[JObject] = resultArray.map(expn => expn.asJSON)
    val allJsonStrings: List[String] = allJSONs.map(json => compact(json))

    assert(allJsonStrings.forall(str => expectedJSONs.contains(str)))
  }
}
