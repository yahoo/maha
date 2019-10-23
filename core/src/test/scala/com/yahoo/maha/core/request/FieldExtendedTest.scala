package com.yahoo.maha.core.request

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{FunSuite, Matchers}

class FieldExtendedTest extends FunSuite with Matchers {

  test("Verify same success returned for optional and non-optional FieldExtended") {
    val json: String =
      s"""
         |{
         |  "name": "Blueberry",
         |  "type": 1
         |}
       """.stripMargin

    val jVal: JValue = parse(json)

    val defaultSuccess = fieldExtended[String]("name")(jVal)
    val optionalSuccess = optionalFieldExtended[String]("name", "default")(jVal)

    assert(defaultSuccess.isSuccess && optionalSuccess.isSuccess)
    assert(defaultSuccess.getOrElse("a").equals(optionalSuccess.getOrElse("b")))
  }

  test("Verify fieldExtended failure returns default value on Option.") {
    val json: String =
      s"""
         |{
         |  "name": "Blueberry",
         |  "type": 1
         |}
       """.stripMargin

    val jVal: JValue = parse(json)


    val failure = fieldExtended[String]("failure")(jVal)
    val working = optionalFieldExtended[String]("failure", "def")(jVal)

    assert(failure.isFailure)
    assert(failure.forall(f => f.contains("NoSuchFieldError")))
    assert(working.isSuccess)
    assert(working.getOrElse("").equals("def"))

    val wrongType = fieldExtended[String]("type")(jVal)
    val wrongTypeDefault = optionalFieldExtended[String]("type", "0")(jVal)

    assert(wrongType.isFailure)
    assert(wrongType.forall(f => f.contains("unexpected value : JInt")))
    assert(wrongTypeDefault.isSuccess)
    assert(wrongTypeDefault.getOrElse("").equals("0"))
  }

}
