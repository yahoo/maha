// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.config

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.scalaz.JsonScalaz._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Created by pranavbhole on 31/05/17.
 */
class BaseJsonModelsTest extends AnyFunSuite with Matchers {

  test("successfully parse json JsonBucketingConfig") {
    val jsonString = """{
                       |	"factoryClass": "name",
                       |	"config": { "c1" : "v1" }
                       |}""".stripMargin
    val json = parse(jsonString)
    val result = fromJSON[JsonBucketingConfig](json)
    assert(result.isSuccess && result.toOption.get.className === "name" && result.toOption.get.json.isInstanceOf[JObject])
  }

  test("successfully parse json JsonUTCTimeProviderConfig") {
    val jsonString = """{
                       |	"factoryClass": "name",
                       |	"config": { "c1" : "v1" }
                       |}""".stripMargin
    val json = parse(jsonString)
    val result = fromJSON[JsonUTCTimeProviderConfig](json)
    assert(result.isSuccess && result.toOption.get.className === "name" && result.toOption.get.json.isInstanceOf[JObject])
  }

  test("successfully parse json JsonQueryGeneratorConfig") {
    val jsonString = """{
                       |	"factoryClass": "name",
                       |	"config": { "c1" : "v1" }
                       |}""".stripMargin
    val json = parse(jsonString)
    val result = fromJSON[JsonQueryGeneratorConfig](json)
    assert(result.isSuccess && result.toOption.get.className === "name" && result.toOption.get.json.isInstanceOf[JObject])
  }

  test("successfully parse json JsonQueryExecutorConfig") {
    val jsonString = """{
                       |	"factoryClass": "name",
                       |	"config": { "c1" : "v1" }
                       |}""".stripMargin
    val json = parse(jsonString)
    val result = fromJSON[JsonQueryExecutorConfig](json)
    assert(result.isSuccess && result.toOption.get.className === "name" && result.toOption.get.json.isInstanceOf[JObject])
  }

  test("Validate JSONHelpers") {
    val jsonString = """{
                   |	"factoryClass": "name",
                   |	"config": { "c1" : "v1" }
                   |}""".stripMargin

    assert(JsonHelpers.nonEmptyString("", "", "").isFailure)
    assert(JsonHelpers.nonEmptyList(List.empty, "", "").isFailure)
    assert(JsonHelpers.nonEmptyString(jsonString, "", "").isSuccess)
    assert(JsonHelpers.nonEmptyList(List(jsonString), "", "").isSuccess)
  }
}
