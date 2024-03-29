// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.request

import com.yahoo.maha.core.request.ReportFormatType.CSVFormat
import com.yahoo.maha.core.{Engine, HiveEngine}
import org.json4s._
import org.json4s.jackson.JsonMethods
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.HashMap

/**
 * Created by huizhang on 11/18/16.
 */
class ParameterTest extends AnyFunSuite with Matchers {

  test("SerializeParameters should serialize a map of parameters into a List") {
    val map_parameters = new HashMap[Parameter, ParameterValue[_]]
    map_parameters.put(Parameter.ReportFormat, ReportFormatValue(ReportFormatType.CSVFormat))
    map_parameters.put(Parameter.DryRun, DryRunValue(false))
    map_parameters.put(Parameter.GeneratedQuery, GeneratedQueryValue("GeneratedQuery"))
    map_parameters.put(Parameter.QueryEngine, QueryEngineValue(Engine.from("hive").get))
    map_parameters.put(Parameter.Debug, DebugValue(true))
    map_parameters.put(Parameter.RequestId, RequestIdValue("RequestId"))
    map_parameters.put(Parameter.UserId, UserIdValue("UserId"))
    map_parameters.put(Parameter.TimeZone, TimeZoneValue("TimeZone"))
    map_parameters.put(Parameter.Schema, SchemaValue("Schema"))
    map_parameters.put(Parameter.Distinct, DistinctValue(true))
    map_parameters.put(Parameter.JobName, JobNameValue("tools_1"))
    map_parameters.put(Parameter.RegistryName, RegistryNameValue("mahaRegistry"))
    map_parameters.put(Parameter.HostName, HostNameValue("127.0.0.1"))

    val result = Parameter.serializeParameters(map_parameters.toMap)
    result.length shouldBe map_parameters.size

    val newMap = result.map(t=> t._1 -> t._2).toMap
    for((k,v) <- map_parameters) {
      newMap.get(k.entryName).get match{
        case JString(x) => v.value match {
          case CSVFormat => x shouldBe "csv"
          case HiveEngine => x shouldBe "Hive"
          case _ => x shouldBe v.value
        }
        case JBool(x) => x shouldBe v.value
        case _ => fail
      }
    }

  }

  test("DeserializeParameters should deserialize a JSON into a Map of parameter values") {
    val inputJson=
      """
        |{
        | "Report-Format": "csv",
        | "Dry-Run": false,
        | "Generated-Query": "Generated-Query",
        | "Query-Engine": "oracle",
        | "debug": true,
        | "Request-Id": "Request-Id",
        | "User-Id": "User-Id",
        | "TimeZone": "TimeZone",
        | "Schema": "Schema",
        | "Distinct": true,
        | "Job-Name": "Job-Name",
        | "RegistryName": "mahaRegistry",
        | "HostName": "127.0.0.1"
        |}
        |""".stripMargin
    val result = Parameter.deserializeParameters(JsonMethods.parse(inputJson))

    result.getOrElse() match{
      case m: Map[Parameter, ParameterValue[_]] => {
        m.size shouldBe 13
        m.get(Parameter.ReportFormat).get shouldBe ReportFormatValue(ReportFormatType.CSVFormat)
        m.get(Parameter.DryRun).get shouldBe DryRunValue(false)
        m.get(Parameter.GeneratedQuery).get shouldBe GeneratedQueryValue("Generated-Query")
        m.get(Parameter.QueryEngine).get shouldBe QueryEngineValue(Engine.from("oracle").get)
        m.get(Parameter.Debug).get shouldBe DebugValue(true)
        m.get(Parameter.RequestId).get shouldBe RequestIdValue("Request-Id")
        m.get(Parameter.UserId).get shouldBe UserIdValue("User-Id")
        m.get(Parameter.TimeZone).get shouldBe TimeZoneValue("TimeZone")
        m.get(Parameter.Schema).get shouldBe SchemaValue("Schema")
        m.get(Parameter.Distinct).get shouldBe DistinctValue(true)
        m.get(Parameter.JobName).get shouldBe JobNameValue("Job-Name")
        m.get(Parameter.RegistryName).get shouldBe RegistryNameValue("mahaRegistry")
        m.get(Parameter.HostName).get shouldBe HostNameValue("127.0.0.1")
      }
      case _ => fail
    }

  }

  test("Should render extra columnInfo") {
    val inputJson =
      """
        |{
        |  "AdditionalColumnInfo":
        |  [
        |    {"field": "colA", "value": "123"},
        |    {"field": "colB", "value": "potatoes"}
        |    ]
        |}
        |""".stripMargin

    val result = Parameter.deserializeParameters(JsonMethods.parse(inputJson))
    val expectedOutput = List(Field("colA", None, Some("123")), Field("colB", None, Some("potatoes")))
    result.getOrElse() match {
      case m: Map[Parameter, ParameterValue[_]] => {
        m.size shouldBe 1
        m(Parameter.AdditionalColumnInfo) shouldBe AdditionalColumnInfoValue(expectedOutput)

      }
    }
    val additionalInfoValue = result.getOrElse().asInstanceOf[Map[Parameter, ParameterValue[_]]].get(Parameter.AdditionalColumnInfo)
    additionalInfoValue.get.asInstanceOf[AdditionalColumnInfoValue].value shouldBe expectedOutput
  }

  test("Demo fixed AdditionalColumnInfo") {
    val inputJson =
      """
        |{
        |  "AdditionalColumnInfo":
        |  [
        |    {"field": "colA", "value": "123"},
        |    {"field": "colB", "value": "potatoes"}
        |    ]
        |}
        |""".stripMargin

    val result = Parameter.deserializeParameters(JsonMethods.parse(inputJson))
    val expectedOutput = List(Field("colA", None, Some("123")), Field("colB", None, Some("potatoes")))
    val serialized = Parameter.serializeParameter(Parameter.AdditionalColumnInfo, result.toOption.get(Parameter.AdditionalColumnInfo))
    println(serialized)
    val deserialized = Parameter.deserializeParameter(serialized._1, serialized._2)
    println(deserialized)
    assert(deserialized.get.isSuccess)
    val addInfo = deserialized.get.toOption.get._2
    addInfo shouldBe AdditionalColumnInfoValue(expectedOutput)
  }

}
