// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import java.nio.charset.StandardCharsets

import com.yahoo.maha.core.CoreSchema.AdvertiserSchema
import com.yahoo.maha.core.request.ReportFormatType.{CSVFormat, ExcelFormat, JsonFormat}
import com.yahoo.maha.core.request._
import org.apache.commons.lang3.StringUtils.EMPTY
import org.joda.time.DateTimeZone
import org.json4s.scalaz.JsonScalaz
import org.scalatest.flatspec.AnyFlatSpec
import scalaz.{IList, ValidationNel}

/**
 * Created by jians on 10/5/15.
 */
class ReportingRequestTest extends AnyFlatSpec {

  implicit class IListExists(ilist: IList[JsonScalaz.Error]) {
    def exists(f : scala.Function1[JsonScalaz.Error, scala.Boolean]): Boolean  = {
      ilist.find(f).isJust
    }
  }

  CoreSchema.register()

  def getReportingRequestSync(jsonString: String) = {
    getReportingRequestValidationSync(jsonString, AdvertiserSchema).toOption.get
  }

  def getReportingRequestValidationSync(jsonString: String) = {
    ReportingRequest.deserializeSync(jsonString.getBytes(StandardCharsets.UTF_8), None)
  }

  def getReportingRequestValidationSyncWithFactBias(jsonString: String) = {
    ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8))
  }

  def getReportingRequestSync(jsonString: String, schema: Schema) = {
    getReportingRequestValidationSync(jsonString, schema).toOption.get
  }

  def getReportingRequestValidationSyncWithFactBias(jsonString: String, schema: Schema) = {
    ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), schema)
  }

  def getReportingRequestValidationSync(jsonString: String, schema: Schema) = {
    ReportingRequest.deserializeSync(jsonString.getBytes(StandardCharsets.UTF_8), schema)
  }

  def getReportingRequest(jsonString: String) = {
    getReportingRequestValidation(jsonString, AdvertiserSchema).toOption.get
  }

  def getReportingRequestValidation(jsonString: String) = {
    ReportingRequest.deserializeAsync(jsonString.getBytes(StandardCharsets.UTF_8), None)
  }

  def getReportingRequest(jsonString: String, schema: Schema) = {
    getReportingRequestValidation(jsonString, schema).toOption.get
  }

  def getReportingRequestValidation(jsonString: String, schema: Schema) = {
    ReportingRequest.deserializeAsync(jsonString.getBytes(StandardCharsets.UTF_8), schema)
  }

  def getReportingRequestValidationAsyncWithAdditionalParameters(jsonString: String, bias: Option[Bias]) = {
    ReportingRequest.deserializeWithAdditionalParameters(jsonString.getBytes(StandardCharsets.UTF_8), bias)
  }

  def getScheduledReportingRequestValidation(jsonString: String, schema: Schema) = {
    ReportingRequest.deserializeWithAdditionalParamsDefaultDayFilter(jsonString.getBytes(StandardCharsets.UTF_8), schema, Option.apply(null), true)
  }

  "ReportingRequest" should "should be extracted from a json object as async request" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Campaign ID", "operator": "like", "value": "12345"},
                              {"field": "Ad Group ID", "operator": "in", "values": ["12345", "67890"]},
                              {"field": "Ad ID", "operator": "not in", "values": ["12345", "67890"]},
                              {"field": "Product ID", "operator": "<>", "value": "-3"},
                              {"field": "Match Type", "operator": "isnull"},
                              {"field": "Pricing Type", "operator": "isnotnull"},
                              {"field": "Day", "operator": "datetimebetween", "from": "2014-04-01T00:00:00.000Z", "to": "2014-04-30T00:00:00.000Z", "format": "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"},
                              {"field": "Turtles", "operator": "==", "compareTo": "Llamas"},
                              {"field": "Viewers", "operator": ">", "value": "10"},
                              {"field": "Viewees", "operator": "<", "value": "10"}
                           ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequest(jsonString, AdvertiserSchema)
    assert(request.requestType === AsyncRequest)
    val ser = new String(ReportingRequest.serialize(request), StandardCharsets.UTF_8)
    val deser = getReportingRequest(ser, AdvertiserSchema)

    assert(request === deser)
    assert(request.numDays === 29)
    assert(request.dayFilter.operator === DateTimeBetweenFilterOperation)
    assert(request.dayFilter.asInstanceOf[DateTimeBetweenFilter].field === "Day")
    assert(request.dayFilter.asInstanceOf[DateTimeBetweenFilter].from === "2014-04-01T00:00:00.000Z")
    assert(request.dayFilter.asInstanceOf[DateTimeBetweenFilter].to === "2014-04-30T00:00:00.000Z")
    assert(request.dayFilter.asInstanceOf[DateTimeBetweenFilter].format === "yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
  }

  "ReportingRequest" should "should be extracted from a json object as sync request" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "2014-04-01", "to": "2014-04-30"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    assert(request.requestType === SyncRequest)
    val ser = new String(ReportingRequest.serialize(request), StandardCharsets.UTF_8)
    val deser = getReportingRequestSync(ser)

    assert(request === deser)
    assert(request.dayFilter.operator === BetweenFilterOperation)
    assert(request.dayFilter.asInstanceOf[BetweenFilter].field === "Day")
    assert(request.dayFilter.asInstanceOf[BetweenFilter].from === "2014-04-01")
    assert(request.dayFilter.asInstanceOf[BetweenFilter].to === "2014-04-30")
  }

  "ReportingRequest" should "fail when datetime between filter with Hour and Minute filters are provided" in {
    val jsonString = s"""{
                          "cube": "k_stats_minute_grain",
                          "selectFields": [
                            {"field": "Week"},
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Source"},
                            {"field": "Clicks"},
                            {"field": "CTR"},
                            {"field": "Reblogs"},
                            {"field": "Reblog Rate"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "datetimebetween", "from": "2020-01-01T00:00:00.000Z", "to": "2020-01-07T00:00:00.000Z", "format": "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"},
                            {"field": "Hour", "operator": "between", "from": "02", "to": "05"},
                            {"field": "Minute", "operator": "between", "from": "59", "to": "03"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""
    val request: ValidationNel[JsonScalaz.Error, ReportingRequest] = getReportingRequestValidation(jsonString, AdvertiserSchema)
    request.leftMap { nel =>
      require(nel.list.exists(e => e.toString.contains("UncategorizedError") && e.toString.contains("Day and Hour filters operator mismatch") === true),
        s"invalid json format did not throw error: $request")
    }
  }

  "ReportingRequest without filters (except Day)" should "should be extracted from a json object" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Day", "operator": "between", "from": "2014-04-01", "to": "2014-04-30"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequest(jsonString, AdvertiserSchema)
    val ser = new String(ReportingRequest.serialize(request), StandardCharsets.UTF_8)

  }

  "ReportingRequest without ordering" should "should be extracted from a json object" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Day", "operator": "between", "from": "2014-04-01", "to": "2014-04-30"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequest(jsonString, AdvertiserSchema)
    val ser = new String(ReportingRequest.serialize(request), StandardCharsets.UTF_8)

  }

  "ReportingRequest without si" should "should be extracted from a json object" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Day", "operator": "between", "from": "2014-04-01", "to": "2014-04-30"}
                          ],
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequest(jsonString, AdvertiserSchema)
    val ser = new String(ReportingRequest.serialize(request), StandardCharsets.UTF_8)

  }

  "ReportingRequest without mr" should "should be extracted from a json object" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Day", "operator": "between", "from": "2014-04-01", "to": "2014-04-30"}
                          ]
                          }"""

    val request: ReportingRequest = getReportingRequest(jsonString, AdvertiserSchema)
    val ser = new String(ReportingRequest.serialize(request), StandardCharsets.UTF_8)

  }

  "ReportingRequest with invalid fields json" should "fail" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"fieldName": "Ad ID"},
                              {"fieldName": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Day", "operator": "between", "from": "2014-04-01", "to": "2014-04-30"},
                              {"field": "Advertiser ID", "operator": "blah", "value": "12345"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ValidationNel[JsonScalaz.Error, ReportingRequest] = getReportingRequestValidation(jsonString, AdvertiserSchema)
    request.leftMap { nel =>
      require(nel.list.exists(e => e.toString.contains("NoSuchFieldError") && e.toString.contains("fieldName") === true),
        s"invalid json format did not throw error: $request")
    }
  }

  "ReportingRequest with invalid filters json" should "fail" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"fieldName": "Day", "operator": "between", "from": "2014-04-01", "to": "2014-04-30"},
                              {"fieldName": "Advertiser ID", "operator": "blah", "value": "12345"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ValidationNel[JsonScalaz.Error, ReportingRequest] = getReportingRequestValidation(jsonString, AdvertiserSchema)
    request.leftMap { nel =>
      require(nel.list.exists(e => e.toString.contains("NoSuchFieldError") && e.toString.contains("fieldName") === true),
        s"invalid json format did not throw error: $request")
    }
  }

  "ReportingRequest with bad filter operator" should "fail with operator error" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Day", "operator": "between", "from": "2014-04-01", "to": "2014-04-30"},
                              {"field": "Advertiser ID", "operator": "blah", "value": "12345"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ValidationNel[JsonScalaz.Error, ReportingRequest] = getReportingRequestValidation(jsonString, AdvertiserSchema)
    request.leftMap { nel =>
      require(nel.list.exists(e => e.toString.contains("operator") && e.toString.contains("blah") === true ),
        s"invalid operator did not throw error: $request")
    }
  }

  "ReportingRequest with bad order operator" should "fail with order error" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "2014-04-01", "to": "2014-04-30"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "blah"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ValidationNel[JsonScalaz.Error, ReportingRequest] = getReportingRequestValidation(jsonString, AdvertiserSchema)
    request.leftMap(_.foreach(e => assert(e.toString.contains("order") && e.toString.contains("blah") === true) ))
  }

  "ReportingRequest with different day and hour operator" should "fail with operator mismatch" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "2014-04-01", "to": "2014-04-30"},
                              {"field": "Hour", "operator": "=", "value": "1"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ValidationNel[JsonScalaz.Error, ReportingRequest] = getReportingRequestValidation(jsonString, AdvertiserSchema)
    request.leftMap { nel =>
      require(nel.list.exists(e => e.toString.contains("operator mismatch") === true ),
        s"operator mismatch did not throw error : $request")
    }
  }

  "ReportingRequest with different day and minute operator" should "fail with operator mismatch" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "2014-04-01", "to": "2014-04-30"},
                              {"field": "Hour", "operator": "between", "from": "01", "to": "02"},
                              {"field": "Minute", "operator": "=", "value": "1"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ValidationNel[JsonScalaz.Error, ReportingRequest] = getReportingRequestValidation(jsonString, AdvertiserSchema)
    request.leftMap { nel =>
      require(nel.list.exists(e => e.toString.contains("operator mismatch") === true ),
        s"operator mismatch did not throw error : $request")
    }
  }

  "ReportingRequest with invalid day filter format" should "fail" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "20140401", "to": "20140430"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ValidationNel[JsonScalaz.Error, ReportingRequest] = getReportingRequestValidation(jsonString, AdvertiserSchema)
    request.leftMap { nel =>
      require(nel.list.exists(e => e.toString.contains("Failed to parse field=Day") === true ),
        s"invalid date format did not throw error: $request")
    }
  }

  "ReportingRequest with invalid hour filter format" should "fail" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "2014-04-01", "to": "2014-04-30"},
                              {"field": "Hour", "operator": "between", "from": "00", "to": "25"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ValidationNel[JsonScalaz.Error, ReportingRequest] = getReportingRequestValidation(jsonString, AdvertiserSchema)
    request.leftMap { nel =>
      require(nel.list.exists(e => e.toString.contains("Failed to parse field=Hour") === true ),
        s"invalid date format did not throw error: $request")
    }
  }

  "ReportingRequest with invalid minute filter format" should "fail" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "2014-04-01", "to": "2014-04-30"},
                              {"field": "Hour", "operator": "between", "from": "01", "to": "02"},
                              {"field": "Minute", "operator": "between", "from": "00", "to": "61"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ValidationNel[JsonScalaz.Error, ReportingRequest] = getReportingRequestValidation(jsonString, AdvertiserSchema)
    request.leftMap { nel =>
      require(nel.list.exists(e => e.toString.contains("Failed to parse field=Minute") === true ),
        s"invalid date format did not throw error: $request")
    }
  }

  "ReportingRequest with missing day filter" should "fail" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ValidationNel[JsonScalaz.Error, ReportingRequest] = getReportingRequestValidation(jsonString, AdvertiserSchema)
    request.leftMap { nel =>
      require(nel.list.exists(e => e.toString.contains("Day filter not found") === true ),
        s"invalid date format did not throw error: $request")
    }
  }

  "ReportingRequest with invalid parameter data type" should "fail" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "2014-04-01", "to": "2014-04-30"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100,
                          "additionalParameters" : {
                               "Content-Type" : false,
                               "Dry-Run" : "string",
                               "Generated-Query" : false,
                               "Query-Engine" : false,
                               "debug" : "string"
                          }
                          }"""

    val request =  ReportingRequest.deserializeWithAdditionalParameters(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)

    assert(request.isFailure)
  }

  "ReportingRequest with valid parameter data type" should "succeed" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "2014-04-01", "to": "2014-04-30"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100,
                          "additionalParameters" : {
                              "Report-Format" : "csv",
                              "Dry-Run" : true,
                              "Generated-Query" : "generated-query",
                              "Query-Engine" : "druid",
                              "debug" : true,
                              "registryName" : "maha",
                              "testName" : "test1",
                              "labels" : ["label1","label2"],
                              "HostName" : "127.0.0.1"
                          }
                          }"""
    val request =  ReportingRequest.deserializeWithAdditionalParameters(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)

    assert(request.isSuccess)
  }

  "ReportingRequest with curators" should "succeed" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "curators": {
                            "timeshift": {
                              "config": {
                                "k1": "v1",
                                "k2": "v2"
                              }
                            },
                            "drilldown": {
                              "config": {
                                "k1": "v1"
                              }
                            }
                          },
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "2014-04-01", "to": "2014-04-30"},
                              {"field": "Hour", "operator": "between", "from": "00", "to": "23"},
                              {"field": "Minute", "operator": "between", "from": "00", "to": "59"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""
    val request: ReportingRequest = getReportingRequest(jsonString, AdvertiserSchema)
    assert(request.curatorJsonConfigMap.nonEmpty)
    assert(request.curatorJsonConfigMap.size === 2)
    assert(request.curatorJsonConfigMap("timeshift").json.values === Map("k1"->"v1", "k2"->"v2"))
    assert(request.curatorJsonConfigMap("drilldown").json.values === Map("k1"->"v1"))
  }

  "ReportingRequest with out curators" should "inject default curator" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "2014-04-01", "to": "2014-04-30"},
                              {"field": "Hour", "operator": "between", "from": "00", "to": "23"},
                              {"field": "Minute", "operator": "between", "from": "00", "to": "59"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""
    val request: ReportingRequest = getReportingRequest(jsonString, AdvertiserSchema)
    assert(request.curatorJsonConfigMap.nonEmpty)
    assert(request.curatorJsonConfigMap.size === 1)
    assert(request.curatorJsonConfigMap("default").json.values === Map.empty)
  }

  "ReportingRequest with day and hour filter between operator" should "succeed" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "2014-04-01", "to": "2014-04-30"},
                              {"field": "Hour", "operator": "between", "from": "00", "to": "23"},
                              {"field": "Minute", "operator": "between", "from": "00", "to": "59"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequest(jsonString, AdvertiserSchema)
    assert(request.dayFilter.operator === BetweenFilterOperation)
    assert(request.dayFilter.asInstanceOf[BetweenFilter].from === "2014-04-01")
    assert(request.dayFilter.asInstanceOf[BetweenFilter].to === "2014-04-30")
    assert(request.hourFilter.isDefined)
    assert(request.hourFilter.get.operator === BetweenFilterOperation)
    assert(request.hourFilter.get.asInstanceOf[BetweenFilter].from === "00")
    assert(request.hourFilter.get.asInstanceOf[BetweenFilter].to === "23")
    assert(request.minuteFilter.isDefined)
    assert(request.minuteFilter.get.operator === BetweenFilterOperation)
    assert(request.minuteFilter.get.asInstanceOf[BetweenFilter].from === "00")
    assert(request.minuteFilter.get.asInstanceOf[BetweenFilter].to === "59")
  }

  "ReportingRequest with day filter in operator" should "succeed" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "in", "values": ["2014-04-01", "2014-04-30"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequest(jsonString, AdvertiserSchema)
    assert(request.queryType === GroupByQuery)
    assert(request.dayFilter.operator === InFilterOperation)
    assert(request.dayFilter.asInstanceOf[InFilter].values === List("2014-04-01", "2014-04-30"))
  }

  "ReportingRequest with day and hour filter equality operator" should "succeed" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"},
                              {"field": "Hour", "operator": "=", "value": "00"},
                              {"field": "Minute", "operator": "=", "value": "59"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequest(jsonString, AdvertiserSchema)
    assert(request.dayFilter.operator === EqualityFilterOperation)
    assert(request.dayFilter.asInstanceOf[EqualityFilter].value === "2014-04-01")
    assert(request.hourFilter.isDefined)
    assert(request.hourFilter.get.operator === EqualityFilterOperation)
    assert(request.hourFilter.get.asInstanceOf[EqualityFilter].value === "00")
    assert(request.minuteFilter.isDefined)
    assert(request.minuteFilter.get.operator === EqualityFilterOperation)
    assert(request.minuteFilter.get.asInstanceOf[EqualityFilter].value === "59")
  }

  "ReportingRequest with empty string for equality filter" should "fail" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "=", "value": ""}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request = getReportingRequestValidation(jsonString, AdvertiserSchema)
    assert(request.isFailure, request)
    assert(request.swap.toOption.get.head.toString contains "value,Day filter cannot have empty string", request)
  }

  "ReportingRequest with empty string for like filter" should "fail" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "like", "value": ""}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request = getReportingRequestValidation(jsonString, AdvertiserSchema)
    assert(request.isFailure, request)
    assert(request.swap.toOption.get.head.toString contains "value,Day filter cannot have empty string", request)
  }

  "ReportingRequest with empty string for IN filter values" should "fail" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "IN", "values": ["12345", ""]},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request = getReportingRequestValidation(jsonString, AdvertiserSchema)
    assert(request.isFailure, request)
    assert(request.swap.toOption.get.head.toString contains "values,Advertiser ID filter cannot have empty string", request)
  }

  "ReportingRequest with empty string for NOT IN filter values" should "fail" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "NOT IN", "values": ["12345", ""]},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request = getReportingRequestValidation(jsonString, AdvertiserSchema)
    assert(request.isFailure, request)
    assert(request.swap.toOption.get.head.toString contains "values,Advertiser ID filter cannot have empty string", request)
  }

  "ReportingRequest with empty list for IN filter values" should "fail" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "IN", "values": []},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request = getReportingRequestValidation(jsonString, AdvertiserSchema)
    assert(request.isFailure, request)
    assert(request.swap.toOption.get.head.toString contains "values,Advertiser ID filter cannot have empty list", request)
  }

  "ReportingRequest with empty list for NOT IN filter values" should "fail" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "NOT IN", "values": []},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request = getReportingRequestValidation(jsonString, AdvertiserSchema)
    assert(request.isFailure, request)
    assert(request.swap.toOption.get.head.toString contains "values,Advertiser ID filter cannot have empty list", request)
  }

  "ReportingRequest with empty string for between from value" should "fail" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "between", "from": "", "to": "2014-04-05"},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request = getReportingRequestValidation(jsonString, AdvertiserSchema)
    assert(request.isFailure, request)
    assert(request.swap.toOption.get.head.toString contains "from,Advertiser ID filter cannot have empty string", request)
  }

  "ReportingRequest with empty string for between to value" should "fail" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "between", "from": "2014-04-01", "to": ""},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request = getReportingRequestValidation(jsonString, AdvertiserSchema)
    assert(request.isFailure, request)
    assert(request.swap.toOption.get.head.toString contains "to,Advertiser ID filter cannot have empty string", request)
  }

  "ReportingRequest" should "successfully deserialize sync request with forceDimensionDriven" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100,
                          "forceDimensionDriven" : true
                          }"""

    val request = getReportingRequestValidationSync(jsonString, AdvertiserSchema)
    assert(request.isSuccess, request)
    assert(request.toOption.get.forceDimensionDriven, request)
    assert(!request.toOption.get.forceFactDriven, request)
  }

  "ReportingRequest" should "successfully deserialize sync request with forceFactDriven" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100,
                          "forceFactDriven" : true
                          }"""

    val request = getReportingRequestValidationSync(jsonString, AdvertiserSchema)
    assert(request.isSuccess, request)
    assert(!request.toOption.get.forceDimensionDriven, request)
    assert(request.toOption.get.forceFactDriven, request)
  }

  "ReportingRequest" should "successfully deserialize sync request with both forceFactDriven and forceDimensionDriven" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100,
                          "forceDimensionDriven" : false,
                          "forceFactDriven" : true
                          }"""

    val request = getReportingRequestValidationSync(jsonString, AdvertiserSchema)
    assert(request.isSuccess, request)
    assert(!request.toOption.get.forceDimensionDriven, request)
    assert(request.toOption.get.forceFactDriven, request)
  }

  "ReportingRequest" should "fail to deserialize sync request with both forceFactDriven and forceDimensionDriven set to true" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100,
                          "forceDimensionDriven" : true,
                          "forceFactDriven" : true
                          }"""

    val request = getReportingRequestValidationSync(jsonString, AdvertiserSchema)
    assert(request.isFailure, request)
    assert(request.swap.toOption.get.head.toString contains "both cannot be true : forceDimensionDriven, forceFactDriven", request)
  }

  "ReportingRequest" should "successfully deserialize sync request with schema" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "schema": "advertiser",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100,
                          "forceDimensionDriven" : false,
                          "forceFactDriven" : true
                          }"""

    val request = getReportingRequestValidationSync(jsonString)
    assert(request.isSuccess, request)
    assert(!request.toOption.get.forceDimensionDriven, request)
    assert(request.toOption.get.forceFactDriven, request)
  }

  "ReportingRequest" should "successfully deserialize sync request with includeRowCount" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "schema": "advertiser",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100,
                          "forceDimensionDriven" : false,
                          "forceFactDriven" : true,
                          "includeRowCount" : true
                          }"""

    val request = getReportingRequestValidationSync(jsonString)
    assert(request.isSuccess, request)
    assert(request.toOption.get.includeRowCount, request)
  }

  "ReportingRequest" should "successfully deserialize sync request with displayName" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "reportDisplayName" : "my special report",
                          "schema": "advertiser",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100,
                          "forceDimensionDriven" : false,
                          "forceFactDriven" : true,
                          "includeRowCount" : true
                          }"""

    val request = getReportingRequestValidationSync(jsonString)
    assert(request.isSuccess, request)
    assert(request.toOption.get.reportDisplayName.get === "my special report", request)
  }

  "ReportingRequest" should "successfully deserialize sync request with displayName as null" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "reportDisplayName" : null,
                          "schema": "advertiser",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100,
                          "forceDimensionDriven" : false,
                          "forceFactDriven" : true,
                          "includeRowCount" : true
                          }"""

    val request = getReportingRequestValidationSync(jsonString)
    assert(request.isSuccess, request)
    assert(request.toOption.get.reportDisplayName.isEmpty, request)
  }

  "ReportingRequest" should "successfully deserialize sync request with quotes in alias name escaped with single quotes" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "reportDisplayName" : null,
                          "schema": "advertiser",
                          "selectFields": [
                              {"field": "Ad ID", "alias": "\"My Awesome Ad ID\""},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100,
                          "forceDimensionDriven" : false,
                          "forceFactDriven" : true,
                          "includeRowCount" : true
                          }"""

    val request = getReportingRequestValidationSync(jsonString)
    assert(request.isSuccess, request)
    assert(request.toOption.get.selectFields.exists(_.alias.contains("'My Awesome Ad ID'")), request)
  }

  "ReportingRequest" should "successfully deserialize sync request with multiple data types in filters" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "reportDisplayName" : null,
                          "schema": "advertiser",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": 12345},
                              {"field": "Campaign ID", "operator": "in", "values": ["1", 2, 10000000000, 3.0, false, null]},
                              {"field": "Ad ID", "operator": "between", "from": "true", "to" : false},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100,
                          "forceDimensionDriven" : false,
                          "forceFactDriven" : true,
                          "includeRowCount" : true
                          }"""

    val request = getReportingRequestValidationSync(jsonString)
    assert(request.isSuccess, request)
    assert(request.toOption.get.filterExpressions.find(_.field == "Advertiser ID").map(_.asInstanceOf[EqualityFilter].value).contains("12345"))
    assert(request.toOption.get.filterExpressions.find(_.field == "Campaign ID").map(_.asInstanceOf[InFilter].values).contains(List("1","2","10000000000","3.0","false")))
    assert(request.toOption.get.filterExpressions.find(_.field == "Ad ID").map(_.asInstanceOf[BetweenFilter].from).contains("true"))
    assert(request.toOption.get.filterExpressions.find(_.field == "Ad ID").map(_.asInstanceOf[BetweenFilter].to).contains("false"))
  }
  
  "ReportingRequest" should "successfully deserialize sync request and force oracle engine" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "reportDisplayName" : null,
                          "schema": "advertiser",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": 12345},
                              {"field": "Campaign ID", "operator": "in", "values": ["1", 2, 10000000000, 3.0, false, null]},
                              {"field": "Ad ID", "operator": "between", "from": "true", "to" : false},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100,
                          "forceDimensionDriven" : false,
                          "forceFactDriven" : true,
                          "includeRowCount" : true
                          }"""

    val request = getReportingRequestValidationSync(jsonString)
    assert(request.isSuccess)
    val forceOracleRequest = ReportingRequest.forceOracle(request.toOption.get)
    assert(forceOracleRequest.additionalParameters.contains(Parameter.QueryEngine))
    assert(forceOracleRequest.additionalParameters(Parameter.QueryEngine) === QueryEngineValue(OracleEngine))
  }

  "ReportingRequest" should "successfully deserialize sync request and force hive engine" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "reportDisplayName" : null,
                          "schema": "advertiser",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": 12345},
                              {"field": "Campaign ID", "operator": "in", "values": ["1", 2, 10000000000, 3.0, false, null]},
                              {"field": "Ad ID", "operator": "between", "from": "true", "to" : false},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100,
                          "forceDimensionDriven" : false,
                          "forceFactDriven" : true,
                          "includeRowCount" : true
                          }"""

    val request = getReportingRequestValidationSync(jsonString)
    assert(request.isSuccess)
    val forceHiveRequest = ReportingRequest.forceHive(request.toOption.get)
    assert(forceHiveRequest.additionalParameters.contains(Parameter.QueryEngine))
    assert(forceHiveRequest.additionalParameters(Parameter.QueryEngine) === QueryEngineValue(HiveEngine))
  }

  "ReportingRequest" should "successfully deserialize sync request and force druid engine" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "reportDisplayName" : null,
                          "schema": "advertiser",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": 12345},
                              {"field": "Campaign ID", "operator": "in", "values": ["1", 2, 10000000000, 3.0, false, null]},
                              {"field": "Ad ID", "operator": "between", "from": "true", "to" : false},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100,
                          "forceDimensionDriven" : false,
                          "forceFactDriven" : true,
                          "includeRowCount" : true
                          }"""

    val request = getReportingRequestValidationSync(jsonString)
    assert(request.isSuccess)
    val forceDruidRequest = ReportingRequest.forceDruid(request.toOption.get)
    assert(forceDruidRequest.additionalParameters.contains(Parameter.QueryEngine))
    assert(forceDruidRequest.additionalParameters(Parameter.QueryEngine) === QueryEngineValue(DruidEngine))
  }

  "ReportingRequest" should "successfully deserialize sync request and set request context" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "reportDisplayName" : null,
                          "schema": "advertiser",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": 12345},
                              {"field": "Campaign ID", "operator": "in", "values": ["1", 2, 10000000000, 3.0, false, null]},
                              {"field": "Ad ID", "operator": "between", "from": "true", "to" : false},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100,
                          "forceDimensionDriven" : false,
                          "forceFactDriven" : true,
                          "includeRowCount" : true
                          }"""

    val request = getReportingRequestValidationSyncWithFactBias(jsonString, AdvertiserSchema)
    assert(request.isSuccess)
    val updatedRequest = ReportingRequest.addRequestContext(request.toOption.get, RequestContext("someReqId", "someUserId"))
    assert(updatedRequest.additionalParameters.contains(Parameter.RequestId))
    assert(updatedRequest.additionalParameters(Parameter.RequestId) === RequestIdValue("someReqId"))
    assert(updatedRequest.additionalParameters.contains(Parameter.UserId))
    assert(updatedRequest.additionalParameters(Parameter.UserId) === UserIdValue("someUserId"))
  }

  "ReportingRequest" should "successfully deserialize request and add timezone" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "reportDisplayName" : null,
                          "schema": "advertiser",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": 12345},
                              {"field": "Campaign ID", "operator": "in", "values": ["1", 2, 10000000000, 3.0, false, null]},
                              {"field": "Ad ID", "operator": "between", "from": "true", "to" : false},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100,
                          "forceDimensionDriven" : false,
                          "forceFactDriven" : true,
                          "includeRowCount" : true
                          }"""

    val request = getReportingRequestValidationSync(jsonString)
    assert(request.isSuccess)
    val requestWithTimeZone = ReportingRequest.withTimeZone(request.toOption.get, DateTimeZone.UTC.toString)
    assert(requestWithTimeZone.additionalParameters.contains(Parameter.TimeZone))
    assert(requestWithTimeZone.additionalParameters(Parameter.TimeZone) === TimeZoneValue(DateTimeZone.UTC.toString))
  }

  "ReportingRequest" should "successfully deserialize request and add hostname" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "reportDisplayName" : null,
                          "schema": "advertiser",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": 12345},
                              {"field": "Campaign ID", "operator": "in", "values": ["1", 2, 10000000000, 3.0, false, null]},
                              {"field": "Ad ID", "operator": "between", "from": "true", "to" : false},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100,
                          "forceDimensionDriven" : false,
                          "forceFactDriven" : true,
                          "includeRowCount" : true
                          }"""

    val request = getReportingRequestValidationSync(jsonString)
    assert(request.isSuccess)
    val requestWithHostName = ReportingRequest.withHostname(request.toOption.get, "127.0.0.1")
    assert(requestWithHostName.additionalParameters.contains(Parameter.HostName))
    assert(requestWithHostName.additionalParameters(Parameter.HostName) === HostNameValue("127.0.0.1"))
  }

  "ReportingRequest" should "successfully deserialize async request and with timezone and schema" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "reportDisplayName" : null,
                          "schema": "advertiser",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": 12345},
                              {"field": "Campaign ID", "operator": "in", "values": ["1", 2, 10000000000, 3.0, false, null]},
                              {"field": "Ad ID", "operator": "between", "from": "true", "to" : false},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100,
                          "forceDimensionDriven" : false,
                          "forceFactDriven" : false,
                          "schema" : "advertiser",
                          "additionalParameters" : { "TimeZone" : "UTC" }
                          }"""

    val request = getReportingRequestValidationAsyncWithAdditionalParameters(jsonString, None)
    assert(request.isSuccess)
    assert(request.toOption.get.schema === AdvertiserSchema)
    assert(request.toOption.get.additionalParameters.contains(Parameter.TimeZone))
    assert(request.toOption.get.additionalParameters(Parameter.TimeZone) === TimeZoneValue(DateTimeZone.UTC.toString))
  }

  "ReportingRequest" should "return correct EntityID and DateRange" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "IN", "values": ["12345", "12"]},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequest(jsonString, AdvertiserSchema)
    assert(ReportingRequest.getDayRange(request)._1.toString == "2014-04-01T00:00:00.000Z")
    assert(ReportingRequest.getDayRange(request)._2.toString == "2014-04-01T00:00:00.000Z")
  }

  "ReportingRequest" should "return correct EntityID and DateRange with different filter operation" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "2014-04-01", "to": "2014-04-07" }
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequest(jsonString, AdvertiserSchema)
    assert(ReportingRequest.getDayRange(request)._1.toString == "2014-04-01T00:00:00.000Z")
    assert(ReportingRequest.getDayRange(request)._2.toString == "2014-04-07T00:00:00.000Z")
  }

  "ReportingRequest" should "successfully serialize and deserialize async request and with report format " in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "reportDisplayName" : null,
                          "schema": "advertiser",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": 12345},
                              {"field": "Campaign ID", "operator": "in", "values": ["1", 2, 10000000000, 3.0, false, null]},
                              {"field": "Ad ID", "operator": "between", "from": "true", "to" : false},
                              {"field": "Day", "operator": "=", "value": "2014-04-01"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100,
                          "forceDimensionDriven" : false,
                          "forceFactDriven" : false,
                          "schema" : "advertiser"
                          }"""

    //check json format
    {
      val serializeRequest = new String(ReportingRequest.serialize(
        ReportingRequest
          .withLabels(ReportingRequest
            .withTestName(ReportingRequest
              .withJsonReportFormat(getReportingRequest(jsonString)), "testname1"), List("lb1","lb2","lb3"))
      ))
      val request = getReportingRequestValidationAsyncWithAdditionalParameters(serializeRequest, None)
      assert(request.isSuccess)
      assert(request.toOption.get.schema === AdvertiserSchema)
      assert(request.toOption.get.additionalParameters.contains(Parameter.ReportFormat))
      assert(request.toOption.get.additionalParameters(Parameter.ReportFormat) === ReportFormatValue(JsonFormat))
      assert(request.toOption.get.additionalParameters(Parameter.TestName) === TestNameValue("testname1"))
      assert(request.toOption.get.additionalParameters(Parameter.Labels) === LabelsValue(List("lb1","lb2","lb3")))
      assert(request.toOption.get.getTestName === Option("testname1"))
      assert(request.toOption.get.getLabels === List("lb1","lb2","lb3"))
    }
    
    //check csv format
    {
      val serializeRequest = new String(ReportingRequest.serialize(ReportingRequest.withCSVReportFormat(getReportingRequest(jsonString))))
      val request = getReportingRequestValidationAsyncWithAdditionalParameters(serializeRequest, None)
      assert(request.isSuccess)
      assert(request.toOption.get.schema === AdvertiserSchema)
      assert(request.toOption.get.additionalParameters.contains(Parameter.ReportFormat))
      assert(request.toOption.get.additionalParameters(Parameter.ReportFormat) === ReportFormatValue(CSVFormat))
    }

    //check excel format
    {
      val serializeRequest = new String(ReportingRequest.serialize(ReportingRequest.withExcelReportFormat(getReportingRequest(jsonString))))
      val request = getReportingRequestValidationAsyncWithAdditionalParameters(serializeRequest, None)
      assert(request.isSuccess)
      assert(request.toOption.get.schema === AdvertiserSchema)
      assert(request.toOption.get.additionalParameters.contains(Parameter.ReportFormat))
      assert(request.toOption.get.additionalParameters(Parameter.ReportFormat) === ReportFormatValue(ExcelFormat))
    }
  }

  "ReportingRequest" should "should be extracted from a scheduled request json with no day filter validation" in {
    val jsonString = """{"cube":"performance_stats",
                    "selectFields":[{"field":"Day","alias":null,"value":null},{"field":"Advertiser ID","alias":null,"value":null},
                    {"field":"Advertiser Name","alias":null,"value":null},{"field":"Advertiser Timezone","alias":null,"value":null},
                    {"field":"Campaign Objective","alias":null,"value":null},{"field":"Budget","alias":null,"value":null},
                    {"field":"Budget Type","alias":null,"value":null},{"field":"Impressions","alias":null,"value":null},
                    {"field":"Average Cost-per-install","alias":null,"value":null},{"field":"Average CPM","alias":null,"value":null},
                    {"field":"Pricing Type","alias":null,"value":null},{"field":"Average Position","alias":null,"value":null},
                    {"field":"Max Bid","alias":null,"value":null}],
                    "filterExpressions":[
                        {"field": "Ad ID", "operator": "between", "from": "true", "to" : false}
                    ],
                    "sortBy":[],"additionalParameters":{"Schema":"advertiser"},"paginationStartIndex":0,"rowsPerPage":-1
                          }"""

    val request: ReportingRequest = getScheduledReportingRequestValidation(jsonString, AdvertiserSchema).toOption.get
    assert(request.requestType === AsyncRequest)
    val ser = new String(ReportingRequest.serialize(request), StandardCharsets.UTF_8)

    assert(request.numDays === 1)
    assert(request.dayFilter === ReportingRequest.getDefaultDayFilter)
    assert(request.additionalParameters.size === 1)
  }

  "ReportingRequest" should "should fail to validate when empty request is passed" in {
    val validation = ReportingRequest.deserializeSync(EMPTY.getBytes(StandardCharsets.UTF_8), None)
    assert(validation.isFailure)
    assert(validation.swap.toOption.get.head.toString contains "invalidInputJson")
  }

  "ReportingRequest" should "should be extracted from a json object as outer filter request" in {
    val jsonString = """{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"operator": "Outer", "outerFilters": [ {"field": "Ad Group ID", "operator": "isnull"} ] },
                              {"field": "Day", "operator": "between", "from": "2014-04-01", "to": "2014-04-30"}
                          ],
                          "sortBy": [
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequest(jsonString, AdvertiserSchema)
    assert(request.requestType === AsyncRequest)
    val ser = new String(ReportingRequest.serialize(request), StandardCharsets.UTF_8)

    assert(request.numDays === 29)
    assert(request.dayFilter.operator === BetweenFilterOperation)
    assert(request.dayFilter.asInstanceOf[BetweenFilter].field === "Day")
    assert(request.dayFilter.asInstanceOf[BetweenFilter].from === "2014-04-01")
    assert(request.dayFilter.asInstanceOf[BetweenFilter].to === "2014-04-30")
    val outerFilters = request.filterExpressions.filter(f => f.operator === OuterFilterOperation)
    assert(outerFilters.size == 1)
    assert(outerFilters.head.asInstanceOf[OuterFilter].filters.head.field === "Ad Group ID")
    assert(outerFilters.head.asInstanceOf[OuterFilter].filters.head.operator === IsNullFilterOperation)
  }

  "ReportingRequest with groupby query type" should "succeed" in {
    val jsonString = """{
                          "queryType": "groupby",
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "in", "values": ["2014-04-01", "2014-04-30"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequest(jsonString, AdvertiserSchema)
    assert(request.queryType === GroupByQuery)
  }

  "ReportingRequest with select query type" should "succeed" in {
    val jsonString = """{
                          "queryType": "scan",
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Ad ID"},
                              {"field": "Day"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "in", "values": ["2014-04-01", "2014-04-30"]}
                          ],
                          "sortBy": [
                              {"field": "Advertiser Id", "order": "Asc"},
                              {"field": "Ad Id", "order": "Desc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":5,
                          "pagination" : {
                              "druid": {
                                  "pagingIdentifiers" : {
                                      "wikipedia_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9" : 5
                                  }
                              }
                          }
                        }"""

    val request: ReportingRequest = getReportingRequest(jsonString, AdvertiserSchema)
    assert(request.queryType === ScanQuery)
    assert(request.pagination.config.contains(DruidEngine))
    val paginationJValue = request.pagination.config(DruidEngine)
    val identifiers = paginationJValue.findField{ case (n, v) => n == "pagingIdentifiers" }
    assert(identifiers.isDefined)
    val segmentOffset = identifiers.get._2.findField{ case (n, v) => n == "wikipedia_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9"}
    assert(segmentOffset.isDefined)
    assert(segmentOffset.get._2.extract[Int] === 5)
  }
}
