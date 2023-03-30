// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.druid

import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core.DruidDerivedFunction.TIME_FORMAT_WITH_REQUEST_CONTEXT
import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension.DruidFuncDimCol
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.request.{ReportingRequest, RequestContext, RowCountQuery}
import org.apache.commons.lang.StringUtils
import org.apache.druid.common.config.NullHandling
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/**
 * Created by hiral on 1/14/16.
 */
class DruidQueryGeneratorTest extends BaseDruidQueryGeneratorTest {

  lazy val defaultRegistry = getDefaultRegistry()

  test("registering Druid query generation multiple times should fail") {
    intercept[IllegalArgumentException] {
      val dummyQueryGenerator = new QueryGenerator[WithDruidEngine] {
        override def generate(queryContext: QueryContext): Query = {
          null
        }

        override def engine: Engine = DruidEngine
      }
      queryGeneratorRegistry.register(DruidEngine, dummyQueryGenerator)
    }
  }

  test("dim query context should fail with UnsupportedOperationException") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    val dimMapping = DefaultQueryPipelineFactory.findDimCandidatesMapping(requestModel.get)
    val dims = DefaultQueryPipelineFactory.findBestDimCandidates(DruidEngine, requestModel.get, dimMapping, DefaultQueryPipelineFactory.druidMultiQueryEngineList)
    val queryContext = new QueryContextBuilder(DimOnlyQuery, requestModel.get).addDimTable(dims).build()
    val druidQueryGenerator = getDruidQueryGenerator
    intercept[UnsupportedOperationException] {
      druidQueryGenerator.generate(queryContext)
    }
  }

  test("limit should be set to defaultMaxRows when maxRows is less than 1") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Pricing Type"},
                            {"field": "Derived Pricing Type"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Average Bid"},
                            {"field": "Average Position"},
                            {"field": "Average Position Maxed"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":0
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """limit":1020"""

    assert(result.contains(json), result)
  }

  test("Druid query should be generated with greater than filter") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Impressions", "operator": ">", "value": "1000"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":0
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """{"type":"greaterThan","aggregation":"Impressions","value":1000}]}"""

    assert(result.contains(json), result)
  }

  test("Druid query should be generated with less than filter") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Impressions", "operator": "<", "value": "1000"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":0
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """{"type":"lessThan","aggregation":"Impressions","value":1000}]}"""

    assert(result.contains(json), result)
  }

  test("Druid query should be generated with IsNull filter") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Impressions", "operator": "<", "value": "1000"},
                            {"field": "Destination URL", "operator": "IsNull"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":0
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    print(s"result: $result")
    val json = """{"type":"lessThan","aggregation":"Impressions","value":1000}]}"""
    assert(result.contains(json), result)
    val isNullFilterJson = """{"type":"selector","dimension":"landing_page_url"}"""
    assert(result.contains(isNullFilterJson), result)
  }

  test("Druid query should be generated with IsNotNull filter") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Impressions", "operator": "<", "value": "1000"},
                            {"field": "Destination URL", "operator": "IsNotNull"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":0
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    print(result)
    val json = """{"type":"lessThan","aggregation":"Impressions","value":1000}]}"""
    assert(result.contains(json), result)
    val isNotNullFilterJson = """{"type":"not","field":{"type":"selector","dimension":"landing_page_url"}}"""
    assert(result.contains(isNotNullFilterJson), result)
  }

  test("Druid query should be generated with Between filter") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Impressions", "operator": "<", "value": "1000"},
                            {"field": "Destination URL", "operator": "between", "from": "A", "to": "ZZ"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":0
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """{"type":"lessThan","aggregation":"Impressions","value":1000}]}"""
    assert(result.contains(json), result)
    val boundFilterJson = """{"type":"bound","dimension":"landing_page_url","lower":"A","upper":"ZZ","lowerStrict":false,"upperStrict":false,"ordering":{"type":"lexicographic"}}"""
    assert(result.contains(boundFilterJson), result)
  }

  test("DruidQueryGenerator: getAggregatorFactory should succeed on DruidFilteredListRollup with filter list size of 2") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Pricing Type"},
                            {"field": "Derived Pricing Type"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Average Bid"},
                            {"field": "Average Position"},
                            {"field": "Impressions"},
                            {"field": "Click Rate Success Case"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":0
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val json = """limit":1020"""

    assert(result.contains(json), result)
  }

  test("DruidQueryGenerator: getAggregatorFactory should succeed on DruidFilteredRollup with requested duplicate of my filter") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Pricing Type"},
                            {"field": "Derived Pricing Type"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Average Bid"},
                            {"field": "Average Position"},
                            {"field": "Impressions"},
                            {"field": "Click Rate Success Case"},
                            {"field": "woeids_unique_users_cantoverride"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Woe ID", "operator": "=", "value": "20"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":0
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val desired = """{"type":"filtered","aggregator":{"type":"thetaSketch","name":"woeids_unique_users_cantoverride","fieldName":"uniqueUserCount","size":2048,"shouldFinalize":true,"isInputThetaSketch":false},"filter":{"type":"or","fields":[{"type":"selector","dimension":"woeids","value":"4563"}]},"name":"woeids_unique_users_cantoverride"}"""
    assert(result.contains(desired), result)
  }

  test("DruidQueryGenerator: getAggregatorFactory should fail on DruidFilteredListRollup with only 1 list element") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Pricing Type"},
                            {"field": "Derived Pricing Type"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Average Bid"},
                            {"field": "Average Position"},
                            {"field": "Impressions"},
                            {"field": "Click Rate"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":0
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(!queryPipelineTry.isSuccess && queryPipelineTry.errorMessage("").contains("ToUse FilteredListAggregator filterList must have 2 or more filters"), "Query pipeline should have failed, but didn't" + queryPipelineTry.errorMessage(""))
  }

  test("limit should be set to defaultMaxRowsAsync when request is Async and rowsPerPage is <= 0") {
    val jsonString =
      s"""{
                          "cube": "user_stats",
                          "selectFields": [
                            {"field": "Impressions"},
                            {"field": "Clicks"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":0
                        }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """limit":100000"""

    assert(result.contains(json), result)
  }

  test("group by strategy should be set to v2 and no chunk period") {
    val jsonString =
      s"""{
                          "cube": "user_stats",
                          "selectFields": [
                            {"field": "Impressions"},
                            {"field": "Clicks"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":0
                        }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val requestModelTry = getRequestModel(request, defaultRegistry)
    assert(requestModelTry.isSuccess, requestModelTry.errorMessage("Building request model failed"))

    val requestModel = requestModelTry.toOption.get.copy(
      dimCardinalityEstimate = Some(DruidQueryGenerator.defaultMaxSingleThreadedDimCardinality + 1)
      , factCost = requestModelTry.toOption.get.factCost.mapValues(rc => rc.copy(costEstimate = DruidQueryGenerator.defaultMaxNoChunkCost + 1))
    )
    val queryPipelineTry = generatePipeline(requestModel)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """"groupByStrategy":"v2""""

    assert(result.contains(json), result)
    assert(!result.contains("chunkPeriod"))
  }

  test("metric should be set to inverted when order is Desc and queryType is topN") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """"metric":{"type":"numeric","metric":"Impressions"}"""

    assert(result.contains(json), result)
  }

  test("for topN queryType aggregations, postAggregations, dimension and filter should be set") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Average Bid"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", "someUser"))
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString


    val json = """\{"queryType":"topN","dataSource":\{"type":"table","name":"fact1"\},"virtualColumns":\[\],"dimension":\{"type":"default","dimension":"id","outputName":"Keyword ID","outputType":"STRING"\},"metric":\{"type":"numeric","metric":"Impressions"\},"threshold":120,"intervals":\{"type":"intervals","intervals":\[".*"\]\},"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\}\],"context":\{"applyLimitPushDown":"false","userId":"someUser","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""

    result should fullyMatch regex json
  }

  test("Successfully generate a query with Javascript extractionFn") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Average Bid"},
                            {"field": "Segments"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", "someUser"))
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"statsDate","value":"[0-9]+"\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"extraction","dimension":"segment_values","outputName":"Segments","outputType":"STRING","extractionFn":\{"type":"javascript","function":"function\(x\) \{ return x > 0; \}","injective":false\}\},\{"type":"default","dimension":"id","outputName":"Keyword ID","outputType":"STRING"\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\{"dimension":"Impressions","direction":"descending","dimensionOrder":\{"type":"numeric"\}\}\],"limit":120\},"context":\{"applyLimitPushDown":"false","userId":"someUser","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""

    result should fullyMatch regex json
  }

  test("Successfully generate a query with Javascript Filter") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Average Bid"},
                            {"field": "Conversion User Count"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Ad ID", "operator": "==", "compareTo": "Ad Group ID"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", "someUser"))
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString


    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\},\{"type":"columnComparison","dimensions":\[\{"type":"default","dimension":"ad_id","outputName":"ad_id","outputType":"STRING"\},\{"type":"default","dimension":"ad_group_id","outputName":"ad_group_id","outputType":"STRING"\}\]\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"id","outputName":"Keyword ID","outputType":"STRING"\}\],"aggregations":\[\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"filtered","aggregator":\{"type":"thetaSketch","name":"Conversion User Count","fieldName":"uniqueUserCount","size":16384,"shouldFinalize":true,"isInputThetaSketch":false\},"filter":\{"type":"javascript","dimension":"segments","function":"function\(x\) \{ return x \> 0; \}"\},"name":"Conversion User Count"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"\/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"applyLimitPushDown":"false","userId":"someUser","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""

    result should fullyMatch regex json
  }

  test("Successfully generate a query with RegEx extractionFn") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Average Bid"},
                            {"field": "Click Exp ID"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", "someUser"))
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    //

    assert(result.contains("{\"type\":\"extraction\",\"dimension\":\"internal_bucket_id\",\"outputName\":\"Click Exp ID\",\"outputType\":\"STRING\",\"extractionFn\":{\"type\":\"regex\",\"expr\":\"(cl-)(.*?)(,)\",\"index\":2,\"replaceMissingValue\":true,\"replaceMissingValueWith\":\"-3\"}}"), result)
  }

  test("Successfully generate a query with RegEx Filter") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Average Bid"},
                            {"field": "Click Exp ID"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Click Exp ID", "operator": "=", "value": "abcd"},
                            {"field": "Ad ID", "operator": "==", "compareTo": "Ad Group ID"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", "someUser"))
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    //

    assert(result.contains("{\"type\":\"selector\",\"dimension\":\"internal_bucket_id\",\"value\":\"abcd\",\"extractionFn\":{\"type\":\"regex\",\"expr\":\"(cl-)(.*?)(,)\",\"index\":2,\"replaceMissingValue\":true,\"replaceMissingValueWith\":\"-3\"}},{\"type\":\"columnComparison\",\"dimensions\":[{\"type\":\"default\",\"dimension\":\"ad_id\",\"outputName\":\"ad_id\",\"outputType\":\"STRING\"},{\"type\":\"default\",\"dimension\":\"ad_group_id\",\"outputName\":\"ad_group_id\",\"outputType\":\"STRING\"}]}]},\"granularity\":{\"type\":\"all\"},\"dimensions\":[{\"type\":\"extraction\",\"dimension\":\"internal_bucket_id\",\"outputName\":\"Click Exp ID\",\"outputType\":\"STRING\",\"extractionFn\":{\"type\":\"regex\",\"expr\":\"(cl-)(.*?)(,)\",\"index\":2,\"replaceMissingValue\":true,\"replaceMissingValueWith\":\"-3\"}}"), result)
  }

  test("Should fail to generate a metric query with Field Comparison Filter") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Average Bid"},
                            {"field": "Conversion User Count"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Min Bid", "operator": "==", "compareTo": "Max Bid"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", "someUser"))
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure && queryPipelineTry.failed.get.getMessage.contains("Column Comparison is not supported on Druid fact fields"), "Column comparison should fail for metrics.")
  }

  test("DruidQueryGenerator: arbitrary static mapping should succeed") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Landing URL Translation"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", "someUser"))
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"extraction","dimension":"landing_page_url","outputName":"Landing URL Translation",\"outputType\":\"STRING\","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"Valid":"Something"\},"isOneToOne":false\},"retainMissingValue":false,"replaceMissingValueWith":"Empty","injective":false,"optimize":true\}\},\{"type":"default","dimension":"id","outputName":"Keyword ID"\,\"outputType\":\"STRING\"}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\}\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\{"dimension":"Impressions","direction":"descending","dimensionOrder":\{"type":"numeric"\}\}\],"limit":120\},"context":\{"applyLimitPushDown":"false","userId":"someUser","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""

    result should fullyMatch regex json
  }

  test("for timeseries queryType aggregations, postAggregations and filter should be set but not dimension") {
    val jsonString =
      s"""{
                          "cube": "k_stats_no_local_time",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Average Bid"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", ""))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"timeseries","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"descending":false,"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":"DAY","aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\}\],"limit":2147483647,"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\}\}"""

    result should fullyMatch regex json
  }

  test("direction in limitSpec should be set to DESCENDING when order is Desc and queryType is groupBy") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Pricing Type"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Average Bid"},
                            {"field": "Clicks"},
                            {"field": "CTR"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"extraction","dimension":"price_type","outputName":"Pricing Type",\"outputType\":\"STRING\","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"-10":"CPE","-20":"CPF","6":"CPV","1":"CPC","2":"CPA","7":"CPCV","3":"CPM"\},"isOneToOne":false\},"retainMissingValue":false,"replaceMissingValueWith":"NONE","injective":false,"optimize":true\}\},\{"type":"default","dimension":"id","outputName":"Keyword ID"\,\"outputType\":\"STRING\"}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"doubleMax","name":"Max Bid","fieldName":"max_bid"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"longSum","name":"Clicks","fieldName":"clicks"\},\{"type":"doubleMin","name":"Min Bid","fieldName":"min_bid"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\},\{"type":"arithmetic","name":"CTR","fn":"/","fields":\[\{"type":"fieldAccess","name":"clicks","fieldName":"Clicks"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\{"dimension":"Impressions","direction":"descending","dimensionOrder":\{"type":"numeric"\}\}\],"limit":120\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""

    result should fullyMatch regex json
  }

  test("dimension based aggregate via filtered aggregate for fact col") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
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
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"stats_source","outputName":"Source"\,\"outputType\":\"STRING\"},\{"type":"default","dimension":"id","outputName":"Keyword ID"\,\"outputType\":\"STRING\"}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"filtered","aggregator":\{"type":"longSum","name":"Reblogs","fieldName":"engagement_count"\},"filter":\{"type":"selector","dimension":"engagement_type","value":"1"\},"name":"Reblogs"\},\{"type":"longSum","name":"Clicks","fieldName":"clicks"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Reblog Rate","fn":"\*","fields":\[\{"type":"arithmetic","name":"_placeHolder_2","fn":"/","fields":\[\{"type":"fieldAccess","name":"Reblogs","fieldName":"Reblogs"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\},\{"type":"constant","name":"_constant_.*","value":100\}\]\},\{"type":"arithmetic","name":"CTR","fn":"/","fields":\[\{"type":"fieldAccess","name":"clicks","fieldName":"Clicks"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\{"dimension":"Impressions","direction":"descending","dimensionOrder":\{"type":"numeric"\}\}\],"limit":120\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""


    result should fullyMatch regex json
  }

  test("dimension extraction function for start of the week dimension") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Source"},
                            {"field": "Clicks"},
                            {"field": "CTR"},
                            {"field": "Reblogs"},
                            {"field": "Reblog Rate"},
                            {"field": "Impressions"},
                            {"field": "Week"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Week", "operator": "in", "values": ["2017-06-12", "2017-06-26"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """"dimension":"statsDate","outputName":"Week","outputType":"STRING","extractionFn":{"type":"time","timeFormat":"yyyyMMdd","resultFormat":"YYYY-w","joda":false}}"""
    val filterjson = """{"type":"selector","dimension":"statsDate","value":"2017-24","extractionFn":{"type":"time","timeFormat":"yyyyMMdd","resultFormat":"YYYY-w","joda":false}},{"type":"selector","dimension":"statsDate","value":"2017-26","extractionFn":{"type":"time","timeFormat":"yyyyMMdd","resultFormat":"YYYY-w","joda":false}}"""

    assert(result.contains(json), result)
    assert(result.contains(filterjson), result)
  }

  test("dimension extraction function for start of the month dimension") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Source"},
                            {"field": "Clicks"},
                            {"field": "CTR"},
                            {"field": "Reblogs"},
                            {"field": "Reblog Rate"},
                            {"field": "Impressions"},
                            {"field": "Month"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Month", "operator": "in", "values": ["2017-06-01", "2017-07-01"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """"dimension":"statsDate","outputName":"Month","outputType":"STRING","extractionFn":{"type":"time","timeFormat":"yyyyMMdd","resultFormat":"yyyy-MM-01","joda":false}}"""
    val filterjson = """{"type":"selector","dimension":"statsDate","value":"2017-06-01","extractionFn":{"type":"time","timeFormat":"yyyyMMdd","resultFormat":"yyyy-MM-01","joda":false}},{"type":"selector","dimension":"statsDate","value":"2017-07-01","extractionFn":{"type":"time","timeFormat":"yyyyMMdd","resultFormat":"yyyy-MM-01","joda":false}}"""

    assert(result.contains(json), result)
    assert(result.contains(filterjson), result)
  }

  test("dimension time extraction function for druid time") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Source"},
                            {"field": "Clicks"},
                            {"field": "CTR"},
                            {"field": "Reblogs"},
                            {"field": "Reblog Rate"},
                            {"field": "Impressions"},
                            {"field": "My Date"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """{"type":"extraction","dimension":"__time","outputName":"My Date","outputType":"STRING","extractionFn":{"type":"timeFormat","format":"YYYY-MM-dd","timeZone":"UTC","granularity":{"type":"none"},"asMillis":false}}"""
    assert(result.contains(json), result)
  }

  test("successfully render filter on column using dimension time extraction function for druid time") {
    val jsonString =
      s"""{
                          "cube": "user_stats_v2",
                          "selectFields": [
                            {"field": "Campaign ID"},
                            {"field": "Ad ID"},
                            {"field": "Source"},
                            {"field": "Clicks"},
                            {"field": "CTR"},
                            {"field": "Reblogs"},
                            {"field": "Reblog Rate"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = s""""filter":{"type":"and","fields":[{"type":"or","fields":[{"type":"selector","dimension":"__time","value":"$fromDate","extractionFn":{"type":"timeFormat","format":"YYYY-MM-dd","timeZone":"UTC","granularity":{"type":"none"},"asMillis":false}},{"type":"selector","dimension":"__time","value":"$toDate","extractionFn":{"type":"timeFormat","format":"YYYY-MM-dd","timeZone":"UTC","granularity":{"type":"none"},"asMillis":false}}]}"""

    assert(result.contains(json), result)
  }

  test("no dimension cols and no sorts should produce timeseries query") {
    val jsonString =
      s"""{
                          "cube": "k_stats_no_local_time",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """{"queryType":"timeseries","""

    assert(result.contains(json), result)
  }

  test("no dimension cols and no sorts on a cube with renderLocalTimeFilter=true should not produce timeseries but groupBy query") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """{"queryType":"groupBy","""

    assert(result.contains(json), result)
  }

  test("fact sort with single dim col should produce topN query") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """{"queryType":"topN","""

    assert(result.contains(json), result)
  }

  test("fact sort with no dim col on a cube with renderLocalTimeFilter=true should produce groupBy query not topN") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """{"queryType":"groupBy","""

    assert(result.contains(json), result)
  }

  test("fact sort with multiple dim col should produce group by query") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Pricing Type"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Average Bid"},
                            {"field": "Average Position"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """{"queryType":"groupBy","""

    assert(result.contains(json), result)
  }

  test("dim fact sync fact driven query should produce all requested fields in same order as in request") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_wo_dim_filters.json")
      .getLines.mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """"""

    assert(result.contains(json), result)
  }

  test("startIndex greater than maximumMaxRows should throw error") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Pricing Type"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Average Bid"},
                            {"field": "Average Position"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":5000,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
  }

  test("limit should be set to 2*maxRows if there is a NonFKDimfilter") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Pricing Type"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Average Bid"},
                            {"field": "Clicks"},
                            {"field": "CTR"},
                            {"field": "Impressions"},
                            {"field": "Campaign Name"},
                            {"field": "Campaign ID"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Campaign Name", "operator": "=", "value": "MegaCampaign"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """limit":220"""

    assert(result.contains(json), result)
  }

  test("queryPipeline should fail when request has NonFKDimfilter and (startIndex + 2*maxRows) > 5000") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Pricing Type"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Average Bid"},
                            {"field": "Clicks"},
                            {"field": "CTR"},
                            {"field": "Impressions"},
                            {"field": "Campaign Name"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Campaign Name", "operator": "=", "value": "MegaCampaign"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":2500
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    assert(queryPipelineTry.checkFailureMessage("requirement failed: Failed to find best candidate, forceEngine=None, engine disqualifyingSet=Set(Druid, Hive, Presto), candidates=Set((fact1,Druid))"))

  }

  test("successfully set group by single threaded to true when cardinality is below max allowed value") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Pricing Type"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Average Bid"},
                            {"field": "Average Position"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to" :"$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModelTry = getRequestModel(request, registry)
    assert(requestModelTry.isSuccess, requestModelTry.errorMessage("Building request model failed"))


    val requestModel = requestModelTry.toOption.get.copy(dimCardinalityEstimate = Some(DruidQueryGenerator.defaultMaxSingleThreadedDimCardinality))
    val queryPipelineTry = generatePipeline(requestModel)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """"groupByIsSingleThreaded":true"""

    assert(result.contains(json), result)
  }
  test("successfully set group by single threaded to false when cardinality is above max allowed value") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Pricing Type"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Average Bid"},
                            {"field": "Average Position"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to" :"$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModelTry = getRequestModel(request, registry)
    assert(requestModelTry.isSuccess, requestModelTry.errorMessage("Building request model failed"))


    val requestModel = requestModelTry.toOption.get.copy(dimCardinalityEstimate = Some(DruidQueryGenerator.defaultMaxSingleThreadedDimCardinality + 1))
    val queryPipelineTry = generatePipeline(requestModel)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """"groupByIsSingleThreaded":false"""

    assert(result.contains(json), result)
  }
  test("successfully set chunk period when dim cardinality is defined and above max cost") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Pricing Type"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Average Bid"},
                            {"field": "Average Position"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to" :"$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModelTry = getRequestModel(request, registry)
    assert(requestModelTry.isSuccess, requestModelTry.errorMessage("Building request model failed"))


    val requestModel = requestModelTry.toOption.get.copy(
      dimCardinalityEstimate = Some(DruidQueryGenerator.defaultMaxSingleThreadedDimCardinality + 1)
      , factCost = requestModelTry.toOption.get.factCost.mapValues(rc => rc.copy(costEstimate = DruidQueryGenerator.defaultMaxNoChunkCost + 1))
    )
    val queryPipelineTry = generatePipeline(requestModel)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """"chunkPeriod":"P3D""""

    assert(result.contains(json), result)
  }
  test("successfully ignore chunk period when dim cardinality is defined and below max cost") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Pricing Type"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Average Bid"},
                            {"field": "Average Position"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to" :"$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModelTry = getRequestModel(request, registry)
    assert(requestModelTry.isSuccess, requestModelTry.errorMessage("Building request model failed"))


    val requestModel = requestModelTry.toOption.get.copy(
      dimCardinalityEstimate = Option(DruidQueryGenerator.defaultMaxSingleThreadedDimCardinality + 1)
      , factCost = requestModelTry.toOption.get.factCost.mapValues(rc => rc.copy(costEstimate = DruidQueryGenerator.defaultMaxNoChunkCost))
    )
    val queryPipelineTry = generatePipeline(requestModel)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    assert(!result.contains("chunkPeriod"))
  }
  test("successfully ignore chunk period when dim cardinality is not defined") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Pricing Type"},
                            {"field": "Max Bid"},
                            {"field": "Min Bid"},
                            {"field": "Average Bid"},
                            {"field": "Average Position"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to" :"$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModelTry = getRequestModel(request, registry)
    assert(requestModelTry.isSuccess, requestModelTry.errorMessage("Building request model failed"))


    val requestModel = requestModelTry.toOption.get.copy(
      dimCardinalityEstimate = None
      , factCost = requestModelTry.toOption.get.factCost.mapValues(rc => rc.copy(costEstimate = DruidQueryGenerator.defaultMaxNoChunkCost + 1))
    )
    val queryPipelineTry = generatePipeline(requestModel)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    assert(!result.contains("chunkPeriod"))
  }
  test("successfully set minTopNThreshold when dim cardinality is defined") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Average Bid"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", "someUser"))
    val requestModelTry = getRequestModel(request, defaultRegistry)
    val requestModel = requestModelTry.toOption.get.copy(dimCardinalityEstimate = Option(12345))
    val queryPipelineTry = generatePipeline(requestModel)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """"minTopNThreshold":12345"""

    assert(result.contains(json), result)
  }
  test("successfully set minTopNThreshold when dim cardinality is not defined") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Average Bid"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", "someUser"))
    val requestModelTry = getRequestModel(request, defaultRegistry)
    val requestModel = requestModelTry.toOption.get.copy(dimCardinalityEstimate = None)
    val queryPipelineTry = generatePipeline(requestModel)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    assert(!result.contains("minTopNThreshold"), result)
  }

  test("namespace lookup extraction functionality") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Average Bid"},
                            {"field": "Impressions"},
                            {"field": "Advertiser Name"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", ""))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Name","outputType":"STRING","extractionFn":{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"name","dimensionOverrideMap":{},"useQueryLevelCache":false}}"""

    assert(result.contains(json), result)
  }

  test("Fail filtering on PassthroughType") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Average Bid"},
                            {"field": "Impressions"},
                            {"field": "Null Type"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Null Type", "operator": "between", "from": "0", "to": "1"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", ""))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isFailure && queryPipelineTry.failed.get.getMessage.contains("Between filter not supported on Druid dimension fields : BetweenFilter(Null Type,0,1)"), queryPipelineTry.errorMessage("Should have gotten expected message."))

  }

  test("namespace lookup extraction functionality for dim") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Average Bid"},
                            {"field": "Impressions"},
                            {"field": "Advertiser Status"},
                            {"field": "Campaign Name"},
                            {"field": "Campaign Start Date"},
                            {"field": "Campaign End Date"},
                            {"field": "Campaign Total"}
                            ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Advertiser Status", "operator": "In", "values": ["ON"]}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", ""))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val expect_empty_lookup = """{"type":"extraction","dimension":"campaign_id_alias","outputName":"Campaign Name","outputType":"STRING","extractionFn":{"type":"mahaRegisteredLookup","lookup":"campaign_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"name","dimensionOverrideMap":{},"useQueryLevelCache":false}}"""
    val expect_replace_with_other = """{"type":"extraction","dimension":"campaign_id_alias","outputName":"Campaign Total","outputType":"STRING","extractionFn":{"type":"mahaRegisteredLookup","lookup":"campaign_lookup","retainMissingValue":false,"replaceMissingValueWith":"Other","injective":false,"optimize":true,"valueColumn":"total","dimensionOverrideMap":{},"useQueryLevelCache":false}}"""
    val expect_time_extract_func = """{"type":"extraction","dimension":"Campaign Start Date","outputName":"Campaign Start Date","outputType":"STRING","extractionFn":{"type":"time","timeFormat":"yyyy-MM-dd HH:mm:ss","resultFormat":"yyyy-MM-dd","joda":false}}"""
    val expect_replace_with_null = """{"type":"extraction","dimension":"campaign_id_alias","outputName":"Campaign End Date","outputType":"STRING","extractionFn":{"type":"mahaRegisteredLookup","lookup":"campaign_lookup","retainMissingValue":false,"replaceMissingValueWith":"null","injective":false,"optimize":true,"valueColumn":"end_time","dimensionOverrideMap":{},"useQueryLevelCache":false}}"""

    assert(result.contains(expect_empty_lookup))
    assert(result.contains(expect_replace_with_other))
    assert(result.contains(expect_time_extract_func))
    assert(result.contains(expect_replace_with_null))
  }

  test("should generate nested groupby query if dim filter is present") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Average Bid"},
                            {"field": "Impressions"},
                            {"field": "Average Position"},
                            {"field": "Advertiser Status"},
                            {"field": "Reseller ID"},
                            {"field": "Reblogs"},
                            {"field": "Click Rate Success Case"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Advertiser Status", "operator": "=", "value": "ON"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", ""))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"query","query":\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\},\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\},\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\},\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\},\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\},\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\},\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\},\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"statsDate","outputName":"Day","outputType":"STRING"\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Status","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"OFF","injective":false,"optimize":true,"valueColumn":"status","dimensionOverrideMap":\{\},"useQueryLevelCache":false\}\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Reseller ID","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"managed_by","dimensionOverrideMap":\{\},"useQueryLevelCache":false\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"filtered","aggregator":\{"type":"roundingDoubleSum","name":"Click Rate Success Case","fieldName":"clicks","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"engagement_type","value":"1"\},\{"type":"selector","dimension":"campaign_id_alias","value":"1"\}\]\},"name":"Click Rate Success Case"\},\{"type":"filtered","aggregator":\{"type":"longSum","name":"Reblogs","fieldName":"engagement_count"\},"filter":\{"type":"selector","dimension":"engagement_type","value":"1"\},"name":"Reblogs"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"doubleMax","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\},\{"type":"arithmetic","name":"Average Position","fn":"/","fields":\[\{"type":"fieldAccess","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"NoopLimitSpec"},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"Advertiser Status","value":"ON"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"Day","outputName":"Day","outputType":"STRING"\},\{"type":"extraction","dimension":"Advertiser Status","outputName":"Advertiser Status","outputType":"STRING","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"ON":"ON"\},"isOneToOne":false\},"retainMissingValue":false,"replaceMissingValueWith":"OFF","injective":false,"optimize":true\}\},\{"type":"default","dimension":"Reseller ID","outputName":"Reseller ID","outputType":"STRING"\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"Impressions"\},\{"type":"roundingDoubleSum","name":"Click Rate Success Case","fieldName":"Click Rate Success Case","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"longSum","name":"Reblogs","fieldName":"Reblogs"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"_sum_avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"doubleMax","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\},\{"type":"arithmetic","name":"Average Position","fn":"/","fields":\[\{"type":"fieldAccess","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":220\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""
    result should fullyMatch regex json
  }

  test("should generate nested groupby query if lookup with decode column is present") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Average Bid"},
                            {"field": "Impressions"},
                            {"field": "Average Position"},
                            {"field": "Advertiser Status"},
                            {"field": "Reblogs"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", ""))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"query","query":\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"statsDate","outputName":"Day","outputType":"STRING"\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Status","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"OFF","injective":false,"optimize":true,"valueColumn":"status","dimensionOverrideMap":\{\},"useQueryLevelCache":false\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"filtered","aggregator":\{"type":"longSum","name":"Reblogs","fieldName":"engagement_count"\},"filter":\{"type":"selector","dimension":"engagement_type","value":"1"\},"name":"Reblogs"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"doubleMax","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\},\{"type":"arithmetic","name":"Average Position","fn":"/","fields":\[\{"type":"fieldAccess","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"NoopLimitSpec"\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"Day","outputName":"Day","outputType":"STRING"\},\{"type":"extraction","dimension":"Advertiser Status","outputName":"Advertiser Status","outputType":"STRING","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"ON":"ON"\},"isOneToOne":false\},"retainMissingValue":false,"replaceMissingValueWith":"OFF","injective":false,"optimize":true\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"Impressions"\},\{"type":"longSum","name":"Reblogs","fieldName":"Reblogs"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"_sum_avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"doubleMax","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\},\{"type":"arithmetic","name":"Average Position","fn":"/","fields":\[\{"type":"fieldAccess","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""


    result should fullyMatch regex json
  }

  test("should not generate nested groupby query if dim filter column present is not of type druid lookup") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Average Bid"},
                            {"field": "Impressions"},
                            {"field": "Average Position"},
                            {"field": "Reseller ID"},
                            {"field": "Reblogs"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", ""))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"statsDate","outputName":"Day","outputType":"STRING"\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Reseller ID","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"managed_by","dimensionOverrideMap":\{\},"useQueryLevelCache":false\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"filtered","aggregator":\{"type":"longSum","name":"Reblogs","fieldName":"engagement_count"\},"filter":\{"type":"selector","dimension":"engagement_type","value":"1"\},"name":"Reblogs"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"doubleMax","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\},\{"type":"arithmetic","name":"Average Position","fn":"/","fields":\[\{"type":"fieldAccess","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""


    result should fullyMatch regex json
  }

  test("should include dim column related to dim filter in nested groupby query if dim filter is present even though dim column is not present in the request") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Average Bid"},
                            {"field": "Impressions"},
                            {"field": "Average Position"},
                            {"field": "Reblogs"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Advertiser Status", "operator": "=", "value": "ON"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", ""))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"query","query":\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"statsDate","outputName":"Day","outputType":"STRING"\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Status","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"OFF","injective":false,"optimize":true,"valueColumn":"status","dimensionOverrideMap":\{\},"useQueryLevelCache":false\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"filtered","aggregator":\{"type":"longSum","name":"Reblogs","fieldName":"engagement_count"\},"filter":\{"type":"selector","dimension":"engagement_type","value":"1"\},"name":"Reblogs"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"doubleMax","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\},\{"type":"arithmetic","name":"Average Position","fn":"/","fields":\[\{"type":"fieldAccess","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"NoopLimitSpec"\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"Advertiser Status","value":"ON"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"Day","outputName":"Day","outputType":"STRING"\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"Impressions"\},\{"type":"longSum","name":"Reblogs","fieldName":"Reblogs"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"_sum_avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"doubleMax","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\},\{"type":"arithmetic","name":"Average Position","fn":"/","fields":\[\{"type":"fieldAccess","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":220\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""


    result should fullyMatch regex json
  }

  test("namespace lookup extraction functionality for a public fact with new dimRevision") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Average Bid"},
                            {"field": "Impressions"},
                            {"field": "External Site Name"},
                            {"field": "Advertiser Status"},
                            {"field": "Currency"},
                            {"field": "Timezone"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Advertiser Status", "operator": "=", "value": "ON"},
                            {"field": "Currency", "operator": "=", "value": "USD"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString, InternalSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"query","query":\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"statsDate","outputName":"Day","outputType":"STRING"\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Status","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"status","dimensionOverrideMap":\{\},"useQueryLevelCache":true\}\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Currency","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"currency","dimensionOverrideMap":\{"-3":"Unknown","":"Unknown"\},"useQueryLevelCache":true\}\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Timezone","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"timezone","decode":\{"columnToCheck":"timezone","valueToCheck":"US","columnIfValueMatched":"timezone","columnIfValueNotMatched":"currency"\},"dimensionOverrideMap":\{\},"useQueryLevelCache":true\}\},\{"type":"extraction","dimension":"external_id","outputName":"External Site Name","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"site_lookup","retainMissingValue":false,"replaceMissingValueWith":"Others","injective":false,"optimize":true,"valueColumn":"external_site_name","dimensionOverrideMap":\{\},"useQueryLevelCache":false\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\}\],"limitSpec":\{"type":"NoopLimitSpec"\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"Advertiser Status","value":"ON"\},\{"type":"selector","dimension":"Currency","value":"USD"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"Day","outputName":"Day","outputType":"STRING"\},\{"type":"default","dimension":"Advertiser Status","outputName":"Advertiser Status","outputType":"STRING"\},\{"type":"default","dimension":"Currency","outputName":"Currency","outputType":"STRING"\},\{"type":"default","dimension":"Timezone","outputName":"Timezone","outputType":"STRING"\},\{"type":"extraction","dimension":"External Site Name","outputName":"External Site Name","outputType":"STRING","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"null":"Others","":"Others"\},"isOneToOne":false\},"retainMissingValue":true,"injective":true,"optimize":true\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"Impressions"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"_sum_avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":220\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""

    result should fullyMatch regex json
  }

  test("should include dimension columns from DruidPostResultDerivedFactCol in DimensionSpec") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Impressions"},
                            {"field": "Advertiser Status"},
                            {"field": "Impression Share"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Advertiser Status", "operator": "=", "value": "ON"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString, InternalSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json =
      """\{"queryType":"groupBy","dataSource":\{"type":"query","query":\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"statsDate","outputName":"Day","outputType":"STRING"\},\{"type":"default","dimension":"show_sov_flag","outputName":"show_sov_flag","outputType":"STRING"\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Status","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"status","dimensionOverrideMap":\{\},"useQueryLevelCache":false\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"longSum","name":"sov_impressions","fieldName":"sov_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Impression Share","fn":"/","fields":\[\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\},\{"type":"fieldAccess","name":"sov_impressions","fieldName":"sov_impressions"\}\]\}\],"limitSpec":\{"type":"NoopLimitSpec"\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"Advertiser Status","value":"ON"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"Day","outputName":"Day","outputType":"STRING"\},\{"type":"default","dimension":"show_sov_flag","outputName":"show_sov_flag","outputType":"STRING"\},\{"type":"default","dimension":"Advertiser Status","outputName":"Advertiser Status","outputType":"STRING"\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"Impressions"\},\{"type":"longSum","name":"sov_impressions","fieldName":"sov_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Impression Share","fn":"/","fields":\[\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\},\{"type":"fieldAccess","name":"sov_impressions","fieldName":"sov_impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":220\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}""".stripMargin

    result should fullyMatch regex json
  }

  test("dimension extraction function for day of the week dimension") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Source"},
                            {"field": "Clicks"},
                            {"field": "CTR"},
                            {"field": "Reblogs"},
                            {"field": "Reblog Rate"},
                            {"field": "Impressions"},
                            {"field": "Day of Week"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """{"type":"extraction","dimension":"statsDate","outputName":"Day of Week","outputType":"STRING","extractionFn":{"type":"time","timeFormat":"yyyyMMdd","resultFormat":"EEEE","joda":false}}"""

    assert(result.contains(json), result)
  }

  test("dimension extraction function for datetime formatter") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Source"},
                            {"field": "Clicks"},
                            {"field": "CTR"},
                            {"field": "Reblogs"},
                            {"field": "Reblog Rate"},
                            {"field": "Impressions"},
                            {"field": "Start Date"},
                            {"field": "Start Hour"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json_date = s"""{"type":"extraction","dimension":"start_time","outputName":"Start Date","outputType":"STRING","extractionFn":{"type":"substring","index":0,"length":8}}"""
    val json_hour = s"""{"type":"extraction","dimension":"start_time","outputName":"Start Hour","outputType":"STRING","extractionFn":{"type":"substring","index":8,"length":2}}"""

    assert(result.contains(json_date), result)
    assert(result.contains(json_hour), result)

  }

  test("dimension filter extraction function for datetime formatter") {
    val jsonString =
      s"""{
                          "cube": "k_stats_start_time",
                          "selectFields": [
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
                            {"field": "Day", "operator": "between", "from": "$toDateMinusOne", "to": "$toDate"},
                            {"field": "Hour", "operator": "between", "from": "02", "to": "05"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Failed to get request model"))
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json_date_filter =
      s"""{"type":"and","fields":[{"type":"or","fields":[{"type":"selector","dimension":"start_time","value":"$toDateMinusOneHive","extractionFn":{"type":"substring","index":0,"length":8}},{"type":"selector","dimension":"start_time","value":"$toDateHive","extractionFn":{"type":"substring","index":0,"length":8}}]}""".stripMargin
    val json_date_filter_case_1 = s"""{"type":"and","fields":[{"type":"or","fields":[{"type":"selector","dimension":"start_time","value":"$toDateHive","extractionFn":{"type":"substring","index":0,"length":8}},{"type":"selector","dimension":"start_time","value":"$toDateMinusOneHive","extractionFn":{"type":"substring","index":0,"length":8}}]}"""
    val json_date_filter_case_2 = s"""{"type":"and","fields":[{"type":"or","fields":[{"type":"selector","dimension":"start_time","value":"$toDateMinusOneHive","extractionFn":{"type":"substring","index":0,"length":8}},{"type":"selector","dimension":"start_time","value":"$toDateHive","extractionFn":{"type":"substring","index":0,"length":8}}]}"""
    val json_hour_filter = s"""{"type":"or","fields":[{"type":"selector","dimension":"start_time","value":"04","extractionFn":{"type":"substring","index":8,"length":2}},{"type":"selector","dimension":"start_time","value":"19","extractionFn":{"type":"substring","index":8,"length":2}},{"type":"selector","dimension":"start_time","value":"20","extractionFn":{"type":"substring","index":8,"length":2}},{"type":"selector","dimension":"start_time","value":"05","extractionFn":{"type":"substring","index":8,"length":2}},{"type":"selector","dimension":"start_time","value":"21","extractionFn":{"type":"substring","index":8,"length":2}},{"type":"selector","dimension":"start_time","value":"13","extractionFn":{"type":"substring","index":8,"length":2}},{"type":"selector","dimension":"start_time","value":"22","extractionFn":{"type":"substring","index":8,"length":2}},{"type":"selector","dimension":"start_time","value":"14","extractionFn":{"type":"substring","index":8,"length":2}},{"type":"selector","dimension":"start_time","value":"23","extractionFn":{"type":"substring","index":8,"length":2}},{"type":"selector","dimension":"start_time","value":"06","extractionFn":{"type":"substring","index":8,"length":2}},{"type":"selector","dimension":"start_time","value":"15","extractionFn":{"type":"substring","index":8,"length":2}},{"type":"selector","dimension":"start_time","value":"00","extractionFn":{"type":"substring","index":8,"length":2}},{"type":"selector","dimension":"start_time","value":"07","extractionFn":{"type":"substring","index":8,"length":2}},{"type":"selector","dimension":"start_time","value":"16","extractionFn":{"type":"substring","index":8,"length":2}},{"type":"selector","dimension":"start_time","value":"01","extractionFn":{"type":"substring","index":8,"length":2}},{"type":"selector","dimension":"start_time","value":"08","extractionFn":{"type":"substring","index":8,"length":2}},{"type":"selector","dimension":"start_time","value":"17","extractionFn":{"type":"substring","index":8,"length":2}},{"type":"selector","dimension":"start_time","value":"09","extractionFn":{"type":"substring","index":8,"length":2}},{"type":"selector","dimension":"start_time","value":"10","extractionFn":{"type":"substring","index":8,"length":2}},{"type":"selector","dimension":"start_time","value":"02","extractionFn":{"type":"substring","index":8,"length":2}},{"type":"selector","dimension":"start_time","value":"11","extractionFn":{"type":"substring","index":8,"length":2}},{"type":"selector","dimension":"start_time","value":"03","extractionFn":{"type":"substring","index":8,"length":2}},{"type":"selector","dimension":"start_time","value":"18","extractionFn":{"type":"substring","index":8,"length":2}},{"type":"selector","dimension":"start_time","value":"12","extractionFn":{"type":"substring","index":8,"length":2}}]}"""

    assert(result.contains(json_date_filter_case_1) || result.contains(json_date_filter_case_2), result)
    assert(result.contains(json_hour_filter), result)

    assert(result.contains(""""groupByStrategy":"v1""""), result)
    assert(result.contains(""""groupByIsSingleThreaded":false"""), result)

  }

  test("dimension filter extraction function for decode dim") {
    val jsonString =
      s"""{
                          "cube": "k_stats_decode_dim",
                          "selectFields": [
                            {"field": "Advertiser ID"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Rendered Type", "operator": "in", "values": ["Multiple"]}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString, InternalSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString


    val json =
      """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"or","fields":\[\{"type":"selector","dimension":"is_slot_ad","value":"1"\}\]\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"advertiser_id","outputName":"Advertiser ID","outputType":"STRING"\}\],"aggregations":\[\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}""".stripMargin
    result should fullyMatch regex json
  }

  test("dimension filter extraction function for decode dim with mapped null string") {
    val jsonString =
      s"""{
                          "cube": "k_stats_decode_dim",
                          "selectFields": [
                            {"field": "Advertiser ID"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Valid Conversion", "operator": "in", "values": ["1"]}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString, InternalSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString


    val json =
      """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"or","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"valid_conversion"\},\{"type":"selector","dimension":"valid_conversion","value":"1"\}\]\}\]\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"advertiser_id","outputName":"Advertiser ID","outputType":"STRING"\}\],"aggregations":\[\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}""".stripMargin
    result should fullyMatch regex json
  }

  test("dimension filter extraction function for decode dim that has staticMapping for source col") {
    val jsonString =
      s"""{
                          "cube": "k_stats_decode_dim",
                          "selectFields": [
                            {"field": "Advertiser ID"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Derived Pricing Type", "operator": "in", "values": ["CPM"]}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString, InternalSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString


    val json =
      """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"or","fields":\[\{"type":"selector","dimension":"price_type","value":"3"\}\]\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"advertiser_id","outputName":"Advertiser ID","outputType":"STRING"\}\],"aggregations":\[\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}""".stripMargin
    result should fullyMatch regex json
  }

  test("dimension filter extraction function for decode dim with values mapped to multiple source values") {
    val jsonString =
      s"""{
                          "cube": "k_stats_decode_dim",
                          "selectFields": [
                            {"field": "Advertiser ID"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Derived Pricing Type", "operator": "in", "values": ["CPV"]}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""


    val request: ReportingRequest = getReportingRequestSync(jsonString, InternalSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString


    val json =
      """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"or","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"price_type","value":"7"\},\{"type":"selector","dimension":"price_type","value":"6"\},\{"type":"selector","dimension":"price_type","value":"8"\}\]\}\]\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"advertiser_id","outputName":"Advertiser ID","outputType":"STRING"\}\],"aggregations":\[\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}""".stripMargin
    result should fullyMatch regex json
  }
/*
  test("where clause: ensure duplicate filter mappings are not propagated into the where clause") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Average Bid"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Source", "operator": "=", "value": "1"},
                            {"field": "Source Name", "operator": "In", "values": [ "1", "2" ] }
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", "someUser"))
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val json = """\{"queryType":"topN","dataSource":\{"type":"table","name":"fact1"\},"virtualColumns":\[\],"dimension":\{"type":"default","dimension":"id","outputName":"Keyword ID"\,\"outputType\":\"STRING\"},"metric":\{"type":"numeric","metric":"Impressions"\},"threshold":120,"intervals":\{"type":"intervals","intervals":\[".*"\]\},"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\},\{"type":"or","fields":\[\{"type":"selector","dimension":"stats_source","value":"1"\},\{"type":"selector","dimension":"stats_source","value":"2"\}\]\}\]\},"granularity":\{"type":"all"\},"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\}\],"context":\{"applyLimitPushDown":"false","userId":"someUser","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""

    result should fullyMatch regex json
  }
*/
  test("Or filter expression with dimension AND fact filters should render properly") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Source"},
                            {"field": "Clicks"},
                            {"field": "CTR"},
                            {"field": "Reblogs"},
                            {"field": "Reblog Rate"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Timezone", "operator": "=", "value": "Enabled"},
                            {"operator": "or", "filterExpressions": [
                              {"field": "Campaign Name", "operator": "=", "value": "Nike"},
                              {"field": "Campaign Total", "operator": "=", "value": "Nike"},
                              {"field": "Advertiser Name", "operator": "like", "value": "2"},
                              {"field": "Ad ID", "operator": "=", "value": "12345"},
                              {"field": "Source", "operator": "=", "value": "1"}]}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ]
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val filterjson = s""""fields":[{"type":"or","fields":[{"type":"search","dimension":"Advertiser Name","query":{"type":"insensitive_contains","value":"2","caseSensitive":false}},{"type":"selector","dimension":"Campaign Total","value":"Nike"},{"type":"selector","dimension":"Campaign Name","value":"Nike"},{"type":"selector","dimension":"Source","value":"1"},{"type":"selector","dimension":"Ad ID","value":"12345"}]}]"""
    val filterFactJson = s"""{"type":"or","fields":[{"type":"selector","dimension":"ad_id","value":"12345"},{"type":"selector","dimension":"stats_source","value":"1"}]}"""
    val exposingFactInInnerQueryJson = s"""{"type":"default","dimension":"ad_id","outputName":"Ad ID","outputType":"STRING"}"""
    assert(result.contains(filterjson) && result.contains(exposingFactInInnerQueryJson) && !result.contains(filterFactJson), result)
  }

  test("Or filter expression with fact filters") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Source"},
                            {"field": "Clicks"},
                            {"field": "CTR"},
                            {"field": "Reblogs"},
                            {"field": "Reblog Rate"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"operator": "or", "filterExpressions": [{"field": "Clicks", "operator": "in", "values": ["1","2"]}, {"field": "Impressions", "operator": "=", "value": "2"}]}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val filterjson = s""""filter":{"type":"and","fields":[{"type":"selector","dimension":"statsDate","value":"${fromDate.replace("-", "")}"},{"type":"selector","dimension":"advertiser_id","value":"12345"}]}"""
    val havingJson = s""""having":{"type":"and","havingSpecs":[{"type":"or","havingSpecs":[{"type":"equalTo","aggregation":"Impressions","value":2},{"type":"or","havingSpecs":[{"type":"equalTo","aggregation":"Clicks","value":1},{"type":"equalTo","aggregation":"Clicks","value":2}]}]}]}"""

    assert(result.contains(filterjson), result)
    assert(result.contains(havingJson), result)
  }

  test("Generate a valid query at minute grain") {
    val fromMinute = "00"
    val toMinute = "60"

    val jsonString =
      s"""{
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
                            {"field": "Day", "operator": "between", "from": "$toDateMinusOne", "to": "$toDate"},
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
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Failed to get request model"))
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val expectectDimensionsJson = """"dimensions":[{"type":"extraction","dimension":"statsDate","outputName":"Week","outputType":"STRING","extractionFn":{"type":"time","timeFormat":"yyyyMMdd","resultFormat":"w","joda":false}},{"type":"default","dimension":"stats_source","outputName":"Source","outputType":"STRING"},{"type":"default","dimension":"id","outputName":"Keyword ID","outputType":"STRING"}"""

    assert(result.contains(expectectDimensionsJson), s"$expectectDimensionsJson \n\n not found in \n\n $result")
  }

  test("Generate a valid query at minute grain with datetime between filter with timestamp type for Day") {
    val cubes = List("k_stats_minute_grain_ts_dtf"
      , "k_stats_minute_grain_ts_period", "k_stats_minute_grain_ts_context", "k_stats_minute_grain_ts_fmt"
      , "k_stats_minute_grain_ts_dtf_no_local")
    val validationMap: Map[String, String] = Map(
      "k_stats_minute_grain_ts_dtf" -> MinuteGrain.toFullFormattedString(baseDate)
      , "k_stats_minute_grain_ts_period" -> DailyGrain.toFullFormattedString(baseDate)
      , "k_stats_minute_grain_ts_context" -> {
        val fmt: DateTimeFormatter = DateTimeFormat.forPattern("YYY-MM-dd HH").withZoneUTC()
        fmt.print(baseDate)
      }
      , "k_stats_minute_grain_ts_fmt" -> {
        val fmt: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMddHHmm").withZoneUTC()
        fmt.print(baseDate)
      }
    )
    cubes.foreach {
      cube =>
        val jsonString =
          s"""{
                          "cube": "$cube",
                          "selectFields": [
                            {"field": "Day"},
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
                            {"field": "Day", "operator": "datetimebetween", "from": "${toDateTimeMinusTenMinutes}", "to": "$toDateTime", "format": "$iso8601Format"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""
        val request: ReportingRequest = getReportingRequestSync(jsonString)
        val requestModel = getRequestModel(request, defaultRegistry)
        assert(requestModel.isSuccess, requestModel.errorMessage("Failed to get request model"))
        val queryPipelineTry = generatePipeline(requestModel.toOption.get)
        assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

        val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
        assert(result.contains("Day") && result.contains("selector"), result)
        validationMap.get(cube).foreach {
          dt => assert(result.contains(dt), s"$dt not found")
        }
    }
  }

  test("Generate a valid query at hourly grain with datetime between filter with timestamp type for Day") {
    val cubes = List("k_stats_hourly_grain_ts_dtf")
    cubes.foreach {
      cube =>
        val jsonString =
          s"""{
                          "cube": "$cube",
                          "selectFields": [
                            {"field": "Day"},
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
                            {"field": "Day", "operator": "datetimebetween", "from": "${toDateTimeMinusTwoHours}", "to": "$toDateTime", "format": "$iso8601Format"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""
        val request: ReportingRequest = getReportingRequestSync(jsonString)
        val requestModel = getRequestModel(request, defaultRegistry)
        assert(requestModel.isSuccess, requestModel.errorMessage("Failed to get request model"))
        val queryPipelineTry = generatePipeline(requestModel.toOption.get)
        assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
        val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
        assert(result.contains("Day") && result.contains("selector") && result.contains(HourlyGrain.toFullFormattedString(baseDate))
          , result)
    }
  }

  test("Generate a valid query at daily grain with datetime between filter with timestamp type for Day") {
    val cubes = List("k_stats_daily_grain_ts_dtf")
    cubes.foreach {
      cube =>
        val jsonString =
          s"""{
                          "cube": "$cube",
                          "selectFields": [
                            {"field": "Day"},
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
                            {"field": "Day", "operator": "datetimebetween", "from": "${fromDateTime}", "to": "$toDateTime", "format": "$iso8601Format"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""
        val request: ReportingRequest = getReportingRequestSync(jsonString)
        val requestModel = getRequestModel(request, defaultRegistry)
        assert(requestModel.isSuccess, requestModel.errorMessage("Failed to get request model"))
        val queryPipelineTry = generatePipeline(requestModel.toOption.get)
        assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

        val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
        assert(result.contains("Day") && result.contains("selector") && result.contains(DailyGrain.toFullFormattedString(baseDate)), result)
    }
  }

  test("successfully generate query with sync group by single threaded optimization for non grain or index optimized request") {
    val fromMinute = "00"
    val toMinute = "60"

    val jsonString =
      s"""{
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
                            {"field": "Day", "operator": "between", "from": "$toDateMinusOne", "to": "$toDate"},
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
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Failed to get request model"))
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val expectedJson = """"groupByIsSingleThreaded":false"""

    assert(result.contains(expectedJson), s"$expectedJson \n\n not found in \n\n $result")
  }

  test("Duplicate registration of the generator") {
    val failRegistry = new QueryGeneratorRegistry
    val dummyOracleQueryGenerator = new QueryGenerator[WithOracleEngine] {
      override def generate(queryContext: QueryContext): Query = {
        null
      }

      override def engine: Engine = OracleEngine
    }
    val dummyFalseQueryGenerator = new QueryGenerator[WithDruidEngine] {
      override def generate(queryContext: QueryContext): Query = {
        null
      }

      override def engine: Engine = DruidEngine
    }
    failRegistry.register(OracleEngine, dummyOracleQueryGenerator)
    failRegistry.register(DruidEngine, dummyFalseQueryGenerator)

    DruidQueryGenerator.register(failRegistry, queryOptimizer = new SyncDruidQueryOptimizer(timeout = 5000), useCustomRoundingSumAggregator = true)
  }
  test("Join key should not be included in dimension bundle if it is not in the original request when using druid lookups") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Impressions"},
                            {"field": "Advertiser Status"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString, InternalSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Status","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"status","dimensionOverrideMap":\{\},"useQueryLevelCache":false\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\}\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""

    result should fullyMatch regex json
  }

  test("Join key should be included in dimension bundle if it is not in the original request when druid+oracle") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Impressions"},
                            {"field": "Keyword Value"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString, InternalSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = queryPipelineFactory.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"id","outputName":"Keyword ID"\,\"outputType\":\"STRING\"}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\}\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""

    result should fullyMatch regex json
  }

  test("test theta sketch intersect set operation with filters on theta sketch aggregators") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Ad ID"},
                            {"field": "Impressions"},
                            {"field": "Clicks"},
                            {"field": "Total Unique User Count"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Age Bucket", "operator": "in", "values": ["18-24", "25-35"]},
                            {"field": "Woe ID", "operator": "in", "values": ["12345", "6789"]}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"},
                            {"field": "Keyword ID", "order": "Asc"},
                            {"field": "Ad ID", "order": "Desc"}
                          ],
                          "paginationStartIndex":1,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\},\{"type":"or","fields":\[\{"type":"selector","dimension":"ageBucket","value":"18-24"\},\{"type":"selector","dimension":"ageBucket","value":"25-35"\}\]\},\{"type":"or","fields":\[\{"type":"selector","dimension":"woeids","value":"12345"\},\{"type":"selector","dimension":"woeids","value":"6789"\}\]\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"id","outputName":"Keyword ID","outputType":"STRING"\},\{"type":"default","dimension":"ad_id","outputName":"Ad ID","outputType":"STRING"\}\],"aggregations":\[\{"type":"longSum","name":"Clicks","fieldName":"clicks"\},\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"filtered","aggregator":\{"type":"thetaSketch","name":"ageBucket_unique_users","fieldName":"uniqueUserCount","size":16384,"shouldFinalize":true,"isInputThetaSketch":false\},"filter":\{"type":"or","fields":\[\{"type":"selector","dimension":"ageBucket","value":"18-24"\},\{"type":"selector","dimension":"ageBucket","value":"25-35"\}\]\},"name":"ageBucket_unique_users"\},\{"type":"filtered","aggregator":\{"type":"thetaSketch","name":"woeids_unique_users","fieldName":"uniqueUserCount","size":16384,"shouldFinalize":true,"isInputThetaSketch":false\},"filter":\{"type":"or","fields":\[\{"type":"selector","dimension":"woeids","value":"12345"\},\{"type":"selector","dimension":"woeids","value":"6789"\}\]\},"name":"woeids_unique_users"\}\],"postAggregations":\[\{"type":"thetaSketchEstimate","name":"Total Unique User Count","field":\{"type":"thetaSketchSetOp","name":"Total Unique User Count","func":"INTERSECT","size":16384,"fields":\[\{"type":"fieldAccess","name":"ageBucket_unique_users","fieldName":"ageBucket_unique_users"\},\{"type":"fieldAccess","name":"woeids_unique_users","fieldName":"woeids_unique_users"\}\]\}\}\],"limitSpec":\{"type":"default","columns":\[\{"dimension":"Impressions","direction":"ascending","dimensionOrder":\{"type":"numeric"\}\},\{"dimension":"Keyword ID","direction":"ascending","dimensionOrder":\{"type":"numeric"\}\},\{"dimension":"Ad ID","direction":"descending","dimensionOrder":\{"type":"numeric"\}\}\],"limit":101\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""

    result should fullyMatch regex json
  }

  test("const fact column and derived const fact column should not be included in aggregators and post aggregators") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Ad ID"},
                            {"field": "Impressions"},
                            {"field": "const_a"},
                            {"field": "const_b"},
                            {"field": "Const Der Fact Col C"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "paginationStartIndex":1,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"ad_id","outputName":"Ad ID","outputType":"STRING"\},\{"type":"default","dimension":"id","outputName":"Keyword ID","outputType":"STRING"\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\}\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\],"limit":101\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""

    result should fullyMatch regex json
  }


  test("Support for Async View Multi Query, API Side Join") {
    val jsonString =
      s"""{ "cube": "a_stats",
         |   "selectFields": [
         |      {
         |         "field": "Advertiser ID"
         |      },
         |      {
         |         "field": "Day"
         |      },
         |      {
         |         "field": "Is Adjustment"
         |      },
         |      {
         |         "field": "Impressions"
         |      },
         |      {
         |         "field": "Spend"
         |      },
         |      {
         |         "field": "Const Der Fact Col A"
         |      }
         |   ],
         |   "filterExpressions": [
         |      {
         |         "field": "Advertiser ID",
         |         "operator": "=",
         |         "value": "12345"
         |      },
         |      {
         |         "field": "Is Adjustment",
         |         "operator": "=",
         |         "value": "Y"
         |      },
         |      {
         |         "field": "Day",
         |         "operator": "=",
         |         "value": "$fromDate"
         |      }
         |   ],
         |   "sortBy": [
         |      {
         |          "field": "Impressions",
         |          "order": "Asc"
         |      },
         |      {
         |          "field": "Spend",
         |          "order": "Desc"
         |      },
         |      {
         |          "field": "Const Der Fact Col A",
         |          "order": "Desc"
         |      }
         |   ]
         |}
      """.stripMargin

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    require(requestModel.isSuccess, requestModel)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    assert(queryPipelineTry.get.queryChain.isInstanceOf[MultiQuery], "Failed to create MultiQuery for Async")

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val expectedJson = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"account_stats"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\},\{"type":"selector","dimension":"\{test_flag\}","value":"0"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"advertiser_id","outputName":"Advertiser ID","outputType":"STRING"\},\{"type":"default","dimension":"stats_date","outputName":"Day","outputType":"STRING"\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"roundingDoubleSum","name":"Spend","fieldName":"spend","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"longSum","name":"clicks","fieldName":"clicks"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Const Der Fact Col A","fn":"/","fields":\[\{"type":"fieldAccess","name":"clicks","fieldName":"clicks"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\{"dimension":"Impressions","direction":"ascending","dimensionOrder":\{"type":"numeric"\}\},\{"dimension":"Spend","direction":"descending","dimensionOrder":\{"type":"numeric"\}\},\{"dimension":"Const Der Fact Col A","direction":"descending","dimensionOrder":\{"type":"numeric"\}\}\],"limit":200\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""

    //println(expectedJson)
    result should fullyMatch regex expectedJson

    val subSequentQuery = queryPipelineTry.toOption.get.queryChain.subsequentQueryList.head.asString
    val expectedSubQueryJson = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"a_adjustments"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\},\{"type":"selector","dimension":"\{test_flag\}","value":"0"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"advertiser_id","outputName":"Advertiser ID","outputType":"STRING"\},\{"type":"default","dimension":"stats_date","outputName":"Day","outputType":"STRING"\}\],"aggregations":\[\{"type":"roundingDoubleSum","name":"Spend","fieldName":"spend","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\}\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\{"dimension":"Spend","direction":"descending","dimensionOrder":\{"type":"numeric"\}\}\],"limit":200\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""
    subSequentQuery should fullyMatch regex expectedSubQueryJson
  }


  test("Fact View Query Tests Adjustment Stats with constant column filter and sorting on const fact columns") {
    val jsonString =
      s"""{ "cube": "a_stats",
         |   "selectFields": [
         |      {
         |         "field": "Advertiser ID"
         |      },
         |      {
         |         "field": "Day"
         |      },
         |      {
         |         "field": "Is Adjustment"
         |      },
         |      {
         |         "field": "Impressions"
         |      },
         |      {
         |         "field": "Spend"
         |      },
         |      {
         |         "field": "Const Der Fact Col A"
         |      }
         |   ],
         |   "filterExpressions": [
         |      {
         |         "field": "Advertiser ID",
         |         "operator": "=",
         |         "value": "1035663"
         |      },
         |      {
         |         "field": "Is Adjustment",
         |         "operator": "=",
         |         "value": "Y"
         |      },
         |      {
         |         "field": "Day",
         |         "operator": "=",
         |         "value": "$fromDate"
         |      }
         |   ],
         |   "sortBy": [
         |      {
         |          "field": "Impressions",
         |          "order": "Asc"
         |      },
         |      {
         |          "field": "Spend",
         |          "order": "Desc"
         |      },
         |      {
         |          "field": "Const Der Fact Col A",
         |          "order": "Desc"
         |      }
         |   ]
         |}
      """.stripMargin

    val request: ReportingRequest = getReportingRequestSyncWithFactBias(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    require(requestModel.isSuccess, requestModel)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val expectedJson = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"account_stats"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"advertiser_id","value":"1035663"\},\{"type":"selector","dimension":"\{test_flag\}","value":"0"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"advertiser_id","outputName":"Advertiser ID","outputType":"STRING"\},\{"type":"default","dimension":"stats_date","outputName":"Day","outputType":"STRING"\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"roundingDoubleSum","name":"Spend","fieldName":"spend","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"longSum","name":"clicks","fieldName":"clicks"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Const Der Fact Col A","fn":"/","fields":\[\{"type":"fieldAccess","name":"clicks","fieldName":"clicks"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\{"dimension":"Impressions","direction":"ascending","dimensionOrder":\{"type":"numeric"\}\},\{"dimension":"Spend","direction":"descending","dimensionOrder":\{"type":"numeric"\}\},\{"dimension":"Const Der Fact Col A","direction":"descending","dimensionOrder":\{"type":"numeric"\}\}\],"limit":200\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""
    result should fullyMatch regex expectedJson

    val subSequentQuery = queryPipelineTry.toOption.get.queryChain.subsequentQueryList.head.asString
    val expectedSubQueryJson = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"a_adjustments"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"advertiser_id","value":"1035663"\},\{"type":"selector","dimension":"\{test_flag\}","value":"0"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"advertiser_id","outputName":"Advertiser ID","outputType":"STRING"\},\{"type":"default","dimension":"stats_date","outputName":"Day","outputType":"STRING"\}\],"aggregations":\[\{"type":"roundingDoubleSum","name":"Spend","fieldName":"spend","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\}\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\{"dimension":"Spend","direction":"descending","dimensionOrder":\{"type":"numeric"\}\}\],"limit":200\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""
    subSequentQuery should fullyMatch regex expectedSubQueryJson
  }

  test("dimension filter extraction function for decode dim should work for more than 2 mappings") {
    val jsonString =
      s"""{
                          "cube": "k_stats_derived_decode_dim",
                          "selectFields": [
                            {"field": "Advertiser ID"},
                            {"field": "Derived Ad ID"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": 123}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString, InternalSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val json =
      """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"123"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"extraction","dimension":"ad_id","outputName":"Derived Ad ID","outputType":"STRING","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"0":"EMPTY","-3":"EMPTY"\},"isOneToOne":false\},"retainMissingValue":true,"injective":false,"optimize":true\}\},\{"type":"default","dimension":"advertiser_id","outputName":"Advertiser ID","outputType":"STRING"\}\],"aggregations":\[\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""
    result should fullyMatch regex json
  }

  test("Druid query should be generated successfully with scan query type") {
    val jsonString =
      s"""{
                          "queryType": "scan",
                          "cube": "k_stats_select",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":5
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"scan","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*Z"\]\},"virtualColumns":\[\],"resultFormat":"list","batchSize":20480,"limit":5,"order":"none","filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"columns":\["id","__time","impressions"\],"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"timeout":5000,"queryId":".*"\},"descending":false,"granularity":\{"type":"all"\}\}"""
    result should fullyMatch regex json
  }

  test("Druid query should fail to generate with scan query type when sort by requested") {
    val jsonString =
      s"""{
                          "queryType": "scan",
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":5
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure, queryPipelineTry.errorMessage("query pipeline should fail"))
    assert(queryPipelineTry.failed.toOption.get.getMessage.contains("druid scan query type does not support sort by functionality!"))
  }

  test("Druid query should fail to generate with scan query type when derived fact requested") {
    val jsonString =
      s"""{
                          "queryType": "scan",
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Derived Pricing Type"},
                            {"field": "CTR"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":5
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure, queryPipelineTry.errorMessage("query pipeline should fail"))
    assert(queryPipelineTry.failed.toOption.get.getMessage.contains("druid scan query does not support derived columns : CTR"))
  }

  test("Druid query should fail to generate with scan query type when filter on derived fact requested") {
    val jsonString =
      s"""{
                          "queryType": "scan",
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Derived Pricing Type"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "CTR", "operator": "=", "value": "1.25"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":5
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure, queryPipelineTry.errorMessage("query pipeline should fail"))
    assert(queryPipelineTry.failed.toOption.get.getMessage.contains("druid scan query type does not support filter on derived columns: CTR"))
  }

  test("Druid query should fail to generate with scan query type when using druid for dimension joins") {
    val jsonString =
      s"""{
                          "queryType": "scan",
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Campaign ID"},
                            {"field": "Campaign Name"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":5
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString, AdvertiserSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)

    assert(queryPipelineTry.isFailure, queryPipelineTry.errorMessage("query pipeline should fail"))
    assert(queryPipelineTry.failed.toOption.get.getMessage.contains("requirement failed: druid scan query type does not support druid lookups!"))
  }

  test("dimension time extraction function for druid time when no timezone is specified in additional parameters") {
    val jsonString =
      s"""{
                          "queryType": "groupby",
                          "cube": "k_stats_date_select",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":5
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val json = """{"type":"extraction","dimension":"__time","outputName":"Day","outputType":"STRING","extractionFn":{"type":"timeFormat","format":"YYYY-MM-dd HH","timeZone":"UTC","granularity":{"type":"none"},"asMillis":false}}"""
    assert(result.contains(json), result)
  }

  test("dimension time extraction function for druid time when timezone is specified in the request") {
    val jsonString =
      s"""{
                          "queryType": "groupby",
                          "cube": "k_stats_date_select",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":5
                        }"""

    val request: ReportingRequest = ReportingRequest.withTimeZone(getReportingRequestSync(jsonString), "America/Los_Angeles")
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """__time","outputName":"Day","outputType":"STRING","extractionFn":{"type":"timeFormat","format":"YYYY-MM-dd HH","timeZone":"America/Los_Angeles","granularity":{"type":"none"},"asMillis":false}}"""
    assert(result.contains(json), result)
  }

  test("Filter on time dimension extracted using request context should render correctly") {
    val jsonString =
      s"""{
                          "queryType": "scan",
                          "cube": "k_stats_date_select",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":5
                        }"""

    val request: ReportingRequest = ReportingRequest.withTimeZone(getReportingRequestSync(jsonString), "America/Los_Angeles")
    val requestModel = getRequestModel(request, defaultRegistry)

    ColumnContext.withColumnContext { implicit cc =>
      val dayCol = DruidFuncDimCol("Date From Req Context", DateType(), TIME_FORMAT_WITH_REQUEST_CONTEXT("YYYY-MM-dd HH"))
      val filters = FilterDruid.renderDateDimFilters(requestModel.toOption.get, Map("Day" -> "Day"), Map("Day" -> dayCol), DailyGrain)
      assert(filters.nonEmpty)
    }
  }

  test("Inner should query should not have limitSpec if 'shouldLimitInnerQueries' is set to false") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Average Bid"},
                            {"field": "Impressions"},
                            {"field": "Advertiser Status"},
                            {"field": "Campaign Name"},
                            {"field": "Campaign Total"}
                            ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Derived Pricing Type", "operator": "between", "from": "1", "to": "20"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Advertiser Status", "operator": "In", "values": ["ON"]}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", ""))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    val druidQueryGenerator = new DruidQueryGenerator(new SyncDruidQueryOptimizer(timeout = 5000), 40000, shouldLimitInnerQueries = false)

    altQueryGeneratorRegistry.register(DruidEngine, druidQueryGenerator)
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val expect_empty_limitspec_inner_query = """"limitSpec":{"type":"NoopLimitSpec"},"context":{"applyLimitPushDown":"false""""
    val expect_nonempty_limitspec_outer_query = """"limitSpec":{"type":"default","columns":[],"limit":220}"""
    val expect_bound_filter = """{"type":"bound","dimension":"Derived Pricing Type","lower":"1","upper":"20","lowerStrict":false,"upperStrict":false,"ordering":{"type":"numeric"}}"""
    assert(result.contains(expect_empty_limitspec_inner_query))
    assert(result.contains(expect_nonempty_limitspec_outer_query))
    assert(result.contains(expect_bound_filter))
  }

  test("should generate nested groupby query if expensive date time filter is present") {
    val jsonString =
      s"""{
                          "cube": "k_stats_expensive_date_time",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Clicks"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = ReportingRequest.withTimeZone(getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", "")), "America/Los_Angeles")
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    //
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"query","query":\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"},"intervals":\{"type":"intervals","intervals":\[".*"\]},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"advertiser_id","value":"12345"}\]},"granularity":\{"type":"all"},"dimensions":\[\{"type":"extraction","dimension":"__time","outputName":"Day","outputType":"STRING","extractionFn":\{"type":"timeFormat","format":"YYYY-MM-dd HH","timeZone":"America/Los_Angeles","granularity":\{"type":"none"},"asMillis":false}}\],"aggregations":\[\{"type":"longSum","name":"Clicks","fieldName":"clicks"},\{"type":"longSum","name":"Impressions","fieldName":"impressions"}\],"postAggregations":\[\],"limitSpec":\{"type":"NoopLimitSpec"\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"},"descending":false}},"intervals":\{"type":"intervals","intervals":\[".*"\]},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"Day","value":".*"},\{"type":"selector","dimension":"Day","value":".*"},\{"type":"selector","dimension":"Day","value":".*"},\{"type":"selector","dimension":"Day","value":".*"},\{"type":"selector","dimension":"Day","value":".*"},\{"type":"selector","dimension":"Day","value":".*"},\{"type":"selector","dimension":"Day","value":".*"},\{"type":"selector","dimension":"Day","value":".*"}\]}\]},"granularity":\{"type":"all"},"dimensions":\[\{"type":"default","dimension":"Day","outputName":"Day","outputType":"STRING"}\],"aggregations":\[\{"type":"longSum","name":"Clicks","fieldName":"Clicks"},\{"type":"longSum","name":"Impressions","fieldName":"Impressions"}\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\],"limit":120},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"},"descending":false}"""

    result should fullyMatch regex json
  }

  test("should generate nested groupby query if expensive date time filter is present and inner groupby should include Day even though it's not in the original request") {
    val jsonString =
      s"""{
                          "cube": "k_stats_expensive_date_time",
                          "selectFields": [
                            {"field": "Clicks"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = ReportingRequest.withTimeZone(getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", "")), "America/Los_Angeles")
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    //
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"query","query":\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"extraction","dimension":"__time","outputName":"Day","outputType":"STRING","extractionFn":\{"type":"timeFormat","format":"YYYY-MM-dd HH","timeZone":"America/Los_Angeles","granularity":\{"type":"none"\},"asMillis":false\}\}\],"aggregations":\[\{"type":"longSum","name":"Clicks","fieldName":"clicks"\},\{"type":"longSum","name":"Impressions","fieldName":"impressions"\}\],"postAggregations":\[\],"limitSpec":\{"type":"NoopLimitSpec"\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"Day","value":".*"\},\{"type":"selector","dimension":"Day","value":".*"\},\{"type":"selector","dimension":"Day","value":".*"\},\{"type":"selector","dimension":"Day","value":".*"\},\{"type":"selector","dimension":"Day","value":".*"\},\{"type":"selector","dimension":"Day","value":".*"\},\{"type":"selector","dimension":"Day","value":".*"\},\{"type":"selector","dimension":"Day","value":".*"\}\]\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\],"aggregations":\[\{"type":"longSum","name":"Clicks","fieldName":"Clicks"\},\{"type":"longSum","name":"Impressions","fieldName":"Impressions"\}\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""

    result should fullyMatch regex json
  }

  test("populate context with hostname") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Average Bid"},
                            {"field": "Impressions"},
                            {"field": "Average Position"},
                            {"field": "Advertiser Status"},
                            {"field": "Reblogs"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithHostName(jsonString, "127.1.1.0")
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    assert(result.contains(""""hostName":"127.1.1.0""""))
  }

  test("namespace lookup extraction functionality for timestamp") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Impressions"},
                            {"field": "Advertiser Last Updated"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = ReportingRequest.forceDruid(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    assert(query.contains("""{"type":"extraction","dimension":"Advertiser Last Updated","outputName":"Advertiser Last Updated","outputType":"STRING","extractionFn":{"type":"timeFormat","format":"YYYYMMdd","timeZone":"UTC","granularity":{"type":"none"},"asMillis":true}}"""))
  }

  test("Successfully generate a query with HyperUniques Filter") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Average Bid"},
                            {"field": "Unique Ad IDs"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Ad ID", "operator": "==", "compareTo": "Ad Group ID"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", "someUser"))
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val json = "{\"type\":\"hyperUnique\",\"name\":\"Unique Ad IDs\",\"fieldName\":\"ad_id\",\"isInputHyperUnique\":false,\"round\":true}"

    assert(result.contains(json))
  }

  test("Successfully generate a query with HyperUniqueCardinalityWrapper") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Average Bid"},
                            {"field": "Unique Ad IDs"},
                            {"field": "Unique Ad IDs Count"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Ad ID", "operator": "==", "compareTo": "Ad Group ID"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", "someUser"))
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = "{\"type\":\"hyperUniqueCardinality\",\"name\":\"Unique Ad IDs Count\",\"fieldName\":\"Unique Ad IDs\"}"
    assert(result.contains(json))
  }

  test("Successfully generate a query with CardinalityRollup") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Average Bid"},
                            {"field": "Ad IDs Cardinality"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Ad ID", "operator": "==", "compareTo": "Ad Group ID"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", "someUser"))
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val json = "{\"type\":\"cardinality\",\"name\":\"Ad IDs Cardinality\",\"fields\":[{\"type\":\"default\",\"dimension\":\"ad_id\",\"outputName\":\"ad_id\",\"outputType\":\"STRING\"}],\"byRow\":false,\"round\":true}"

    assert(result.contains(json))
  }

  test("Successfully set query priority for async request") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Average Bid"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Hour", "operator": "=", "value": "10"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = ReportingRequest.forceDruid(getReportingRequestAsync(jsonString))
    val requestModel = getRequestModel(request, defaultRegistry)
    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator(new AsyncDruidQueryOptimizer())) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val json = """"priority":-1"""
    assert(result.contains(json), json)
  }

  test("test row count groupby query") {
    val jsonString_inner =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Advertiser ID"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "mr": 100,
                          "curators" : {
                            "rowcount" : {
                              "config" : {
                                "isFactDriven": true
                              }
                            }
                          }
                        }"""

    val request1: ReportingRequest = getReportingRequestSync(jsonString_inner).copy(queryType = RowCountQuery)
    val requestModel1 = getRequestModel(request1, getDefaultRegistry())
    val queryPipelineTry1 = generatePipeline(requestModel1.toOption.get)
    assert(queryPipelineTry1.isSuccess, queryPipelineTry1.errorMessage("Fail to get the query pipeline"))

    val result1 = queryPipelineTry1.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    //    println(result1)
    assert(StringUtils.countMatches(result1, """"queryType":"groupBy"""") == 2, "Failed to generate 2-level groupby query")
    assert(!result1.contains(""""limitSpec":{"type":"default","columns":[],"limit":200}"""), "Failed to remove inner limitSpec")
    assert(result1.contains(""""aggregations":[{"type":"count","name":"TOTALROWS"}]"""), "Failed to generate row count aggregator")

    val jsonString_outer =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Advertiser ID"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Advertiser Status", "operator": "=", "value": "ON"}
                          ],
                          "mr":100,
                          "curators" : {
                            "rowcount" : {
                              "config" : {
                                "isFactDriven": true
                              }
                            }
                          }
                        }"""

    // request from RowCountCurator will change queryType to RowCountQuery
    val request2: ReportingRequest = getReportingRequestSync(jsonString_outer).copy(queryType = RowCountQuery)
    val requestModel2 = getRequestModel(request2, getDefaultRegistry())
    val queryPipelineTry2 = generatePipeline(requestModel2.toOption.get)
    assert(queryPipelineTry2.isSuccess, queryPipelineTry2.errorMessage("Fail to get the query pipeline"))

    val result2 = queryPipelineTry2.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    //    println(result2)
    assert(StringUtils.countMatches(result2, """"queryType":"groupBy"""") == 3, "Failed to generate 3-level groupby query")
    assert(!result2.contains(""""limitSpec":{"type":"default","columns":[],"limit":200}"""), "Failed to remove limitSpec")
    assert(result2.contains(""""aggregations":[{"type":"count","name":"TOTALROWS"}]"""), "Failed to generate row count aggregator")
  }

  test("Test generating a query for a request with derived fact column using JavaScript Aggregator") {
    val jsonString =
      s"""{
                        "cube": "k_stats",
                        "selectFields": [
                          {"field": "Day"},
                          {"field": "Impressions"},
                          {"field": "Clicks"},
                          {"field": "Variance"},

                          {"field": "Advertiser ID"}
                        ],
                        "filterExpressions": [
                          {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                          {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                        ],
                        "paginationStartIndex":20,
                        "rowsPerPage":100
                      }"""

    val request: ReportingRequest = ReportingRequest.forceDruid(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    assert(query.contains(""""postAggregations":[{"type":"javascript","name":"Variance","fieldNames":["Clicks","Impressions"],"function":"function(clicks,impressions){return clicks * Math.sqrt(impressions);}"}]"""))
  }

  test("limit should be set when rowsPerPage is specified for Async") {
    val jsonString =
      s"""{
                          "cube": "user_stats",
                          "selectFields": [
                            {"field": "Impressions"},
                            {"field": "Clicks"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":2
                        }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val json = """limit":22"""

    assert(result.contains(json), result)
  }

  test("Test generating a query for a request with derived fact column using ThetaSketchEstimateWrapper") {
    val jsonString =
      s"""{
                        "cube": "k_stats",
                        "selectFields": [
                          {"field": "Day"},
                          {"field": "Impressions"},
                          {"field": "Clicks"},
                          {"field": "segments_unique_users"},
                          {"field": "Conversion User Count"},
                          {"field": "Conv Segments Unique User Count"},

                          {"field": "Advertiser ID"}
                        ],
                        "filterExpressions": [
                          {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                          {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                        ],
                        "paginationStartIndex":20,
                        "rowsPerPage":100
                      }"""

    val request: ReportingRequest = ReportingRequest.forceDruid(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    assert(query.contains(s""""postAggregations":[{"type":"arithmetic","name":"Conv Segments Unique User Count","fn":"+","fields":[{"type":"thetaSketchEstimate","name":"conv_unique_users","field":{"type":"fieldAccess","name":"conv_unique_users","fieldName":"Conversion User Count"}},{"type":"thetaSketchEstimate","name":"segments_unique_users","field":{"type":"fieldAccess","name":"segments_unique_users","fieldName":"segments_unique_users"}}]}]"""))
  }

  test("Test generating a query with a derived column which is used in both its derived column and its derived derived column") {
    val jsonString =
      s"""{
                        "cube": "k_stats",
                        "selectFields": [
                          {"field": "Day"},
                          {"field": "Impressions"},
                          {"field": "Clicks"},
                          {"field": "segments_unique_users"},
                          {"field": "Variance"},
                          {"field": "Conversion User Count"},
                          {"field": "Conv Segments Unique User Count"},
                          {"field": "Segment Count By Variance"},

                          {"field": "Advertiser ID"}
                        ],
                        "filterExpressions": [
                          {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                          {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                        ],
                        "paginationStartIndex":20,
                        "rowsPerPage":100
                      }"""

    val request: ReportingRequest = ReportingRequest.forceDruid(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    print(query)
    assert(query.contains(s""""postAggregations":[{"type":"arithmetic","name":"Conv Segments Unique User Count","fn":"+","fields":[{"type":"thetaSketchEstimate","name":"conv_unique_users","field":{"type":"fieldAccess","name":"conv_unique_users","fieldName":"Conversion User Count"}},{"type":"thetaSketchEstimate","name":"segments_unique_users","field":{"type":"fieldAccess","name":"segments_unique_users","fieldName":"segments_unique_users"}}]},{"type":"javascript","name":"variance","fieldNames":["Clicks","Impressions"],"function":"function(clicks,impressions){return clicks * Math.sqrt(impressions);}"},{"type":"arithmetic","name":"Derived User Count Plus Variance","fn":"+","fields":[{"type":"fieldAccess","name":"Conv Segments Unique User Count","fieldName":"Conv Segments Unique User Count"},{"type":"fieldAccess","name":"variance","fieldName":"variance"}]},{"type":"arithmetic","name":"Segment Count By Variance","fn":"/","fields":[{"type":"fieldAccess","name":"Derived User Count Plus Variance","fieldName":"Derived User Count Plus Variance"},{"type":"fieldAccess","name":"variance","fieldName":"variance"}]}]"""))
  }

  test("Successfully generate a time extraction function when period granularity is specified") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Impressions"},
                            {"field": "Week Start"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", "someUser"))
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"extraction","dimension":"__time","outputName":"Week Start","outputType":"STRING","extractionFn":\{"type":"timeFormat","format":"yyyy-MM-dd","timeZone":"UTC","granularity":"WEEK","asMillis":false\}\},\{"type":"default","dimension":"id","outputName":"Keyword ID","outputType":"STRING"\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\}\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"applyLimitPushDown":"false","userId":"someUser","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""
    result should fullyMatch regex json
  }

  test("Successfully generate a filter on an aggregated fact col even when the fact is not in the selected fields") {
    val jsonString =
      s"""{
                      "cube": "k_stats",
                      "selectFields": [
                        {"field": "Keyword ID"},
                        {"field": "Keyword Value"},
                        {"field": "Clicks"}
                      ],
                      "filterExpressions": [
                        {"field": "Day", "operator": "=", "value": "$fromDate"},
                        {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                        {"field": "Impressions", "operator": ">", "value": "5"}
                      ],
                      "paginationStartIndex":20,
                      "rowsPerPage":100
                    }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", "someUser"))
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"id","outputName":"Keyword ID","outputType":"STRING"\}\],"aggregations":\[\{"type":"longSum","name":"Clicks","fieldName":"clicks"\},\{"type":"longSum","name":"Impressions","fieldName":"impressions"\}\],"postAggregations":\[\],"having":\{"type":"and","havingSpecs":\[\{"type":"greaterThan","aggregation":"Impressions","value":5\}\]\},"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"applyLimitPushDown":"false","userId":"someUser","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""
    result should fullyMatch regex json
  }

  test("Druid query should be generated with NotLike filter") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Advertiser ID"},
                            {"field": "Impressions"}
                            ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Campaign Name", "operator": "not like", "value": "test"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = ReportingRequest.forceDruid(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    assert(result.contains(""""filter":{"type":"and","fields":[{"type":"not","field":{"type":"search","dimension":"Campaign Name","query":{"type":"insensitive_contains","value":"test","caseSensitive":false}}}]}"""))
  }

  test("Druid query with Lookups should contain column alias if available") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Advertiser ID"},
                            {"field": "Campaign Name"}
                            ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Campaign Name", "operator": "not like", "value": "test"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = ReportingRequest.forceDruid(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    //println(result)
  }

  test("Druid query should retain missing value if LOOKUP_WITH_EMPTY_VALUE_OVERRIDE is used") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Advertiser ID"},
                            {"field": "Impressions"},
                            {"field": "Campaign Name Ext"}
                            ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = ReportingRequest.forceDruid(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    assert(result.contains(s""""extractionFn":{"type":"mahaRegisteredLookup","lookup":"campaign_lookup","retainMissingValue":true,"injective":false,"optimize":true,"valueColumn":"name","dimensionOverrideMap":{},"useQueryLevelCache":false}}"""))
  }

  test("Druid query should be generated with sort on lookup dimensions") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Campaign ID"},
                            {"field": "Campaign Name"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Campaign Name", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":0
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = getRequestModel(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    //println(result)
    val expected = s""""limitSpec":{"type":"default","columns":[{"dimension":"Campaign Name","direction":"ascending","dimensionOrder":{"type":"lexicographic"}}],"limit":1020}""".stripMargin
    assert(result.contains(expected))
  }

  test("Multiple filters on same column") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Campaign ID"},
                            {"field": "Campaign Name"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Campaign Name", "operator": "IsNotNull"},
                            {"field": "Campaign Name", "operator": "<>", "value": "-3"}
                          ],
                          "sortBy": [
                            {"field": "Campaign Name", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":0
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestFilterExpressions = Vector(EqualityFilter("Advertiser ID", "12345"), IsNotNullFilter("Campaign Name"), NotEqualToFilter("Campaign Name", "-3"))
    assert(request.filterExpressions.equals(requestFilterExpressions))

    val requestModel = getRequestModel(request, getDefaultRegistry())
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))
    val requestModelDimFilters = scala.collection.immutable.TreeSet(PushDownFilter(EqualityFilter("Advertiser ID", "12345")), NotEqualToFilter("Campaign Name", "-3"), IsNotNullFilter("Campaign Name"))
    assert(requestModel.get.dimensionsCandidates.head.filters.equals(requestModelDimFilters))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val queryPipelineDimFilters = scala.collection.immutable.TreeSet(PushDownFilter(EqualityFilter("Advertiser ID", "12345")), NotEqualToFilter("Campaign Name", "-3"), IsNotNullFilter("Campaign Name"))
    assert(queryPipelineTry.get.bestDimCandidates.head.filters.equals(queryPipelineDimFilters))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    //println(result)
    val expectedFilter = s""""filter":{"type":"and","fields":[{"type":"not","field":{"type":"selector","dimension":"Campaign Name","value":"-3"}},{"type":"not","field":{"type":"selector","dimension":"Campaign Name"}}]}"""
    assert(result.contains(expectedFilter))
  }

  test("Query with both aliases") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Campaign ID"},
                            {"field": "Campaign Name"},
                            {"field": "Ad Format Name"},
                            {"field": "Ad Format Sub Type"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "in", "values": ["$fromDate", "$toDate"]},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Campaign Name", "operator": "<>", "value": "-3"}
                          ],
                          "sortBy": [
                            {"field": "Campaign Name", "order": "Asc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":0
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)

    val requestModel = getRequestModel(request, getDefaultRegistry())
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val expectedQuery = """\{"queryType":"groupBy","dataSource":\{"type":"query","query":\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*",".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"campaign_id_alias","outputName":"Campaign ID","outputType":"STRING"\},\{"type":"extraction","dimension":"ad_format_id","outputName":"Ad Format Name","outputType":"STRING","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"98":"DPA View More","8":"Video with HTML Endcard","100":"DPA Single Image Ad","4":"Single image","9":"Carousel","99":"DPA Extended Carousel","35":"Product Ad","5":"Single image","6":"Single image","2":"Single image","101":"DPA Carousel Ad","7":"Video","97":"DPA Collection Ad","3":"Single image"\},"isOneToOne":false\},"retainMissingValue":false,"replaceMissingValueWith":"Other","injective":false,"optimize":true\}\},\{"type":"extraction","dimension":"ad_format_id","outputName":"Ad Format Sub Type","outputType":"STRING","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"98":"DPA View More","100":"DPA Single Image Ad","99":"DPA Extended Carousel","35":"Product Ad","101":"DPA Carousel Ad","97":"DPA Collection Ad"\},"isOneToOne":false\},"retainMissingValue":false,"replaceMissingValueWith":"N/A","injective":false,"optimize":true\}\},\{"type":"extraction","dimension":"campaign_id_alias","outputName":"Campaign Name","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"campaign_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"name","dimensionOverrideMap":\{\},"useQueryLevelCache":false\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\}\],"postAggregations":\[\],"limitSpec":\{"type":"NoopLimitSpec"\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}\},"intervals":\{"type":"intervals","intervals":\[".*",".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"not","field":\{"type":"selector","dimension":"Campaign Name","value":"-3"\}\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"Campaign ID","outputName":"Campaign ID","outputType":"STRING"\},\{"type":"default","dimension":"Ad Format Name","outputName":"Ad Format Name","outputType":"STRING"\},\{"type":"default","dimension":"Ad Format Sub Type","outputName":"Ad Format Sub Type","outputType":"STRING"\},\{"type":"default","dimension":"Campaign Name","outputName":"Campaign Name","outputType":"STRING"\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"Impressions"\}\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\{"dimension":"Campaign Name","direction":"ascending","dimensionOrder":\{"type":"lexicographic"\}\}\],"limit":2020\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""
    result should fullyMatch regex expectedQuery
  }


  test("Query with both aliases with filter and sort by") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Ad Format Name"},
                              {"field": "Ad Format Sub Type"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Ad Format Name", "operator": "=", "value": "Product Ad"},
                              {"field": "Ad Format Sub Type", "operator": "<>", "value": "DPA Single Image Ad"},
                              {"field": "Ad Format Sub Type", "operator": "=", "value": "DPA Collection Ad"},
                              {"field": "Campaign Name", "operator": "IsNotNull"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              { "field": "Advertiser ID", "order": "Asc"},
                              { "field": "Ad Format Name", "order": "Asc"}
                          ]
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)

    val requestModel = getRequestModel(request, getDefaultRegistry())
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val expectedQuery = """\{"queryType":"groupBy","dataSource":\{"type":"query","query":\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":.*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"not","field":\{"type":"selector","dimension":"ad_format_id","value":"100"\}\},\{"type":"selector","dimension":"ad_format_id","value":"35"\},\{"type":"selector","dimension":"ad_format_id","value":"97"\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"advertiser_id","outputName":"Advertiser ID","outputType":"STRING"\},\{"type":"extraction","dimension":"ad_format_id","outputName":"Ad Format Name","outputType":"STRING","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"98":"DPA View More","8":"Video with HTML Endcard","100":"DPA Single Image Ad","4":"Single image","9":"Carousel","99":"DPA Extended Carousel","35":"Product Ad","5":"Single image","6":"Single image","2":"Single image","101":"DPA Carousel Ad","7":"Video","97":"DPA Collection Ad","3":"Single image"\},"isOneToOne":false\},"retainMissingValue":false,"replaceMissingValueWith":"Other","injective":false,"optimize":true\}\},\{"type":"extraction","dimension":"ad_format_id","outputName":"Ad Format Sub Type","outputType":"STRING","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"98":"DPA View More","100":"DPA Single Image Ad","99":"DPA Extended Carousel","35":"Product Ad","101":"DPA Carousel Ad","97":"DPA Collection Ad"\},"isOneToOne":false\},"retainMissingValue":false,"replaceMissingValueWith":"N/A","injective":false,"optimize":true\}\},\{"type":"extraction","dimension":"campaign_id_alias","outputName":"Campaign Name","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"campaign_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"name","dimensionOverrideMap":\{\},"useQueryLevelCache":false\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\}\],"postAggregations":\[\],"limitSpec":\{"type":"NoopLimitSpec"\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"not","field":\{"type":"selector","dimension":"Campaign Name"\}\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"Advertiser ID","outputName":"Advertiser ID","outputType":"STRING"\},\{"type":"default","dimension":"Ad Format Name","outputName":"Ad Format Name","outputType":"STRING"\},\{"type":"default","dimension":"Ad Format Sub Type","outputName":"Ad Format Sub Type","outputType":"STRING"\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"Impressions"\}\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\{"dimension":"Advertiser ID","direction":"ascending","dimensionOrder":\{"type":"numeric"\}\},\{"dimension":"Ad Format Name","direction":"ascending","dimensionOrder":\{"type":"numeric"\}\}\],"limit":400\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""
    result should fullyMatch regex expectedQuery

  }

  test("Query on one hour of one day") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$toDate", "to": "$toDate"},
                              {"field": "Hour", "operator": "between", "from": "00", "to": "00"}

                          ],
                          "sortBy": [
                              { "field": "Advertiser ID", "order": "Asc"}
                          ]
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)

    val requestModel = getRequestModel(request, getDefaultRegistry())
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val expectedContents = s""""applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000"""
    assert(result.contains(expectedContents))

  }

  test("Query on two hours of one day") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$toDate", "to": "$toDate"},
                              {"field": "Hour", "operator": "between", "from": "00", "to": "01"}

                          ],
                          "sortBy": [
                              { "field": "Advertiser ID", "order": "Asc"}
                          ]
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)

    val requestModel = getRequestModel(request, getDefaultRegistry())
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val expectedContents = s""""applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000"""
    assert(result.contains(expectedContents))

  }

  test("Query on one hour of two days") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$toDateMinusOne", "to": "$toDate"},
                              {"field": "Hour", "operator": "between", "from": "00", "to": "00"}

                          ],
                          "sortBy": [
                              { "field": "Advertiser ID", "order": "Asc"}
                          ]
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)

    val requestModel = getRequestModel(request, getDefaultRegistry())
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val expectedContents = s""""applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000"""
    assert(result.contains(expectedContents))

  }

  test("Successfully generate a query with Javascript extractionFn and Javascript filter") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Average Bid"},
                            {"field": "Segments"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Segments", "operator": "=", "value": "True"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", "someUser"))
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json1 = """{"type":"selector","dimension":"segment_values","value":"True","extractionFn":{"type":"javascript","function":"function(x) { return x > 0; }","injective":false}}"""
    val json2 = """{"type":"extraction","dimension":"segment_values","outputName":"Segments","outputType":"STRING","extractionFn":{"type":"javascript","function":"function(x) { return x > 0; }","injective":false}}"""
    assert (result.contains(json1), "Missing selector JSON in result: " + result)
    assert (result.contains(json2), "Missing dimension JSON in result: " + result)
  }

  test("Generate a valid query at daily grain with datetime between filter with timestamp type for Day, use Union table def") {
    val cubes = List("k_stats_daily_grain_ts_dtf2")
    cubes.foreach {
      cube =>
        val jsonString =
          s"""{
                          "cube": "$cube",
                          "selectFields": [
                            {"field": "Day"},
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
                            {"field": "Day", "operator": "datetimebetween", "from": "${fromDateTime}", "to": "$toDateTime", "format": "$iso8601Format"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""
        val request: ReportingRequest = getReportingRequestSync(jsonString)
        val requestModel = getRequestModel(request, defaultRegistry)
        assert(requestModel.isSuccess, requestModel.errorMessage("Failed to get request model"))
        val queryPipelineTry = generatePipeline(requestModel.toOption.get)
        assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

        val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
        assert(result.contains("Day") && result.contains("selector") && result.contains(DailyGrain.toFullFormattedString(baseDate)), result)
        assert(result.contains(""""dataSource":{"type":"union","dataSources":[{"type":"table","name":"fact1"},{"type":"table","name":"fact2"}]}"""))
    }
  }

  test("Generate a single-source query at daily grain with datetime between filter with timestamp type for Day, with malformed csv") {
    val cubes = List("k_stats_daily_grain_ts_dtf3")
    cubes.foreach {
      cube =>
        val jsonString =
          s"""{
                          "cube": "$cube",
                          "selectFields": [
                            {"field": "Day"},
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
                            {"field": "Day", "operator": "datetimebetween", "from": "${fromDateTime}", "to": "$toDateTime", "format": "$iso8601Format"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""
        val request: ReportingRequest = getReportingRequestSync(jsonString)
        val requestModel = getRequestModel(request, defaultRegistry)
        assert(requestModel.isSuccess, requestModel.errorMessage("Failed to get request model"))
        val queryPipelineTry = generatePipeline(requestModel.toOption.get)
        assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

        val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
        assert(result.contains("Day") && result.contains("selector") && result.contains(DailyGrain.toFullFormattedString(baseDate)), result)
        assert(result.contains("\"dataSource\":{\"type\":\"table\",\"name\":\"fact1\"}"))
    }
  }

  test("Should generate a Druid query with Hour filtered but not selected.") {
    val jsonString =
      s"""{
                          "cube": "k_stats_date_select",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Clicks"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "Between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Hour", "operator": "Between", "from": "01", "to": "01"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", "someUser"))
    val requestModel = getRequestModel(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json1 = """{"type":"extraction","dimension":"__time","outputName":"Hour","outputType":"STRING","extractionFn":{"type":"timeFormat","format":"HH","timeZone":"UTC","granularity":{"type":"none"},"asMillis":false}}"""
    val json2 = """{"type":"extraction","dimension":"__time","outputName":"Day","outputType":"STRING","extractionFn":{"type":"timeFormat","format":"YYYY-MM-dd HH","timeZone":"UTC","granularity":{"type":"none"},"asMillis":false}}"""
    val json3 = """"context":{"groupByStrategy":"v2","applyLimitPushDown":"false","userId":"someUser","uncoveredIntervalsLimit":100,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"}"""
    assert(result.contains(json1), "Assume valid Hour extraction: " + result)
    assert(result.contains(json2), "Assumed valid Day extraction: " + result)
    assert(result.contains(json3), "Addumed uncoveredIntervals could be overwritten: " + result)
  }

  test("Validate lookup override works") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Average Bid"},
                            {"field": "Impressions"},
                            {"field": "External Site Name"},
                            {"field": "Advertiser Status"},
                            {"field": "Currency"},
                            {"field": "Timezone"},
                            {"field": "Timezone2"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Advertiser Status", "operator": "=", "value": "ON"},
                            {"field": "Currency", "operator": "=", "value": "USD"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString, InternalSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(defaultFactEngine))(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val expected = """{"type":"extraction","dimension":"advertiser_id","outputName":"Timezone2","outputType":"STRING","extractionFn":{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"COL_IS_MISSING!!","injective":false,"optimize":true,"valueColumn":"timezone","decode":{"columnToCheck":"timezone","valueToCheck":"US","columnIfValueMatched":"timezone","columnIfValueNotMatched":"currency"},"dimensionOverrideMap":{},"useQueryLevelCache":true}}"""
    assert(result.contains(expected))
  }


}

