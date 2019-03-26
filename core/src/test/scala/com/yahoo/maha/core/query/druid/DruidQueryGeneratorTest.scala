// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.druid

import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core._
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.request.{ReportingRequest, RequestContext}

/**
 * Created by hiral on 1/14/16.
 */
class DruidQueryGeneratorTest extends BaseDruidQueryGeneratorTest {

  lazy val defaultRegistry = getDefaultRegistry()

  test("registering Druid query generation multiple times should fail") {
    intercept[IllegalArgumentException] {
      val dummyQueryGenerator = new QueryGenerator[WithDruidEngine] {
        override def generate(queryContext: QueryContext): Query = { null }
        override def engine: Engine = DruidEngine
      }
      queryGeneratorRegistry.register(DruidEngine, dummyQueryGenerator)
    }
  }

  test("dim query context should fail with UnsupportedOperationException") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val dimMapping = DefaultQueryPipelineFactory.findDimCandidatesMapping(requestModel.get)
    val dims = DefaultQueryPipelineFactory.findBestDimCandidates(DruidEngine, requestModel.get.schema, dimMapping, DefaultQueryPipelineFactory.druidMultiQueryEngineList)
    val queryContext = new QueryContextBuilder(DimOnlyQuery, requestModel.get).addDimTable(dims).build()
    val druidQueryGenerator = getDruidQueryGenerator
    intercept[UnsupportedOperationException] {
      druidQueryGenerator.generate(queryContext)
    }
  }

  test("limit should be set to defaultMaxRows when maxRows is less than 1") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """limit":1020"""

    assert(result.contains(json), result)
  }

  test("Druid query should be generated with greater than filter") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """{"type":"greaterThan","aggregation":"Impressions","value":1000}]}"""

    assert(result.contains(json), result)
  }

  test("Druid query should be generated with less than filter") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """{"type":"lessThan","aggregation":"Impressions","value":1000}]}"""

    assert(result.contains(json), result)
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    println(result)
    val json = """limit":1020"""

    assert(result.contains(json), result)
  }

  test("DruidQueryGenerator: getAggregatorFactory should fail on DruidFilteredListRollup with only 1 list element") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(!queryPipelineTry.isSuccess && queryPipelineTry.errorMessage("").contains("ToUse FilteredListAggregator filterList must have 2 or more filters"), "Query pipeline should have failed, but didn't" + queryPipelineTry.errorMessage(""))
  }

  test("limit should be set to defaultMaxRowsAsync when request is Async") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """limit":100000"""

    assert(result.contains(json), result)
  }

  test("group by strategy should be set to v2 and no chunk period") {
    val jsonString = s"""{
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
    val requestModelTry = RequestModel.from(request, defaultRegistry)
    assert(requestModelTry.isSuccess, requestModelTry.errorMessage("Building request model failed"))

    val requestModel = requestModelTry.toOption.get.copy(
      dimCardinalityEstimate = Some(DruidQueryGenerator.defaultMaxSingleThreadedDimCardinality + 1)
      , factCost = requestModelTry.toOption.get.factCost.mapValues(rc => rc.copy(costEstimate = DruidQueryGenerator.defaultMaxNoChunkCost + 1))
    )
    val queryPipelineTry = generatePipeline(requestModel)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """"groupByStrategy":"v2""""

    assert(result.contains(json), result)
    assert(!result.contains("chunkPeriod"))
  }

  test("metric should be set to inverted when order is Desc and queryType is topN") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """"metric":{"type":"numeric","metric":"Impressions"}"""

    assert(result.contains(json), result)
  }

  test("for topN queryType aggregations, postAggregations, dimension and filter should be set") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val json = """\{"queryType":"topN","dataSource":\{"type":"table","name":"fact1"\},"virtualColumns":\[\],"dimension":\{"type":"default","dimension":"id","outputName":"Keyword ID","outputType":"STRING"\},"metric":\{"type":"numeric","metric":"Impressions"\},"threshold":120,"intervals":\{"type":"intervals","intervals":\[".*"\]\},"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\}\],"context":\{"applyLimitPushDown":"false","userId":"someUser","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""

    result should fullyMatch regex json
  }

  test("Successfully generate a query with Javascript extractionFn") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"statsDate","value":"[0-9]+"\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"extraction","dimension":"segments","outputName":"Segments","outputType":"STRING","extractionFn":\{"type":"javascript","function":"function\(x\) \{ return x > 0; \}","injective":false\}\},\{"type":"default","dimension":"id","outputName":"Keyword ID","outputType":"STRING"\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\{"dimension":"Impressions","direction":"descending","dimensionOrder":\{"type":"numeric"\}\}\],"limit":120\},"context":\{"applyLimitPushDown":"false","userId":"someUser","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""

    result should fullyMatch regex json
  }

  test("Successfully generate a query with Javascript Filter") {
    val jsonString = s"""{
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
                            {"field": "Keyword ID", "operator": "==", "compareTo": "Ad Group ID"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", "someUser"))
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\},\{"type":"columnComparison","dimensions":\[\{"type":"default","dimension":"id","outputName":"id","outputType":"STRING"\},\{"type":"default","dimension":"ad_group_id","outputName":"ad_group_id","outputType":"STRING"\}\]\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"id","outputName":"Keyword ID","outputType":"STRING"\}\],"aggregations":\[\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"filtered","aggregator":\{"type":"thetaSketch","name":"Conversion User Count","fieldName":"uniqueUserCount","size":16384,"shouldFinalize":true,"isInputThetaSketch":false\},"filter":\{"type":"javascript","dimension":"segments","function":"function\(x\) \{ return x \> 0; \}"\},"name":"Conversion User Count"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"\/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"applyLimitPushDown":"false","userId":"someUser","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""

    result should fullyMatch regex json
  }

  test("Successfully generate a query with RegEx extractionFn") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

//    println(result)

    assert(result.contains("{\"type\":\"extraction\",\"dimension\":\"internal_bucket_id\",\"outputName\":\"Click Exp ID\",\"outputType\":\"STRING\",\"extractionFn\":{\"type\":\"regex\",\"expr\":\"(cl-)(.*?)(,)\",\"index\":2,\"replaceMissingValue\":true,\"replaceMissingValueWith\":\"-3\"}}"), result)
  }

  test("Successfully generate a query with RegEx Filter") {
    val jsonString = s"""{
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
                            {"field": "Keyword ID", "operator": "==", "compareTo": "Ad Group ID"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", "someUser"))
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

//    println(result)

    assert(result.contains("{\"type\":\"selector\",\"dimension\":\"internal_bucket_id\",\"value\":\"abcd\",\"extractionFn\":{\"type\":\"regex\",\"expr\":\"(cl-)(.*?)(,)\",\"index\":2,\"replaceMissingValue\":true,\"replaceMissingValueWith\":\"-3\"}},{\"type\":\"columnComparison\",\"dimensions\":[{\"type\":\"default\",\"dimension\":\"id\",\"outputName\":\"id\",\"outputType\":\"STRING\"},{\"type\":\"default\",\"dimension\":\"ad_group_id\",\"outputName\":\"ad_group_id\",\"outputType\":\"STRING\"}]}]},\"granularity\":{\"type\":\"all\"},\"dimensions\":[{\"type\":\"extraction\",\"dimension\":\"internal_bucket_id\",\"outputName\":\"Click Exp ID\",\"outputType\":\"STRING\",\"extractionFn\":{\"type\":\"regex\",\"expr\":\"(cl-)(.*?)(,)\",\"index\":2,\"replaceMissingValue\":true,\"replaceMissingValueWith\":\"-3\"}}"), result)
  }

  test("Should fail to generate a metric query with Field Comparison Filter") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure && queryPipelineTry.failed.get.getMessage.contains("Column Comparison is not supported on Druid fact fields"), "Column comparison should fail for metrics.")
  }

  test("DruidQueryGenerator: arbitrary static mapping should succeed") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"extraction","dimension":"landing_page_url","outputName":"Landing URL Translation",\"outputType\":\"STRING\","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"Valid":"Something"\},"isOneToOne":false\},"retainMissingValue":false,"replaceMissingValueWith":"Empty","injective":false,"optimize":true\}\},\{"type":"default","dimension":"id","outputName":"Keyword ID"\,\"outputType\":\"STRING\"}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\}\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\{"dimension":"Impressions","direction":"descending","dimensionOrder":\{"type":"numeric"\}\}\],"limit":120\},"context":\{"applyLimitPushDown":"false","userId":"someUser","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""

    result should fullyMatch regex json
  }

  test("for timeseries queryType aggregations, postAggregations and filter should be set but not dimension") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"timeseries","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"descending":false,"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":"DAY","aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\}\],"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\}\}"""

    result should fullyMatch regex json
  }

  test("direction in limitSpec should be set to DESCENDING when order is Desc and queryType is groupBy") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"extraction","dimension":"price_type","outputName":"Pricing Type",\"outputType\":\"STRING\","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"-10":"CPE","-20":"CPF","6":"CPV","1":"CPC","2":"CPA","7":"CPCV","3":"CPM"\},"isOneToOne":false\},"retainMissingValue":false,"replaceMissingValueWith":"NONE","injective":false,"optimize":true\}\},\{"type":"default","dimension":"id","outputName":"Keyword ID"\,\"outputType\":\"STRING\"}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"doubleMax","name":"Max Bid","fieldName":"max_bid"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"longSum","name":"Clicks","fieldName":"clicks"\},\{"type":"doubleMin","name":"Min Bid","fieldName":"min_bid"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\},\{"type":"arithmetic","name":"CTR","fn":"/","fields":\[\{"type":"fieldAccess","name":"clicks","fieldName":"Clicks"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\{"dimension":"Impressions","direction":"descending","dimensionOrder":\{"type":"numeric"\}\}\],"limit":120\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""

    result should fullyMatch regex json
  }

  test("dimension based aggregate via filtered aggregate for fact col") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"stats_source","outputName":"Source"\,\"outputType\":\"STRING\"},\{"type":"default","dimension":"id","outputName":"Keyword ID"\,\"outputType\":\"STRING\"}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"filtered","aggregator":\{"type":"longSum","name":"Reblogs","fieldName":"engagement_count"\},"filter":\{"type":"selector","dimension":"engagement_type","value":"1"\},"name":"Reblogs"\},\{"type":"longSum","name":"Clicks","fieldName":"clicks"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Reblog Rate","fn":"\*","fields":\[\{"type":"arithmetic","name":"_placeHolder_2","fn":"/","fields":\[\{"type":"fieldAccess","name":"Reblogs","fieldName":"Reblogs"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\},\{"type":"constant","name":"_constant_.*","value":100\}\]\},\{"type":"arithmetic","name":"CTR","fn":"/","fields":\[\{"type":"fieldAccess","name":"clicks","fieldName":"Clicks"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\{"dimension":"Impressions","direction":"descending","dimensionOrder":\{"type":"numeric"\}\}\],"limit":120\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""


    result should fullyMatch regex json
  }

  test("dimension extraction function for start of the week dimension") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """"dimension":"statsDate","outputName":"Week","outputType":"STRING","extractionFn":{"type":"time","timeFormat":"yyyyMMdd","resultFormat":"YYYY-w"}}"""
    val filterjson = """{"type":"selector","dimension":"statsDate","value":"2017-24","extractionFn":{"type":"time","timeFormat":"yyyyMMdd","resultFormat":"YYYY-w"}},{"type":"selector","dimension":"statsDate","value":"2017-26","extractionFn":{"type":"time","timeFormat":"yyyyMMdd","resultFormat":"YYYY-w"}}"""

    assert(result.contains(json), result)
    assert(result.contains(filterjson), result)
  }

  test("dimension extraction function for start of the month dimension") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """"dimension":"statsDate","outputName":"Month","outputType":"STRING","extractionFn":{"type":"time","timeFormat":"yyyyMMdd","resultFormat":"yyyy-MM-01"}}"""
    val filterjson = """{"type":"selector","dimension":"statsDate","value":"2017-06-01","extractionFn":{"type":"time","timeFormat":"yyyyMMdd","resultFormat":"yyyy-MM-01"}},{"type":"selector","dimension":"statsDate","value":"2017-07-01","extractionFn":{"type":"time","timeFormat":"yyyyMMdd","resultFormat":"yyyy-MM-01"}}"""

    assert(result.contains(json), result)
    assert(result.contains(filterjson), result)
  }

  test("dimension time extraction function for druid time") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """{"type":"extraction","dimension":"__time","outputName":"My Date","outputType":"STRING","extractionFn":{"type":"timeFormat","format":"YYYY-MM-dd","timeZone":"UTC","granularity":{"type":"none"},"asMillis":false}}"""

    assert(result.contains(json), result)
  }

  test("successfully render filter on column using dimension time extraction function for druid time") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = s""""filter":{"type":"and","fields":[{"type":"or","fields":[{"type":"selector","dimension":"__time","value":"$fromDate","extractionFn":{"type":"timeFormat","format":"YYYY-MM-dd","timeZone":"UTC","granularity":{"type":"none"},"asMillis":false}},{"type":"selector","dimension":"__time","value":"$toDate","extractionFn":{"type":"timeFormat","format":"YYYY-MM-dd","timeZone":"UTC","granularity":{"type":"none"},"asMillis":false}}]}"""

    assert(result.contains(json), result)
  }

  test("no dimension cols and no sorts should produce timeseries query") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator)//do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """{"queryType":"timeseries","""

    assert(result.contains(json), result)
  }

  test("fact sort with single dim col should produce topN query") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """{"queryType":"topN","""

    assert(result.contains(json), result)
  }

  test("fact sort with multiple dim col should produce group by query") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """{"queryType":"groupBy","""

    assert(result.contains(json), result)
  }

  test("dim fact sync fact driven query should produce all requested fields in same order as in request") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_wo_dim_filters.json")
      .getLines.mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """"""

    assert(result.contains(json), result)
  }

  test("startIndex greater than maximumMaxRows should throw error") {
      val jsonString = s"""{
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
      val requestModel = RequestModel.from(request, defaultRegistry)
      val queryPipelineTry = generatePipeline(requestModel.toOption.get)
      assert(queryPipelineTry.isFailure, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
  }

  test("limit should be set to 2*maxRows if there is a NonFKDimfilter") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """limit":220"""

    assert(result.contains(json), result)
  }

  test("queryPipeline should fail when request has NonFKDimfilter and (startIndex + 2*maxRows) > 5000") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    assert(queryPipelineTry.checkFailureMessage("requirement failed: Failed to find best candidate, forceEngine=None, engine disqualifyingSet=Set(Druid, Hive, Presto), candidates=Set((fact1,Druid))"))

  }

  test("successfully set group by single threaded to true when cardinality is below max allowed value") {
    val jsonString = s"""{
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
    val requestModelTry = RequestModel.from(request, registry)
    assert(requestModelTry.isSuccess, requestModelTry.errorMessage("Building request model failed"))


    val requestModel = requestModelTry.toOption.get.copy(dimCardinalityEstimate = Some(DruidQueryGenerator.defaultMaxSingleThreadedDimCardinality))
    val queryPipelineTry = generatePipeline(requestModel)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """"groupByIsSingleThreaded":true"""

    assert(result.contains(json), result)
  }
  test("successfully set group by single threaded to false when cardinality is above max allowed value") {
    val jsonString = s"""{
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
    val requestModelTry = RequestModel.from(request, registry)
    assert(requestModelTry.isSuccess, requestModelTry.errorMessage("Building request model failed"))


    val requestModel = requestModelTry.toOption.get.copy(dimCardinalityEstimate = Some(DruidQueryGenerator.defaultMaxSingleThreadedDimCardinality + 1))
    val queryPipelineTry = generatePipeline(requestModel)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """"groupByIsSingleThreaded":false"""

    assert(result.contains(json), result)
  }
  test("successfully set chunk period when dim cardinality is defined and above max cost") {
    val jsonString = s"""{
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
    val requestModelTry = RequestModel.from(request, registry)
    assert(requestModelTry.isSuccess, requestModelTry.errorMessage("Building request model failed"))


    val requestModel = requestModelTry.toOption.get.copy(
      dimCardinalityEstimate = Some(DruidQueryGenerator.defaultMaxSingleThreadedDimCardinality + 1)
    , factCost = requestModelTry.toOption.get.factCost.mapValues(rc => rc.copy(costEstimate = DruidQueryGenerator.defaultMaxNoChunkCost + 1))
    )
    val queryPipelineTry = generatePipeline(requestModel)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """"chunkPeriod":"P3D""""

    assert(result.contains(json), result)
  }
  test("successfully ignore chunk period when dim cardinality is defined and below max cost") {
    val jsonString = s"""{
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
    val requestModelTry = RequestModel.from(request, registry)
    assert(requestModelTry.isSuccess, requestModelTry.errorMessage("Building request model failed"))


    val requestModel = requestModelTry.toOption.get.copy(
      dimCardinalityEstimate = Option(DruidQueryGenerator.defaultMaxSingleThreadedDimCardinality + 1)
      , factCost = requestModelTry.toOption.get.factCost.mapValues(rc => rc.copy(costEstimate = DruidQueryGenerator.defaultMaxNoChunkCost))
    )
    val queryPipelineTry = generatePipeline(requestModel)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    assert(!result.contains("chunkPeriod"))
  }
  test("successfully ignore chunk period when dim cardinality is not defined") {
    val jsonString = s"""{
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
    val requestModelTry = RequestModel.from(request, registry)
    assert(requestModelTry.isSuccess, requestModelTry.errorMessage("Building request model failed"))


    val requestModel = requestModelTry.toOption.get.copy(
      dimCardinalityEstimate = None
      , factCost = requestModelTry.toOption.get.factCost.mapValues(rc => rc.copy(costEstimate = DruidQueryGenerator.defaultMaxNoChunkCost + 1))
    )
    val queryPipelineTry = generatePipeline(requestModel)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    assert(!result.contains("chunkPeriod"))
  }
  test("successfully set minTopNThreshold when dim cardinality is defined") {
    val jsonString = s"""{
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
    val requestModelTry = RequestModel.from(request, defaultRegistry)
    val requestModel = requestModelTry.toOption.get.copy(dimCardinalityEstimate = Option(12345))
    val queryPipelineTry = generatePipeline(requestModel)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """"minTopNThreshold":12345"""

    assert(result.contains(json), result)
  }
  test("successfully set minTopNThreshold when dim cardinality is not defined") {
    val jsonString = s"""{
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
    val requestModelTry = RequestModel.from(request, defaultRegistry)
    val requestModel = requestModelTry.toOption.get.copy(dimCardinalityEstimate = None)
    val queryPipelineTry = generatePipeline(requestModel)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    assert(!result.contains("minTopNThreshold"), result)
  }

  test("namespace lookup extraction functionality") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Name","outputType":"STRING","extractionFn":{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"name","dimensionOverrideMap":{},"useQueryLevelCache":false}}"""

    assert(result.contains(json), result)
  }

  test("namespace lookup extraction functionality for dim") {
    val jsonString = s"""{
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
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Advertiser Status", "operator": "In", "values": ["ON"]}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSyncWithAdditionalParameters(jsonString, RequestContext("abc123", ""))
    val registry = defaultRegistry
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val expect_empty_lookup = """{"type":"extraction","dimension":"campaign_id_alias","outputName":"Campaign Name","outputType":"STRING","extractionFn":{"type":"mahaRegisteredLookup","lookup":"campaign_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"name","dimensionOverrideMap":{},"useQueryLevelCache":false}}"""
    val expect_other = """{"type":"extraction","dimension":"campaign_id_alias","outputName":"Campaign Total","outputType":"STRING","extractionFn":{"type":"mahaRegisteredLookup","lookup":"campaign_lookup","retainMissingValue":false,"replaceMissingValueWith":"Other","injective":false,"optimize":true,"valueColumn":"total","dimensionOverrideMap":{},"useQueryLevelCache":false}}"""

    assert(result.contains(expect_empty_lookup))
    assert(result.contains(expect_other))
  }

  test("should generate nested groupby query if dim filter is present") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"query","query":\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\},\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\},\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\},\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\},\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\},\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\},\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\},\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"statsDate","outputName":"Day","outputType":"STRING"\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Status","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"OFF","injective":false,"optimize":true,"valueColumn":"status","dimensionOverrideMap":\{\},"useQueryLevelCache":false\}\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Reseller ID","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"managed_by","dimensionOverrideMap":\{\},"useQueryLevelCache":false\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"filtered","aggregator":\{"type":"roundingDoubleSum","name":"Click Rate Success Case","fieldName":"clicks","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"engagement_type","value":"1"\},\{"type":"selector","dimension":"campaign_id_alias","value":"1"\}\]\},"name":"Click Rate Success Case"\},\{"type":"filtered","aggregator":\{"type":"longSum","name":"Reblogs","fieldName":"engagement_count"\},"filter":\{"type":"selector","dimension":"engagement_type","value":"1"\},"name":"Reblogs"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"doubleMax","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\},\{"type":"arithmetic","name":"Average Position","fn":"/","fields":\[\{"type":"fieldAccess","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":220\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"Advertiser Status","value":"ON"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"Day","outputName":"Day","outputType":"STRING"\},\{"type":"extraction","dimension":"Advertiser Status","outputName":"Advertiser Status","outputType":"STRING","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"ON":"ON"\},"isOneToOne":false\},"retainMissingValue":false,"replaceMissingValueWith":"OFF","injective":false,"optimize":true\}\},\{"type":"default","dimension":"Reseller ID","outputName":"Reseller ID","outputType":"STRING"\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"Impressions"\},\{"type":"roundingDoubleSum","name":"Click Rate Success Case","fieldName":"Click Rate Success Case","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"longSum","name":"Reblogs","fieldName":"Reblogs"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"_sum_avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"doubleMax","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\},\{"type":"arithmetic","name":"Average Position","fn":"/","fields":\[\{"type":"fieldAccess","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":220\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""

    result should fullyMatch regex json
  }

  test("should generate nested groupby query if lookup with decode column is present") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"query","query":\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"statsDate","outputName":"Day","outputType":"STRING"\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Status","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"OFF","injective":false,"optimize":true,"valueColumn":"status","dimensionOverrideMap":\{\},"useQueryLevelCache":false\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"filtered","aggregator":\{"type":"longSum","name":"Reblogs","fieldName":"engagement_count"\},"filter":\{"type":"selector","dimension":"engagement_type","value":"1"\},"name":"Reblogs"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"doubleMax","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\},\{"type":"arithmetic","name":"Average Position","fn":"/","fields":\[\{"type":"fieldAccess","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"Day","outputName":"Day","outputType":"STRING"\},\{"type":"extraction","dimension":"Advertiser Status","outputName":"Advertiser Status","outputType":"STRING","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"ON":"ON"\},"isOneToOne":false\},"retainMissingValue":false,"replaceMissingValueWith":"OFF","injective":false,"optimize":true\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"Impressions"\},\{"type":"longSum","name":"Reblogs","fieldName":"Reblogs"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"_sum_avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"doubleMax","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\},\{"type":"arithmetic","name":"Average Position","fn":"/","fields":\[\{"type":"fieldAccess","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""


    result should fullyMatch regex json
  }

  test("should not generate nested groupby query if dim filter column present is not of type druid lookup") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"statsDate","outputName":"Day","outputType":"STRING"\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Reseller ID","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"managed_by","dimensionOverrideMap":\{\},"useQueryLevelCache":false\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"filtered","aggregator":\{"type":"longSum","name":"Reblogs","fieldName":"engagement_count"\},"filter":\{"type":"selector","dimension":"engagement_type","value":"1"\},"name":"Reblogs"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"doubleMax","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\},\{"type":"arithmetic","name":"Average Position","fn":"/","fields":\[\{"type":"fieldAccess","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""


    result should fullyMatch regex json
  }

  test("should include dim column related to dim filter in nested groupby query if dim filter is present even though dim column is not present in the request") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"query","query":\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"statsDate","outputName":"Day","outputType":"STRING"\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Status","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"OFF","injective":false,"optimize":true,"valueColumn":"status","dimensionOverrideMap":\{\},"useQueryLevelCache":false\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"filtered","aggregator":\{"type":"longSum","name":"Reblogs","fieldName":"engagement_count"\},"filter":\{"type":"selector","dimension":"engagement_type","value":"1"\},"name":"Reblogs"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"doubleMax","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\},\{"type":"arithmetic","name":"Average Position","fn":"/","fields":\[\{"type":"fieldAccess","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":220\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"Advertiser Status","value":"ON"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"Day","outputName":"Day","outputType":"STRING"\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"Impressions"\},\{"type":"longSum","name":"Reblogs","fieldName":"Reblogs"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"_sum_avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"doubleMax","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\},\{"type":"arithmetic","name":"Average Position","fn":"/","fields":\[\{"type":"fieldAccess","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":220\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""


    result should fullyMatch regex json
  }

  test("namespace lookup extraction functionality for a public fact with new dimRevision") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"query","query":\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"statsDate","outputName":"Day","outputType":"STRING"\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Status","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"status","dimensionOverrideMap":\{\},"useQueryLevelCache":true\}\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Currency","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"currency","dimensionOverrideMap":\{"-3":"Unknown","":"Unknown"\},"useQueryLevelCache":true\}\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Timezone","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"timezone","decode":\{"columnToCheck":"timezone","valueToCheck":"US","columnIfValueMatched":"timezone","columnIfValueNotMatched":"currency"\},"dimensionOverrideMap":\{\},"useQueryLevelCache":true\}\},\{"type":"extraction","dimension":"external_id","outputName":"External Site Name","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"site_lookup","retainMissingValue":false,"replaceMissingValueWith":"Others","injective":false,"optimize":true,"valueColumn":"external_site_name","dimensionOverrideMap":\{\},"useQueryLevelCache":false\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":220\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"Advertiser Status","value":"ON"\},\{"type":"selector","dimension":"Currency","value":"USD"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"Day","outputName":"Day","outputType":"STRING"\},\{"type":"default","dimension":"Advertiser Status","outputName":"Advertiser Status","outputType":"STRING"\},\{"type":"default","dimension":"Currency","outputName":"Currency","outputType":"STRING"\},\{"type":"default","dimension":"Timezone","outputName":"Timezone","outputType":"STRING"\},\{"type":"extraction","dimension":"External Site Name","outputName":"External Site Name","outputType":"STRING","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"null":"Others","":"Others"\},"isOneToOne":false\},"retainMissingValue":true,"injective":true,"optimize":true\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"Impressions"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"_sum_avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":220\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""

    result should fullyMatch regex json
  }

  test("should include dimension columns from DruidPostResultDerivedFactCol in DimensionSpec") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json =
      """\{"queryType":"groupBy","dataSource":\{"type":"query","query":\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"statsDate","outputName":"Day","outputType":"STRING"\},\{"type":"default","dimension":"show_sov_flag","outputName":"show_sov_flag","outputType":"STRING"\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Status","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"status","dimensionOverrideMap":\{\},"useQueryLevelCache":false\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"longSum","name":"sov_impressions","fieldName":"sov_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Impression Share","fn":"/","fields":\[\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\},\{"type":"fieldAccess","name":"sov_impressions","fieldName":"sov_impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":220\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"Advertiser Status","value":"ON"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"Day","outputName":"Day","outputType":"STRING"\},\{"type":"default","dimension":"show_sov_flag","outputName":"show_sov_flag","outputType":"STRING"\},\{"type":"default","dimension":"Advertiser Status","outputName":"Advertiser Status","outputType":"STRING"\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"Impressions"\},\{"type":"longSum","name":"sov_impressions","fieldName":"sov_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Impression Share","fn":"/","fields":\[\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\},\{"type":"fieldAccess","name":"sov_impressions","fieldName":"sov_impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":220\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}""".stripMargin

    result should fullyMatch regex json
  }

  test("dimension extraction function for day of the week dimension") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """{"type":"extraction","dimension":"statsDate","outputName":"Day of Week","outputType":"STRING","extractionFn":{"type":"time","timeFormat":"yyyyMMdd","resultFormat":"EEEE"}}"""

    assert(result.contains(json), result)
  }

  test("dimension extraction function for datetime formatter") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json_date = s"""{"type":"extraction","dimension":"start_time","outputName":"Start Date","outputType":"STRING","extractionFn":{"type":"substring","index":0,"length":8}}"""
    val json_hour = s"""{"type":"extraction","dimension":"start_time","outputName":"Start Hour","outputType":"STRING","extractionFn":{"type":"substring","index":8,"length":2}}"""

    assert(result.contains(json_date), result)
    assert(result.contains(json_hour), result)

  }

  test("dimension filter extraction function for datetime formatter") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Failed to get request model"))
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
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

  test("dimension filter extraction function for decode dim"){
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
    val requestModel = RequestModel.from(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    println(result)
    val json =
      """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"or","fields":\[\{"type":"selector","dimension":"is_slot_ad","value":"1"\}\]\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"advertiser_id","outputName":"Advertiser ID","outputType":"STRING"\}\],"aggregations":\[\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}""".stripMargin
    result should fullyMatch regex json
  }

  test("dimension filter extraction function for decode dim with mapped null string"){
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
    val requestModel = RequestModel.from(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    println(result)
    val json =
      """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"or","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"valid_conversion","value":""\},\{"type":"selector","dimension":"valid_conversion","value":"1"\}\]\}\]\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"advertiser_id","outputName":"Advertiser ID","outputType":"STRING"\}\],"aggregations":\[\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}""".stripMargin
    result should fullyMatch regex json
  }

  test("dimension filter extraction function for decode dim that has staticMapping for source col"){
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
    val requestModel = RequestModel.from(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    println(result)
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
    val requestModel = RequestModel.from(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    println(result)
    val json =
    """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"or","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"price_type","value":"7"\},\{"type":"selector","dimension":"price_type","value":"6"\},\{"type":"selector","dimension":"price_type","value":"8"\}\]\}\]\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"advertiser_id","outputName":"Advertiser ID","outputType":"STRING"\}\],"aggregations":\[\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}""".stripMargin
    result should fullyMatch regex json
  }

  test("where clause: ensure duplicate filter mappings are not propagated into the where clause") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val json = """\{"queryType":"topN","dataSource":\{"type":"table","name":"fact1"\},"virtualColumns":\[\],"dimension":\{"type":"default","dimension":"id","outputName":"Keyword ID"\,\"outputType\":\"STRING\"},"metric":\{"type":"numeric","metric":"Impressions"\},"threshold":120,"intervals":\{"type":"intervals","intervals":\[".*"\]\},"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\},\{"type":"or","fields":\[\{"type":"selector","dimension":"stats_source","value":"1"\},\{"type":"selector","dimension":"stats_source","value":"2"\}\]\}\]\},"granularity":\{"type":"all"\},"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"roundingDoubleSum","name":"_sum_avg_bid","fieldName":"avg_bid","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"count","name":"_count_avg_bid"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\}\],"context":\{"applyLimitPushDown":"false","userId":"someUser","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""

    result should fullyMatch regex json
  }

  test("Or filter expression with dimension filters") {
    val jsonString = s"""{
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
                            {"operator": "or", "filterExpressions": [{"field": "Source", "operator": "in", "values": ["1","2"]}, {"field": "Keyword ID", "operator": "=", "value": "2"}]}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Desc"}
                          ],
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    
    val filterjson = s""""filter":{"type":"and","fields":[{"type":"selector","dimension":"statsDate","value":"${fromDate.replace("-","")}"},{"type":"selector","dimension":"advertiser_id","value":"12345"},{"type":"or","fields":[{"type":"or","fields":[{"type":"selector","dimension":"stats_source","value":"1"},{"type":"selector","dimension":"stats_source","value":"2"}]},{"type":"selector","dimension":"id","value":"2"}]}]}"""

    assert(result.contains(filterjson), result)
  }

  test("Or filter expression with fact filters") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    
    val filterjson = s""""filter":{"type":"and","fields":[{"type":"selector","dimension":"statsDate","value":"${fromDate.replace("-","")}"},{"type":"selector","dimension":"advertiser_id","value":"12345"}]}"""
    val havingJson = s""""having":{"type":"and","havingSpecs":[{"type":"or","havingSpecs":[{"type":"or","havingSpecs":[{"type":"equalTo","aggregation":"Clicks","value":1},{"type":"equalTo","aggregation":"Clicks","value":2}]},{"type":"equalTo","aggregation":"Impressions","value":2}]}]}"""

    assert(result.contains(filterjson), result)
    assert(result.contains(havingJson), result)
  }

  test("Generate a valid query at minute grain") {
    val fromMinute = "00"
    val toMinute = "60"

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
    val requestModel = RequestModel.from(request, defaultRegistry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Failed to get request model"))
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val expectectDimensionsJson = """"dimensions":[{"type":"extraction","dimension":"statsDate","outputName":"Week","outputType":"STRING","extractionFn":{"type":"time","timeFormat":"yyyyMMdd","resultFormat":"w"}},{"type":"default","dimension":"stats_source","outputName":"Source","outputType":"STRING"},{"type":"default","dimension":"id","outputName":"Keyword ID","outputType":"STRING"}"""

    assert(result.contains(expectectDimensionsJson), s"$expectectDimensionsJson \n\n not found in \n\n $result")
  }

  test("successfully generate query with sync group by single threaded optimization for non grain or index optimized request") {
    val fromMinute = "00"
    val toMinute = "60"

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
    val requestModel = RequestModel.from(request, defaultRegistry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Failed to get request model"))
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

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

    DruidQueryGenerator.register(failRegistry, queryOptimizer = new SyncDruidQueryOptimizer(timeout = 5000))
  }
  test("Join key should not be included in dimension bundle if it is not in the original request when using druid lookups") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Status","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"status","dimensionOverrideMap":\{\},"useQueryLevelCache":false\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\}\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""

    result should fullyMatch regex json
  }

  test("Join key should be included in dimension bundle if it is not in the original request when druid+oracle") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = queryPipelineFactory.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"id","outputName":"Keyword ID"\,\"outputType\":\"STRING\"}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\}\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""

    result should fullyMatch regex json
  }

  test("test theta sketch intersect set operation with filters on theta sketch aggregators") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    println(result)

    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\},\{"type":"or","fields":\[\{"type":"selector","dimension":"ageBucket","value":"18-24"\},\{"type":"selector","dimension":"ageBucket","value":"25-35"\}\]\},\{"type":"or","fields":\[\{"type":"selector","dimension":"woeids","value":"12345"\},\{"type":"selector","dimension":"woeids","value":"6789"\}\]\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"id","outputName":"Keyword ID","outputType":"STRING"\},\{"type":"default","dimension":"ad_id","outputName":"Ad ID","outputType":"STRING"\}\],"aggregations":\[\{"type":"longSum","name":"Clicks","fieldName":"clicks"\},\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"filtered","aggregator":\{"type":"thetaSketch","name":"ageBucket_unique_users","fieldName":"uniqueUserCount","size":16384,"shouldFinalize":true,"isInputThetaSketch":false\},"filter":\{"type":"or","fields":\[\{"type":"selector","dimension":"ageBucket","value":"18-24"\},\{"type":"selector","dimension":"ageBucket","value":"25-35"\}\]\},"name":"ageBucket_unique_users"\},\{"type":"filtered","aggregator":\{"type":"thetaSketch","name":"woeids_unique_users","fieldName":"uniqueUserCount","size":16384,"shouldFinalize":true,"isInputThetaSketch":false\},"filter":\{"type":"or","fields":\[\{"type":"selector","dimension":"woeids","value":"12345"\},\{"type":"selector","dimension":"woeids","value":"6789"\}\]\},"name":"woeids_unique_users"\}\],"postAggregations":\[\{"type":"thetaSketchEstimate","name":"Total Unique User Count","field":\{"type":"thetaSketchSetOp","name":"Total Unique User Count","func":"INTERSECT","size":16384,"fields":\[\{"type":"fieldAccess","name":"ageBucket_unique_users","fieldName":"ageBucket_unique_users"\},\{"type":"fieldAccess","name":"woeids_unique_users","fieldName":"woeids_unique_users"\}\]\}\}\],"limitSpec":\{"type":"default","columns":\[\{"dimension":"Impressions","direction":"ascending","dimensionOrder":\{"type":"numeric"\}\},\{"dimension":"Keyword ID","direction":"ascending","dimensionOrder":\{"type":"numeric"\}\},\{"dimension":"Ad ID","direction":"descending","dimensionOrder":\{"type":"numeric"\}\}\],"limit":101\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""

    result should fullyMatch regex json
  }

  test("const fact column and derived const fact column should not be included in aggregators and post aggregators") {
    val jsonString = s"""{
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
    val requestModel = RequestModel.from(request, defaultRegistry)
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    println(result)

    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"ad_id","outputName":"Ad ID","outputType":"STRING"\},\{"type":"default","dimension":"id","outputName":"Keyword ID","outputType":"STRING"\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\}\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\],"limit":101\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""

    println(json)
    result should fullyMatch regex json
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
    val requestModel = RequestModel.from(request, registry)
    require(requestModel.isSuccess, requestModel)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val expectedJson = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"account_stats"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"advertiser_id","value":"1035663"\},\{"type":"selector","dimension":"\{test_flag\}","value":"0"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"advertiser_id","outputName":"Advertiser ID","outputType":"STRING"\},\{"type":"default","dimension":"stats_date","outputName":"Day","outputType":"STRING"\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"roundingDoubleSum","name":"Spend","fieldName":"spend","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\},\{"type":"longSum","name":"clicks","fieldName":"clicks"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Const Der Fact Col A","fn":"/","fields":\[\{"type":"fieldAccess","name":"clicks","fieldName":"clicks"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\{"dimension":"Impressions","direction":"ascending","dimensionOrder":\{"type":"numeric"\}\},\{"dimension":"Spend","direction":"descending","dimensionOrder":\{"type":"numeric"\}\},\{"dimension":"Const Der Fact Col A","direction":"descending","dimensionOrder":\{"type":"numeric"\}\}\],"limit":200\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""
    println(result)
    println(expectedJson)
    result should fullyMatch regex expectedJson

    val subSequentQuery = queryPipelineTry.toOption.get.queryChain.subsequentQueryList.head.asString
    val expectedSubQueryJson = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"a_adjustments"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"advertiser_id","value":"1035663"\},\{"type":"selector","dimension":"\{test_flag\}","value":"0"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"advertiser_id","outputName":"Advertiser ID","outputType":"STRING"\},\{"type":"default","dimension":"stats_date","outputName":"Day","outputType":"STRING"\}\],"aggregations":\[\{"type":"roundingDoubleSum","name":"Spend","fieldName":"spend","scale":10,"enableRoundingDoubleSumAggregatorFactory":true\}\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\{"dimension":"Spend","direction":"descending","dimensionOrder":\{"type":"numeric"\}\}\],"limit":200\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""
    println(subSequentQuery)
    println(expectedSubQueryJson)
    subSequentQuery should fullyMatch regex expectedSubQueryJson
  }

  test("dimension filter extraction function for decode dim should work for more than 2 mappings"){
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
    val requestModel = RequestModel.from(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    println(result)
    val json =
        """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"123"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"extraction","dimension":"ad_id","outputName":"Derived Ad ID","outputType":"STRING","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"0":"EMPTY","-3":"EMPTY"\},"isOneToOne":false\},"retainMissingValue":true,"injective":false,"optimize":true\}\},\{"type":"default","dimension":"advertiser_id","outputName":"Advertiser ID","outputType":"STRING"\}\],"aggregations":\[\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""
    result should fullyMatch regex json
  }

  test("Druid query should be generated successfully with select query type") {
    val jsonString = s"""{
                          "queryType": "select",
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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json= """\{"queryType":"select","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"descending":false,"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"id","outputName":"Keyword ID","outputType":"STRING"\},\{"type":"extraction","dimension":"__time","outputName":"Day","outputType":"STRING","extractionFn":\{"type":"timeFormat","format":"YYYY-MM-dd HH","timeZone":"UTC","granularity":\{"type":"none"\},"asMillis":false\}\}\],"metrics":\["impressions"\],"virtualColumns":\[\],"pagingSpec":\{"pagingIdentifiers":\{\},"threshold":5,"fromNext":true\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"timeout":5000,"queryId":".*"\}\}"""
    result should fullyMatch regex json
  }

  test("Druid query should be generated successfully with select query type with pagination") {
    val jsonString = s"""{
                          "queryType": "select",
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
                          "paginationStartIndex":5,
                          "rowsPerPage":5,
                          "pagination": {
                            "druid": {
                              "pagingIdentifiers" : {
                                "wikipedia_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9" : 5
                              }
                            }
                          }

                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val requestModel = RequestModel.from(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json= """\{"queryType":"select","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"descending":false,"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"id","outputName":"Keyword ID","outputType":"STRING"\}\],"metrics":\["impressions"\],"virtualColumns":\[],"pagingSpec":\{"pagingIdentifiers":\{"wikipedia_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9":5\},"threshold":5,"fromNext":true\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"timeout":5000,"queryId":".*"\}\}"""
    result should fullyMatch regex json
  }

  test("Druid query should fail to generate with select query type when sort by requested") {
    val jsonString = s"""{
                          "queryType": "select",
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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure, queryPipelineTry.errorMessage("query pipeline should fail"))
    assert(queryPipelineTry.failed.toOption.get.getMessage.contains("druid select query type does not support sort by functionality!"))
  }

  test("Druid query should fail to generate with select query type when derived fact requested") {
    val jsonString = s"""{
                          "queryType": "select",
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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure, queryPipelineTry.errorMessage("query pipeline should fail"))
    assert(queryPipelineTry.failed.toOption.get.getMessage.contains("druid select query does not support derived columns : CTR"))
  }

  test("Druid query should fail to generate with select query type when filter on derived fact requested") {
    val jsonString = s"""{
                          "queryType": "select",
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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure, queryPipelineTry.errorMessage("query pipeline should fail"))
    assert(queryPipelineTry.failed.toOption.get.getMessage.contains("druid select query type does not support filter on derived columns: CTR"))
  }

  test("Druid query should fail to generate with select query type when using druid for dimension joins") {
    val jsonString = s"""{
                          "queryType": "select",
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
    val requestModel = RequestModel.from(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)

    assert(queryPipelineTry.isFailure, queryPipelineTry.errorMessage("query pipeline should fail"))
    assert(queryPipelineTry.failed.toOption.get.getMessage.contains("requirement failed: druid select query type does not support druid lookups!"))
  }
}
