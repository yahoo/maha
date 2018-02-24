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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
    val dimMapping = DefaultQueryPipelineFactory.findDimCandidatesMapping(requestModel.get)
    val dims = DefaultQueryPipelineFactory.findBestDimCandidates(DruidEngine, requestModel.get.schema, dimMapping)
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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """limit":1020"""

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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
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
    val requestModelTry = RequestModel.from(request, getDefaultRegistry())
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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val json = """\{"queryType":"topN","dataSource":\{"type":"table","name":"fact1"\},"dimension":\{"type":"default","dimension":"id","outputName":"Keyword ID"\},"metric":\{"type":"numeric","metric":"Impressions"\},"threshold":120,"intervals":\{"type":"intervals","intervals":\[".*"\]\},"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"doubleSum","name":"_sum_avg_bid","fieldName":"avg_bid"\},\{"type":"count","name":"_count_avg_bid"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\}\],"context":\{"applyLimitPushDown":"false","userId":"someUser","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""

    result should fullyMatch regex json
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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"extraction","dimension":"landing_page_url","outputName":"Landing URL Translation","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"Valid":"Something"\},"isOneToOne":false\},"retainMissingValue":false,"replaceMissingValueWith":"Empty","injective":false,"optimize":true\}\},\{"type":"default","dimension":"id","outputName":"Keyword ID"\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\}\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\{"dimension":"Impressions","direction":"descending","dimensionOrder":\{"type":"numeric"\}\}\],"limit":120\},"context":\{"applyLimitPushDown":"false","userId":"someUser","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""

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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"timeseries","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"descending":false,"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"duration","duration":86400000,"origin":".*"\},"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"doubleSum","name":"_sum_avg_bid","fieldName":"avg_bid"\},\{"type":"count","name":"_count_avg_bid"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\}\],"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\}\}"""

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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"extraction","dimension":"price_type","outputName":"Pricing Type","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"-10":"CPE","-20":"CPF","6":"CPV","1":"CPC","2":"CPA","7":"CPCV","3":"CPM"\},"isOneToOne":false\},"retainMissingValue":false,"replaceMissingValueWith":"NONE","injective":false,"optimize":true\}\},\{"type":"default","dimension":"id","outputName":"Keyword ID"\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"doubleMax","name":"Max Bid","fieldName":"max_bid"\},\{"type":"doubleSum","name":"_sum_avg_bid","fieldName":"avg_bid"\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"longSum","name":"Clicks","fieldName":"clicks"\},\{"type":"doubleMin","name":"Min Bid","fieldName":"min_bid"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\},\{"type":"arithmetic","name":"CTR","fn":"/","fields":\[\{"type":"fieldAccess","name":"clicks","fieldName":"Clicks"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\{"dimension":"Impressions","direction":"descending","dimensionOrder":\{"type":"numeric"\}\}\],"limit":120\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""

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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"stats_source","outputName":"Source"\},\{"type":"default","dimension":"id","outputName":"Keyword ID"\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"filtered","aggregator":\{"type":"longSum","name":"Reblogs","fieldName":"engagement_count"\},"filter":\{"type":"selector","dimension":"engagement_type","value":"1"\},"name":"Reblogs"\},\{"type":"longSum","name":"Clicks","fieldName":"clicks"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Reblog Rate","fn":"\*","fields":\[\{"type":"arithmetic","name":"_placeHolder_2","fn":"/","fields":\[\{"type":"fieldAccess","name":"Reblogs","fieldName":"Reblogs"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\},\{"type":"constant","name":"_constant_.*","value":100\}\]\},\{"type":"arithmetic","name":"CTR","fn":"/","fields":\[\{"type":"fieldAccess","name":"clicks","fieldName":"Clicks"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\{"dimension":"Impressions","direction":"descending","dimensionOrder":\{"type":"numeric"\}\}\],"limit":120\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""


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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """"dimension":"statsDate","outputName":"Week","extractionFn":{"type":"time","timeFormat":"yyyyMMdd","resultFormat":"yyyy-w"}}"""
    val filterjson = """{"type":"selector","dimension":"statsDate","value":"2017-24","extractionFn":{"type":"time","timeFormat":"yyyyMMdd","resultFormat":"yyyy-w"}},{"type":"selector","dimension":"statsDate","value":"2017-26","extractionFn":{"type":"time","timeFormat":"yyyyMMdd","resultFormat":"yyyy-w"}}"""

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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """"dimension":"statsDate","outputName":"Month","extractionFn":{"type":"time","timeFormat":"yyyyMMdd","resultFormat":"yyyy-MM-01"}}"""
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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """{"type":"extraction","dimension":"__time","outputName":"My Date","extractionFn":{"type":"timeFormat","format":"YYYY-MM-dd","timeZone":"UTC","granularity":{"type":"none"}}}"""

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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = s""""filter":{"type":"and","fields":[{"type":"or","fields":[{"type":"selector","dimension":"__time","value":"$fromDate","extractionFn":{"type":"timeFormat","format":"YYYY-MM-dd","timeZone":"UTC","granularity":{"type":"none"}}},{"type":"selector","dimension":"__time","value":"$toDate","extractionFn":{"type":"timeFormat","format":"YYYY-MM-dd","timeZone":"UTC","granularity":{"type":"none"}}}]}"""

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
    val registry = getDefaultRegistry()
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
    val registry = getDefaultRegistry()
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
    val registry = getDefaultRegistry()
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
    val registry = getDefaultRegistry()
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
                          "paginationStartIndex":5001,
                          "rowsPerPage":100
                        }"""

      val request: ReportingRequest = getReportingRequestSync(jsonString)
      val requestModel = RequestModel.from(request, getDefaultRegistry())
      val queryPipelineTry = generatePipeline(requestModel.toOption.get)
      assert(!queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
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
    val registry = getDefaultRegistry()
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
    val registry = getDefaultRegistry()
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
    val registry = getDefaultRegistry()
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
    val registry = getDefaultRegistry()
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
    val registry = getDefaultRegistry()
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
    val requestModelTry = RequestModel.from(request, getDefaultRegistry())
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
    val requestModelTry = RequestModel.from(request, getDefaultRegistry())
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Name","extractionFn":{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"name"}}"""

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
                            {"field": "Campaign Name"}
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"query","query":\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"statsDate","outputName":"Day"\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Status","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"status"\}\},\{"type":"extraction","dimension":"campaign_id_alias","outputName":"Campaign Name","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"campaign_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"name"\}\}],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"doubleSum","name":"_sum_avg_bid","fieldName":"avg_bid"\},\{"type":"count","name":"_count_avg_bid"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":220\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"Advertiser Status","value":"ON"\}\]\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"Day","outputName":"Day"\},\{"type":"extraction","dimension":"Advertiser Status","outputName":"Advertiser Status","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"ON":"ON"\},"isOneToOne":false\},"retainMissingValue":false,"replaceMissingValueWith":"OFF","injective":false,"optimize":true\}\}\,\{"type":"default","dimension":"Campaign Name","outputName":"Campaign Name"\}],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"Impressions"\},\{"type":"doubleSum","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"count","name":"_count_avg_bid"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":220\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""


    result should fullyMatch regex json
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"query","query":\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\},\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\},\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\},\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\},\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\},\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\},\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\},\{"type":"selector","dimension":"statsDate","value":"[0-9]{8}"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"statsDate","outputName":"Day"\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Status","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"status"\}\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Reseller ID","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"managed_by"\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"filtered","aggregator":\{"type":"doubleSum","name":"Click Rate Success Case","fieldName":"clicks"\},"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"engagement_type","value":"1"\},\{"type":"selector","dimension":"campaign_id_alias","value":"1"\}\]\},"name":"Click Rate Success Case"\},\{"type":"filtered","aggregator":\{"type":"longSum","name":"Reblogs","fieldName":"engagement_count"\},"filter":\{"type":"selector","dimension":"engagement_type","value":"1"\},"name":"Reblogs"\},\{"type":"doubleSum","name":"_sum_avg_bid","fieldName":"avg_bid"\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"doubleMax","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\},\{"type":"arithmetic","name":"Average Position","fn":"/","fields":\[\{"type":"fieldAccess","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":220\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"Advertiser Status","value":"ON"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"Day","outputName":"Day"\},\{"type":"extraction","dimension":"Advertiser Status","outputName":"Advertiser Status","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"ON":"ON"\},"isOneToOne":false\},"retainMissingValue":false,"replaceMissingValueWith":"OFF","injective":false,"optimize":true\}\},\{"type":"default","dimension":"Reseller ID","outputName":"Reseller ID"\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"Impressions"\},\{"type":"doubleSum","name":"Click Rate Success Case","fieldName":"Click Rate Success Case"\},\{"type":"longSum","name":"Reblogs","fieldName":"Reblogs"\},\{"type":"doubleSum","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"doubleMax","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\},\{"type":"arithmetic","name":"Average Position","fn":"/","fields":\[\{"type":"fieldAccess","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":220\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""

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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"query","query":\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"statsDate","outputName":"Day"\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Status","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"status"\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"filtered","aggregator":\{"type":"longSum","name":"Reblogs","fieldName":"engagement_count"\},"filter":\{"type":"selector","dimension":"engagement_type","value":"1"\},"name":"Reblogs"\},\{"type":"doubleSum","name":"_sum_avg_bid","fieldName":"avg_bid"\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"doubleMax","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\},\{"type":"arithmetic","name":"Average Position","fn":"/","fields":\[\{"type":"fieldAccess","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"Day","outputName":"Day"\},\{"type":"extraction","dimension":"Advertiser Status","outputName":"Advertiser Status","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"ON":"ON"\},"isOneToOne":false\},"retainMissingValue":false,"replaceMissingValueWith":"OFF","injective":false,"optimize":true\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"Impressions"\},\{"type":"longSum","name":"Reblogs","fieldName":"Reblogs"\},\{"type":"doubleSum","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"doubleMax","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\},\{"type":"arithmetic","name":"Average Position","fn":"/","fields":\[\{"type":"fieldAccess","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""


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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"statsDate","outputName":"Day"\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Reseller ID","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"managed_by"\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"filtered","aggregator":\{"type":"longSum","name":"Reblogs","fieldName":"engagement_count"\},"filter":\{"type":"selector","dimension":"engagement_type","value":"1"\},"name":"Reblogs"\},\{"type":"doubleSum","name":"_sum_avg_bid","fieldName":"avg_bid"\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"doubleMax","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\},\{"type":"arithmetic","name":"Average Position","fn":"/","fields":\[\{"type":"fieldAccess","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""


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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"query","query":\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"statsDate","outputName":"Day"\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Status","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"status"\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"filtered","aggregator":\{"type":"longSum","name":"Reblogs","fieldName":"engagement_count"\},"filter":\{"type":"selector","dimension":"engagement_type","value":"1"\},"name":"Reblogs"\},\{"type":"doubleSum","name":"_sum_avg_bid","fieldName":"avg_bid"\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"doubleMax","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\},\{"type":"arithmetic","name":"Average Position","fn":"/","fields":\[\{"type":"fieldAccess","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":220\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"Advertiser Status","value":"ON"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"Day","outputName":"Day"\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"Impressions"\},\{"type":"longSum","name":"Reblogs","fieldName":"Reblogs"\},\{"type":"doubleSum","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"count","name":"_count_avg_bid"\},\{"type":"doubleMax","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\},\{"type":"arithmetic","name":"Average Position","fn":"/","fields":\[\{"type":"fieldAccess","name":"avg_pos_times_impressions","fieldName":"avg_pos_times_impressions"\},\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":220\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""


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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"query","query":\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"statsDate","outputName":"Day"\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Status","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"status"\}\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Currency","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"currency"\}\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Timezone","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"decode":\{"columnToCheck":"timezone","valueToCheck":"US","columnIfValueMatched":"timezone","columnIfValueNotMatched":"currency"\}\}\},\{"type":"extraction","dimension":"external_id","outputName":"External Site Name","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"site_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"external_site_name"\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"doubleSum","name":"_sum_avg_bid","fieldName":"avg_bid"\},\{"type":"count","name":"_count_avg_bid"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":220\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"Advertiser Status","value":"ON"\},\{"type":"selector","dimension":"Currency","value":"USD"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"Day","outputName":"Day"\},\{"type":"default","dimension":"Advertiser Status","outputName":"Advertiser Status"\},\{"type":"default","dimension":"Currency","outputName":"Currency"\},\{"type":"default","dimension":"Timezone","outputName":"Timezone"\},\{"type":"extraction","dimension":"External Site Name","outputName":"External Site Name","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"null":"Others","":"Others"\},"isOneToOne":false\},"retainMissingValue":true,"injective":true,"optimize":true\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"Impressions"\},\{"type":"doubleSum","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"count","name":"_count_avg_bid"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":220\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""


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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json =
      """\{"queryType":"groupBy","dataSource":\{"type":"query","query":\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"statsDate","outputName":"Day"\},\{"type":"default","dimension":"show_sov_flag","outputName":"show_sov_flag"\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Status","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"status"\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"longSum","name":"sov_impressions","fieldName":"sov_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Impression Share","fn":"/","fields":\[\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\},\{"type":"fieldAccess","name":"sov_impressions","fieldName":"sov_impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":220\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"Advertiser Status","value":"ON"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"Day","outputName":"Day"\},\{"type":"default","dimension":"show_sov_flag","outputName":"show_sov_flag"\},\{"type":"default","dimension":"Advertiser Status","outputName":"Advertiser Status"\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"Impressions"\},\{"type":"longSum","name":"sov_impressions","fieldName":"sov_impressions"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Impression Share","fn":"/","fields":\[\{"type":"fieldAccess","name":"impressions","fieldName":"Impressions"\},\{"type":"fieldAccess","name":"sov_impressions","fieldName":"sov_impressions"\}\]\}\],"limitSpec":\{"type":"default","columns":\[\],"limit":220\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}""".stripMargin


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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json = """{"type":"extraction","dimension":"statsDate","outputName":"Day of Week","extractionFn":{"type":"time","timeFormat":"yyyyMMdd","resultFormat":"EEEE"}}"""

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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    val json_date = s"""{"type":"extraction","dimension":"start_time","outputName":"Start Date","extractionFn":{"type":"substring","index":0,"length":8}}"""
    val json_hour = s"""{"type":"extraction","dimension":"start_time","outputName":"Start Hour","extractionFn":{"type":"substring","index":8,"length":2}}"""


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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    val json = """\{"queryType":"topN","dataSource":\{"type":"table","name":"fact1"\},"dimension":\{"type":"default","dimension":"id","outputName":"Keyword ID"\},"metric":\{"type":"numeric","metric":"Impressions"\},"threshold":120,"intervals":\{"type":"intervals","intervals":\[".*"\]\},"filter":\{"type":"and","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"advertiser_id","value":"12345"\},\{"type":"or","fields":\[\{"type":"selector","dimension":"stats_source","value":"1"\},\{"type":"selector","dimension":"stats_source","value":"2"\}\]\}\]\},"granularity":\{"type":"all"\},"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\},\{"type":"doubleSum","name":"_sum_avg_bid","fieldName":"avg_bid"\},\{"type":"count","name":"_count_avg_bid"\}\],"postAggregations":\[\{"type":"arithmetic","name":"Average Bid","fn":"/","fields":\[\{"type":"fieldAccess","name":"_sum_avg_bid","fieldName":"_sum_avg_bid"\},\{"type":"fieldAccess","name":"_count_avg_bid","fieldName":"_count_avg_bid"\}\]\}\],"context":\{"applyLimitPushDown":"false","userId":"someUser","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":"abc123"\},"descending":false\}"""

    result should fullyMatch regex json
  }

  test("Or filter expression with dimension filters") {
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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    println(result)
    val filterjson = s""""filter":{"type":"and","fields":[{"type":"selector","dimension":"statsDate","value":"${fromDate.replace("-","")}"},{"type":"selector","dimension":"advertiser_id","value":"12345"},{"type":"or","fields":[{"type":"or","fields":[{"type":"selector","dimension":"stats_source","value":"1"},{"type":"selector","dimension":"stats_source","value":"2"}]},{"type":"selector","dimension":"id","value":"2"}]}]}"""

    assert(result.contains(filterjson), result)
  }

  test("Or filter expression with fact filters") {
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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    println(result)
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
    val requestModel = RequestModel.from(request, getDefaultRegistry())
    assert(requestModel.isSuccess, requestModel.errorMessage("Failed to get request model"))
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    println(result)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator) //do not include local time filter
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    println(result)
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Status","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"MAHA_LOOKUP_EMPTY","injective":false,"optimize":true,"valueColumn":"status"\}\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\}\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""

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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry, revision = Option.apply(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = queryPipelineFactory.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString
    println(result)
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact1"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\},\{"type":"selector","dimension":"statsDate","value":".*"\}\]\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"id","outputName":"Keyword ID"\}\],"aggregations":\[\{"type":"longSum","name":"Impressions","fieldName":"impressions"\}\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\],"limit":120\},"context":\{"groupByStrategy":"v2","applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}"""

    result should fullyMatch regex json
  }

}
