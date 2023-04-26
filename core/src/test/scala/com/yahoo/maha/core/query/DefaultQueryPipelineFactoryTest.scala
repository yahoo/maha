// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core.bucketing._
import com.yahoo.maha.core.query.druid.{DruidQuery, DruidQueryGenerator, SyncDruidQueryOptimizer}
import com.yahoo.maha.core.query.hive.{HiveQueryGenerator, HiveQueryGeneratorV2}
import com.yahoo.maha.core.query.oracle.OracleQueryGenerator
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.core.{BetweenFilter, DefaultPartitionColumnRenderer, EqualityFilter, RequestModel, _}
import com.yahoo.maha.executor.{MockDruidQueryExecutor, MockHiveQueryExecutor, MockOracleQueryExecutor, MockPostgresQueryExecutor, MockBigqueryQueryExecutor}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import scala.util.Try

/**
 * Created by hiral on 4/8/16.
 */

object DefaultQueryPipelineFactoryTest {
  implicit class PipelineRunner(pipeline: QueryPipeline) {
    val queryExecutorContext = new QueryExecutorContext
    
    def withDruidCallback(callback: QueryRowList => Unit) : PipelineRunner = {
      val e = new MockDruidQueryExecutor(callback)
      queryExecutorContext.register(e)
      this
    }
    def withOracleCallback(callback: QueryRowList => Unit) : PipelineRunner = {
      val e = new MockOracleQueryExecutor(callback)
      queryExecutorContext.register(e)
      this
    }
    def withPostgresCallback(callback: QueryRowList => Unit) : PipelineRunner = {
      val e = new MockPostgresQueryExecutor(callback)
      queryExecutorContext.register(e)
      this
    }
    def withBigqueryCallback(callback: QueryRowList => Unit) : PipelineRunner = {
      val e = new MockBigqueryQueryExecutor(callback)
      queryExecutorContext.register(e)
      this
    }
    def withHiveCallback(callback: QueryRowList => Unit) : PipelineRunner = {
      val e = new MockHiveQueryExecutor(callback)
      queryExecutorContext.register(e)
      this
    }
    def run(queryAttributes: QueryAttributes = QueryAttributes.empty) : Try[QueryPipelineResult] = {
      pipeline.execute(queryExecutorContext, queryAttributes)
    }
  }
  
}

class DefaultQueryPipelineFactoryTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with BaseQueryGeneratorTest with SharedDimSchema with BaseQueryContextTest {
  val factRequestWithNoSort = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Ad Group Status"},
                              {"field": "Ad Group ID"},
                              {"field": "Source"},
                              {"field": "Pricing Type"},
                              {"field": "Destination URL"},
                              {"field": "Spend"},
                              {"field": "Impressions"},
                              {"field": "Clicks"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [],
                          "includeRowCount" : false,
                          "forceDimensionDriven": false,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""
  val factRequestWithNoSortAndKeywordIdForInvalidQuery = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Ad Group Status"},
                              {"field": "Ad Group ID"},
                              {"field": "Source"},
                              {"field": "Pricing Type"},
                              {"field": "Destination URL"},
                              {"field": "Spend"},
                              {"field": "Impressions"},
                              {"field": "Clicks"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [],
                          "includeRowCount" : false,
                          "forceDimensionDriven": false,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""
  val factRequestWithMetricSort = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Ad Group Status"},
                              {"field": "Ad Group ID"},
                              {"field": "Source"},
                              {"field": "Pricing Type"},
                              {"field": "Destination URL"},
                              {"field": "Spend"},
                              {"field": "Impressions"},
                              {"field": "Clicks"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Spend", "order": "DESC"},
                              {"field": "Ad Group ID", "order": "DESC"}
                          ],
                          "includeRowCount" : false,
                          "forceDimensionDriven": false,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""
  val factOnlyRequestWithMetricSort = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Ad Group ID"},
                              {"field": "Source"},
                              {"field": "Pricing Type"},
                              {"field": "Impressions"},
                              {"field": "Clicks"},
                              {"field": "Campaign ID"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "ASC"}
                          ],
                          "includeRowCount" : false,
                          "forceDimensionDriven": false,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""
  val requestWithMetricSort = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Ad Group Status"},
                              {"field": "Ad Group ID"},
                              {"field": "Source"},
                              {"field": "Pricing Type"},
                              {"field": "Destination URL"},
                              {"field": "Impressions"},
                              {"field": "Clicks"},
                              {"field": "Advertiser Currency"},
                              {"field": "Campaign Device ID"},
                              {"field": "Campaign ID"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "ASC"}
                          ],
                          "includeRowCount" : true,
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""
  val requestWithMetricSortNoPK = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Keyword Value"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Keyword Status", "operator": "not in", "values": ["DELETED"]},
                              {"field": "Ad Group Status", "operator": "not in", "values": ["DELETED"]},
                              {"field": "Campaign Status", "operator": "not in", "values": ["DELETED"]},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "DESC"}
                          ],
                          "includeRowCount" : true,
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""
  val requestWithDimSortNoPK = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Keyword Value"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Keyword Status", "operator": "not in", "values": ["DELETED"]},
                              {"field": "Ad Group Status", "operator": "not in", "values": ["DELETED"]},
                              {"field": "Campaign Status", "operator": "not in", "values": ["DELETED"]},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Keyword Value", "order": "DESC"}
                          ],
                          "includeRowCount" : true,
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""
  val requestWithMetricSortPage2 = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Ad Group Status"},
                              {"field": "Ad Group ID"},
                              {"field": "Source"},
                              {"field": "Pricing Type"},
                              {"field": "Destination URL"},
                              {"field": "Impressions"},
                              {"field": "Clicks"},
                              {"field": "Advertiser Currency"},
                              {"field": "Campaign Device ID"},
                              {"field": "Campaign ID"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "ASC"}
                          ],
                          "includeRowCount" : true,
                          "forceDimensionDriven": true,
                          "paginationStartIndex":10,
                          "rowsPerPage":10
                          }"""
  val factRequestWithMetricSortNoDimensionIds = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Ad Group Status"},
                              {"field": "Source"},
                              {"field": "Pricing Type"},
                              {"field": "Destination URL"},
                              {"field": "Impressions"},
                              {"field": "Clicks"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "ASC"}
                          ],
                          "includeRowCount" : true,
                          "forceDimensionDriven": false,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

  val factRequestWithMetricSortDimFilter = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Ad Group Status"},
                              {"field": "Ad Group ID"},
                              {"field": "Source"},
                              {"field": "Pricing Type"},
                              {"field": "Destination URL"},
                              {"field": "Impressions"},
                              {"field": "Clicks"},
                              {"field": "Advertiser Currency"},
                              {"field": "Campaign Device ID"},
                              {"field": "Campaign ID"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Advertiser Currency", "operator": "in", "values": ["USD"]},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "ASC"}
                          ],
                          "includeRowCount" : false,
                          "forceDimensionDriven": false,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""


  val requestWithIdSort = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Ad Group Status"},
                              {"field": "Ad Group ID"},
                              {"field": "Source"},
                              {"field": "Pricing Type"},
                              {"field": "Destination URL"},
                              {"field": "Impressions"},
                              {"field": "Clicks"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Ad Group ID", "order": "ASC"}
                          ],
                          "includeRowCount" : true,
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

  val requestWithMetricSortAndDay = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Day"},
                              {"field": "Advertiser ID"},
                              {"field": "Ad Group Status"},
                              {"field": "Ad Group ID"},
                              {"field": "Source"},
                              {"field": "Pricing Type"},
                              {"field": "Destination URL"},
                              {"field": "Impressions"},
                              {"field": "Clicks"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "DESC"}
                          ],
                          "includeRowCount" : true,
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

  val requestWithDimOnly = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Ad Group Status"},
                              {"field": "Ad Group ID"},
                              {"field": "Keyword Value"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Advertiser ID", "order": "DESC"}
                          ],
                          "includeRowCount" : false,
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

  val requestWithDruidFactAndDim = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Day"},
                              {"field": "Advertiser ID"},
                              {"field": "Country Name"},
                              {"field": "Impressions"},
                              {"field": "Clicks"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "DESC"}
                          ],
                          "includeRowCount" : true,
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

  val factRequestWithDruidFactAndDim = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Day"},
                              {"field": "Advertiser ID"},
                              {"field": "Country Name"},
                              {"field": "Impressions"},
                              {"field": "Clicks"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "DESC"}
                          ],
                          "includeRowCount" : false,
                          "forceDimensionDriven": false
                          }"""

  val factRequestWithOracleAndDruidDim = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Day"},
                              {"field": "Advertiser ID"},
                              {"field": "Advertiser Name"},
                              {"field": "Impressions"},
                              {"field": "Clicks"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "DESC"}
                          ],
                          "includeRowCount" : false,
                          "forceDimensionDriven": false
                          }"""

  val factRequestWithNoDims = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Day"},
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Clicks"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "DESC"}
                          ],
                          "includeRowCount" : false,
                          "forceDimensionDriven": false
                          }"""

  val requestWithOutOfRangeDruidDim = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Day"},
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Clicks"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDateMinus10", "to": "$toDateMinus10"}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "DESC"}
                          ],
                          "includeRowCount" : true,
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

  val requestWithFkInjected = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Ad Group Status"},
                              {"field": "Ad Group ID"},
                              {"field": "Source"},
                              {"field": "Pricing Type"},
                              {"field": "Destination URL"},
                              {"field": "Impressions"},
                              {"field": "Clicks"},
                              {"field": "Advertiser Currency"},
                              {"field": "Campaign Device ID"},
                              {"field": "Campaign ID"},
                              {"field": "Reseller ID"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "ASC"}
                          ],
                          "includeRowCount" : true,
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

  implicit private[this] val queryExecutionContext = new QueryExecutorContext
  
  import DefaultQueryPipelineFactoryTest._
  override protected def beforeAll(): Unit = {
    OracleQueryGenerator.register(queryGeneratorRegistry, DefaultPartitionColumnRenderer)
    DruidQueryGenerator.register(queryGeneratorRegistry, useCustomRoundingSumAggregator = false)
    HiveQueryGenerator.register(queryGeneratorRegistry, DefaultPartitionColumnRenderer, TestUDFRegistrationFactory())
    HiveQueryGeneratorV2.register(queryGeneratorRegistry, DefaultPartitionColumnRenderer, TestUDFRegistrationFactory())
  }

  lazy val defaultRegistry = getDefaultRegistry()

  test("force injectFilterSet to fk primary key") {
    val request: ReportingRequest = getReportingRequestSync(requestWithFkInjected)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[MultiEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[MultiEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
    val result = pipeline.withDruidCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Campaign ID", 10)
        row.addValue("Ad Group ID", 202)
        row.addValue("Impressions", 100)
        row.addValue("Clicks", 1)
        row.addValue("Source", 2)
        row.addValue("Pricing Type", "CPC")
        row.addValue("Destination URL", "url-10")
        rl.addRow(row)
    }.withOracleCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Advertiser ID", 1)
        row.addValue("Ad Group Status", "ON")
        row.addValue("Campaign ID", 10)
        row.addValue("Ad Group ID", 202)
        rl.addRow(row)
    }.run()

    assert(result.isSuccess, result)
    result.toOption.get.rowList.foreach {
      row =>
        assert(row.getValue("Impressions") === 100)
        assert(row.getValue("Clicks") === 1)
    }
  }

  test("successfully generate sync single engine query") {

    val request: ReportingRequest = ReportingRequest.forceOracle(getReportingRequestSync(requestWithMetricSort))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get
    
    assert(pipeline.queryChain.isInstanceOf[SingleEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[SingleEngineQuery].drivingQuery.isInstanceOf[OracleQuery])
    
    val result = pipeline.withOracleCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Advertiser ID", 1)
        row.addValue("Ad Group Status", "ON")
        row.addValue("Ad Group ID", 10)
        row.addValue("Source", 2)
        row.addValue("Pricing Type", "CPC")
        row.addValue("Destination URL", "url-10")
        row.addValue("Impressions", 100)
        row.addValue("Clicks", 1)
        rl.addRow(row)
    }.run()
    
    assert(result.isSuccess, result)
    result.toOption.get.rowList.foreach {
      row =>
        assert(row.getValue("Impressions") === 100)
        assert(row.getValue("Clicks") === 1)
    }
  }
  test("successfully generate Druid + Oracle multi engine query with Dim lookup for dimDriven sync request") {

    val requestWithMetricSort = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Advertiser Status"},
                              {"field": "Impressions"},
                              {"field": "Clicks"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "ASC"}
                          ],
                          "includeRowCount" : true,
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(requestWithMetricSort)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[MultiEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[MultiEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
    val drivingQuery = pipeline.queryChain.drivingQuery.asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"query","query":\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact_druid"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"stats_date","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"213"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"advertiser_id","outputName":"Advertiser ID","outputType":"STRING"\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Status","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"OFF","injective":false,"optimize":true,"valueColumn":"status","dimensionOverrideMap":\{\},"useQueryLevelCache":false\}\}\],"aggregations":\[\{"type":"longSum","name":"Clicks","fieldName":"clicks"\},\{"type":"longSum","name":"Impressions","fieldName":"impressions"\}\],"postAggregations":\[\],"limitSpec":\{"type":"NoopLimitSpec"\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":300000,"queryId":".*"\},"descending":false\}\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"Advertiser ID","outputName":"Advertiser ID","outputType":"STRING"\},\{"type":"extraction","dimension":"Advertiser Status","outputName":"Advertiser Status","outputType":"STRING","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"ON":"ON"\},"isOneToOne":false\},"retainMissingValue":false,"replaceMissingValueWith":"OFF","injective":false,"optimize":true\}\}\],"aggregations":\[\{"type":"longSum","name":"Clicks","fieldName":"Clicks"\},\{"type":"longSum","name":"Impressions","fieldName":"Impressions"\}\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\{"dimension":"Impressions","direction":"ascending","dimensionOrder":\{"type":"numeric"\}\}\],"limit":100\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":300000,"queryId":".*"\},"descending":false\}"""
    drivingQuery should fullyMatch regex json

    val result = pipeline.withDruidCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Advertiser ID", 1)
        row.addValue("Advertiser Status", "ON")
        row.addValue("Impressions", 100)
        row.addValue("Clicks", 1)
        rl.addRow(row)
    }.withOracleCallback {
      rl =>
        val row1 = rl.newRow
        row1.addValue("Advertiser ID", 1)
        row1.addValue("Advertiser Status", "ON")
        rl.addRow(row1)
        val row2 = rl.newRow
        row2.addValue("Advertiser ID", 2)
        row2.addValue("Advertiser Status", "OFF")
        rl.addRow(row2)
    }.run()

    val subsequentQuery = pipeline.queryChain.subsequentQueryList.head.asString
    assert(subsequentQuery.contains("advertiser_oracle"))
    assert(result.isSuccess, result)
    assert(result.get.rowList.length == 2)
    var count = 1
    result.toOption.get.rowList.foreach {
      row =>
        if(count == 1) {
          assert(row.getValue("Advertiser ID") === 1)
          assert(row.getValue("Advertiser Status") === "ON")
          assert(row.getValue("Impressions") === 100)
          assert(row.getValue("Clicks") === 1)
        } else {
          assert(row.getValue("Advertiser ID") === 2)
          assert(row.getValue("Advertiser Status") === "OFF")
          assert(row.getValue("Impressions") === null)
          assert(row.getValue("Clicks") === null)
        }
        count = count + 1
    }
  }
  test("successfully generate Oracle + Druid multi engine query with Dim lookup for dimDriven sync request") {

    val requestWithMetricSort = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Advertiser Status"},
                              {"field": "Impressions"},
                              {"field": "Clicks"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [],
                          "includeRowCount" : true,
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(requestWithMetricSort)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[MultiEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[MultiEngineQuery].drivingQuery.isInstanceOf[OracleQuery])
    val drivingQuery = pipeline.queryChain.drivingQuery.asString
//    println(drivingQuery)
    val expectedQuery = """SELECT  *
                          |      FROM (
                          |       SELECT "Advertiser ID", "Advertiser Status", "TOTALROWS", ROWNUM AS ROW_NUMBER
                          |                FROM(SELECT ao0.id "Advertiser ID", ao0."Advertiser Status" "Advertiser Status", Count(*) OVER() TOTALROWS
                          |                    FROM
                          |                  (SELECT  DECODE(status, 'ON', 'ON', 'OFF') AS "Advertiser Status", id
                          |              FROM advertiser_oracle
                          |              WHERE (id = 213)
                          |               ) ao0
                          |
                          |
                          |                    ))
                          |                    WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100""".stripMargin

    drivingQuery should equal (expectedQuery) (after being whiteSpaceNormalised)

    val result = pipeline.withOracleCallback {
      rl =>
        val row1 = rl.newRow
        row1.addValue("Advertiser ID", 1)
        row1.addValue("Advertiser Status", "ON")
        rl.addRow(row1)
        val row2 = rl.newRow
        row2.addValue("Advertiser ID", 2)
        row2.addValue("Advertiser Status", "OFF")
        rl.addRow(row2)
    }.withDruidCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Advertiser ID", 1)
        row.addValue("Advertiser Status", "ON")
        row.addValue("Impressions", 100)
        row.addValue("Clicks", 1)
        rl.addRow(row)
    }.run()

    val subsequentQuery = pipeline.queryChain.subsequentQueryList.head.asString
//    println(subsequentQuery)
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"query","query":\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact_druid"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"stats_date","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"213"\},\{"type":"or","fields":\[\{"type":"selector","dimension":"advertiser_id","value":"2"\},\{"type":"selector","dimension":"advertiser_id","value":"1"\}\]\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"advertiser_id","outputName":"Advertiser ID","outputType":"STRING"\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Status","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"OFF","injective":false,"optimize":true,"valueColumn":"status","dimensionOverrideMap":\{\},"useQueryLevelCache":false\}\}\],"aggregations":\[\{"type":"longSum","name":"Clicks","fieldName":"clicks"\},\{"type":"longSum","name":"Impressions","fieldName":"impressions"\}\],"postAggregations":\[\],"limitSpec":\{"type":"NoopLimitSpec"\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":300000,"queryId":".*"\},"descending":false\}\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"Advertiser ID","outputName":"Advertiser ID","outputType":"STRING"\},\{"type":"extraction","dimension":"Advertiser Status","outputName":"Advertiser Status","outputType":"STRING","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"ON":"ON"\},"isOneToOne":false\},"retainMissingValue":false,"replaceMissingValueWith":"OFF","injective":false,"optimize":true\}\}\],"aggregations":\[\{"type":"longSum","name":"Clicks","fieldName":"Clicks"\},\{"type":"longSum","name":"Impressions","fieldName":"Impressions"\}\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\],"limit":100\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":300000,"queryId":".*"\},"descending":false\}"""
    subsequentQuery should fullyMatch regex json

    assert(result.isSuccess, result)
    assert(result.get.rowList.length == 2)
    var count = 1
    result.toOption.get.rowList.foreach {
      row =>
        if(count == 1) {
          assert(row.getValue("Advertiser ID") === 1)
          assert(row.getValue("Advertiser Status") === "ON")
          assert(row.getValue("Impressions") === 100)
          assert(row.getValue("Clicks") === 1)
        } else {
          assert(row.getValue("Advertiser ID") === 2)
          assert(row.getValue("Advertiser Status") === "OFF")
          assert(row.getValue("Impressions") === null)
          assert(row.getValue("Clicks") === null)
        }
        count = count + 1
    }
  }
  test("successfully generate Druid single engine query with Dim lookup for factDriven sync request") {

    val requestWithMetricSort = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Advertiser Status"},
                              {"field": "Impressions"},
                              {"field": "Clicks"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "ASC"}
                          ],
                          "forceDimensionDriven": false,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(requestWithMetricSort)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[SingleEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[SingleEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
    val drivingQuery = pipeline.queryChain.drivingQuery.asString
    val json = """\{"queryType":"groupBy","dataSource":\{"type":"query","query":\{"queryType":"groupBy","dataSource":\{"type":"table","name":"fact_druid"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"stats_date","value":".*"\},\{"type":"selector","dimension":"stats_date","value":".*"\}\]\},\{"type":"selector","dimension":"advertiser_id","value":"213"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"advertiser_id","outputName":"Advertiser ID","outputType":"STRING"\},\{"type":"extraction","dimension":"advertiser_id","outputName":"Advertiser Status","outputType":"STRING","extractionFn":\{"type":"mahaRegisteredLookup","lookup":"advertiser_lookup","retainMissingValue":false,"replaceMissingValueWith":"OFF","injective":false,"optimize":true,"valueColumn":"status","dimensionOverrideMap":\{\},"useQueryLevelCache":false\}\}\],"aggregations":\[\{"type":"longSum","name":"Clicks","fieldName":"clicks"\},\{"type":"longSum","name":"Impressions","fieldName":"impressions"\}\],"postAggregations":\[\],"limitSpec":\{"type":"NoopLimitSpec"\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":300000,"queryId":".*"\},"descending":false\}\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"Advertiser ID","outputName":"Advertiser ID","outputType":"STRING"\},\{"type":"extraction","dimension":"Advertiser Status","outputName":"Advertiser Status","outputType":"STRING","extractionFn":\{"type":"lookup","lookup":\{"type":"map","map":\{"ON":"ON"\},"isOneToOne":false\},"retainMissingValue":false,"replaceMissingValueWith":"OFF","injective":false,"optimize":true\}\}\],"aggregations":\[\{"type":"longSum","name":"Clicks","fieldName":"Clicks"\},\{"type":"longSum","name":"Impressions","fieldName":"Impressions"\}\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\{"dimension":"Impressions","direction":"ascending","dimensionOrder":\{"type":"numeric"\}\}\],"limit":100\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":300000,"queryId":".*"\},"descending":false\}"""
    drivingQuery should fullyMatch regex json

    val result = pipeline.withDruidCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Advertiser ID", 1)
        row.addValue("Advertiser Status", "ON")
        row.addValue("Impressions", 100)
        row.addValue("Clicks", 1)
        rl.addRow(row)
    }.run()

    assert(result.isSuccess, result)
    result.toOption.get.rowList.foreach {
      row =>
        assert(row.getValue("Advertiser ID") === 1)
        assert(row.getValue("Advertiser Status") === "ON")
        assert(row.getValue("Impressions") === 100)
        assert(row.getValue("Clicks") === 1)
    }
  }
  test("successfully generate sync multi engine query for oracle + druid") {
    val request: ReportingRequest = ReportingRequest.forceDruid(getReportingRequestSync(requestWithIdSort))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[MultiEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[MultiEngineQuery].drivingQuery.isInstanceOf[OracleQuery])

    val result = pipeline.withOracleCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Advertiser ID", 1)
        row.addValue("Ad Group Status", "ON")
        row.addValue("Ad Group ID", 10)
        row.addValue("Source", 2)
        row.addValue("Pricing Type", "CPC")
        row.addValue("Destination URL", "url-10")
        rl.addRow(row)
    }.withDruidCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Ad Group ID", 10)
        row.addValue("Impressions", 100)
        row.addValue("Clicks", 1)
        rl.addRow(row)
    }.run()

    assert(result.isSuccess, result)
    result.toOption.get.rowList.foreach {
      row =>
        assert(row.getValue("Impressions") === 100)
        assert(row.getValue("Clicks") === 1)
    }
  }

  test("successfully generate sync multi engine query for oracle + druid with no PK in request") {
    val request: ReportingRequest = getReportingRequestSync(requestWithDimSortNoPK)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[MultiEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[MultiEngineQuery].drivingQuery.isInstanceOf[OracleQuery])
    val drivingQuery = pipeline.queryChain.drivingQuery.asString
    assert(drivingQuery contains("Keyword ID"))
    val result = pipeline.withOracleCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Keyword ID", 10)
        row.addValue("Keyword Value", "value")
        rl.addRow(row)
    }.withDruidCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Keyword ID", 10)
        row.addValue("Impressions", 100)
        rl.addRow(row)
    }.run()

    val subsequentQuery = pipeline.queryChain.subsequentQueryList.head.asString
    assert(subsequentQuery contains("Keyword ID"))
    assert(result.isSuccess, result)
    result.toOption.get.rowList.foreach {
      row =>
        assert(row.getValue("Keyword Value") === "value")
        assert(row.getValue("Impressions") === 100)
    }
  }

  test("successfully generate sync multi engine query for druid + oracle") {
    val request: ReportingRequest = getReportingRequestSync(requestWithMetricSort)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[MultiEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[MultiEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
    val result = pipeline.withDruidCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Ad Group ID", 10)
        row.addValue("Impressions", 100)
        row.addValue("Clicks", 1)
        row.addValue("Source", 2)
        row.addValue("Pricing Type", "CPC")
        row.addValue("Destination URL", "url-10")
        rl.addRow(row)
    }.withOracleCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Advertiser ID", 1)
        row.addValue("Ad Group Status", "ON")
        row.addValue("Ad Group ID", 10)
        rl.addRow(row)
    }.run()

    assert(result.isSuccess, result)
    result.toOption.get.rowList.foreach {
      row =>
        assert(row.getValue("Impressions") === 100)
        assert(row.getValue("Clicks") === 1)
    }
  }
  test("successfully generate sync multi engine query for druid + oracle with no PK in request") {
    val request: ReportingRequest = getReportingRequestSync(requestWithMetricSortNoPK)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[MultiEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[MultiEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
    val result = pipeline.withDruidCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Keyword ID", 10)
        row.addValue("Impressions", 100)
        rl.addRow(row)
    }.withOracleCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Keyword ID", 10)
        row.addValue("Keyword Value", "value")
        rl.addRow(row)
    }.run()

    val subsequentQuery = pipeline.queryChain.subsequentQueryList.head.asString
    assert(subsequentQuery contains("Keyword ID"))
    assert(result.isSuccess, result)
    result.toOption.get.rowList.foreach {
      row =>
        assert(row.getValue("Keyword Value") === "value")
        assert(row.getValue("Impressions") === 100)
    }
  }

  test("successfully generate sync query for oracle and not druid + oracle when fact driven with low cardinality filter") {
    val request: ReportingRequest = getReportingRequestSync(factRequestWithMetricSortDimFilter)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[SingleEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[SingleEngineQuery].drivingQuery.isInstanceOf[OracleQuery])
    val result = pipeline.withOracleCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Advertiser ID", 1)
        row.addValue("Ad Group Status", "ON")
        row.addValue("Source", 2)
        row.addValue("Pricing Type", "CPC")
        row.addValue("Destination URL", "url-10")
        row.addValue("Impressions", 100)
        row.addValue("Clicks", 1)
        rl.addRow(row)
    }.run()

    assert(result.isSuccess, result)
    result.toOption.get.rowList.foreach {
      row =>
        assert(row.getValue("Impressions") === 100)
        assert(row.getValue("Clicks") === 1)
    }
  }

  test("successfully generate sync query for oracle and not druid + oracle when fact driven with no dimension ids") {
    val request: ReportingRequest = getReportingRequestSync(factRequestWithMetricSortNoDimensionIds)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[SingleEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[SingleEngineQuery].drivingQuery.isInstanceOf[OracleQuery])
    val result = pipeline.withOracleCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Advertiser ID", 1)
        row.addValue("Ad Group Status", "ON")
        row.addValue("Source", 2)
        row.addValue("Pricing Type", "CPC")
        row.addValue("Destination URL", "url-10")
        row.addValue("Impressions", 100)
        row.addValue("Clicks", 1)
        rl.addRow(row)
    }.run()

    assert(result.isSuccess, result)
    result.toOption.get.rowList.foreach {
      row =>
        assert(row.getValue("Impressions") === 100)
        assert(row.getValue("Clicks") === 1)
    }
  }
  test("successfully generate sync multi engine query for druid + oracle with fact query with no sort") {
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(factRequestWithNoSort))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[MultiEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[MultiEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
    val result = pipeline.withDruidCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Advertiser ID", 1)
        row.addValue("Ad Group ID", 10)
        row.addValue("Impressions", 100)
        row.addValue("Clicks", 1)
        row.addValue("Source", 2)
        row.addValue("Pricing Type", "CPC")
        row.addValue("Destination URL", "url-10")
        rl.addRow(row)
    }.withOracleCallback {
      case rl: IndexedRowList =>
        val row = rl.newRow
        row.addValue("Advertiser ID", 1)
        row.addValue("Ad Group Status", "ON")
        row.addValue("Ad Group ID", 10)
        rl.updateRow(row)
      case any =>
        throw new UnsupportedOperationException("shouldn't be here")
    }.run()

    assert(result.isSuccess, result)
    result.toOption.get.rowList.foreach {
      row =>
        assert(row.getValue("Impressions") === 100)
        assert(row.getValue("Clicks") === 1)
    }
  }
  test("successfully generate fact driven sync multi engine query for druid + oracle with noop when no data from druid") {
    val request: ReportingRequest = getReportingRequestSync(factRequestWithMetricSort)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[MultiEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[MultiEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
    val result = pipeline.withDruidCallback {
      rl => //no data returned from druid
    }.withOracleCallback {
      rl => assert(false, "Should not get here!")
    }.run()

    assert(result.isSuccess, result)
    assert(result.toOption.get.rowList.isEmpty)
    assert(pipeline.queryChain.subsequentQueryList.isEmpty)
  }
  test("successfully generate sync multi engine query for druid + oracle with rerun on oracle when no data from druid") {
    val request: ReportingRequest = getReportingRequestSync(requestWithMetricSort)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[MultiEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[MultiEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
    val result = pipeline.withDruidCallback {
      rl => //no data returned from druid
    }.withOracleCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Advertiser ID", 1)
        row.addValue("Ad Group Status", "ON")
        row.addValue("Ad Group ID", 10)
        row.addValue("Source", 2)
        row.addValue("Pricing Type", "CPC")
        row.addValue("Destination URL", "url-10")
        row.addValue("Impressions", 100)
        row.addValue("Clicks", 1)
        rl.addRow(row)
    }.run()

    assert(result.isSuccess, result)
    result.toOption.get.rowList.foreach {
      row =>
        assert(row.getValue("Impressions") === 100)
        assert(row.getValue("Clicks") === 1)
    }
    
    val subQuery = pipeline.queryChain.subsequentQueryList.head.asInstanceOf[OracleQuery]
    assert(subQuery.aliasColumnMap.contains("Impressions"))
    assert(subQuery.aliasColumnMap.contains("Clicks"))
    assert(!subQuery.queryContext.requestModel.includeRowCount)

  }
  test("successfully generate sync multi engine query for druid + oracle with rerun on oracle when partial page data from druid on page 2") {
    val request: ReportingRequest = getReportingRequestSync(requestWithMetricSortPage2)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[MultiEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[MultiEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
    val result = pipeline.withDruidCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Advertiser ID", 1)
        row.addValue("Ad Group ID", 10)
        row.addValue("Impressions", 100)
        row.addValue("Clicks", 1)
        rl.addRow(row)
    }.withOracleCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Advertiser ID", 1)
        row.addValue("Ad Group Status", "ON")
        row.addValue("Ad Group ID", 10)
        row.addValue("Source", 2)
        row.addValue("Pricing Type", "CPC")
        row.addValue("Destination URL", "url-10")
        row.addValue("Impressions", 100)
        row.addValue("Clicks", 1)
        rl.addRow(row)
        val row2 = rl.newRow
        row2.addValue("Advertiser ID", 1)
        row2.addValue("Ad Group Status", "ON")
        row2.addValue("Ad Group ID", 11)
        row2.addValue("Source", 2)
        row2.addValue("Pricing Type", "CPC")
        row2.addValue("Destination URL", "url-11")
        rl.addRow(row2)
    }.run()

    assert(result.isSuccess, result)
    result.toOption.get.rowList.foreach {
      row =>
        if(row.getValue("Ad Group ID") == 10) {
          assert(row.getValue("Impressions") === 100)
          assert(row.getValue("Clicks") === 1)
          assert(row.getValue("Destination URL") === "url-10")
        } else {
          assert(row.getValue("Destination URL") === "url-11")
        }
    }

    val subQuery = pipeline.queryChain.subsequentQueryList.head.asInstanceOf[OracleQuery]
    assert(subQuery.aliasColumnMap.contains("Impressions"))
    assert(subQuery.aliasColumnMap.contains("Clicks"))
    //the row count will come from the multi engine row count code path
    assert(!subQuery.queryContext.requestModel.includeRowCount)
    assert(subQuery.queryContext.isInstanceOf[CombinedQueryContext], s"incorrect query context : ${subQuery.queryContext.getClass.getSimpleName}")

  }
  test("successfully generate sync multi engine query for druid + oracle with rerun on oracle when no data from oracle") {
    val request: ReportingRequest = getReportingRequestSync(requestWithMetricSort)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[MultiEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[MultiEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
    var oracleExecutorRunCount = 0
    val result = pipeline.withDruidCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Ad Group ID", 10)
        row.addValue("Impressions", 100)
        row.addValue("Clicks", 1)
        rl.addRow(row)
    }.withOracleCallback {
      rl =>
        if(oracleExecutorRunCount == 0) {
          //first run, return nothing
        } else {
          //second run, return data
          val row = rl.newRow
          row.addValue("Advertiser ID", 1)
          row.addValue("Ad Group Status", "ON")
          row.addValue("Ad Group ID", 12)
          row.addValue("Source", 2)
          row.addValue("Pricing Type", "CPC")
          row.addValue("Destination URL", "url-10")
          row.addValue("Impressions", 101)
          row.addValue("Clicks", 2)
          rl.addRow(row)
        }
        oracleExecutorRunCount += 1
    }.run()
    assert(result.isSuccess, result)
    assert(oracleExecutorRunCount === 2)
    result.toOption.get.rowList.foreach {
      row =>
        assert(row.getValue("Ad Group ID") === 12)
        assert(row.getValue("Impressions") === 101)
        assert(row.getValue("Clicks") === 2)
    }
    assert(pipeline.queryChain.subsequentQueryList.size === 2)
    val subQuery = pipeline.queryChain.subsequentQueryList.last.asInstanceOf[OracleQuery]
    assert(subQuery.aliasColumnMap.contains("Impressions"))
    assert(subQuery.aliasColumnMap.contains("Clicks"))
    assert(!subQuery.queryContext.requestModel.includeRowCount)
  }
  test("successfully generate sync query for dim sort + fact filter") {
    val request: ReportingRequest = {
      val request = getReportingRequestSync(requestWithIdSort)
      request.copy(filterExpressions = request.filterExpressions.+:(BetweenFilter("Impressions", "10", "1000")))
    }
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[SingleEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[SingleEngineQuery].drivingQuery.isInstanceOf[OracleQuery])
  }
  test("successfully generate sync query for dim sort + dim filter") {
    val request: ReportingRequest = {
      val request = ReportingRequest.forceDruid(getReportingRequestSync(requestWithIdSort))
      request.copy(filterExpressions = request.filterExpressions.+:(EqualityFilter("Ad Group Status", "ON")))
    }
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[MultiEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[MultiEngineQuery].drivingQuery.isInstanceOf[OracleQuery])
  }
  test("successfully generate sync query for dim sort + fact & dim filters") {
    val request: ReportingRequest = {
      val request = getReportingRequestSync(requestWithIdSort)
      request.copy(filterExpressions = request.filterExpressions ++ IndexedSeq(EqualityFilter("Ad Group Status", "ON"), BetweenFilter("Impressions", "10", "1000")))
    }
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[SingleEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[SingleEngineQuery].drivingQuery.isInstanceOf[OracleQuery])
  }
  test("successfully generate sync query for fact sort + fact filter") {
    val request: ReportingRequest = {
      val request = getReportingRequestSync(requestWithMetricSort)
      request.copy(filterExpressions = request.filterExpressions.+:(BetweenFilter("Impressions", "10", "1000")))
    }
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[MultiEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[MultiEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
  }
  test("successfully generate sync query for fact sort + dim filter") {
    val request: ReportingRequest = {
      val request = getReportingRequestSync(requestWithMetricSort)
      request.copy(filterExpressions = request.filterExpressions.+:(EqualityFilter("Ad Group Status", "ON")))
    }
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[MultiEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[MultiEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
  }
  test("successfully generate sync query for fact sort + fact & dim filters") {
    val request: ReportingRequest = {
      val request = getReportingRequestSync(requestWithMetricSort)
      request.copy(filterExpressions = request.filterExpressions ++ IndexedSeq(EqualityFilter("Ad Group Status", "ON"), BetweenFilter("Impressions", "10", "1000")))
    }
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[MultiEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[MultiEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
  }
  test("successfully generate async single engine query") {
    val request: ReportingRequest = getReportingRequestAsync(requestWithMetricSortAndDay)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[SingleEngineQuery])

    val result = pipeline.withOracleCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Advertiser ID", 1)
        row.addValue("Ad Group Status", "ON")
        row.addValue("Ad Group ID", 10)
        row.addValue("Source", 2)
        row.addValue("Pricing Type", "CPC")
        row.addValue("Destination URL", "url-10")
        row.addValue("Impressions", 100)
        row.addValue("Clicks", 1)
        rl.addRow(row)
    }.run()

    assert(result.isSuccess, result)
    result.toOption.get.rowList.foreach {
      row =>
        assert(row.getValue("Impressions") === 100)
        assert(row.getValue("Clicks") === 1)
    }
  }

  test("successfully run async dim only query") {
    val request: ReportingRequest = getReportingRequestAsync(requestWithDimOnly)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[SingleEngineQuery])

    val result = pipeline.withOracleCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Advertiser ID", 1)
        row.addValue("Ad Group Status", "ON")
        row.addValue("Ad Group ID", 10)
        row.addValue("Keyword Value", "kv")
        rl.addRow(row)
    }.run()

    assert(result.isSuccess, result)
    result.toOption.get.rowList.foreach {
      row =>
        assert(row.getValue("Keyword Value") === "kv")
    }
  }

  test("successfully generate multiple query pipelines") {

    val request1: ReportingRequest = ReportingRequest.forceOracle(getReportingRequestSync(requestWithMetricSort))
    val registry = defaultRegistry
    val requestModel1 = getRequestModel(request1, registry)
    assert(requestModel1.isSuccess, requestModel1.errorMessage("Building request model failed"))

    val request2: ReportingRequest = getReportingRequestSync(requestWithIdSort)
    val requestModel2 = getRequestModel(request2, registry)
    assert(requestModel2.isSuccess, requestModel2.errorMessage("Building request model failed"))

    val queryPipelineTries = queryPipelineFactory.from((requestModel1.get, Some(requestModel2.get)), QueryAttributes.empty)
    assert(queryPipelineTries._1.isSuccess)
    assert(queryPipelineTries._2.get.isSuccess)
  }

  test("successfully generate sync query for fact sort + dim filter where Druid is engine for both fact and dim") {
    val request = getReportingRequestSync(requestWithDruidFactAndDim)

    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[SingleEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[SingleEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
  }
  test("successfully generate sync query for fact sort + dim filter where Oracle is engine since max lookback is out of Druid dim range") {
    val request = getReportingRequestSync(requestWithOutOfRangeDruidDim)

    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[MultiEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[MultiEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
    assert(pipeline.queryChain.asInstanceOf[MultiEngineQuery].engines.contains(OracleEngine))
  }
  test("successfully generate async query for fact sort + dim filter where Oracle is engine since max lookback is out of Druid dim range") {
    val request = getReportingRequestAsync(requestWithOutOfRangeDruidDim)

    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[SingleEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[SingleEngineQuery].drivingQuery.isInstanceOf[OracleQuery])
  }
  test("query with dim IN filter with more than allowed limit should fail if engine is oracle") {
    val request: ReportingRequest = {
      val request = ReportingRequest.forceOracle(getReportingRequestSync(requestWithIdSort))
      request.copy(filterExpressions = request.filterExpressions ++ IndexedSeq(EqualityFilter("Ad Group Status", "ON"), InFilter("Ad Group ID", List("1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70","71","72","73","74","75","76","77","78","79","80","81","82","83","84","85","86","87","88","89","90","91","92","93","94","95","96","97","98","99","100","101","102","103","104","105","106","107","108","109","110","111","112","113","114","115","116","117","118","119","120","121","122","123","124","125","126","127","128","129","130","131","132","133","134","135","136","137","138","139","140","141","142","143","144","145","146","147","148","149","150","151","152","153","154","155","156","157","158","159","160","161","162","163","164","165","166","167","168","169","170","171","172","173","174","175","176","177","178","179","180","181","182","183","184","185","186","187","188","189","190","191","192","193","194","195","196","197","198","199","200","201","202","203","204","205","206","207","208","209","210","211","212","213","214","215","216","217","218","219","220","221","222","223","224","225","226","227","228","229","230","231","232","233","234","235","236","237","238","239","240","241","242","243","244","245","246","247","248","249","250","251","252","253","254","255","256","257","258","259","260","261","262","263","264","265","266","267","268","269","270","271","272","273","274","275","276","277","278","279","280","281","282","283","284","285","286","287","288","289","290","291","292","293","294","295","296","297","298","299","300","301","302","303","304","305","306","307","308","309","310","311","312","313","314","315","316","317","318","319","320","321","322","323","324","325","326","327","328","329","330","331","332","333","334","335","336","337","338","339","340","341","342","343","344","345","346","347","348","349","350","351","352","353","354","355","356","357","358","359","360","361","362","363","364","365","366","367","368","369","370","371","372","373","374","375","376","377","378","379","380","381","382","383","384","385","386","387","388","389","390","391","392","393","394","395","396","397","398","399","400","401","402","403","404","405","406","407","408","409","410","411","412","413","414","415","416","417","418","419","420","421","422","423","424","425","426","427","428","429","430","431","432","433","434","435","436","437","438","439","440","441","442","443","444","445","446","447","448","449","450","451","452","453","454","455","456","457","458","459","460","461","462","463","464","465","466","467","468","469","470","471","472","473","474","475","476","477","478","479","480","481","482","483","484","485","486","487","488","489","490","491","492","493","494","495","496","497","498","499","500","501","502","503","504","505","506","507","508","509","510","511","512","513","514","515","516","517","518","519","520","521","522","523","524","525","526","527","528","529","530","531","532","533","534","535","536","537","538","539","540","541","542","543","544","545","546","547","548","549","550","551","552","553","554","555","556","557","558","559","560","561","562","563","564","565","566","567","568","569","570","571","572","573","574","575","576","577","578","579","580","581","582","583","584","585","586","587","588","589","590","591","592","593","594","595","596","597","598","599","600","601","602","603","604","605","606","607","608","609","610","611","612","613","614","615","616","617","618","619","620","621","622","623","624","625","626","627","628","629","630","631","632","633","634","635","636","637","638","639","640","641","642","643","644","645","646","647","648","649","650","651","652","653","654","655","656","657","658","659","660","661","662","663","664","665","666","667","668","669","670","671","672","673","674","675","676","677","678","679","680","681","682","683","684","685","686","687","688","689","690","691","692","693","694","695","696","697","698","699","700","701","702","703","704","705","706","707","708","709","710","711","712","713","714","715","716","717","718","719","720","721","722","723","724","725","726","727","728","729","730","731","732","733","734","735","736","737","738","739","740","741","742","743","744","745","746","747","748","749","750","751","752","753","754","755","756","757","758","759","760","761","762","763","764","765","766","767","768","769","770","771","772","773","774","775","776","777","778","779","780","781","782","783","784","785","786","787","788","789","790","791","792","793","794","795","796","797","798","799","800","801","802","803","804","805","806","807","808","809","810","811","812","813","814","815","816","817","818","819","820","821","822","823","824","825","826","827","828","829","830","831","832","833","834","835","836","837","838","839","840","841","842","843","844","845","846","847","848","849","850","851","852","853","854","855","856","857","858","859","860","861","862","863","864","865","866","867","868","869","870","871","872","873","874","875","876","877","878","879","880","881","882","883","884","885","886","887","888","889","890","891","892","893","894","895","896","897","898","899","900","901","902","903","904","905","906","907","908","909","910","911","912","913","914","915","916","917","918","919","920","921","922","923","924","925","926","927","928","929","930","931","932","933","934","935","936","937","938","939","940","941","942","943","944","945","946","947","948","949","950","951","952","953","954","955","956","957","958","959","960","961","962","963","964","965","966","967","968","969","970","971","972","973","974","975","976","977","978","979","980","981","982","983","984","985","986","987","988","989","990","991","992","993","994","995","996","997","998","999","1000"))))
    }
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure, "should fail if In filter with more than allowed limit")
    assert(queryPipelineTry.failed.get.getMessage === "requirement failed: Failed to find best candidate, forceEngine=Some(Oracle), engine disqualifyingSet=Set(Hive, Presto), candidates=Set((fact_druid,Druid), (fact_hive,Hive), (fact_oracle,Oracle))")

  }
  test("query with fact IN filter with more than allowed limit should fail if engine is oracle") {
    val request: ReportingRequest = {
      val request = ReportingRequest.forceOracle(getReportingRequestSync(requestWithIdSort))
      request.copy(filterExpressions = request.filterExpressions ++ IndexedSeq(EqualityFilter("Ad Group Status", "ON"), InFilter("Impressions", List("1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70","71","72","73","74","75","76","77","78","79","80","81","82","83","84","85","86","87","88","89","90","91","92","93","94","95","96","97","98","99","100","101","102","103","104","105","106","107","108","109","110","111","112","113","114","115","116","117","118","119","120","121","122","123","124","125","126","127","128","129","130","131","132","133","134","135","136","137","138","139","140","141","142","143","144","145","146","147","148","149","150","151","152","153","154","155","156","157","158","159","160","161","162","163","164","165","166","167","168","169","170","171","172","173","174","175","176","177","178","179","180","181","182","183","184","185","186","187","188","189","190","191","192","193","194","195","196","197","198","199","200","201","202","203","204","205","206","207","208","209","210","211","212","213","214","215","216","217","218","219","220","221","222","223","224","225","226","227","228","229","230","231","232","233","234","235","236","237","238","239","240","241","242","243","244","245","246","247","248","249","250","251","252","253","254","255","256","257","258","259","260","261","262","263","264","265","266","267","268","269","270","271","272","273","274","275","276","277","278","279","280","281","282","283","284","285","286","287","288","289","290","291","292","293","294","295","296","297","298","299","300","301","302","303","304","305","306","307","308","309","310","311","312","313","314","315","316","317","318","319","320","321","322","323","324","325","326","327","328","329","330","331","332","333","334","335","336","337","338","339","340","341","342","343","344","345","346","347","348","349","350","351","352","353","354","355","356","357","358","359","360","361","362","363","364","365","366","367","368","369","370","371","372","373","374","375","376","377","378","379","380","381","382","383","384","385","386","387","388","389","390","391","392","393","394","395","396","397","398","399","400","401","402","403","404","405","406","407","408","409","410","411","412","413","414","415","416","417","418","419","420","421","422","423","424","425","426","427","428","429","430","431","432","433","434","435","436","437","438","439","440","441","442","443","444","445","446","447","448","449","450","451","452","453","454","455","456","457","458","459","460","461","462","463","464","465","466","467","468","469","470","471","472","473","474","475","476","477","478","479","480","481","482","483","484","485","486","487","488","489","490","491","492","493","494","495","496","497","498","499","500","501","502","503","504","505","506","507","508","509","510","511","512","513","514","515","516","517","518","519","520","521","522","523","524","525","526","527","528","529","530","531","532","533","534","535","536","537","538","539","540","541","542","543","544","545","546","547","548","549","550","551","552","553","554","555","556","557","558","559","560","561","562","563","564","565","566","567","568","569","570","571","572","573","574","575","576","577","578","579","580","581","582","583","584","585","586","587","588","589","590","591","592","593","594","595","596","597","598","599","600","601","602","603","604","605","606","607","608","609","610","611","612","613","614","615","616","617","618","619","620","621","622","623","624","625","626","627","628","629","630","631","632","633","634","635","636","637","638","639","640","641","642","643","644","645","646","647","648","649","650","651","652","653","654","655","656","657","658","659","660","661","662","663","664","665","666","667","668","669","670","671","672","673","674","675","676","677","678","679","680","681","682","683","684","685","686","687","688","689","690","691","692","693","694","695","696","697","698","699","700","701","702","703","704","705","706","707","708","709","710","711","712","713","714","715","716","717","718","719","720","721","722","723","724","725","726","727","728","729","730","731","732","733","734","735","736","737","738","739","740","741","742","743","744","745","746","747","748","749","750","751","752","753","754","755","756","757","758","759","760","761","762","763","764","765","766","767","768","769","770","771","772","773","774","775","776","777","778","779","780","781","782","783","784","785","786","787","788","789","790","791","792","793","794","795","796","797","798","799","800","801","802","803","804","805","806","807","808","809","810","811","812","813","814","815","816","817","818","819","820","821","822","823","824","825","826","827","828","829","830","831","832","833","834","835","836","837","838","839","840","841","842","843","844","845","846","847","848","849","850","851","852","853","854","855","856","857","858","859","860","861","862","863","864","865","866","867","868","869","870","871","872","873","874","875","876","877","878","879","880","881","882","883","884","885","886","887","888","889","890","891","892","893","894","895","896","897","898","899","900","901","902","903","904","905","906","907","908","909","910","911","912","913","914","915","916","917","918","919","920","921","922","923","924","925","926","927","928","929","930","931","932","933","934","935","936","937","938","939","940","941","942","943","944","945","946","947","948","949","950","951","952","953","954","955","956","957","958","959","960","961","962","963","964","965","966","967","968","969","970","971","972","973","974","975","976","977","978","979","980","981","982","983","984","985","986","987","988","989","990","991","992","993","994","995","996","997","998","999","1000"))))
    }
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure, "should fail if In filter with more than allowed limit")
    assert(queryPipelineTry.failed.get.getMessage === "requirement failed: Failed to find best candidate, forceEngine=Some(Oracle), engine disqualifyingSet=Set(Druid, Hive, Presto), candidates=Set((fact_druid,Druid), (fact_hive,Hive), (fact_oracle,Oracle))")
  }
  test("query with fact IN filter with 1000 values should succeed if engine is druid") {
    val request: ReportingRequest = {
      val request = ReportingRequest.forceDruid(getReportingRequestSync(factOnlyRequestWithMetricSort))
      request.copy(filterExpressions = request.filterExpressions ++ IndexedSeq(InFilter("Impressions", List("1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70","71","72","73","74","75","76","77","78","79","80","81","82","83","84","85","86","87","88","89","90","91","92","93","94","95","96","97","98","99","100","101","102","103","104","105","106","107","108","109","110","111","112","113","114","115","116","117","118","119","120","121","122","123","124","125","126","127","128","129","130","131","132","133","134","135","136","137","138","139","140","141","142","143","144","145","146","147","148","149","150","151","152","153","154","155","156","157","158","159","160","161","162","163","164","165","166","167","168","169","170","171","172","173","174","175","176","177","178","179","180","181","182","183","184","185","186","187","188","189","190","191","192","193","194","195","196","197","198","199","200","201","202","203","204","205","206","207","208","209","210","211","212","213","214","215","216","217","218","219","220","221","222","223","224","225","226","227","228","229","230","231","232","233","234","235","236","237","238","239","240","241","242","243","244","245","246","247","248","249","250","251","252","253","254","255","256","257","258","259","260","261","262","263","264","265","266","267","268","269","270","271","272","273","274","275","276","277","278","279","280","281","282","283","284","285","286","287","288","289","290","291","292","293","294","295","296","297","298","299","300","301","302","303","304","305","306","307","308","309","310","311","312","313","314","315","316","317","318","319","320","321","322","323","324","325","326","327","328","329","330","331","332","333","334","335","336","337","338","339","340","341","342","343","344","345","346","347","348","349","350","351","352","353","354","355","356","357","358","359","360","361","362","363","364","365","366","367","368","369","370","371","372","373","374","375","376","377","378","379","380","381","382","383","384","385","386","387","388","389","390","391","392","393","394","395","396","397","398","399","400","401","402","403","404","405","406","407","408","409","410","411","412","413","414","415","416","417","418","419","420","421","422","423","424","425","426","427","428","429","430","431","432","433","434","435","436","437","438","439","440","441","442","443","444","445","446","447","448","449","450","451","452","453","454","455","456","457","458","459","460","461","462","463","464","465","466","467","468","469","470","471","472","473","474","475","476","477","478","479","480","481","482","483","484","485","486","487","488","489","490","491","492","493","494","495","496","497","498","499","500","501","502","503","504","505","506","507","508","509","510","511","512","513","514","515","516","517","518","519","520","521","522","523","524","525","526","527","528","529","530","531","532","533","534","535","536","537","538","539","540","541","542","543","544","545","546","547","548","549","550","551","552","553","554","555","556","557","558","559","560","561","562","563","564","565","566","567","568","569","570","571","572","573","574","575","576","577","578","579","580","581","582","583","584","585","586","587","588","589","590","591","592","593","594","595","596","597","598","599","600","601","602","603","604","605","606","607","608","609","610","611","612","613","614","615","616","617","618","619","620","621","622","623","624","625","626","627","628","629","630","631","632","633","634","635","636","637","638","639","640","641","642","643","644","645","646","647","648","649","650","651","652","653","654","655","656","657","658","659","660","661","662","663","664","665","666","667","668","669","670","671","672","673","674","675","676","677","678","679","680","681","682","683","684","685","686","687","688","689","690","691","692","693","694","695","696","697","698","699","700","701","702","703","704","705","706","707","708","709","710","711","712","713","714","715","716","717","718","719","720","721","722","723","724","725","726","727","728","729","730","731","732","733","734","735","736","737","738","739","740","741","742","743","744","745","746","747","748","749","750","751","752","753","754","755","756","757","758","759","760","761","762","763","764","765","766","767","768","769","770","771","772","773","774","775","776","777","778","779","780","781","782","783","784","785","786","787","788","789","790","791","792","793","794","795","796","797","798","799","800","801","802","803","804","805","806","807","808","809","810","811","812","813","814","815","816","817","818","819","820","821","822","823","824","825","826","827","828","829","830","831","832","833","834","835","836","837","838","839","840","841","842","843","844","845","846","847","848","849","850","851","852","853","854","855","856","857","858","859","860","861","862","863","864","865","866","867","868","869","870","871","872","873","874","875","876","877","878","879","880","881","882","883","884","885","886","887","888","889","890","891","892","893","894","895","896","897","898","899","900","901","902","903","904","905","906","907","908","909","910","911","912","913","914","915","916","917","918","919","920","921","922","923","924","925","926","927","928","929","930","931","932","933","934","935","936","937","938","939","940","941","942","943","944","945","946","947","948","949","950","951","952","953","954","955","956","957","958","959","960","961","962","963","964","965","966","967","968","969","970","971","972","973","974","975","976","977","978","979","980","981","982","983","984","985","986","987","988","989","990","991","992","993","994","995","996","997","998","999","1000"))))
    }
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "should have succeeded")
  }
  test("dim IN filter with more than allowed limit should not be considered in bestDimCandidates if dim engine is oracle") {
    val request: ReportingRequest = {
      val request = getReportingRequestSync(requestWithIdSort)
      request.copy(filterExpressions = request.filterExpressions ++ IndexedSeq(EqualityFilter("Ad Group ID", "1"), InFilter("Ad Group Status", List("1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70","71","72","73","74","75","76","77","78","79","80","81","82","83","84","85","86","87","88","89","90","91","92","93","94","95","96","97","98","99","100","101","102","103","104","105","106","107","108","109","110","111","112","113","114","115","116","117","118","119","120","121","122","123","124","125","126","127","128","129","130","131","132","133","134","135","136","137","138","139","140","141","142","143","144","145","146","147","148","149","150","151","152","153","154","155","156","157","158","159","160","161","162","163","164","165","166","167","168","169","170","171","172","173","174","175","176","177","178","179","180","181","182","183","184","185","186","187","188","189","190","191","192","193","194","195","196","197","198","199","200","201","202","203","204","205","206","207","208","209","210","211","212","213","214","215","216","217","218","219","220","221","222","223","224","225","226","227","228","229","230","231","232","233","234","235","236","237","238","239","240","241","242","243","244","245","246","247","248","249","250","251","252","253","254","255","256","257","258","259","260","261","262","263","264","265","266","267","268","269","270","271","272","273","274","275","276","277","278","279","280","281","282","283","284","285","286","287","288","289","290","291","292","293","294","295","296","297","298","299","300","301","302","303","304","305","306","307","308","309","310","311","312","313","314","315","316","317","318","319","320","321","322","323","324","325","326","327","328","329","330","331","332","333","334","335","336","337","338","339","340","341","342","343","344","345","346","347","348","349","350","351","352","353","354","355","356","357","358","359","360","361","362","363","364","365","366","367","368","369","370","371","372","373","374","375","376","377","378","379","380","381","382","383","384","385","386","387","388","389","390","391","392","393","394","395","396","397","398","399","400","401","402","403","404","405","406","407","408","409","410","411","412","413","414","415","416","417","418","419","420","421","422","423","424","425","426","427","428","429","430","431","432","433","434","435","436","437","438","439","440","441","442","443","444","445","446","447","448","449","450","451","452","453","454","455","456","457","458","459","460","461","462","463","464","465","466","467","468","469","470","471","472","473","474","475","476","477","478","479","480","481","482","483","484","485","486","487","488","489","490","491","492","493","494","495","496","497","498","499","500","501","502","503","504","505","506","507","508","509","510","511","512","513","514","515","516","517","518","519","520","521","522","523","524","525","526","527","528","529","530","531","532","533","534","535","536","537","538","539","540","541","542","543","544","545","546","547","548","549","550","551","552","553","554","555","556","557","558","559","560","561","562","563","564","565","566","567","568","569","570","571","572","573","574","575","576","577","578","579","580","581","582","583","584","585","586","587","588","589","590","591","592","593","594","595","596","597","598","599","600","601","602","603","604","605","606","607","608","609","610","611","612","613","614","615","616","617","618","619","620","621","622","623","624","625","626","627","628","629","630","631","632","633","634","635","636","637","638","639","640","641","642","643","644","645","646","647","648","649","650","651","652","653","654","655","656","657","658","659","660","661","662","663","664","665","666","667","668","669","670","671","672","673","674","675","676","677","678","679","680","681","682","683","684","685","686","687","688","689","690","691","692","693","694","695","696","697","698","699","700","701","702","703","704","705","706","707","708","709","710","711","712","713","714","715","716","717","718","719","720","721","722","723","724","725","726","727","728","729","730","731","732","733","734","735","736","737","738","739","740","741","742","743","744","745","746","747","748","749","750","751","752","753","754","755","756","757","758","759","760","761","762","763","764","765","766","767","768","769","770","771","772","773","774","775","776","777","778","779","780","781","782","783","784","785","786","787","788","789","790","791","792","793","794","795","796","797","798","799","800","801","802","803","804","805","806","807","808","809","810","811","812","813","814","815","816","817","818","819","820","821","822","823","824","825","826","827","828","829","830","831","832","833","834","835","836","837","838","839","840","841","842","843","844","845","846","847","848","849","850","851","852","853","854","855","856","857","858","859","860","861","862","863","864","865","866","867","868","869","870","871","872","873","874","875","876","877","878","879","880","881","882","883","884","885","886","887","888","889","890","891","892","893","894","895","896","897","898","899","900","901","902","903","904","905","906","907","908","909","910","911","912","913","914","915","916","917","918","919","920","921","922","923","924","925","926","927","928","929","930","931","932","933","934","935","936","937","938","939","940","941","942","943","944","945","946","947","948","949","950","951","952","953","954","955","956","957","958","959","960","961","962","963","964","965","966","967","968","969","970","971","972","973","974","975","976","977","978","979","980","981","982","983","984","985","986","987","988","989","990","991","992","993","994","995","996","997","998","999","1000"))))
    }
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure)
  }
  test("query with OR filter should fail if engine is oracle") {
    val request: ReportingRequest = {
      val request = ReportingRequest.forceOracle(getReportingRequestSync(requestWithIdSort))
      request.copy(filterExpressions = request.filterExpressions ++ IndexedSeq(OrFilter(List(InFilter("Impressions", List("1"))))))
    }
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure, "should fail if In filter with more than allowed limit")
    assert(queryPipelineTry.failed.get.getMessage === "requirement failed: Failed to find best candidate, forceEngine=Some(Oracle), engine disqualifyingSet=Set(Druid, Hive, Presto), candidates=Set((fact_druid,Druid), (fact_hive,Hive), (fact_oracle,Oracle))")
  }
  test("query with OR filter should fail if engine is Hive") {
    val request: ReportingRequest = {
      val request = ReportingRequest.forceHive(getReportingRequestAsync(requestWithIdSort))
      request.copy(filterExpressions = request.filterExpressions ++ IndexedSeq(OrFilter(List(InFilter("Impressions", List("1"))))))
    }
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure, "should fail if In filter with more than allowed limit")
    assert(queryPipelineTry.failed.get.getMessage === "requirement failed: Failed to find best candidate, forceEngine=Some(Hive), engine disqualifyingSet=Set(Druid), candidates=Set((fact_druid,Druid), (fact_hive,Hive), (fact_oracle,Oracle))")
  }

  test("Validate subsequent Oracle + Druid query generation to dim.") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Ad Group Status"},
                              {"field": "Ad Group ID"},
                              {"field": "Source"},
                              {"field": "Pricing Type"},
                              {"field": "Destination URL"},
                              {"field": "Impressions"},
                              {"field": "Clicks"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "=", "value": "$fromDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "213"}
                          ],
                          "sortBy": [
                            {"field": "Impressions", "order": "Asc"},
                            {"field": "Ad Group ID", "order": "Asc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""
    val request: ReportingRequest = ReportingRequest.forceDruid(getReportingRequestSync(jsonString))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, new DruidQueryGenerator(new SyncDruidQueryOptimizer(), 40000, useCustomRoundingSumAggregator = true)) //do not include local time filter
    altQueryGeneratorRegistry.register(OracleEngine, new OracleQueryGenerator(DefaultPartitionColumnRenderer))
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    
    val result = queryPipelineTry.toOption.get.withDruidCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Ad Group ID", 10)
        row.addValue("Impressions", 100)
        row.addValue("Source", 2)
        row.addValue("Pricing Type", "CPC")
        row.addValue("Destination URL", "url-10")
        rl.addRow(row)
    }.withOracleCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Advertiser ID", 1)
        row.addValue("Ad Group Status", "ON")
        row.addValue("Ad Group ID", 10)
        rl.addRow(row)
    }.run()

    assert(result.isSuccess, result)
    val oracleQuery = queryPipelineTry.toOption.get.queryChain.subsequentQueryList.head
  }

  test("fail to generate sync query for presto") {
    val request: ReportingRequest = ReportingRequest.forcePresto(getReportingRequestSync(requestWithIdSort))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure, queryPipelineTry.errorMessage("Expected failure to get the query pipeline, Presto is not supported for sync queries"))
    assert(queryPipelineTry.failed.get.getMessage === "requirement failed: Failed to find best candidate, forceEngine=Some(Presto), engine disqualifyingSet=Set(Hive, Presto), candidates=Set((fact_druid,Druid), (fact_hive,Hive), (fact_oracle,Oracle))")
  }

  test("successfully generate fallback query on sync request and execute it") {
    val request: ReportingRequest = getReportingRequestSync(factRequestWithOracleAndDruidDim)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[SingleEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[SingleEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
    assert(pipeline.fallbackQueryChainOption.isDefined)
    assert(pipeline.isInstanceOf[QueryPipelineWithFallback])
    assert(pipeline.asInstanceOf[QueryPipelineWithFallback].fallbackQueryChain.isInstanceOf[SingleEngineQuery])
    assert(pipeline.asInstanceOf[QueryPipelineWithFallback].fallbackQueryChain.asInstanceOf[SingleEngineQuery].drivingQuery.isInstanceOf[OracleQuery])
    val result = pipeline.withDruidCallback {
      rl => throw new IllegalStateException("error!")
    }.withOracleCallback {
      rl =>
        assert(true, "Should get here!")
        val row = rl.newRow
        row.addValue("Day", fromDate)
        row.addValue("Advertiser ID", 1)
        row.addValue("Advertiser Name", "ON")
        row.addValue("Impressions", 100)
        row.addValue("Clicks", 1)
        rl.addRow(row)
    }.run()

    assert(result.isSuccess, result)
    assert(!result.toOption.get.rowList.isEmpty)
  }

  test("successfully generate fallback query on sync request with force engine and execute it") {
    val request: ReportingRequest = ReportingRequest.forceDruid(getReportingRequestSync(factRequestWithOracleAndDruidDim))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[SingleEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[SingleEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
    assert(pipeline.fallbackQueryChainOption.isDefined)
    assert(pipeline.isInstanceOf[QueryPipelineWithFallback])
    assert(pipeline.asInstanceOf[QueryPipelineWithFallback].fallbackQueryChain.isInstanceOf[SingleEngineQuery])
    assert(pipeline.asInstanceOf[QueryPipelineWithFallback].fallbackQueryChain.asInstanceOf[SingleEngineQuery].drivingQuery.isInstanceOf[OracleQuery])
    val result = pipeline.withDruidCallback {
      rl => throw new IllegalStateException("error!")
    }.withOracleCallback {
      rl =>
        assert(true, "Should get here!")
        val row = rl.newRow
        row.addValue("Day", fromDate)
        row.addValue("Advertiser ID", 1)
        row.addValue("Advertiser Name", "ON")
        row.addValue("Impressions", 100)
        row.addValue("Clicks", 1)
        rl.addRow(row)
    }.run()

    assert(result.isSuccess, result)
    assert(!result.toOption.get.rowList.isEmpty)
  }

  test("successfully generate fallback query on sync request and fail to execute it due to missing executor") {
    val request: ReportingRequest = getReportingRequestSync(factRequestWithOracleAndDruidDim)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[SingleEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[SingleEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
    assert(pipeline.fallbackQueryChainOption.isDefined)
    assert(pipeline.isInstanceOf[QueryPipelineWithFallback])
    assert(pipeline.asInstanceOf[QueryPipelineWithFallback].fallbackQueryChain.isInstanceOf[SingleEngineQuery])
    assert(pipeline.asInstanceOf[QueryPipelineWithFallback].fallbackQueryChain.asInstanceOf[SingleEngineQuery].drivingQuery.isInstanceOf[OracleQuery])
    val result = pipeline.withDruidCallback {
      rl => throw new IllegalStateException("error!")
    }.run()

    assert(result.isFailure, result)
    assert(result.failed.get.getMessage startsWith "Executor not found for engine=Oracle", result)
  }

  test("successfully generate fallback query on async request and execute it") {
    val request: ReportingRequest = getReportingRequestAsync(factRequestWithOracleAndDruidDim)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[SingleEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[SingleEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
    assert(pipeline.fallbackQueryChainOption.isDefined)
    assert(pipeline.isInstanceOf[QueryPipelineWithFallback])
    assert(pipeline.asInstanceOf[QueryPipelineWithFallback].fallbackQueryChain.isInstanceOf[SingleEngineQuery])
    assert(pipeline.asInstanceOf[QueryPipelineWithFallback].fallbackQueryChain.asInstanceOf[SingleEngineQuery].drivingQuery.isInstanceOf[OracleQuery])
    val result = pipeline.withDruidCallback {
      rl => throw new IllegalStateException("error!")
    }.withOracleCallback {
      rl =>
        assert(true, "Should get here!")
        val row = rl.newRow
        row.addValue("Day", fromDate)
        row.addValue("Advertiser ID", 1)
        row.addValue("Advertiser Name", "ON")
        row.addValue("Impressions", 100)
        row.addValue("Clicks", 1)
        rl.addRow(row)
    }.run()

    assert(result.isSuccess, result)
    assert(!result.toOption.get.rowList.isEmpty)
  }

  test("successfully generate fallback query on async request with force engine and execute it") {
    val request: ReportingRequest = ReportingRequest.forceDruid(getReportingRequestAsync(factRequestWithOracleAndDruidDim))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[SingleEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[SingleEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
    assert(pipeline.fallbackQueryChainOption.isDefined)
    assert(pipeline.isInstanceOf[QueryPipelineWithFallback])
    assert(pipeline.asInstanceOf[QueryPipelineWithFallback].fallbackQueryChain.isInstanceOf[SingleEngineQuery])
    assert(pipeline.asInstanceOf[QueryPipelineWithFallback].fallbackQueryChain.asInstanceOf[SingleEngineQuery].drivingQuery.isInstanceOf[OracleQuery])
    val result = pipeline.withDruidCallback {
      rl => throw new IllegalStateException("error!")
    }.withOracleCallback {
      rl =>
        assert(true, "Should get here!")
        val row = rl.newRow
        row.addValue("Day", fromDate)
        row.addValue("Advertiser ID", 1)
        row.addValue("Advertiser Name", "ON")
        row.addValue("Impressions", 100)
        row.addValue("Clicks", 1)
        rl.addRow(row)
    }.run()

    assert(result.isSuccess, result)
    assert(!result.toOption.get.rowList.isEmpty)
  }

  test("successfully execute pipeline while failing to create fallback query") {
    val request: ReportingRequest = getReportingRequestAsync(factRequestWithDruidFactAndDim)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[SingleEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[SingleEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
    assert(pipeline.fallbackQueryChainOption.isEmpty)
    assert(pipeline.isInstanceOf[DefaultQueryPipeline])
    val result = pipeline.withDruidCallback {
      rl => throw new IllegalStateException("error!")
    }.withOracleCallback {
      rl =>
        assert(false, "Should not get here!")
    }.run()

    assert(result.isFailure, result)
    assert(result.failed.get.getMessage === "query execution failed", result)
  }

  test("successfully generate fallback query on async request with force engine and execute it when no joins required") {
    val request: ReportingRequest = ReportingRequest.forceDruid(getReportingRequestAsync(factRequestWithNoDims))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[SingleEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[SingleEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
    assert(pipeline.fallbackQueryChainOption.isDefined)
    assert(pipeline.isInstanceOf[QueryPipelineWithFallback])
    assert(pipeline.asInstanceOf[QueryPipelineWithFallback].fallbackQueryChain.isInstanceOf[SingleEngineQuery])
    assert(pipeline.asInstanceOf[QueryPipelineWithFallback].fallbackQueryChain.asInstanceOf[SingleEngineQuery].drivingQuery.isInstanceOf[OracleQuery])
    val result = pipeline.withDruidCallback {
      rl => throw new IllegalStateException("error!")
    }.withOracleCallback {
      rl =>
        assert(true, "Should get here!")
        val row = rl.newRow
        row.addValue("Day", fromDate)
        row.addValue("Advertiser ID", 1)
        row.addValue("Impressions", 100)
        row.addValue("Clicks", 1)
        rl.addRow(row)
    }.run()

    assert(result.isSuccess, result)
    assert(!result.toOption.get.rowList.isEmpty)
  }

  test("successfully generate query with queryGeneratorBucket defined") {
    val request: ReportingRequest = ReportingRequest.forceHive(getReportingRequestAsync(requestWithMetricSortAndDay))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    object TestBucketingConfig extends BucketingConfig {
      override def getConfigForCube(cube: String): Option[CubeBucketingConfig] = None
      override def getConfigForQueryGen(engine: Engine): Option[QueryGenBucketingConfig] = {
        Some(QueryGenBucketingConfig.builder()
          .internalBucketPercentage(Map(Version.v0 -> 100, Version.v2 -> 0))
          .externalBucketPercentage(Map(Version.v0 -> 100, Version.v2 -> 0))
          .dryRunPercentage(Map(Version.v2 -> 100))
          .build())
      }
    }

    val bucketSelector = new BucketSelector(registry, TestBucketingConfig)

    val queryPipelineTry = queryPipelineFactory.fromBucketSelector(new Tuple2(requestModel.get, None), QueryAttributes.empty, bucketSelector, BucketParams())
    assert(queryPipelineTry._1.isSuccess, queryPipelineTry._1.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry._1.toOption.get

    assert(pipeline.queryChain.isInstanceOf[SingleEngineQuery])
    assert(queryPipelineTry._2.isDefined && queryPipelineTry._2.get.isSuccess)
    assert(queryPipelineTry._2.get.get.queryChain.drivingQuery.engine.equals(HiveEngine))
    assert(queryPipelineTry._3.isEmpty)
  }

  test("successfully generate query with force queryGen version") {
    val request: ReportingRequest = ReportingRequest.forceHive(getReportingRequestAsync(requestWithMetricSortAndDay))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    object TestBucketingConfig extends BucketingConfig {
      override def getConfigForCube(cube: String): Option[CubeBucketingConfig] = None
      override def getConfigForQueryGen(engine: Engine): Option[QueryGenBucketingConfig] = {
        Some(QueryGenBucketingConfig.builder()
          .internalBucketPercentage(Map(Version.v0 -> 100, Version.v2 -> 0))
          .externalBucketPercentage(Map(Version.v0 -> 100, Version.v2 -> 0))
          .dryRunPercentage(Map(Version.v2 -> 100))
          .build())
      }
    }

    val bucketSelector = new BucketSelector(registry, TestBucketingConfig)

    val queryPipelineBuilderTry = queryPipelineFactory.builder(requestModel.get, QueryAttributes.empty, Option(bucketSelector), BucketParams(forceQueryGenVersion = Option(Version.v2)))._1
    assert(queryPipelineBuilderTry.isSuccess, queryPipelineBuilderTry)
    val pipeline = queryPipelineBuilderTry.get.build()

    assert(pipeline.queryChain.isInstanceOf[SingleEngineQuery])
    assert(Version.v2.equals(pipeline.queryChain.drivingQuery.queryGenVersion.get))

    val queryPipelineTry = queryPipelineFactory.fromQueryGenVersion(requestModel.get, QueryAttributes.empty, Version.v2)
    assert(queryPipelineTry.isSuccess, queryPipelineTry)
    assert(Version.v2.equals(queryPipelineTry.get.queryChain.drivingQuery.queryGenVersion.get))
  }

  test("successfully generate query with queryGeneratorBucket defined and no dryRun requestModel") {
    val request: ReportingRequest = ReportingRequest.forceHive(getReportingRequestAsync(requestWithMetricSortAndDay))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    object TestBucketingConfig extends BucketingConfig {
      override def getConfigForCube(cube: String): Option[CubeBucketingConfig] = None
      override def getConfigForQueryGen(engine: Engine): Option[QueryGenBucketingConfig] = {
        Some(QueryGenBucketingConfig.builder()
          .internalBucketPercentage(Map(Version.v0 -> 100, Version.v2 -> 0))
          .externalBucketPercentage(Map(Version.v0 -> 100, Version.v2 -> 0))
          .dryRunPercentage(Map(Version.v2 -> 100))
          .build())
      }
    }

    val bucketSelector = new BucketSelector(registry, TestBucketingConfig)

    val queryPipelineTry = queryPipelineFactory.fromBucketSelector(requestModel.get, QueryAttributes.empty, bucketSelector, BucketParams())
    assert(queryPipelineTry._1.isSuccess, queryPipelineTry._1.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry._1.toOption.get

    assert(pipeline.queryChain.isInstanceOf[SingleEngineQuery])
    assert(queryPipelineTry._2.isDefined && queryPipelineTry._2.get.isSuccess)
    assert(queryPipelineTry._2.get.get.queryChain.drivingQuery.engine.equals(HiveEngine))
  }

  test("test fact and dim revisioning logic") {
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(factRequestWithNoSort))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry, revision = Some(10))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    val query = pipeline.queryChain.drivingQuery.asString
    assert(query.contains("stats_source2"))
  }

  test("Drop into V0 if selected revision is fake.") {
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(factRequestWithNoSortAndKeywordIdForInvalidQuery))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry, revision = Some(10))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))
    assert(requestModel.toOption.get.dimensionsCandidates.head.dim.name == "ad_group")
  }
}
