// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core._
import com.yahoo.maha.core.query.druid.{DruidQuery, DruidQueryGenerator, SyncDruidQueryOptimizer}
import com.yahoo.maha.core.query.hive.HiveQueryGenerator
import com.yahoo.maha.core.query.oracle.OracleQueryGenerator
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.core.{BetweenFilter, DefaultPartitionColumnRenderer, EqualityFilter, RequestModel}
import com.yahoo.maha.executor.{MockDruidQueryExecutor, MockHiveQueryExecutor, MockOracleQueryExecutor}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.util.Try

/**
 * Created by hiral on 4/8/16.
 */

object DefaultQueryPipelineFactoryTest {
  implicit class PipelineRunner(pipeline: QueryPipeline) {
    val queryExecutorContext = new QueryExecutorContext
    
    def withDruidCallback(callback: RowList => Unit) : PipelineRunner = {
      val e = new MockDruidQueryExecutor(callback)
      queryExecutorContext.register(e)
      this
    }
    def withOracleCallback(callback: RowList => Unit) : PipelineRunner = {
      val e = new MockOracleQueryExecutor(callback)
      queryExecutorContext.register(e)
      this
    }
    def withHiveCallback(callback: RowList => Unit) : PipelineRunner = {
      val e = new MockHiveQueryExecutor(callback)
      queryExecutorContext.register(e)
      this
    }
    def run(queryAttributes: QueryAttributes = QueryAttributes.empty) : Try[(RowList, QueryAttributes)] = {
      pipeline.execute(queryExecutorContext, queryAttributes)
    }
  }
  
}

class DefaultQueryPipelineFactoryTest extends FunSuite with Matchers with BeforeAndAfterAll with BaseQueryGeneratorTest with SharedDimSchema with BaseQueryContextTest {
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
    DruidQueryGenerator.register(queryGeneratorRegistry)
    HiveQueryGenerator.register(queryGeneratorRegistry, DefaultPartitionColumnRenderer, TestUDFRegistrationFactory())
  }


  test("force injectFilterSet to fk primary key") {
    val request: ReportingRequest = getReportingRequestSync(requestWithFkInjected)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
        rl.addRow(row)
    }.withOracleCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Advertiser ID", 1)
        row.addValue("Ad Group Status", "ON")
        row.addValue("Campaign ID", 10)
        row.addValue("Ad Group ID", 202)
        row.addValue("Source", 2)
        row.addValue("Pricing Type", "CPC")
        row.addValue("Destination URL", "url-10")
        rl.addRow(row)
    }.run()

    assert(result.isSuccess, result)
    result.toOption.get._1.foreach {
      row =>
        assert(row.getValue("Impressions") === 100)
        assert(row.getValue("Clicks") === 1)
    }
  }

  test("successfully generate sync single engine query") {

    val request: ReportingRequest = ReportingRequest.forceOracle(getReportingRequestSync(requestWithMetricSort))
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    result.toOption.get._1.foreach {
      row =>
        assert(row.getValue("Impressions") === 100)
        assert(row.getValue("Clicks") === 1)
    }
  }
  test("successfully generate Druid single engine query with Dim lookup") {

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

    val request: ReportingRequest = ReportingRequest.forceDruid(getReportingRequestSync(requestWithMetricSort))
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[SingleEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[SingleEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])

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
    result.toOption.get._1.foreach {
      row =>
        assert(row.getValue("Advertiser ID") === 1)
        assert(row.getValue("Advertiser Status") === "ON")
        assert(row.getValue("Impressions") === 100)
        assert(row.getValue("Clicks") === 1)
    }
  }
  test("successfully generate sync multi engine query for oracle + druid") {
    val request: ReportingRequest = ReportingRequest.forceDruid(getReportingRequestSync(requestWithIdSort))
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    result.toOption.get._1.foreach {
      row =>
        assert(row.getValue("Impressions") === 100)
        assert(row.getValue("Clicks") === 1)
    }
  }
  test("successfully generate sync multi engine query for druid + oracle") {
    val request: ReportingRequest = getReportingRequestSync(requestWithMetricSort)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
        rl.addRow(row)
    }.run()

    assert(result.isSuccess, result)
    result.toOption.get._1.foreach {
      row =>
        assert(row.getValue("Impressions") === 100)
        assert(row.getValue("Clicks") === 1)
    }
  }

  test("successfully generate sync query for oracle and not druid + oracle when fact driven with no dimension ids") {
    val request: ReportingRequest = getReportingRequestSync(factRequestWithMetricSortNoDimensionIds)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    result.toOption.get._1.foreach {
      row =>
        assert(row.getValue("Impressions") === 100)
        assert(row.getValue("Clicks") === 1)
    }
  }
  test("successfully generate sync multi engine query for druid + oracle with fact query with no sort") {
    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(factRequestWithNoSort))
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
      case rl: IndexedRowList =>
        val row = rl.newRow
        row.addValue("Advertiser ID", 1)
        row.addValue("Ad Group Status", "ON")
        row.addValue("Ad Group ID", 10)
        row.addValue("Source", 2)
        row.addValue("Pricing Type", "CPC")
        row.addValue("Destination URL", "url-10")
        rl.updateRow(row)
      case any =>
        throw new UnsupportedOperationException("shouldn't be here")
    }.run()

    assert(result.isSuccess, result)
    result.toOption.get._1.foreach {
      row =>
        assert(row.getValue("Impressions") === 100)
        assert(row.getValue("Clicks") === 1)
    }
  }
  test("successfully generate fact driven sync multi engine query for druid + oracle with noop when no data from druid") {
    val request: ReportingRequest = getReportingRequestSync(factRequestWithMetricSort)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    assert(result.toOption.get._1.isEmpty)
    assert(pipeline.queryChain.subsequentQueryList.isEmpty)
  }
  test("successfully generate sync multi engine query for druid + oracle with rerun on oracle when no data from druid") {
    val request: ReportingRequest = getReportingRequestSync(requestWithMetricSort)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    result.toOption.get._1.foreach {
      row =>
        assert(row.getValue("Impressions") === 100)
        assert(row.getValue("Clicks") === 1)
    }
    
    val subQuery = pipeline.queryChain.subsequentQueryList.head.asInstanceOf[OracleQuery]
    assert(subQuery.aliasColumnMap.contains("Impressions"))
    assert(subQuery.aliasColumnMap.contains("Clicks"))
    assert(!subQuery.queryContext.requestModel.includeRowCount)

  }
  test("successfully generate sync multi engine query for druid + oracle with rerun on oracle when no data from oracle") {
    val request: ReportingRequest = getReportingRequestSync(requestWithMetricSort)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    result.toOption.get._1.foreach {
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[MultiEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[MultiEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
  }
  test("successfully generate async single engine query") {
    val request: ReportingRequest = getReportingRequestAsync(requestWithMetricSortAndDay)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    result.toOption.get._1.foreach {
      row =>
        assert(row.getValue("Impressions") === 100)
        assert(row.getValue("Clicks") === 1)
    }
  }

  test("successfully generate multiple query pipelines") {

    val request1: ReportingRequest = ReportingRequest.forceOracle(getReportingRequestSync(requestWithMetricSort))
    val registry = getDefaultRegistry()
    val requestModel1 = RequestModel.from(request1, registry)
    assert(requestModel1.isSuccess, requestModel1.errorMessage("Building request model failed"))

    val request2: ReportingRequest = getReportingRequestSync(requestWithIdSort)
    val requestModel2 = RequestModel.from(request2, registry)
    assert(requestModel2.isSuccess, requestModel2.errorMessage("Building request model failed"))

    val queryPipelineTries = queryPipelineFactory.from((requestModel1.get, Some(requestModel2.get)), QueryAttributes.empty)
    assert(queryPipelineTries._1.isSuccess)
    assert(queryPipelineTries._2.get.isSuccess)
  }

  test("successfully generate sync query for fact sort + dim filter where Druid is engine for both fact and dim") {
    val request = getReportingRequestSync(requestWithDruidFactAndDim)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[SingleEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[SingleEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
  }
  test("successfully generate sync query for fact sort + dim filter where Oracle is engine since max lookback is out of Druid dim range") {
    val request = getReportingRequestSync(requestWithOutOfRangeDruidDim)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure, "should fail if In filter with more than allowed limit")
    assert(queryPipelineTry.failed.get.getMessage === "requirement failed: Failed to find best candidate, forceEngine=Some(Oracle), engine disqualifyingSet=Set(Hive), candidates=Set((fact_druid,Druid), (fact_hive,Hive), (fact_oracle,Oracle))")

  }
  test("query with fact IN filter with more than allowed limit should fail if engine is oracle") {
    val request: ReportingRequest = {
      val request = ReportingRequest.forceOracle(getReportingRequestSync(requestWithIdSort))
      request.copy(filterExpressions = request.filterExpressions ++ IndexedSeq(EqualityFilter("Ad Group Status", "ON"), InFilter("Impressions", List("1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70","71","72","73","74","75","76","77","78","79","80","81","82","83","84","85","86","87","88","89","90","91","92","93","94","95","96","97","98","99","100","101","102","103","104","105","106","107","108","109","110","111","112","113","114","115","116","117","118","119","120","121","122","123","124","125","126","127","128","129","130","131","132","133","134","135","136","137","138","139","140","141","142","143","144","145","146","147","148","149","150","151","152","153","154","155","156","157","158","159","160","161","162","163","164","165","166","167","168","169","170","171","172","173","174","175","176","177","178","179","180","181","182","183","184","185","186","187","188","189","190","191","192","193","194","195","196","197","198","199","200","201","202","203","204","205","206","207","208","209","210","211","212","213","214","215","216","217","218","219","220","221","222","223","224","225","226","227","228","229","230","231","232","233","234","235","236","237","238","239","240","241","242","243","244","245","246","247","248","249","250","251","252","253","254","255","256","257","258","259","260","261","262","263","264","265","266","267","268","269","270","271","272","273","274","275","276","277","278","279","280","281","282","283","284","285","286","287","288","289","290","291","292","293","294","295","296","297","298","299","300","301","302","303","304","305","306","307","308","309","310","311","312","313","314","315","316","317","318","319","320","321","322","323","324","325","326","327","328","329","330","331","332","333","334","335","336","337","338","339","340","341","342","343","344","345","346","347","348","349","350","351","352","353","354","355","356","357","358","359","360","361","362","363","364","365","366","367","368","369","370","371","372","373","374","375","376","377","378","379","380","381","382","383","384","385","386","387","388","389","390","391","392","393","394","395","396","397","398","399","400","401","402","403","404","405","406","407","408","409","410","411","412","413","414","415","416","417","418","419","420","421","422","423","424","425","426","427","428","429","430","431","432","433","434","435","436","437","438","439","440","441","442","443","444","445","446","447","448","449","450","451","452","453","454","455","456","457","458","459","460","461","462","463","464","465","466","467","468","469","470","471","472","473","474","475","476","477","478","479","480","481","482","483","484","485","486","487","488","489","490","491","492","493","494","495","496","497","498","499","500","501","502","503","504","505","506","507","508","509","510","511","512","513","514","515","516","517","518","519","520","521","522","523","524","525","526","527","528","529","530","531","532","533","534","535","536","537","538","539","540","541","542","543","544","545","546","547","548","549","550","551","552","553","554","555","556","557","558","559","560","561","562","563","564","565","566","567","568","569","570","571","572","573","574","575","576","577","578","579","580","581","582","583","584","585","586","587","588","589","590","591","592","593","594","595","596","597","598","599","600","601","602","603","604","605","606","607","608","609","610","611","612","613","614","615","616","617","618","619","620","621","622","623","624","625","626","627","628","629","630","631","632","633","634","635","636","637","638","639","640","641","642","643","644","645","646","647","648","649","650","651","652","653","654","655","656","657","658","659","660","661","662","663","664","665","666","667","668","669","670","671","672","673","674","675","676","677","678","679","680","681","682","683","684","685","686","687","688","689","690","691","692","693","694","695","696","697","698","699","700","701","702","703","704","705","706","707","708","709","710","711","712","713","714","715","716","717","718","719","720","721","722","723","724","725","726","727","728","729","730","731","732","733","734","735","736","737","738","739","740","741","742","743","744","745","746","747","748","749","750","751","752","753","754","755","756","757","758","759","760","761","762","763","764","765","766","767","768","769","770","771","772","773","774","775","776","777","778","779","780","781","782","783","784","785","786","787","788","789","790","791","792","793","794","795","796","797","798","799","800","801","802","803","804","805","806","807","808","809","810","811","812","813","814","815","816","817","818","819","820","821","822","823","824","825","826","827","828","829","830","831","832","833","834","835","836","837","838","839","840","841","842","843","844","845","846","847","848","849","850","851","852","853","854","855","856","857","858","859","860","861","862","863","864","865","866","867","868","869","870","871","872","873","874","875","876","877","878","879","880","881","882","883","884","885","886","887","888","889","890","891","892","893","894","895","896","897","898","899","900","901","902","903","904","905","906","907","908","909","910","911","912","913","914","915","916","917","918","919","920","921","922","923","924","925","926","927","928","929","930","931","932","933","934","935","936","937","938","939","940","941","942","943","944","945","946","947","948","949","950","951","952","953","954","955","956","957","958","959","960","961","962","963","964","965","966","967","968","969","970","971","972","973","974","975","976","977","978","979","980","981","982","983","984","985","986","987","988","989","990","991","992","993","994","995","996","997","998","999","1000"))))
    }
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure, "should fail if In filter with more than allowed limit")
    assert(queryPipelineTry.failed.get.getMessage === "requirement failed: Failed to find best candidate, forceEngine=Some(Oracle), engine disqualifyingSet=Set(Druid, Hive), candidates=Set((fact_druid,Druid), (fact_hive,Hive), (fact_oracle,Oracle))")
  }
  test("query with fact IN filter with 1000 values should succeed if engine is druid") {
    val request: ReportingRequest = {
      val request = ReportingRequest.forceDruid(getReportingRequestSync(requestWithIdSort))
      request.copy(filterExpressions = request.filterExpressions ++ IndexedSeq(EqualityFilter("Ad Group Status", "ON"), InFilter("Impressions", List("1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70","71","72","73","74","75","76","77","78","79","80","81","82","83","84","85","86","87","88","89","90","91","92","93","94","95","96","97","98","99","100","101","102","103","104","105","106","107","108","109","110","111","112","113","114","115","116","117","118","119","120","121","122","123","124","125","126","127","128","129","130","131","132","133","134","135","136","137","138","139","140","141","142","143","144","145","146","147","148","149","150","151","152","153","154","155","156","157","158","159","160","161","162","163","164","165","166","167","168","169","170","171","172","173","174","175","176","177","178","179","180","181","182","183","184","185","186","187","188","189","190","191","192","193","194","195","196","197","198","199","200","201","202","203","204","205","206","207","208","209","210","211","212","213","214","215","216","217","218","219","220","221","222","223","224","225","226","227","228","229","230","231","232","233","234","235","236","237","238","239","240","241","242","243","244","245","246","247","248","249","250","251","252","253","254","255","256","257","258","259","260","261","262","263","264","265","266","267","268","269","270","271","272","273","274","275","276","277","278","279","280","281","282","283","284","285","286","287","288","289","290","291","292","293","294","295","296","297","298","299","300","301","302","303","304","305","306","307","308","309","310","311","312","313","314","315","316","317","318","319","320","321","322","323","324","325","326","327","328","329","330","331","332","333","334","335","336","337","338","339","340","341","342","343","344","345","346","347","348","349","350","351","352","353","354","355","356","357","358","359","360","361","362","363","364","365","366","367","368","369","370","371","372","373","374","375","376","377","378","379","380","381","382","383","384","385","386","387","388","389","390","391","392","393","394","395","396","397","398","399","400","401","402","403","404","405","406","407","408","409","410","411","412","413","414","415","416","417","418","419","420","421","422","423","424","425","426","427","428","429","430","431","432","433","434","435","436","437","438","439","440","441","442","443","444","445","446","447","448","449","450","451","452","453","454","455","456","457","458","459","460","461","462","463","464","465","466","467","468","469","470","471","472","473","474","475","476","477","478","479","480","481","482","483","484","485","486","487","488","489","490","491","492","493","494","495","496","497","498","499","500","501","502","503","504","505","506","507","508","509","510","511","512","513","514","515","516","517","518","519","520","521","522","523","524","525","526","527","528","529","530","531","532","533","534","535","536","537","538","539","540","541","542","543","544","545","546","547","548","549","550","551","552","553","554","555","556","557","558","559","560","561","562","563","564","565","566","567","568","569","570","571","572","573","574","575","576","577","578","579","580","581","582","583","584","585","586","587","588","589","590","591","592","593","594","595","596","597","598","599","600","601","602","603","604","605","606","607","608","609","610","611","612","613","614","615","616","617","618","619","620","621","622","623","624","625","626","627","628","629","630","631","632","633","634","635","636","637","638","639","640","641","642","643","644","645","646","647","648","649","650","651","652","653","654","655","656","657","658","659","660","661","662","663","664","665","666","667","668","669","670","671","672","673","674","675","676","677","678","679","680","681","682","683","684","685","686","687","688","689","690","691","692","693","694","695","696","697","698","699","700","701","702","703","704","705","706","707","708","709","710","711","712","713","714","715","716","717","718","719","720","721","722","723","724","725","726","727","728","729","730","731","732","733","734","735","736","737","738","739","740","741","742","743","744","745","746","747","748","749","750","751","752","753","754","755","756","757","758","759","760","761","762","763","764","765","766","767","768","769","770","771","772","773","774","775","776","777","778","779","780","781","782","783","784","785","786","787","788","789","790","791","792","793","794","795","796","797","798","799","800","801","802","803","804","805","806","807","808","809","810","811","812","813","814","815","816","817","818","819","820","821","822","823","824","825","826","827","828","829","830","831","832","833","834","835","836","837","838","839","840","841","842","843","844","845","846","847","848","849","850","851","852","853","854","855","856","857","858","859","860","861","862","863","864","865","866","867","868","869","870","871","872","873","874","875","876","877","878","879","880","881","882","883","884","885","886","887","888","889","890","891","892","893","894","895","896","897","898","899","900","901","902","903","904","905","906","907","908","909","910","911","912","913","914","915","916","917","918","919","920","921","922","923","924","925","926","927","928","929","930","931","932","933","934","935","936","937","938","939","940","941","942","943","944","945","946","947","948","949","950","951","952","953","954","955","956","957","958","959","960","961","962","963","964","965","966","967","968","969","970","971","972","973","974","975","976","977","978","979","980","981","982","983","984","985","986","987","988","989","990","991","992","993","994","995","996","997","998","999","1000"))))
    }
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "should have succeeded")
  }
  test("dim IN filter with more than allowed limit should not be considered in bestDimCandidates if dim engine is oracle") {
    val request: ReportingRequest = {
      val request = getReportingRequestSync(requestWithIdSort)
      request.copy(filterExpressions = request.filterExpressions ++ IndexedSeq(EqualityFilter("Ad Group ID", "1"), InFilter("Ad Group Status", List("1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70","71","72","73","74","75","76","77","78","79","80","81","82","83","84","85","86","87","88","89","90","91","92","93","94","95","96","97","98","99","100","101","102","103","104","105","106","107","108","109","110","111","112","113","114","115","116","117","118","119","120","121","122","123","124","125","126","127","128","129","130","131","132","133","134","135","136","137","138","139","140","141","142","143","144","145","146","147","148","149","150","151","152","153","154","155","156","157","158","159","160","161","162","163","164","165","166","167","168","169","170","171","172","173","174","175","176","177","178","179","180","181","182","183","184","185","186","187","188","189","190","191","192","193","194","195","196","197","198","199","200","201","202","203","204","205","206","207","208","209","210","211","212","213","214","215","216","217","218","219","220","221","222","223","224","225","226","227","228","229","230","231","232","233","234","235","236","237","238","239","240","241","242","243","244","245","246","247","248","249","250","251","252","253","254","255","256","257","258","259","260","261","262","263","264","265","266","267","268","269","270","271","272","273","274","275","276","277","278","279","280","281","282","283","284","285","286","287","288","289","290","291","292","293","294","295","296","297","298","299","300","301","302","303","304","305","306","307","308","309","310","311","312","313","314","315","316","317","318","319","320","321","322","323","324","325","326","327","328","329","330","331","332","333","334","335","336","337","338","339","340","341","342","343","344","345","346","347","348","349","350","351","352","353","354","355","356","357","358","359","360","361","362","363","364","365","366","367","368","369","370","371","372","373","374","375","376","377","378","379","380","381","382","383","384","385","386","387","388","389","390","391","392","393","394","395","396","397","398","399","400","401","402","403","404","405","406","407","408","409","410","411","412","413","414","415","416","417","418","419","420","421","422","423","424","425","426","427","428","429","430","431","432","433","434","435","436","437","438","439","440","441","442","443","444","445","446","447","448","449","450","451","452","453","454","455","456","457","458","459","460","461","462","463","464","465","466","467","468","469","470","471","472","473","474","475","476","477","478","479","480","481","482","483","484","485","486","487","488","489","490","491","492","493","494","495","496","497","498","499","500","501","502","503","504","505","506","507","508","509","510","511","512","513","514","515","516","517","518","519","520","521","522","523","524","525","526","527","528","529","530","531","532","533","534","535","536","537","538","539","540","541","542","543","544","545","546","547","548","549","550","551","552","553","554","555","556","557","558","559","560","561","562","563","564","565","566","567","568","569","570","571","572","573","574","575","576","577","578","579","580","581","582","583","584","585","586","587","588","589","590","591","592","593","594","595","596","597","598","599","600","601","602","603","604","605","606","607","608","609","610","611","612","613","614","615","616","617","618","619","620","621","622","623","624","625","626","627","628","629","630","631","632","633","634","635","636","637","638","639","640","641","642","643","644","645","646","647","648","649","650","651","652","653","654","655","656","657","658","659","660","661","662","663","664","665","666","667","668","669","670","671","672","673","674","675","676","677","678","679","680","681","682","683","684","685","686","687","688","689","690","691","692","693","694","695","696","697","698","699","700","701","702","703","704","705","706","707","708","709","710","711","712","713","714","715","716","717","718","719","720","721","722","723","724","725","726","727","728","729","730","731","732","733","734","735","736","737","738","739","740","741","742","743","744","745","746","747","748","749","750","751","752","753","754","755","756","757","758","759","760","761","762","763","764","765","766","767","768","769","770","771","772","773","774","775","776","777","778","779","780","781","782","783","784","785","786","787","788","789","790","791","792","793","794","795","796","797","798","799","800","801","802","803","804","805","806","807","808","809","810","811","812","813","814","815","816","817","818","819","820","821","822","823","824","825","826","827","828","829","830","831","832","833","834","835","836","837","838","839","840","841","842","843","844","845","846","847","848","849","850","851","852","853","854","855","856","857","858","859","860","861","862","863","864","865","866","867","868","869","870","871","872","873","874","875","876","877","878","879","880","881","882","883","884","885","886","887","888","889","890","891","892","893","894","895","896","897","898","899","900","901","902","903","904","905","906","907","908","909","910","911","912","913","914","915","916","917","918","919","920","921","922","923","924","925","926","927","928","929","930","931","932","933","934","935","936","937","938","939","940","941","942","943","944","945","946","947","948","949","950","951","952","953","954","955","956","957","958","959","960","961","962","963","964","965","966","967","968","969","970","971","972","973","974","975","976","977","978","979","980","981","982","983","984","985","986","987","988","989","990","991","992","993","994","995","996","997","998","999","1000"))))
    }
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess)
    assert(queryPipelineTry.get.queryChain.drivingQuery.engine === DruidEngine)
    assert(queryPipelineTry.get.bestDimCandidates.isEmpty)

  }
  test("query with OR filter should fail if engine is oracle") {
    val request: ReportingRequest = {
      val request = ReportingRequest.forceOracle(getReportingRequestSync(requestWithIdSort))
      request.copy(filterExpressions = request.filterExpressions ++ IndexedSeq(OrFliter(List(InFilter("Impressions", List("1"))))))
    }
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure, "should fail if In filter with more than allowed limit")
    assert(queryPipelineTry.failed.get.getMessage === "requirement failed: Failed to find best candidate, forceEngine=Some(Oracle), engine disqualifyingSet=Set(Hive), candidates=Set((fact_druid,Druid), (fact_hive,Hive), (fact_oracle,Oracle))")
  }
  test("query with OR filter should fail if engine is Hive") {
    val request: ReportingRequest = {
      val request = ReportingRequest.forceHive(getReportingRequestAsync(requestWithIdSort))
      request.copy(filterExpressions = request.filterExpressions ++ IndexedSeq(OrFliter(List(InFilter("Impressions", List("1"))))))
    }
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure, "should fail if In filter with more than allowed limit")
    assert(queryPipelineTry.failed.get.getMessage === "requirement failed: Failed to find best candidate, forceEngine=Some(Hive), engine disqualifyingSet=Set(Druid), candidates=Set((fact_druid,Druid), (fact_hive,Hive), (fact_oracle,Oracle))")
  }

  test("Validate subsequent Oracle + Druid query generation to dim.")
  {
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, new DruidQueryGenerator(new SyncDruidQueryOptimizer(), 40000)) //do not include local time filter
    altQueryGeneratorRegistry.register(OracleEngine, new OracleQueryGenerator(DefaultPartitionColumnRenderer))
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)
    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val query =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]]
    println(query.asString)
    val result = queryPipelineTry.toOption.get.withDruidCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Ad Group ID", 10)
        row.addValue("Impressions", 100)
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
        rl.addRow(row)
    }.run()

    assert(result.isSuccess, result)
    val oracleQuery = queryPipelineTry.toOption.get.queryChain.subsequentQueryList.head
  }
}
