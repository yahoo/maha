// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core._
import com.yahoo.maha.core.query.druid.{DruidQuery, DruidQueryGenerator}
import com.yahoo.maha.core.query.hive.HiveQueryGenerator
import com.yahoo.maha.core.query.oracle.OracleQueryGenerator
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.core.{EqualityFilter, BetweenFilter, DefaultPartitionColumnRenderer, RequestModel}
import com.yahoo.maha.executor.{MockHiveQueryExecutor, MockOracleQueryExecutor, MockDruidQueryExecutor}
import org.scalatest.{BeforeAndAfterAll, Matchers, FunSuite}

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

  implicit private[this] val queryExecutionContext = new QueryExecutorContext
  
  import DefaultQueryPipelineFactoryTest._
  override protected def beforeAll(): Unit = {
    OracleQueryGenerator.register(queryGeneratorRegistry, DefaultPartitionColumnRenderer)
    DruidQueryGenerator.register(queryGeneratorRegistry)
    HiveQueryGenerator.register(queryGeneratorRegistry, DefaultPartitionColumnRenderer, TestUDFRegistrationFactory())
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
}
