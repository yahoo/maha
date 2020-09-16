// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core.bucketing.BucketParams
import com.yahoo.maha.core.query.druid.{DruidQueryGenerator, SyncDruidQueryOptimizer}
import com.yahoo.maha.core.query.hive.HiveQueryGenerator
import com.yahoo.maha.core.query.oracle.OracleQueryGenerator
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.core.{DefaultPartitionColumnRenderer, RequestModel, _}
import com.yahoo.maha.executor.{MockDruidQueryExecutor, MockHiveQueryExecutor, MockOracleQueryExecutor}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import scala.util.Try

/**
  * Created by ryanwagner on 2017/12/5
  */

object QueryPipelineWithFallbackTest {
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
    def run(queryAttributes: QueryAttributes = QueryAttributes.empty) : Try[QueryPipelineResult] = {
      pipeline.execute(queryExecutorContext, queryAttributes)
    }
  }

}

class QueryPipelineWithFallbackTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with BaseQueryGeneratorTest with SharedDimSchema with BaseQueryContextTest {
  private[this] def getDruidQueryGenerator() : DruidQueryGenerator = {
    new DruidQueryGenerator(new SyncDruidQueryOptimizer(), 40000)
  }

  private[this] def getDruidQueryExecutor() : MockDruidQueryExecutor = {
    new MockDruidQueryExecutor({
      rl =>
        val row = rl.newRow
        row.addValue("Advertiser ID", 14)
        row.addValue("Advertiser Status", "ON")
        row.addValue("Impressions", 10)
        rl.addRow(row)
        val row2 = rl.newRow
        row2.addValue("Advertiser ID", 13)
        row2.addValue("Advertiser Status", "ON")
        row2.addValue("Impressions", 20)
        rl.addRow(row2)
    })
  }

  implicit private[this] val queryExecutionContext = new QueryExecutorContext

  override protected def beforeAll(): Unit = {
    OracleQueryGenerator.register(queryGeneratorRegistry, DefaultPartitionColumnRenderer)
    DruidQueryGenerator.register(queryGeneratorRegistry, useCustomRoundingSumAggregator = true)
    HiveQueryGenerator.register(queryGeneratorRegistry, DefaultPartitionColumnRenderer, TestUDFRegistrationFactory())
  }

  test("successfully generate Druid single engine query with Dim lookup") {

    val jsonRequest =
      s"""{
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

    val request: ReportingRequest = getReportingRequestAsync(jsonRequest)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    altQueryGeneratorRegistry.register(OracleEngine, new OracleQueryGenerator(DefaultPartitionColumnRenderer))
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)

    val queryPipelineTry = queryPipelineFactoryLocal.from(requestModel.toOption.get, QueryAttributes.empty)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val oracleExecutor = new MockOracleQueryExecutor(
      { rl =>
        //println(rl.query.asString)
        val expected =
          s"""
             |SELECT *
             |FROM (SELECT to_char(ffst0.stats_date, 'YYYYMMdd') "Day", to_char(ffst0.id) "Keyword ID", coalesce(ffst0."impresssions", 1) "Impressions", ao1."Advertiser Status" "Advertiser Status"
             |      FROM (SELECT
             |                   advertiser_id, id, stats_date, SUM(impresssions) AS "impresssions"
             |            FROM fd_fact_s_term FactAlias
             |            WHERE (advertiser_id = 5485) AND (stats_date IN (to_date('${fromDate}', 'YYYYMMdd'),to_date('${toDate}', 'YYYYMMdd')))
             |            GROUP BY advertiser_id, id, stats_date
             |
            |           ) ffst0
             |           LEFT OUTER JOIN
             |           (SELECT  DECODE(status, 'ON', 'ON', 'OFF') AS "Advertiser Status", id
             |            FROM advertiser_oracle
             |            WHERE (id = 5485)
             |             )
             |           ao1 ON (ffst0.advertiser_id = ao1.id)
             |
            |)
             |   ORDER BY "Impressions" ASC NULLS LAST""".stripMargin

        rl.query.asString should equal (expected) (after being whiteSpaceNormalised)

        val row = rl.newRow
        row.addValue("Keyword ID", 14)
        row.addValue("Advertiser Status", "ON")
        row.addValue("Impressions", 10)
        rl.addRow(row)
        val row2 = rl.newRow
        row2.addValue("Keyword ID", 13)
        row2.addValue("Advertiser Status", "ON")
        row2.addValue("Impressions", 20)
        rl.addRow(row2)
      })

    val queryExecContext: QueryExecutorContext = new QueryExecutorContext
    queryExecContext.register(getDruidQueryExecutor())
    queryExecContext.register(oracleExecutor)

    val queryChain = queryPipelineTry.toOption.get.queryChain
    val factBest = queryPipelineTry.toOption.get.factBestCandidate
    val dimBest = queryPipelineTry.toOption.get.bestDimCandidates
    val newPipelineBuilder = new QueryPipelineBuilder(queryChain, factBest, dimBest, true).withFallbackQueryChain(queryPipelineTry.toOption.get.queryChain)
    val newQuery = newPipelineBuilder.build()

    val result = newQuery.execute(queryExecContext)
    val secondResult = newQuery.execute(queryExecContext, QueryAttributes.empty)
    assert(result.isSuccess, "Query execution failed")
    assert(secondResult.isSuccess, "Second query execution failed")
    assert(!newQuery.execute(new QueryExecutorContext).isSuccess, "Empty context should return a failure")
    assert(!newQuery.execute(new QueryExecutorContext, QueryAttributes.empty).isSuccess, "Empty context should return a failure")


  }

  test("Discard engines in disqualify set for fallback query" ) {

    val jsonRequest =
      s"""{
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

    val request: ReportingRequest = getReportingRequestAsync(jsonRequest)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val altQueryGeneratorRegistry = new QueryGeneratorRegistry
    altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator()) //do not include local time filter
    altQueryGeneratorRegistry.register(OracleEngine, new OracleQueryGenerator(DefaultPartitionColumnRenderer))
    altQueryGeneratorRegistry.register(HiveEngine, new HiveQueryGenerator(DefaultPartitionColumnRenderer, Set.empty))
    val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory()(altQueryGeneratorRegistry)

    var builder = queryPipelineFactoryLocal.builder(requestModel.toOption.get, QueryAttributes.empty, None, BucketParams(), Set.empty)
    var pipeline = builder._1.toOption.get.build()
    assert(pipeline.isInstanceOf[QueryPipelineWithFallback])
    assert(pipeline.fallbackQueryChainOption.isDefined, "Expected fallback query to be present")
    var fallbackEngine = pipeline.fallbackQueryChainOption.get.drivingQuery.engine
    assert(OracleEngine.equals(fallbackEngine), s"Expected engine: Oracle, Actual: $fallbackEngine")

    builder  = queryPipelineFactoryLocal.builder(requestModel.toOption.get, QueryAttributes.empty, None, BucketParams(), Set(OracleEngine))
    pipeline = builder._1.toOption.get.build()
    assert(pipeline.isInstanceOf[QueryPipelineWithFallback])
    assert(pipeline.fallbackQueryChainOption.isDefined, "Expected fallback query to be present")
    fallbackEngine = pipeline.fallbackQueryChainOption.get.drivingQuery.engine
    assert(HiveEngine.equals(fallbackEngine), s"Expected engine: Hive, Actual: $fallbackEngine")


    builder  = queryPipelineFactoryLocal.builder(requestModel.toOption.get, QueryAttributes.empty, None, BucketParams(), Set(OracleEngine, HiveEngine))
    pipeline = builder._1.toOption.get.build()
    assert(pipeline.fallbackQueryChainOption.isEmpty, s"No fallback query expected: $pipeline")
  }
}
