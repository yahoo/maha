// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core._
import com.yahoo.maha.core.query.druid.{DruidQuery, DruidQueryGenerator}
import com.yahoo.maha.core.query.hive.HiveQueryGenerator
import com.yahoo.maha.core.query.oracle.OracleQueryGenerator
import com.yahoo.maha.core.request.{QueryEngineValue, Parameter, ReportingRequest}
import org.scalatest.{BeforeAndAfterAll, Matchers, FunSuite}

import scala.collection.SortedSet

/**
 * Created by hiral on 11/16/15.
 */
class QueryContextTest extends FunSuite with Matchers with BeforeAndAfterAll with BaseQueryGeneratorTest with SharedDimSchema with BaseQueryContextTest {

  override protected def beforeAll(): Unit = {
    OracleQueryGenerator.register(queryGeneratorRegistry, DefaultPartitionColumnRenderer)
    HiveQueryGenerator.register(queryGeneratorRegistry, DefaultPartitionColumnRenderer, TestUDFRegistrationFactory())
    DruidQueryGenerator.register(queryGeneratorRegistry)
  }

  private def getOracleQuery(qc: QueryContext) : OracleQuery = {
    queryGeneratorRegistry.getGenerator(OracleEngine).get.asInstanceOf[OracleQueryGenerator].generate(qc).asInstanceOf[OracleQuery]
  }

  private def getHiveQuery(qc: QueryContext) : HiveQuery = {
    queryGeneratorRegistry.getGenerator(HiveEngine).get.asInstanceOf[HiveQueryGenerator].generate(qc).asInstanceOf[HiveQuery]
  }

  private def getDruidQuery(qc: QueryContext) : DruidQuery[_] = {
    queryGeneratorRegistry.getGenerator(DruidEngine).get.asInstanceOf[DruidQueryGenerator].generate(qc).asInstanceOf[DruidQuery[_]]
  }


  test("dim only query should generate full SQL") {
    val jsonString = dimOnlyQueryJson

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    //test with oracle
    {
      val builder = new QueryContextBuilder(DimOnlyQuery, requestModel.get)
      val dimMapping = DefaultQueryPipelineFactory.findDimCandidatesMapping(requestModel.get)
      val dims = DefaultQueryPipelineFactory.findBestDimCandidates(OracleEngine, requestModel.get.schema, dimMapping)
      builder.addDimTable(dims)
      val result = getOracleQuery(builder.build()).asString
      assert(result.contains("""SELECT /*+ CampaignHint */ DECODE(status, 'ON', 'ON', 'OFF') AS "Campaign Status", id"""))
    }
    
  }

  test("dim only query with no dim should fail") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Campaign Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Campaign Status", "order": "ASC"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    intercept[IllegalArgumentException] {
      val builder = new QueryContextBuilder(DimOnlyQuery, requestModel.get)
      builder.build()
    }
  }

  test("fact only query should generate full SQL") {
    val jsonString = factOnlyQueryJson

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    //test with oracle
    {
      val builder = new QueryContextBuilder(FactOnlyQuery, requestModel.get)
      val fact = DefaultQueryPipelineFactory.findBestFactCandidate(requestModel.get, dimEngines = Set(OracleEngine), queryGeneratorRegistry = queryGeneratorRegistry)
      builder.addFactBestCandidate(fact)
      val result = getOracleQuery(builder.build()).asString
      println(s"Fact Query Result:\n$result")
      assert(result.contains("""landing_page_url, stats_date, CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END price_type, ad_group_id, stats_source, advertiser_id, campaign_id, SUM(CASE WHEN ((clicks >= 1) AND (clicks <= 800)) THEN clicks ELSE 0 END) AS "clicks", SUM(impressions) AS "impressions""""))
    }

    //test with hive
    {
      val builder = new QueryContextBuilder(FactOnlyQuery, requestModel.get)
      val fact = DefaultQueryPipelineFactory.findBestFactCandidate(requestModel.get, dimEngines = Set(HiveEngine), queryGeneratorRegistry = queryGeneratorRegistry)
      builder.addFactBestCandidate(fact)
      val result = getHiveQuery(builder.build()).asString
      assert(result.contains("""SELECT CONCAT_WS(",",NVL(mang_day, ''), NVL(advertiser_id, ''), NVL(campaign_id, ''), NVL(ad_group_id, ''), NVL(mang_source, ''), NVL(mang_pricing_type, ''), NVL(mang_destination_url, ''), NVL(mang_impressions, ''), NVL(mang_clicks, ''))"""))
    }

    //test with druid
    {
      val builder = new QueryContextBuilder(FactOnlyQuery, requestModel.get)
      val fact = DefaultQueryPipelineFactory.findBestFactCandidate(requestModel.get, dimEngines = Set(DruidEngine), queryGeneratorRegistry = queryGeneratorRegistry)
      builder.addFactBestCandidate(fact)
      val result = getDruidQuery(builder.build()).asString
      assert(result.contains("""{"queryType":"groupBy","dataSource":{"type":"table","name":"fact_druid"},"""))
      assert(result.contains("""{"type":"selector","dimension":"advertiser_id","value":"213"}"""))
      assert(result.contains("""granularity":{"type":"all"""))
      assert(result.contains("""dimensions":[{"type":"default","dimension":"landing_page_url","outputName":"Destination URL"},{"type":"default","dimension":"stats_date","outputName":"Day"},{"type":"extraction","dimension":"price_type","outputName":"Pricing Type","extractionFn":{"type":"lookup","lookup":{"type":"map","map":{"-10":"CPE","-20":"CPF","6":"CPV","1":"CPC","2":"CPA","7":"CPCV","3":"CPM"},"isOneToOne":false},"retainMissingValue":false,"replaceMissingValueWith":"NONE","injective":false,"optimize":true}},{"type":"default","dimension":"ad_group_id","outputName":"Ad Group ID"},{"type":"default","dimension":"stats_source","outputName":"Source"},{"type":"default","dimension":"advertiser_id","outputName":"Advertiser ID"},{"type":"default","dimension":"campaign_id","outputName":"Campaign ID"}],"aggregations":[{"type":"longSum","name":"Clicks","fieldName":"clicks"},{"type":"longSum","name":"Impressions","fieldName":"impressions"}],"postAggregations":[],"limitSpec":{"type":"default","columns":[{"dimension":"Clicks","direction":"ascending","dimensionOrder":{"type":"numeric"}}],"limit":100},"context":{"""))
    }
  }

  test("fact only query with no fact should fail") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Day"},
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
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
                              {"field": "Clicks", "order": "ASC"}
                          ],
                          "forceDimensionDriven": false,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))
    intercept[IllegalArgumentException] {
      val builder = new QueryContextBuilder(FactOnlyQuery, requestModel.get)
      builder.build()
    }
  }

  test("fact only query with dim should fail") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Day"},
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
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
                              {"field": "Clicks", "order": "ASC"}
                          ],
                          "forceDimensionDriven": false,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))
    intercept[IllegalArgumentException] {
      val builder = new QueryContextBuilder(FactOnlyQuery, requestModel.get)
      builder.addDimTable(SortedSet.empty[DimensionBundle])
      builder.build()
    }
  }

  test("dim fact dim driven query should generate full SQL") {
    val jsonString = combinedQueryJson

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    //test with oracle only
    {
      val builder = new QueryContextBuilder(DimFactQuery, requestModel.get.copy(additionalParameters = Map(Parameter.QueryEngine -> QueryEngineValue(OracleEngine))))
      val fact = DefaultQueryPipelineFactory.findBestFactCandidate(requestModel.get, dimEngines = Set(OracleEngine), queryGeneratorRegistry = queryGeneratorRegistry)
      val dimMapping = DefaultQueryPipelineFactory.findDimCandidatesMapping(requestModel.get)
      val dims = DefaultQueryPipelineFactory.findBestDimCandidates(OracleEngine, requestModel.get.schema, dimMapping)
      builder.addDimTable(dims)
      builder.addFactBestCandidate(fact)
      val result = getOracleQuery(builder.build()).asString
      assert(result.contains("""landing_page_url, stats_date, CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END price_type, ad_group_id, stats_source, advertiser_id, SUM(CASE WHEN ((clicks >= 1) AND (clicks <= 800)) THEN clicks ELSE 0 END) AS "clicks", SUM(impressions) AS "impressions""""))
    }

    //test with oracle dim only
    {
      val builder = new QueryContextBuilder(DimOnlyQuery, requestModel.get)
      val dimMapping = DefaultQueryPipelineFactory.findDimCandidatesMapping(requestModel.get)
      val dims = DefaultQueryPipelineFactory.findBestDimCandidates(OracleEngine, requestModel.get.schema, dimMapping)
      builder.addDimTable(dims)
      val result = getOracleQuery(builder.build()).asString
      assert(result.contains("""SELECT  DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", advertiser_id, id"""))
    }

    //test with druid fact only
    {
      val model = requestModel.get.copy(additionalParameters = Map(Parameter.QueryEngine -> QueryEngineValue(DruidEngine)))
      val builder = new QueryContextBuilder(FactOnlyQuery, model)
      val fact = DefaultQueryPipelineFactory.findBestFactCandidate(model, dimEngines = Set(DruidEngine), queryGeneratorRegistry = queryGeneratorRegistry)
      builder.addFactBestCandidate(fact)
      val result = getDruidQuery(builder.build()).asString
      assert(result.contains("""{"queryType":"groupBy","dataSource":{"type":"table","name":"fact_druid"},"""))
      assert(result.contains("""{"type":"selector","dimension":"advertiser_id","value":"213"}"""))
      assert(result.contains("""granularity":{"type":"all"""))
      assert(result.contains("""dimensions":[{"type":"default","dimension":"landing_page_url","outputName":"Destination URL"},{"type":"default","dimension":"stats_date","outputName":"Day"},{"type":"extraction","dimension":"price_type","outputName":"Pricing Type","extractionFn":{"type":"lookup","lookup":{"type":"map","map":{"-10":"CPE","-20":"CPF","6":"CPV","1":"CPC","2":"CPA","7":"CPCV","3":"CPM"},"isOneToOne":false},"retainMissingValue":false,"replaceMissingValueWith":"NONE","injective":false,"optimize":true}},{"type":"default","dimension":"ad_group_id","outputName":"Ad Group ID"},{"type":"default","dimension":"stats_source","outputName":"Source"},{"type":"default","dimension":"advertiser_id","outputName":"Advertiser ID"}],"aggregations":[{"type":"longSum","name":"Clicks","fieldName":"clicks"},{"type":"longSum","name":"Impressions","fieldName":"impressions"}],"postAggregations":[],"limitSpec":{"type":"default","columns":[{"dimension":"Impressions","direction":"ascending","dimensionOrder":{"type":"numeric"}}],"limit":100},"context":{"""))
    }
  }

  test("dim fact fact driven query should generate full SQL") {
    val jsonString = s"""{
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
                              {"field": "Clicks", "order": "ASC"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    //test with oracle only
    {
      val builder = new QueryContextBuilder(DimFactQuery, requestModel.get)
      val fact = DefaultQueryPipelineFactory.findBestFactCandidate(requestModel.get, dimEngines = Set(OracleEngine), queryGeneratorRegistry = queryGeneratorRegistry)
      val dimMapping = DefaultQueryPipelineFactory.findDimCandidatesMapping(requestModel.get)
      val dims = DefaultQueryPipelineFactory.findBestDimCandidates(OracleEngine, requestModel.get.schema, dimMapping)
      builder.addDimTable(dims)
      builder.addFactBestCandidate(fact)
      val result = getOracleQuery(builder.build()).asString
      assert(result.contains("""landing_page_url, stats_date, CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END price_type, ad_group_id, stats_source, advertiser_id, SUM(CASE WHEN ((clicks >= 1) AND (clicks <= 800)) THEN clicks ELSE 0 END) AS "clicks", SUM(impressions) AS "impressions""""))
    }

    //test with hive only
    {
      val builder = new QueryContextBuilder(DimFactQuery, requestModel.get)
      val fact = DefaultQueryPipelineFactory.findBestFactCandidate(requestModel.get, dimEngines = Set(HiveEngine), queryGeneratorRegistry = queryGeneratorRegistry)
      val dimMapping = DefaultQueryPipelineFactory.findDimCandidatesMapping(requestModel.get)
      val dims = DefaultQueryPipelineFactory.findBestDimCandidates(HiveEngine, requestModel.get.schema, dimMapping)
      builder.addDimTable(dims)
      builder.addFactBestCandidate(fact)
      val result = getHiveQuery(builder.build()).asString
      println(s"Result:\n$result")
      assert(result.contains("""SELECT CONCAT_WS(",",NVL(mang_day, ''), NVL(advertiser_id, ''), NVL(mang_ad_group_status, ''), NVL(ag1_id, ''), NVL(mang_source, ''), NVL(mang_pricing_type, ''), NVL(mang_destination_url, ''), NVL(mang_impressions, ''), NVL(mang_clicks, ''))"""))
    }

    //test with oracle dim only
    {
      val builder = new QueryContextBuilder(DimOnlyQuery, requestModel.get)
      val dimMapping = DefaultQueryPipelineFactory.findDimCandidatesMapping(requestModel.get)
      val dims = DefaultQueryPipelineFactory.findBestDimCandidates(OracleEngine, requestModel.get.schema, dimMapping)
      builder.addDimTable(dims)
      val result = getOracleQuery(builder.build()).asString
      assert(result.contains("""SELECT  DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", advertiser_id, id"""))
    }

    //test with druid fact only
    {
      val builder = new QueryContextBuilder(FactOnlyQuery, requestModel.get)
      val fact = DefaultQueryPipelineFactory.findBestFactCandidate(requestModel.get, dimEngines = Set(DruidEngine), queryGeneratorRegistry = queryGeneratorRegistry)
      builder.addFactBestCandidate(fact)
      val result = getDruidQuery(builder.build()).asString
      assert(result.contains("""{"queryType":"groupBy","dataSource":{"type":"table","name":"fact_druid"},"""))
      assert(result.contains("""{"type":"selector","dimension":"advertiser_id","value":"213"}"""))
      assert(result.contains("""granularity":{"type":"all"""))
      assert(result.contains("""dimensions":[{"type":"default","dimension":"landing_page_url","outputName":"Destination URL"},{"type":"default","dimension":"stats_date","outputName":"Day"},{"type":"extraction","dimension":"price_type","outputName":"Pricing Type","extractionFn":{"type":"lookup","lookup":{"type":"map","map":{"-10":"CPE","-20":"CPF","6":"CPV","1":"CPC","2":"CPA","7":"CPCV","3":"CPM"},"isOneToOne":false},"retainMissingValue":false,"replaceMissingValueWith":"NONE","injective":false,"optimize":true}},{"type":"default","dimension":"ad_group_id","outputName":"Ad Group ID"},{"type":"default","dimension":"stats_source","outputName":"Source"},{"type":"default","dimension":"advertiser_id","outputName":"Advertiser ID"}],"aggregations":[{"type":"longSum","name":"Clicks","fieldName":"clicks"},{"type":"longSum","name":"Impressions","fieldName":"impressions"}],"postAggregations":[],"limitSpec":{"type":"default","columns":[{"dimension":"Clicks","direction":"ascending","dimensionOrder":{"type":"numeric"}}],"limit":100},"context":{"""))
    }
  }

  test("dim fact query with no fact should fail") {
    val jsonString = s"""{
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
                              {"field": "Clicks", "order": "ASC"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))
    intercept[IllegalArgumentException] {
      val builder = new QueryContextBuilder(DimFactQuery, requestModel.get)
      val dimMapping = DefaultQueryPipelineFactory.findDimCandidatesMapping(requestModel.get)
      val dims = DefaultQueryPipelineFactory.findBestDimCandidates(OracleEngine, requestModel.get.schema, dimMapping)
      builder.addDimTable(dims)
      builder.build()
    }
  }

  test("Dim Only Query Context test") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Campaign Name"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))
    val builder = new QueryContextBuilder(DimOnlyQuery, requestModel.get)
    val dimMapping = DefaultQueryPipelineFactory.findDimCandidatesMapping(requestModel.get)
    val dims = DefaultQueryPipelineFactory.findBestDimCandidates(OracleEngine, requestModel.get.schema, dimMapping)
    builder.addDimTable(dims)
    require(builder.dims.map(d=> d.dim.name).contains("campaign_oracle"))

    val queryContext = builder.build()
    require(queryContext.isInstanceOf[DimensionQueryContext])
    require(queryContext.asInstanceOf[DimensionQueryContext].dims.size == 1)
    require(queryContext.primaryTableName == "campaign_oracle")
  }

  test("FactualQuery Context test") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Clicks"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))
    val builder = new QueryContextBuilder(FactOnlyQuery, requestModel.get)
    val bestFact = DefaultQueryPipelineFactory.findBestFactCandidate(requestModel.get, Set.empty, Set(OracleEngine, DruidEngine), queryGeneratorRegistry = queryGeneratorRegistry)
    builder.addFactBestCandidate(bestFact)
    val queryContext = builder.build()
    require(queryContext.isInstanceOf[FactualQueryContext])
    require(queryContext.asInstanceOf[FactualQueryContext].factBestCandidate.fact.name ==  "fact_druid")
  }

}
