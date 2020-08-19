// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.oracle

import java.nio.charset.StandardCharsets

import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core._
import com.yahoo.maha.core.fact.Fact.ViewTable
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.query.druid.DruidQuery
import com.yahoo.maha.core.request._
import com.yahoo.maha.executor.{MockDruidQueryExecutor, MockOracleQueryExecutor}


/**
 * Created by jians on 11/12/15.
 */
class OracleQueryGeneratorTest extends BaseOracleQueryGeneratorTest {
  lazy val defaultRegistry = getDefaultRegistry()

  test("registering Oracle query generation multiple times should fail") {
    intercept[IllegalArgumentException] {
      val dummyQueryGenerator = new QueryGenerator[WithOracleEngine] {
        override def generate(queryContext: QueryContext): Query = { null }
        override def engine: Engine = OracleEngine
      }
      queryGeneratorRegistry.register(OracleEngine, dummyQueryGenerator)
    }
  }

  test("dim fact sync fact driven query should produce all requested fields in same order as in request") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_wo_dim_filters.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val select = """SELECT f0.campaign_id "Campaign ID", coalesce(f0."impressions", 1) "Impressions", ao1.name "Advertiser Name", ao1."Advertiser Status" "Advertiser Status", Count(*) OVER() TOTALROWS"""
    assert(result.contains(select), result)
  }

  test("dim fact sync fact driven query with multiple dim join should produce all requested fields in same order as in request") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_with_multi_dim_join.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val select = """SELECT co2.id "Campaign ID", coalesce(f0."impressions", 1) "Impressions", ao1.name "Advertiser Name", co2."Campaign Status" "Campaign Status", Count(*) OVER() TOTALROWS"""
    assert(result.contains(select), result)
  }

  test("dim fact async fact driven query should produce all requested fields in same order as in request") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_wo_dim_filters.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val select = """SELECT f0.campaign_id "Campaign ID", coalesce(f0."impressions", 1) "Impressions", ao1.name "Advertiser Name", ao1."Advertiser Status" "Advertiser Status""""
    assert(result.contains(select), result)
  }

  test("dim fact sync dimension driven query should produce all requested fields in same order as in request with in Subquery Clause") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_dim_driven_wo_dim_filters.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val select = """SELECT ago1.campaign_id "Campaign ID", coalesce(f0."impressions", 1) "Impressions", ago1."Ad Group Status" "Ad Group Status""""
    assert(result.contains(select), result)
    assert(result.contains("campaign_id IN (SELECT id FROM campaign_oracle WHERE (DECODE(status, 'ON', 'ON', 'OFF') IN ('ON'))"),result)
  }

  test("dim fact async fact driven query with dim filters should use INNER JOIN") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_w_dim_filters.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("INNER JOIN"), "Query should use INNER JOIN if requested dim filters")

  }

  test("dim fact async fact driven query with dim filters should use INNER JOIN and use new partitioning scheme") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_w_dim_filters_new_part.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("INNER JOIN"), "Query should use INNER JOIN if requested dim filters")

  }

  test("dim fact async fact driven query without dim filters should use LEFT OUTER JOIN") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_wo_dim_filters.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, s"Fail to get the query pipeline, $queryPipelineTry")
    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("LEFT OUTER JOIN"), "Query should use JOIN")
  }

  test("dim fact sync dimension driven query should use RIGHT OUTER JOIN") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_dim_driven_total_rows.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("RIGHT OUTER JOIN"), "Query should use RIGHT OUTER JOIN")
    assert(result.contains("TOTALROWS"), "Query should have total row column")
    assert(result.contains("ROW_NUMBER"), "Query should have pagination wrapper")
  }

  test("dim fact sync dimension driven query without total rows should use RIGHT OUTER JOIN") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_dim_driven_wo_dim_filters.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("RIGHT OUTER JOIN"), "Query should use RIGHT OUTER JOIN")
    assert(!result.contains("TOTALROWS"), "Query should not have total row column")
    assert(result.contains("ROW_NUMBER"), "Query should have pagination wrapper")
    assert(result.contains("ROW_NUMBER >= 21"), "Min position should be 21")
    assert(result.contains("ROW_NUMBER <= 120"), "Max position should be 120")
  }

  test("dim fact async fact driven query without dim filters should use LEFT OUTER JOIN and has no pagination") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_wo_dim_filters.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("LEFT OUTER JOIN"), "Query should use LEFT OUTER JOIN")
    assert(!result.contains("ROW_NUMBER"), "Query should not have pagination")
  }

  test("dim fact async fact driven query with dim filters should use INNER JOIN and has no pagination") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_w_dim_filters.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("INNER JOIN"), "Query should use INNER JOIN if requested dim filters")
    assert(!result.contains("ROW_NUMBER"), "Query should not have pagination")
  }

  test("dim fact async fact driven query without dim sort should use LEFT OUTER JOIN and has no pagination") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_wo_dim_sort.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("LEFT OUTER JOIN"), "Query should use LEFT OUTER JOIN")
    assert(!result.contains("ROW_NUMBER"), "Query should not have pagination")
  }

  test("dim fact async fact driven query with dim sort should use JOIN and has no pagination") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_w_dim_sort.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("LEFT OUTER JOIN"), "Query should use LEFT OUTER JOIN")
    assert(!result.contains("ROW_NUMBER"), "Query should not have pagination")
  }

  test("dim fact sync fact driven with fact column sort with total rows should use LEFT OUTER JOIN with pagination and total row column") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_w_fact_sort_total_rows.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("LEFT OUTER JOIN"), "Query should use LEFT OUTER JOIN")
    assert(result.contains("ROW_NUMBER"), "Query should have pagination")
    assert(result.contains("TOTALROWS"), "Query should have total row column")
  }

  test("dim fact sync fact driven with fact column sort without total rows should use LEFT OUTER JOIN with pagination") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_w_fact_sort.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("LEFT OUTER JOIN"), "Query should use LEFT OUTER JOIN")
    assert(result.contains("ROW_NUMBER"), "Query should have pagination")
    assert(result.contains("ROW_NUMBER >= 21"), "Min position should be 21")
    assert(result.contains("ROW_NUMBER <= 120"), "Max position should be 120")
    assert(!result.contains("TOTALROWS"), "Query should not have total row column")
  }

  test("dim fact sync fact driven with fact column filter with total rows should use LEFT OUTER JOIN with pagination and total row column"){
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_w_fact_filter_total_rows.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("LEFT OUTER JOIN"), "Query should use LEFT OUTER JOIN")
    assert(result.contains("ROW_NUMBER"), "Query should have pagination")
    assert(result.contains("TOTALROWS"), "Query should have total row column")
  }

  test("dim fact sync fact driven with fact column filter without total rows should use LEFT OUTER JOIN with pagination"){
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_w_fact_filter.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("LEFT OUTER JOIN"), "Query should use LEFT OUTER JOIN")
    assert(result.contains("ROW_NUMBER"), "Query should have pagination")
    assert(result.contains("ROW_NUMBER >= 21"), "Min position should be 21")
    assert(result.contains("ROW_NUMBER <= 120"), "Max position should be 120")
    assert(!result.contains("TOTALROWS"), "Query should not have total row column")
  }

  test("dim fact sync fact driven query with int static mapped fields and filters should succeed") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_w_static_mapping.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))
    assert(requestModel.toOption.get.isFactDriven)


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("LEFT OUTER JOIN"), "Query should use LEFT OUTER JOIN")
    assert(result.contains("ROW_NUMBER"), "Query should have pagination")
    assert(result.contains("ROW_NUMBER >= 21"), "Min position should be 21")
    assert(result.contains("ROW_NUMBER <= 120"), "Max position should be 120")
    assert(!result.contains("TOTALROWS"), "Query should not have total row column")
    assert(result.contains("pricing_type IN (-10,2)"), "Query should contain filter on price_type")
    val pricingTypeInnerColum = """CASE WHEN (pricing_type IN (1)) THEN 'CPC' WHEN (pricing_type IN (6)) THEN 'CPV' WHEN (pricing_type IN (2)) THEN 'CPA' WHEN (pricing_type IN (-10)) THEN 'CPE' WHEN (pricing_type IN (-20)) THEN 'CPF' WHEN (pricing_type IN (7)) THEN 'CPCV' WHEN (pricing_type IN (3)) THEN 'CPM' ELSE 'NONE' END pricing_type"""
    assert(result.contains(pricingTypeInnerColum), "Query should contain case when for Pricing Type")
  }

  test("dim fact sync fact driven query with default value fields should be in applied in inner select columns") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_w_default_value.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))
    assert(requestModel.toOption.get.isFactDriven)


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("LEFT OUTER JOIN"), "Query should use LEFT OUTER JOIN")
    assert(result.contains("ROW_NUMBER"), "Query should have pagination")
    assert(result.contains("ROW_NUMBER >= 21"), "Min position should be 21")
    assert(result.contains("ROW_NUMBER <= 120"), "Max position should be 120")
    assert(!result.contains("TOTALROWS"), "Query should not have total row column")
    assert(result.contains("coalesce(f0.\"impressions\", 1)"), "Query should contain default value")
    assert(result.contains("coalesce(ROUND(f0.\"spend\", 10), 0.0) "), "Query should contain default value")
    assert(result.contains("coalesce(ROUND(f0.\"max_bid\", 10), 0.0)"), "Query should contain default value")
    assert(result.contains("(CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(CASE WHEN ((avg_pos >= 0.1) AND (avg_pos <= 500)) THEN avg_pos ELSE 0.0 END * impressions) / (SUM(impressions)) END) AS \"avg_pos\""), "Query should contain default value")
    assert(result.contains("coalesce(f0.\"impressions\", 1) \"Impressions\", coalesce(ROUND(f0.\"spend\", 10), 0.0) \"Spend\", coalesce(ROUND(f0.\"max_bid\", 10), 0.0) \"Max Bid\", coalesce(ROUND(CASE WHEN ((f0.\"avg_pos\" >= 0.1) AND (f0.\"avg_pos\" <= 500)) THEN f0.\"avg_pos\" ELSE 0.0 END, 10), 0.0) \"Average Position\""), "Query should contain default value")
  }

  test("dim fact sync fact driven with constant requested fields should contain constant fields") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_w_constant_field.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))
    assert(requestModel.toOption.get.isFactDriven)


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("'2' AS \"Source\""), "Constant field does not exsit")
  }


  test("dim fact sync fact driven query with filter on fact col should be applied in having clause") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_w_fact_filter.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))
    assert(requestModel.toOption.get.isFactDriven)


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("LEFT OUTER JOIN"), "Query should use LEFT OUTER JOIN")
    assert(result.contains("ROW_NUMBER"), "Query should have pagination")
    assert(result.contains("ROW_NUMBER >= 21"), "Min position should be 21")
    assert(result.contains("ROW_NUMBER <= 120"), "Max position should be 120")
    assert(!result.contains("TOTALROWS"), "Query should not have total row column")
    assert(result.contains(
      "(SUM(impressions) >= 0 AND SUM(impressions) <= 300)"),
      "Query should contain default value")
  }

  test("dim fact sync dimension driven query with requested fields in multiple dimensions should not fail") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Keyword ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Ad Group Status"},
                              {"field": "Campaign Status"},
                              {"field": "Count"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Campaign Status", "order": "ASC"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected =
      s"""
        |SELECT *
        |FROM (SELECT t3.id "Keyword ID", ago2.campaign_id "Campaign ID", coalesce(f0."impressions", 1) "Impressions", ago2."Ad Group Status" "Ad Group Status", co1."Campaign Status" "Campaign Status", f0."Count" "Count"
        |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) CONDITIONAL_HINT1 CONDITIONAL_HINT2 CONDITIONAL_HINT3 */
        |                   ad_group_id, campaign_id, keyword_id, SUM(impressions) AS "impressions", COUNT(*) AS "Count"
        |            FROM fact2 FactAlias
        |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
        |            GROUP BY ad_group_id, campaign_id, keyword_id
        |
        |           ) f0
        |           RIGHT OUTER JOIN
        |               ( (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  parent_id, id, advertiser_id
        |            FROM targetingattribute
        |            WHERE (advertiser_id = 12345)
        |             ) WHERE ROWNUM <= 120) D ) WHERE ROW_NUMBER >= 21 AND ROW_NUMBER <= 120) t3
        |          INNER JOIN
        |            (SELECT  campaign_id, DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", id, advertiser_id
        |            FROM ad_group_oracle
        |            WHERE (advertiser_id = 12345)
        |             ) ago2
        |              ON( t3.advertiser_id = ago2.advertiser_id AND t3.parent_id = ago2.id )
        |               INNER JOIN
        |            (SELECT /*+ CampaignHint */ DECODE(status, 'ON', 'ON', 'OFF') AS "Campaign Status", id, advertiser_id
        |            FROM campaign_oracle
        |            WHERE (advertiser_id = 12345)
        |             ) co1
        |              ON( ago2.advertiser_id = co1.advertiser_id AND ago2.campaign_id = co1.id )
        |               )  ON (f0.keyword_id = t3.id)
        |
        |)
        |   ORDER BY "Campaign Status" ASC NULLS LAST
      """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("dim fact sync dimension driven query with dim filters in multiple dimensions should not fail") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Keyword ID"},
                              {"field": "Keyword Value"},
                              {"field": "Campaign ID"},
                              {"field": "Campaign Name"},
                              {"field": "Advertiser Currency"},
                              {"field": "Impressions"},
                              {"field": "Spend"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Keyword Status", "operator": "not in", "values": ["OFF"]},
                              {"field": "Ad Group Status", "operator": "not in", "values": ["OFF"]},
                              {"field": "Campaign Status", "operator": "not in", "values": ["OFF"]}
                          ],
                          "sortBy": [
                              {"field": "Spend", "order": "DESC"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected =
      s"""SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
         |FROM (SELECT t4.id "Keyword ID", t4.value "Keyword Value", ago3.campaign_id "Campaign ID", co2.campaign_name "Campaign Name", ao1.currency "Advertiser Currency", coalesce(f0."impressions", 1) "Impressions", coalesce(ROUND(f0."spend", 10), 0.0) "Spend"
         |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) CONDITIONAL_HINT1 CONDITIONAL_HINT2 CONDITIONAL_HINT3 */
         |                   ad_group_id, advertiser_id, campaign_id, keyword_id, SUM(impressions) AS "impressions", SUM(spend) AS "spend"
         |            FROM fact2 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY ad_group_id, advertiser_id, campaign_id, keyword_id
         |
         |           ) f0
         |           RIGHT OUTER JOIN
         |               ( (SELECT  parent_id, advertiser_id, value, id
         |            FROM targetingattribute
         |            WHERE (advertiser_id = 12345) AND (status NOT IN ('OFF'))
         |             ) t4
         |          INNER JOIN
         |            (SELECT  advertiser_id, campaign_id, id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 12345) AND (DECODE(status, 'ON', 'ON', 'OFF') NOT IN ('OFF'))
         |             ) ago3
         |              ON( t4.advertiser_id = ago3.advertiser_id AND t4.parent_id = ago3.id )
         |               INNER JOIN
         |            (SELECT /*+ CampaignHint */ advertiser_id, campaign_name, id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345) AND (DECODE(status, 'ON', 'ON', 'OFF') NOT IN ('OFF'))
         |             ) co2
         |              ON( ago3.advertiser_id = co2.advertiser_id AND ago3.campaign_id = co2.id )
         |               INNER JOIN
         |            (SELECT  currency, id
         |            FROM advertiser_oracle
         |            WHERE (id = 12345)
         |             ) ao1
         |              ON( co2.advertiser_id = ao1.id )
         |               )  ON (f0.keyword_id = t4.id)
         |
         |
         |)
         |   ORDER BY "Spend" DESC NULLS LAST) WHERE ROWNUM <= 120) D ) WHERE ROW_NUMBER >= 21 AND ROW_NUMBER <= 120
         |
      """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("verify dim query can generate inner select and group by with static mapping") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Keyword ID"},
                              {"field": "Impressions"},
                              {"field": "Device ID"},
                              {"field": "Network Type"},
                              {"field": "Pricing Type"},
                              {"field": "Campaign Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Source Name", "operator": "In", "values": [ "Native", "Search" ] }
                          ],
                          "sortBy": [
                              {"field": "Campaign Status", "order": "ASC"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected =
      s"""
         |SELECT *
         |FROM (SELECT t3.id "Keyword ID", coalesce(f0."impressions", 1) "Impressions", COALESCE(f0.device_id, 'UNKNOWN') "Device ID", COALESCE(f0.network_type, 'NONE') "Network Type", COALESCE(f0.pricing_type, 'NONE') "Pricing Type", co1."Campaign Status" "Campaign Status"
         |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) CONDITIONAL_HINT1 CONDITIONAL_HINT2 CONDITIONAL_HINT3 */
         |                   CASE WHEN (device_id IN (1)) THEN 'Desktop' WHEN (device_id IN (2)) THEN 'Tablet' WHEN (device_id IN (3)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END device_id, DECODE(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_SYNDICATION', 'Content Syndication', 'EXTERNAL', 'Yahoo Partners', 'INTERNAL', 'Yahoo Properties', 'NONE') network_type, CASE WHEN (pricing_type IN (1)) THEN 'CPC' WHEN (pricing_type IN (6)) THEN 'CPV' WHEN (pricing_type IN (2)) THEN 'CPA' WHEN (pricing_type IN (-10)) THEN 'CPE' WHEN (pricing_type IN (-20)) THEN 'CPF' WHEN (pricing_type IN (7)) THEN 'CPCV' WHEN (pricing_type IN (3)) THEN 'CPM' ELSE 'NONE' END pricing_type, campaign_id, keyword_id, SUM(impressions) AS "impressions"
         |            FROM fact2 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_source IN (1,2)) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY CASE WHEN (device_id IN (1)) THEN 'Desktop' WHEN (device_id IN (2)) THEN 'Tablet' WHEN (device_id IN (3)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END, DECODE(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_SYNDICATION', 'Content Syndication', 'EXTERNAL', 'Yahoo Partners', 'INTERNAL', 'Yahoo Properties', 'NONE'), CASE WHEN (pricing_type IN (1)) THEN 'CPC' WHEN (pricing_type IN (6)) THEN 'CPV' WHEN (pricing_type IN (2)) THEN 'CPA' WHEN (pricing_type IN (-10)) THEN 'CPE' WHEN (pricing_type IN (-20)) THEN 'CPF' WHEN (pricing_type IN (7)) THEN 'CPCV' WHEN (pricing_type IN (3)) THEN 'CPM' ELSE 'NONE' END, campaign_id, keyword_id
         |
         |           ) f0
         |           RIGHT OUTER JOIN
         |               ( (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  id, parent_id, advertiser_id
         |            FROM targetingattribute
         |            WHERE (advertiser_id = 12345)
         |             ) WHERE ROWNUM <= 120) D ) WHERE ROW_NUMBER >= 21 AND ROW_NUMBER <= 120) t3
         |           INNER JOIN
         |            (SELECT  id, campaign_id, advertiser_id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 12345)
         |             ) ago2
         |              ON( t3.advertiser_id = ago2.advertiser_id AND t3.parent_id = ago2.id )
         |               INNER JOIN
         |            (SELECT /*+ CampaignHint */ DECODE(status, 'ON', 'ON', 'OFF') AS "Campaign Status", id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             ) co1
         |              ON( ago2.advertiser_id = co1.advertiser_id AND ago2.campaign_id = co1.id )
         |               )  ON (f0.keyword_id = t3.id)
         |
 |)
         |   ORDER BY "Campaign Status" ASC NULLS LAST
      """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }





  test("dim fact sync dimension driven query with non hash partitioned dimension with singleton snapshot column should generate full SQL with max snapshot column") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Impressions"},
                            {"field": "Column2 Status"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Column2 Status", "order": "Asc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Non-hash partitioned dimension with singleton snapshot failed"))
    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("max_snapshot_ts_"), "Query should contain snapshot column")
  }

  test("dim fact sync dimension driven query with non hash partitioned dimension without singleton snapshot column should fail") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Impressions"},
                            {"field": "Column Status"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Column Status", "order": "Asc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure, queryPipelineTry.errorMessage("Non-hash partitioned dimension without singleton snapshot should fail"))
    // TODO: Catch the exception[IllegalArgument]
    queryPipelineTry.failed.get.getMessage should startWith ("requirement failed: No singleton column defined for non hash partitioned dimension")
  }

  test("dim fact async dimension driven query with non hash partitioned dimension with singleton snapshot column should generate full SQL with max snapshot column") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Impressions"},
                            {"field": "Column2 Status"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Column2 Status", "order": "Asc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Non-hash partitioned dimension with singleton snapshot failed"))
    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("max_snapshot_ts_"), "Query should contain snapshot column")
  }

  test("dim fact async dimension driven query with non hash partitioned dimension without singleton snapshot column should fail") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Impressions"},
                            {"field": "Column Status"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Column Status", "order": "Asc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure, queryPipelineTry.errorMessage("Non-hash partitioned dimension without singleton snapshot should fail"))
    // TODO: Catch the exception[IllegalArgument]
    queryPipelineTry.failed.get.getMessage should startWith ("requirement failed: No singleton column defined for non hash partitioned dimension")
  }

  test("dim fact sync dimension driven query with hint annotation should have hint comment in the final sql string") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Impressions"},
                            {"field": "Column2 Status"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDateMinusOne", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Column2 Status", "order": "Asc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Non-hash partitioned dimension with singleton snapshot failed"))
    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    //since we are using fromDateMinus10, the fact rows should be high enough to render the min rows estimate based hint
    assert(result.contains("/*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) CONDITIONAL_HINT1 CONDITIONAL_HINT2 CONDITIONAL_HINT3 CONDITIONAL_HINT5 */"), "Query should contain dimension hint")
  }

  test("dim fact async fact driven query with hint annotation should have static hint comment in the final sql string") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_w_hint.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("/*+ PARALLEL_INDEX(cb_campaign_k_stats 4) CONDITIONAL_HINT1 CONDITIONAL_HINT2 CONDITIONAL_HINT4 */"), "Query should contain dimension hint")
  }

  test("dim fact sync dimension driven query with dimension id filters should generate full SQL with in subquery clause") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_dim_driven_w_dim_id_filters.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("IN (SELECT"), "Query should contain in subquery")
  }

  test("dim fact sync dimension driven query with between day filter exceeding the max days window should fail") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Ad Group Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "${getPlusDays(fromDate, -2)}", "to": "$toDate"},
                              {"field": "Campaign Status", "operator": "in", "values": ["ON"]}
                          ],
                          "sortBy": [
                              {"field": "Ad Group Status", "order": "Asc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isFailure, requestModel.errorMessage("Building request model should failed because days window exceeds the maximum"))
    assert(requestModel.checkFailureMessage("Max days window"), requestModel.errorMessage("Invalid error message"))
  }

  test("dim fact sync dimension driven query with between day filter exceeding the max days look back should fail") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Ad Group Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "${getPlusDays(fromDate, -20)}", "to": "${getPlusDays(toDate, -20)}"},
                              {"field": "Campaign Status", "operator": "in", "values": ["ON"]}
                          ],
                          "sortBy": [
                              {"field": "Ad Group Status", "order": "Asc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isFailure, requestModel.errorMessage("Building request model should failed because days look back exceeds the maximum"))
    assert(requestModel.checkFailureMessage("Max look back window"), requestModel.errorMessage("Invalid error message"))
  }

  test("dim fact sync dimension driven query with in day filter exceeding the max days window should fail") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Ad Group Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "in", "values": ["${getPlusDays(toDate,-8)}","${getPlusDays(toDate,-7)}","${getPlusDays(toDate,-6)}","${getPlusDays(toDate,-5)}","${getPlusDays(toDate,-4)}","${getPlusDays(toDate,-3)}","${getPlusDays(toDate,-2)}","${getPlusDays(toDate,-1)}","$toDate"]},
                              {"field": "Campaign Status", "operator": "in", "values": ["ON"]}
                          ],
                          "sortBy": [
                              {"field": "Ad Group Status", "order": "Asc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isFailure, requestModel.errorMessage("Building request model should failed because days window exceeds the maximum"))
    assert(requestModel.checkFailureMessage("Max days window"), requestModel.errorMessage("Invalid error message"))
  }

  test("dim fact sync dimension driven query with in day filter exceeding the max days look back should fail") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Ad Group Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "in", "values": ["2015-01-01"]},
                              {"field": "Campaign Status", "operator": "in", "values": ["ON"]}
                          ],
                          "sortBy": [
                              {"field": "Ad Group Status", "order": "Asc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isFailure, requestModel.errorMessage("Building request model should failed because days look back exceeds the maximum"))
    assert(requestModel.checkFailureMessage("Max look back window"), requestModel.errorMessage("Invalid error message"))
  }

  test("dim fact sync dimension driven query with equality day filter exceeding the max days look back should fail") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Ad Group Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "=", "value": "2015-01-01"},
                              {"field": "Campaign Status", "operator": "in", "values": ["ON"]}
                          ],
                          "sortBy": [
                              {"field": "Ad Group Status", "order": "Asc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isFailure, requestModel.errorMessage("Building request model should failed because days look back exceeds the maximum"))
    assert(requestModel.checkFailureMessage("Max look back window"), requestModel.errorMessage("Invalid error message"))
  }

  test("dim fact sync dimension driven query should have dim driven hint") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Campaign ID"},
                            {"field": "Impressions"},
                            {"field": "Ad Group Status"}
                          ],
                          "filterExpressions": [
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Campaign Status", "operator": "not in", "values": ["OFF"]}
                          ],
                          "sortBy": [
                            {"field": "Ad Group Status", "order": "Asc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("NOT IN ('OFF')"), "Query should contain NOT IN")
    assert(result.contains("RIGHT OUTER JOIN"), "Query should be ROJ")
    assert(result.contains("/*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) CONDITIONAL_HINT1 CONDITIONAL_HINT2 CONDITIONAL_HINT3 */"), "Query should contain dim driven hint")
  }

  test("dim fact sync fact driven query should have static hint") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_w_hint.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("LEFT OUTER JOIN"), "Query should use LEFT OUTER JOIN")
    assert(result.contains("/*+ PARALLEL_INDEX(cb_campaign_k_stats 4) CONDITIONAL_HINT1 CONDITIONAL_HINT2 CONDITIONAL_HINT4 */"), "Query should contain dim driven hint")
  }

  test("dim fact sync fact driven query with request DecType fields that contains max and min should return query with max and min range") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_dec_max_min.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val select = """(CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(CASE WHEN ((avg_pos >= 0.1) AND (avg_pos <= 500)) THEN avg_pos ELSE 0.0 END * impressions) / (SUM(impressions)) END) AS "avg_pos""""
    assert(result.contains(select), result)
  }

  test("dim fact sync fact driven query with request IntType fields that contains max and min should return query with max and min range") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_int_max_min.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val select = """SUM(CASE WHEN ((clicks >= 1) AND (clicks <= 800)) THEN clicks ELSE 0 END) AS "clicks""""
    assert(result.contains(select), result)
  }

  test("dim fact sync fact driven query with request fields that contains divide operation should round the division result") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_division.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val select = """ROUND(f0."Average CPC", 10)"""
    assert(result.contains(select), result)
  }

  test("dim fact sync fact driven query with request fields that contains safe divide operation should round the division result") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_safe_division.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val select = """ROUND(f0."CTR", 10) "CTR""""
    assert(result.contains(select), result)
  }

  test("dim fact sync dim driven query with filter fields that contain case insensitive field should use lower function") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Campaign ID"},
                            {"field": "Impressions"},
                            {"field": "Campaign Name"},
                            {"field": "Campaign Status"},
                            {"field": "CTR"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Campaign Name", "operator": "=", "value": "MegaCampaign"}
                          ],
                          "sortBy": [
                            {"field": "Campaign Name", "order": "Asc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val where = """lower(campaign_name) = lower('MegaCampaign')"""
    assert(result.contains(where), result)
  }

  test("dim fact sync dim driven query with fields that map to same column should generate query") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Campaign ID"},
                            {"field": "Campaign Name"},
                            {"field": "Campaign Status"},
                            {"field": "Total Impressions"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Campaign Name", "operator": "=", "value": "MegaCampaign"}
                          ],
                          "sortBy": [
                            {"field": "Campaign Name", "order": "Asc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val where = """coalesce(f0."impressions", 1) "Total Impressions", coalesce(f0."impressions", 1) "Impressions""""
    assert(result.contains(where), result)
  }

  test("Fact Driven Multidimensional query with dim sortBy") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Campaign Name"},
                            {"field": "Ad Group Name"},
                            {"field": "Ad Title"},
                            {"field": "Impressions"},
                            {"field": "CTR"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Campaign Name", "order": "Asc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected =
      s"""SELECT "Keyword ID", "Keyword Value", "Campaign Name", "Ad Group Name", "Ad Title", impressions AS "Impressions", CTR AS "CTR"
         |FROM (SELECT f0.keyword_id "Keyword ID", t4.value "Keyword Value", co1.campaign_name "Campaign Name", ago2.name "Ad Group Name", ado3.title "Ad Title", SUM(impressions) AS impressions, (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS CTR, SUM(clicks) AS clicks
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_campaign_k_stats 4) */
         |                   ad_group_id, advertiser_id, ad_id, campaign_id, keyword_id, SUM(CASE WHEN ((clicks >= 1) AND (clicks <= 800)) THEN clicks ELSE 0 END) AS clicks, SUM(impressions) AS impressions
         |            FROM fact1 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY ad_group_id, advertiser_id, ad_id, campaign_id, keyword_id
         |
         |           ) f0
         |                     LEFT OUTER JOIN
         |           (SELECT /*+ CampaignHint */ campaign_name, id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           co1 ON ( f0.advertiser_id = co1.advertiser_id AND f0.campaign_id = co1.id)
         |           LEFT OUTER JOIN
         |           (SELECT  campaign_id, name, id, advertiser_id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           ago2 ON ( f0.advertiser_id = ago2.advertiser_id AND f0.ad_group_id = ago2.id)
         |           LEFT OUTER JOIN
         |           (SELECT  ad_group_id, campaign_id, title, id, advertiser_id
         |            FROM ad_dim_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           ado3 ON ( f0.advertiser_id = ado3.advertiser_id AND f0.ad_id = ado3.id)
         |           LEFT OUTER JOIN
         |           (SELECT  parent_id, value, id, advertiser_id
         |            FROM targetingattribute
         |            WHERE (advertiser_id = 12345)
         |             )
         |           t4 ON ( f0.advertiser_id = t4.advertiser_id AND f0.keyword_id = t4.id)
         |
         |          GROUP BY f0.keyword_id, t4.value, co1.campaign_name, ago2.name, ado3.title
         |)
         |   ORDER BY "Campaign Name" ASC NULLS LAST
        |""".stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("successfully generate OGB query for fact driven multidimensional query with missing indirect relation") {
    val jsonString = s"""{
                          "cube": "k_stats_new",
                          "selectFields": [
                            {"field": "Frequency"},
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Campaign Name"},
                            {"field": "Impressions"},
                            {"field": "CTR"},
                            {"field": "Spend"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Spend", "order": "Desc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected =
      s"""
         |SELECT "Frequency", "Keyword ID", "Keyword Value", "Campaign Name", impressions AS "Impressions", CTR AS "CTR", spend AS "Spend"
         |FROM (SELECT ksf0.frequency "Frequency", ksf0.keyword_id "Keyword ID", t2.value "Keyword Value", co1.campaign_name "Campaign Name", SUM(impressions) AS impressions, (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS CTR, SUM(spend) AS spend, SUM(clicks) AS clicks
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_campaign_k_stats 4) */
         |                   advertiser_id, frequency, campaign_id, keyword_id, SUM(CASE WHEN ((clicks >= 1) AND (clicks <= 800)) THEN clicks ELSE 0 END) AS clicks, SUM(impressions) AS impressions, SUM(spend) AS spend
         |            FROM k_stats_fact1 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY advertiser_id, frequency, campaign_id, keyword_id
         |
         |           ) ksf0
         |                     LEFT OUTER JOIN
         |           (SELECT /*+ CampaignHint */ campaign_name, id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           co1 ON ( ksf0.advertiser_id = co1.advertiser_id AND ksf0.campaign_id = co1.id)
         |           LEFT OUTER JOIN
         |           (SELECT  value, id, advertiser_id
         |            FROM targetingattribute
         |            WHERE (advertiser_id = 12345)
         |             )
         |           t2 ON ( ksf0.advertiser_id = t2.advertiser_id AND ksf0.keyword_id = t2.id)
         |
 |          GROUP BY ksf0.frequency, ksf0.keyword_id, t2.value, co1.campaign_name
         |)
         |   ORDER BY "Spend" DESC NULLS LAST
         |""".stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("successfully generate non-OGB query for fact driven multidimensional query with indirect relation in request") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Ad Group Name"},
                            {"field": "Campaign Name"},
                            {"field": "Impressions"},
                            {"field": "CTR"},
                            {"field": "Spend"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Spend", "order": "Desc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected =
      s"""SELECT *
         |FROM (SELECT f0.keyword_id "Keyword ID", t3.value "Keyword Value", ago2.name "Ad Group Name", co1.campaign_name "Campaign Name", coalesce(f0."impressions", 1) "Impressions", ROUND(f0."CTR", 10) "CTR", coalesce(ROUND(f0."spend", 10), 0.0) "Spend"
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_campaign_k_stats 4) CONDITIONAL_HINT1 CONDITIONAL_HINT2 CONDITIONAL_HINT4 */
         |                   ad_group_id, campaign_id, keyword_id, SUM(impressions) AS "impressions", SUM(spend) AS "spend", (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS "CTR"
         |            FROM fact2 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY ad_group_id, campaign_id, keyword_id
         |
         |           ) f0
         |           LEFT OUTER JOIN
         |           (SELECT /*+ CampaignHint */ campaign_name, id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           co1 ON (f0.campaign_id = co1.id)
         |           LEFT OUTER JOIN
         |           (SELECT  campaign_id, name, id, advertiser_id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           ago2 ON (f0.ad_group_id = ago2.id)
         |           LEFT OUTER JOIN
         |           (SELECT  parent_id, value, id, advertiser_id
         |            FROM targetingattribute
         |            WHERE (advertiser_id = 12345)
         |             )
         |           t3 ON (f0.keyword_id = t3.id)
         |
         |)
         |   ORDER BY "Spend" DESC NULLS LAST
         |""".stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Dim Driven Multidimensional query with Keywords and Ad should fail as we do not have parent info of AD in keywords table") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Campaign Name"},
                            {"field": "Ad Group Name"},
                            {"field": "Ad Title"},
                            {"field": "Impressions"},
                            {"field": "CTR"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Campaign Name", "order": "Asc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure)
    queryPipelineTry.failed.get.getMessage should startWith("requirement failed: Failed to determine join condition between targetingattribute and ad_dim_oracle")
  }

  test("MultiDims Sync Query keyword level with 2 parent dimensions") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Campaign ID"},
                            {"field": "Ad Group Name"},
                            {"field": "Ad Group ID"},
                            {"field": "Keyword Value"},
                            {"field": "Impressions"},
                            {"field": "Campaign Name"},
                            {"field": "CTR"}
                            ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected =
      s"""
         |SELECT *
         |FROM (SELECT t3.id "Keyword ID", ago2.campaign_id "Campaign ID", ago2.name "Ad Group Name", t3.parent_id "Ad Group ID", t3.value "Keyword Value", coalesce(f0."impressions", 1) "Impressions", co1.campaign_name "Campaign Name", ROUND(f0."CTR", 10) "CTR"
         |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) CONDITIONAL_HINT1 CONDITIONAL_HINT2 CONDITIONAL_HINT3 */
         |                   ad_group_id, campaign_id, keyword_id, SUM(impressions) AS "impressions", (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS "CTR"
         |            FROM fact2 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |GROUP BY ad_group_id, campaign_id, keyword_id
         |
         |           ) f0
         |           RIGHT OUTER JOIN
         |               ( (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  parent_id, value, id, advertiser_id
         |            FROM targetingattribute
         |            WHERE (advertiser_id = 12345)
         |             ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100) t3
         |           INNER JOIN
         |            (SELECT  campaign_id, name, id, advertiser_id
         |            FROM ad_group_oracle
         |
         |             ) ago2
         |              ON( t3.advertiser_id = ago2.advertiser_id AND t3.parent_id = ago2.id )
         |               INNER JOIN
         |            (SELECT /*+ CampaignHint */ campaign_name, id, advertiser_id
         |            FROM campaign_oracle
         |
         |             ) co1
         |              ON( ago2.advertiser_id = co1.advertiser_id AND ago2.campaign_id = co1.id )
         |               )  ON (f0.keyword_id = t3.id)
         |
 |)
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("MultiDims Sync Query keyword level with 3 parent dimensions, targetingattribute as primary dim") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Campaign ID"},
                            {"field": "Ad Group Name"},
                            {"field": "Ad Group ID"},
                            {"field": "Keyword Value"},
                            {"field": "Impressions"},
                            {"field": "Campaign Name"},
                            {"field": "CTR"},
                            {"field": "Advertiser Name"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected =
      s"""SELECT *
         |FROM (SELECT t4.id "Keyword ID", ago3.campaign_id "Campaign ID", ago3.name "Ad Group Name", t4.parent_id "Ad Group ID", t4.value "Keyword Value", coalesce(f0."impressions", 1) "Impressions", co2.campaign_name "Campaign Name", ROUND(f0."CTR", 10) "CTR", ao1.name "Advertiser Name"
         |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) CONDITIONAL_HINT1 CONDITIONAL_HINT2 CONDITIONAL_HINT3 */
         |                   ad_group_id, advertiser_id, campaign_id, keyword_id, SUM(impressions) AS "impressions", (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS "CTR"
         |            FROM fact2 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY ad_group_id, advertiser_id, campaign_id, keyword_id
         |
         |           ) f0
         |           RIGHT OUTER JOIN
         |               ( (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  parent_id, advertiser_id, value, id
         |            FROM targetingattribute
         |            WHERE (advertiser_id = 12345)
         |             ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100) t4
         |           INNER JOIN
         |            (SELECT  advertiser_id, campaign_id, name, id
         |            FROM ad_group_oracle
         |
         |             ) ago3
         |              ON( t4.advertiser_id = ago3.advertiser_id AND t4.parent_id = ago3.id )
         |               INNER JOIN
         |            (SELECT /*+ CampaignHint */ advertiser_id, campaign_name, id
         |            FROM campaign_oracle
         |
         |             ) co2
         |              ON( ago3.advertiser_id = co2.advertiser_id AND ago3.campaign_id = co2.id )
         |               INNER JOIN
         |            (SELECT  name, id
         |            FROM advertiser_oracle
         |
         |             ) ao1
         |              ON( co2.advertiser_id = ao1.id )
         |               )  ON (f0.keyword_id = t4.id)
         |
         |
         |
         |)
         |""".stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("MultiDims Sync Query keyword level with 1 grand parent dimensions should succeed, keywords as primary dim") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Impressions"},
                            {"field": "Campaign Name"},
                            {"field": "CTR"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)

    assert(queryPipelineTry.isSuccess)
    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = s"""SELECT *
                      |FROM (SELECT t3.id "Keyword ID", coalesce(f0."impressions", 1) "Impressions", co1.campaign_name "Campaign Name", ROUND(f0."CTR", 10) "CTR"
                      |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) CONDITIONAL_HINT1 CONDITIONAL_HINT2 CONDITIONAL_HINT3 */
                      |                   keyword_id, campaign_id, SUM(impressions) AS "impressions", (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS "CTR"
                      |            FROM fact2 FactAlias
                      |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
                      |            GROUP BY keyword_id, campaign_id
                      |
                      |           ) f0
                      |           RIGHT OUTER JOIN
                      |               ( (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  id, parent_id, advertiser_id
                      |            FROM targetingattribute
                      |            WHERE (advertiser_id = 12345)
                      |             ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100) t3
                      |           INNER JOIN
                      |            (SELECT  id, campaign_id, advertiser_id
                      |            FROM ad_group_oracle
                      |
                      |             ) ago2
                      |              ON( t3.advertiser_id = ago2.advertiser_id AND t3.parent_id = ago2.id )
                      |               INNER JOIN
                      |            (SELECT /*+ CampaignHint */ campaign_name, id, advertiser_id
                      |            FROM campaign_oracle
                      |
                      |             ) co1
                      |              ON( ago2.advertiser_id = co1.advertiser_id AND ago2.campaign_id = co1.id )
                      |               )  ON (f0.keyword_id = t3.id)
                      |
                      |
                      |
                      |)
                     |""".stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("MultiDims Sync Query keyword level with 3 parent dimensions: Should Generate sortBy correctly") {
    val jsonString =
      s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Campaign ID"},
                            {"field": "Ad Group Name"},
                            {"field": "Ad Group ID"},
                            {"field": "Advertiser ID"},
                            {"field": "Keyword Value"},
                            {"field": "Impressions"},
                            {"field": "Campaign Name"},
                            {"field": "CTR"},
                            {"field": "Advertiser Name"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                         "sortBy": [
                               {"field": "Ad Group Name", "order": "Asc"},
                               {"field": "Keyword Value", "order": "Asc"},
                               {"field": "Campaign Name", "order": "Asc"},
                               {"field": "Advertiser Name", "order": "Asc"},
                               {"field": "Ad Group ID", "order": "DESC"},
                               {"field": "Keyword ID", "order": "DESC"},
                               {"field": "Campaign ID", "order": "DESC"},
                               {"field": "Advertiser ID", "order": "DESC"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected =
      s"""SELECT *
         |FROM (SELECT t4.id "Keyword ID", ago3.campaign_id "Campaign ID", ago3.name "Ad Group Name", t4.parent_id "Ad Group ID", t4.advertiser_id "Advertiser ID", t4.value "Keyword Value", coalesce(f0."impressions", 1) "Impressions", co2.campaign_name "Campaign Name", ROUND(f0."CTR", 10) "CTR", ao1.name "Advertiser Name"
         |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) CONDITIONAL_HINT1 CONDITIONAL_HINT2 CONDITIONAL_HINT3 */
         |                   ad_group_id, advertiser_id, campaign_id, keyword_id, SUM(impressions) AS "impressions", (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS "CTR"
         |            FROM fact2 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY ad_group_id, advertiser_id, campaign_id, keyword_id
         |
         |           ) f0
         |           RIGHT OUTER JOIN
         |               ( (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  value, parent_id, id, advertiser_id
         |            FROM targetingattribute
         |            WHERE (advertiser_id = 12345)
         |            ORDER BY 1 ASC NULLS LAST, 2 DESC , 3 DESC , 4 DESC  ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100) t4
         |           INNER JOIN
         |            (SELECT  name, id, campaign_id, advertiser_id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 12345)
         |             ) ago3
         |              ON( t4.advertiser_id = ago3.advertiser_id AND t4.parent_id = ago3.id )
         |               INNER JOIN
         |            (SELECT /*+ CampaignHint */ campaign_name, id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             ) co2
         |              ON( ago3.advertiser_id = co2.advertiser_id AND ago3.campaign_id = co2.id )
         |               INNER JOIN
         |            (SELECT  name, id
         |            FROM advertiser_oracle
         |            WHERE (id = 12345)
         |             ) ao1
         |              ON( co2.advertiser_id = ao1.id )
         |               )  ON (f0.keyword_id = t4.id)
         |
         |
         |
         |)
         |""".stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Group by over derived expression should append index instead of entire dervied expression") {
    val jsonString = s"""{
                           "cube": "k_stats",
                           "selectFields": [
                             {"field": "Keyword ID"},
                             {"field": "Campaign ID"},
                             {"field": "Month"},
                             {"field": "Ad Group ID"},
                             {"field": "Week"},
                             {"field": "Day"},
                             {"field": "Impressions"},
                             {"field": "Clicks"},
                             {"field": "CTR"}
                           ],
                           "filterExpressions": [
                             {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                             {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                           ],
                           "paginationStartIndex":0,
                           "rowsPerPage":100
                         }"""

    val request: ReportingRequest = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema).toOption.get
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = s"""
                      |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
                      |FROM (SELECT f0.keyword_id "Keyword ID", f0.campaign_id "Campaign ID", f0."Month" "Month", f0.ad_group_id "Ad Group ID", f0."Week" "Week", to_char(f0.stats_date, 'YYYY-MM-DD') "Day", coalesce(f0."impressions", 1) "Impressions", coalesce(f0."clicks", 0) "Clicks", ROUND(f0."CTR", 10) "CTR"
                      |      FROM (SELECT /*+ PARALLEL_INDEX(cb_campaign_k_stats 4) CONDITIONAL_HINT1 CONDITIONAL_HINT2 CONDITIONAL_HINT4 */
                      |                   stats_date, ad_group_id, campaign_id, keyword_id, TRUNC(stats_date, 'MM') AS "Month", TRUNC(stats_date, 'IW') AS "Week", SUM(CASE WHEN ((clicks >= 1) AND (clicks <= 800)) THEN clicks ELSE 0 END) AS "clicks", SUM(impressions) AS "impressions", (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS "CTR"
                      |            FROM fact2
                      |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
                      |            GROUP BY stats_date, ad_group_id, campaign_id, keyword_id, TRUNC(stats_date, 'MM'), TRUNC(stats_date, 'IW')
                      |
                      |           ) f0
                      |
                      |
                      |
                      |) ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100
                      |""".stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("successfully generate query with debug enabled") {
    val jsonString = s"""{
                           "cube": "k_stats",
                           "selectFields": [
                             {"field": "Keyword ID"},
                             {"field": "Month"},
                             {"field": "Ad Group ID"},
                             {"field": "Week"},
                             {"field": "Day"},
                             {"field": "Impressions"},
                             {"field": "Clicks"},
                             {"field": "CTR"}
                           ],
                           "filterExpressions": [
                             {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                             {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                           ],
                           "paginationStartIndex":0,
                           "rowsPerPage":100,
                           "forceDimensionDriven": true
                         }"""

    val request: ReportingRequest = ReportingRequest
      .deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema).toOption.get
      .copy(additionalParameters = Map(Parameter.Debug -> DebugValue(value = true)))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))
    assert(requestModel.toOption.get.isDebugEnabled, requestModel.errorMessage("Debug should be enabled!"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

  }

  test("Given a request with parameters in filter and not in requested fields, then the output query should not those parameters in select list") {
    val jsonString = s"""{
                           "cube": "k_stats",
                           "selectFields": [
                             {
                               "field": "Month",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Advertiser ID",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                             {
                               "field": "Advertiser ID",
                               "operator": "=",
                               "value": "12345",
                               "values": [],
                               "from": null,
                               "to": null
                             },
                             {
                               "field": "Day",
                               "operator": "between",
                               "value": null,
                               "values": [],
                               "from": "$fromDate",
                               "to": "$toDate"
                             },
                             {
                               "field": "Clicks",
                               "operator": "between",
                               "value": null,
                               "values": [],
                               "from": "1",
                               "to": "9007199254740991"
                             }
                           ]
                         }"""

    val request: ReportingRequest = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema).toOption.get
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = s"""
                      |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
                      |FROM (SELECT f0."Month" "Month", f0.advertiser_id "Advertiser ID"
                      |      FROM (SELECT /*+ PARALLEL_INDEX(cb_campaign_k_stats 4) CONDITIONAL_HINT1 CONDITIONAL_HINT2 CONDITIONAL_HINT4 */
                      |                   advertiser_id, TRUNC(stats_date, 'MM') AS "Month"
                      |            FROM fact2
                      |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
                      |            GROUP BY advertiser_id, TRUNC(stats_date, 'MM')
                      |            HAVING (SUM(clicks) >= 1 AND SUM(clicks) <= 9007199254740991)
                      |           ) f0
                      |
                      |
                      |
                      |) ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200""".stripMargin


    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("successfully generate dim driven dim only query with filters") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Campaign ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Ad Group ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Advertiser Status",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Campaign Name",
                               "alias": null,
                               "value": null
                             }, {"field": "Ad ID"}
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Advertiser Status", "operator": "in", "values": ["ON"]},
                              {"field": "Ad ID", "operator": "==", "compareTo": "Ad Group ID"}
                           ],
                           "paginationStartIndex":0,
                           "rowsPerPage":100,
                           "includeRowCount": true,
                           "forceDimensionDriven": true
                         }"""

    val request: ReportingRequest = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema).toOption.get
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)

    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = """SELECT  *
                     |      FROM (SELECT co1.id "Campaign ID", ado2.ad_group_id "Ad Group ID", ao0."Advertiser Status" "Advertiser Status", co1.campaign_name "Campaign Name", ado2.id "Ad ID", Count(*) OVER() TOTALROWS, ROWNUM as ROW_NUMBER
                     |            FROM
                     |               ( (SELECT  advertiser_id, campaign_id, ad_group_id, id
                     |            FROM ad_dim_oracle
                     |            WHERE (id = ad_group_id) AND (advertiser_id = 12345)
                     |             ) ado2
                     |          INNER JOIN
                     |            (SELECT /*+ CampaignHint */ advertiser_id, campaign_name, id
                     |            FROM campaign_oracle
                     |            WHERE (advertiser_id = 12345)
                     |             ) co1
                     |              ON( ado2.advertiser_id = co1.advertiser_id AND ado2.campaign_id = co1.id )
                     |               INNER JOIN
                     |            (SELECT  DECODE(status, 'ON', 'ON', 'OFF') AS "Advertiser Status", id
                     |            FROM advertiser_oracle
                     |            WHERE (id = 12345) AND (DECODE(status, 'ON', 'ON', 'OFF') IN ('ON'))
                     |             ) ao0
                     |              ON( co1.advertiser_id = ao0.id )
                     |               )
                     |
                     |           )
                     |             WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100""".stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("should compare two strings with first one insensitive") {
    val jsonString = s"""{
                           "cube": "k_stats",
                           "selectFields": [
                             {
                               "field": "Campaign ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Ad Group ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Destination URL",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Source URL",
                               "alias": null,
                               "value": null
                             }, {"field": "Ad ID"}
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Source URL", "operator": "==", "compareTo": "Destination URL"}
                           ],
                           "paginationStartIndex":0,
                           "rowsPerPage":100,
                           "includeRowCount": true,
                           "forceDimensionDriven": true
                         }"""

    val request: ReportingRequest = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema).toOption.get
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)

    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = s"""SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
                     |FROM (SELECT ado1.campaign_id "Campaign ID", ado1.ad_group_id "Ad Group ID", f0.landing_page_url "Destination URL", f0.target_page_url "Source URL", ado1.id "Ad ID", Count(*) OVER() TOTALROWS
                     |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) */
                     |                   target_page_url, landing_page_url, ad_group_id, ad_id, campaign_id
                     |            FROM fact1 FactAlias
                     |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (lower(target_page_url) = landing_page_url) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
                     |            GROUP BY target_page_url, landing_page_url, ad_group_id, ad_id, campaign_id
                     |
                     |           ) f0
                     |           RIGHT OUTER JOIN
                     |                (SELECT  ad_group_id, campaign_id, id, advertiser_id
                     |            FROM ad_dim_oracle
                     |            WHERE (advertiser_id = 12345)
                     |             ) ado1
                     |            ON (f0.ad_id = ado1.id)
                     |
                     |)
                     |   ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100""".stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("should compare two strings with second one insensitive") {
    val jsonString = s"""{
                           "cube": "k_stats",
                           "selectFields": [
                             {
                               "field": "Campaign ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Ad Group ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Destination URL",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Source URL",
                               "alias": null,
                               "value": null
                             }, {"field": "Ad ID"}
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Destination URL", "operator": "==", "compareTo": "Source URL"}
                           ],
                           "paginationStartIndex":0,
                           "rowsPerPage":100,
                           "includeRowCount": true,
                           "forceDimensionDriven": true
                         }"""

    val request: ReportingRequest = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema).toOption.get
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)

    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = s"""SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
                     |FROM (SELECT ado1.campaign_id "Campaign ID", ado1.ad_group_id "Ad Group ID", f0.landing_page_url "Destination URL", f0.target_page_url "Source URL", ado1.id "Ad ID", Count(*) OVER() TOTALROWS
                     |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) */
                     |                   target_page_url, landing_page_url, ad_group_id, ad_id, campaign_id
                     |            FROM fact1 FactAlias
                     |            WHERE (advertiser_id = 12345) AND (landing_page_url = lower(target_page_url)) AND (stats_source = 2) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
                     |            GROUP BY target_page_url, landing_page_url, ad_group_id, ad_id, campaign_id
                     |
                     |           ) f0
                     |           RIGHT OUTER JOIN
                     |                (SELECT  ad_group_id, campaign_id, id, advertiser_id
                     |            FROM ad_dim_oracle
                     |            WHERE (advertiser_id = 12345)
                     |             ) ado1
                     |            ON (f0.ad_id = ado1.id)
                     |
                     |)
                     |   ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100""".stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("should fail to compare two dimensions of different dataTypes.") {
    val jsonString = s"""{
                           "cube": "k_stats",
                           "selectFields": [
                             {
                               "field": "Campaign ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Ad Group ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Destination URL",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Source URL",
                               "alias": null,
                               "value": null
                             },
                             {"field": "Source"}
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Source", "operator": "==", "compareTo": "Source URL"}
                           ],
                           "paginationStartIndex":0,
                           "rowsPerPage":100,
                           "includeRowCount": true,
                           "forceDimensionDriven": true
                         }"""

    val request: ReportingRequest = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema).toOption.get
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isFailure && requestModel.failed.get.getMessage.contains("Both fields being compared must be the same Data Type."))
  }

  test("should fail to compare metric to non-metric.") {
    val jsonString = s"""{
                           "cube": "k_stats",
                           "selectFields": [
                             {
                               "field": "Campaign ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Ad Group ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Destination URL",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Source URL",
                               "alias": null,
                               "value": null
                             },
                             {"field": "Source"}
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Source URL", "operator": "==", "compareTo": "Clicks"}
                           ],
                           "paginationStartIndex":0,
                           "rowsPerPage":100,
                           "includeRowCount": true,
                           "forceDimensionDriven": true
                         }"""

    val request: ReportingRequest = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema).toOption.get
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isFailure && requestModel.failed.get.getMessage.contains("Both fields being compared must be the same Data Type."))
  }

  test("should fail to compare anything to an invalid field.") {
    val jsonString = s"""{
                           "cube": "k_stats",
                           "selectFields": [
                             {
                               "field": "Campaign ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Ad Group ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Destination URL",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Source URL",
                               "alias": null,
                               "value": null
                             },
                             {"field": "Source"}
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Ad Impressions Flag", "operator": "==", "compareTo": "Invalid Column"}
                           ],
                           "paginationStartIndex":0,
                           "rowsPerPage":100,
                           "includeRowCount": true,
                           "forceDimensionDriven": true
                         }"""

    val request: ReportingRequest = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema).toOption.get
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isFailure && requestModel.failed.get.getMessage.contains("10009 Field found only in Dimension table is not comparable with Fact fields"))
  }

  test("should fail comparing different data types in dimension table comparison.") {
    val jsonString = s"""{
                           "cube": "k_stats",
                           "selectFields": [
                             {
                               "field": "Ad ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Ad Impressions Flag",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Ad Group ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Campaign ID",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Ad Impressions Flag", "operator": "==", "compareTo": "Ad Title"}
                           ],
                           "paginationStartIndex":0,
                           "rowsPerPage":100,
                           "includeRowCount": true,
                           "forceDimensionDriven": true
                         }"""

    val request: ReportingRequest = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema).toOption.get
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isFailure && requestModel.failed.get.getMessage.contains("Both fields being compared must be the same Data Type."))
  }

  test("should fail comparing fact to Invalid Column.") {
    val jsonString = s"""{
                           "cube": "k_stats",
                           "selectFields": [
                             {
                               "field": "Ad ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Ad Impressions Flag",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Ad Group ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Campaign ID",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Spend", "operator": "==", "compareTo": "Invalid Column"}
                           ],
                           "paginationStartIndex":0,
                           "rowsPerPage":100,
                           "includeRowCount": true,
                           "forceDimensionDriven": true
                         }"""

    val request: ReportingRequest = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema).toOption.get
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isFailure && requestModel.failed.get.getMessage.contains("10009 Field found only in Dimension table is not comparable with Fact fields"))
  }

  test("should fail comparing dimension to fact table.") {
    val jsonString = s"""{
                           "cube": "k_stats",
                           "selectFields": [
                             {
                               "field": "Ad ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Ad Impressions Flag",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Ad Group ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Campaign ID",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Ad Impressions Flag", "operator": "==", "compareTo": "Spend"}
                           ],
                           "paginationStartIndex":0,
                           "rowsPerPage":100,
                           "includeRowCount": true,
                           "forceDimensionDriven": true
                         }"""

    val request: ReportingRequest = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema).toOption.get
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isFailure && requestModel.failed.get.getMessage.contains("10009 Field found only in Dimension table is not comparable with Fact fields"))
  }

  test("should succeed to compare two metrics of same dataTypes.") {
    val jsonString = s"""{
                           "cube": "k_stats",
                           "selectFields": [
                             {
                               "field": "Campaign ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Ad Group ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Destination URL",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Source URL",
                               "alias": null,
                               "value": null
                             },
                             {"field": "Average Position"},
                             {"field": "Spend"}
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Average Position", "operator": "==", "compareTo": "Spend"}
                           ],
                           "paginationStartIndex":0,
                           "rowsPerPage":100,
                           "includeRowCount": true,
                           "forceDimensionDriven": true
                         }"""

    val request: ReportingRequest = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema).toOption.get
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)

    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = s"""SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
                      |FROM (SELECT ago1.campaign_id "Campaign ID", ago1.id "Ad Group ID", f0.landing_page_url "Destination URL", f0.target_page_url "Source URL", coalesce(ROUND(CASE WHEN ((f0."avg_pos" >= 0.1) AND (f0."avg_pos" <= 500)) THEN f0."avg_pos" ELSE 0.0 END, 10), 0.0) "Average Position", coalesce(ROUND(f0."spend", 10), 0.0) "Spend", Count(*) OVER() TOTALROWS
                      |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) CONDITIONAL_HINT1 CONDITIONAL_HINT2 CONDITIONAL_HINT3 */
                      |                   target_page_url, landing_page_url, ad_group_id, campaign_id, SUM(spend) AS "spend", (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(CASE WHEN ((avg_pos >= 0.1) AND (avg_pos <= 500)) THEN avg_pos ELSE 0.0 END * impressions) / (SUM(impressions)) END) AS "avg_pos"
                      |            FROM fact2 FactAlias
                      |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
                      |            GROUP BY target_page_url, landing_page_url, ad_group_id, campaign_id
                      |            HAVING ((CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(CASE WHEN ((avg_pos >= 0.1) AND (avg_pos <= 500)) THEN avg_pos ELSE 0.0 END * impressions) / (SUM(impressions)) END) = SUM(spend))
                      |           ) f0
                      |           INNER JOIN
                      |                (SELECT  campaign_id, id, advertiser_id
                      |            FROM ad_group_oracle
                      |            WHERE (advertiser_id = 12345)
                      |             ) ago1
                      |            ON (f0.ad_group_id = ago1.id)
                      |
 |)
                      |   ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100
                      |            """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("successfully generate distinct dim only query") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Advertiser Status",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ],
                           "paginationStartIndex":0,
                           "rowsPerPage":100,
                           "forceDimensionDriven": true
                         }"""

    val request: ReportingRequest = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema).toOption.get
      .copy(additionalParameters = Map(Parameter.Debug -> DebugValue(value = true), Parameter.Distinct -> DistinctValue(true)))

    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)

    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected =
      """
        |SELECT *
        |      FROM (SELECT DISTINCT ao0."Advertiser Status" "Advertiser Status", ROWNUM as ROW_NUMBER
        |            FROM
        |                (SELECT  DECODE(status, 'ON', 'ON', 'OFF') AS "Advertiser Status", id
        |            FROM advertiser_oracle
        |            WHERE (id = 12345)
        |             ) ao0
        |
        |
        |           )
        |             WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100
      """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("successfully generate sync force dim driven dim only query with filters and order by and row count") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Campaign ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Ad Group ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Advertiser Status",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Campaign Name",
                               "alias": null,
                               "value": null
                             },
                             { "field" : "Source", "value" : "2", "alias" : "Source"}
                           ],
                          "filterExpressions": [
                             {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                             {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ],"sortBy": [
                              {"field": "Campaign ID", "order": "Asc"}
                           ],
                           "paginationStartIndex":0,
                           "rowsPerPage":100,
                           "forceDimensionDriven": true,
                           "includeRowCount": true
                          }"""

    val requestOption = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(requestOption.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = """
                     |SELECT  *
                     |      FROM (SELECT ago2.campaign_id "Campaign ID", ago2.id "Ad Group ID", ao0."Advertiser Status" "Advertiser Status", co1.campaign_name "Campaign Name", '2' AS "Source", Count(*) OVER() TOTALROWS, ROWNUM as ROW_NUMBER
                     |            FROM
                     |               ( (SELECT  campaign_id, advertiser_id, id
                     |            FROM ad_group_oracle
                     |            WHERE (advertiser_id = 12345)
                     |            ORDER BY 1 ASC  ) ago2
                     |          INNER JOIN
                     |            (SELECT /*+ CampaignHint */ id, advertiser_id, campaign_name
                     |            FROM campaign_oracle
                     |            WHERE (advertiser_id = 12345)
                     |             ) co1
                     |              ON( ago2.advertiser_id = co1.advertiser_id AND ago2.campaign_id = co1.id )
                     |               INNER JOIN
                     |            (SELECT  DECODE(status, 'ON', 'ON', 'OFF') AS "Advertiser Status", id
                     |            FROM advertiser_oracle
                     |            WHERE (id = 12345)
                     |             ) ao0
                     |              ON( co1.advertiser_id = ao0.id )
                     |               )
                     |
                     |           )
                     |             WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100
                     |""".stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }


  test("successfully generate async  dim only query with filters and order by and row count") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Campaign ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Ad Group ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Advertiser Status",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Campaign Name",
                               "alias": null,
                               "value": null
                             },
                             { "field" : "Source", "value" : "2", "alias" : "Source"}
                           ],
                          "filterExpressions": [
                             {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                             {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ],"sortBy": [
                              {"field": "Campaign ID", "order": "Asc"}
                           ],
                           "paginationStartIndex":0,
                           "rowsPerPage":100,
                           "forceDimensionDriven": true,
                           "includeRowCount": true
                          }"""

    val requestOption = ReportingRequest.deserializeAsync(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(requestOption.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))
    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = """
                     |SELECT  *
                     |        FROM (SELECT ago2.campaign_id "Campaign ID", ago2.id "Ad Group ID", ao0."Advertiser Status" "Advertiser Status", co1.campaign_name "Campaign Name", '2' AS "Source", Count(*) OVER() TOTALROWS, ROWNUM as ROW_NUMBER
                     |              FROM
                     |                 ( (SELECT  campaign_id, advertiser_id, id
                     |              FROM ad_group_oracle
                     |              WHERE (advertiser_id = 12345)
                     |              ORDER BY 1 ASC  ) ago2
                     |            INNER JOIN
                     |              (SELECT /*+ CampaignHint */ id, advertiser_id, campaign_name
                     |              FROM campaign_oracle
                     |              WHERE (advertiser_id = 12345)
                     |               ) co1
                     |                ON( ago2.advertiser_id = co1.advertiser_id AND ago2.campaign_id = co1.id )
                     |                 INNER JOIN
                     |              (SELECT  DECODE(status, 'ON', 'ON', 'OFF') AS "Advertiser Status", id
                     |              FROM advertiser_oracle
                     |              WHERE (id = 12345)
                     |               ) ao0
                     |                ON( co1.advertiser_id = ao0.id )
                     |                 )
                     |
                     |             )
                     |               WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100
                     |""".stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("successfully generate fact driven query with right outer join when schema required fields are not present in the fact") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Campaign ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Ad Group ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Advertiser Status",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Campaign Name",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Custom",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Impressions",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "CTR",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                              {"field": "Reseller ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ],
                           "forceDimensionDriven": false
                         }"""

    val request: ReportingRequest = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), ResellerSchema).toOption.get
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = s"""
                      |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
                      |FROM (SELECT co2.id "Campaign ID", af0.ad_group_id "Ad Group ID", ao1."Advertiser Status" "Advertiser Status", co2.campaign_name "Campaign Name", coalesce(ROUND(CASE WHEN ((af0."custom_col" >= 0) AND (af0."custom_col" <= 10)) THEN af0."custom_col" ELSE 0 END, 10), 0) "Custom", coalesce(af0."impressions", 1) "Impressions", ROUND(af0."CTR", 10) "CTR"
                      |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
                      |                   advertiser_id, campaign_id, ad_group_id, SUM(impressions) AS "impressions", (SUM(clicks * max_bid)) AS "custom_col", (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS "CTR"
                      |            FROM ad_fact1 FactAlias
                      |            WHERE (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
                      |            GROUP BY advertiser_id, campaign_id, ad_group_id
                      |
                      |           ) af0
                      |           INNER JOIN
                      |           (SELECT  DECODE(status, 'ON', 'ON', 'OFF') AS "Advertiser Status", id
                      |            FROM advertiser_oracle
                      |            WHERE (managed_by = 12345)
                      |             )
                      |           ao1 ON (af0.advertiser_id = ao1.id)
                      |           INNER JOIN
                      |           (SELECT /*+ CampaignHint */ advertiser_id, campaign_name, id
                      |            FROM campaign_oracle
                      |
                      |             )
                      |           co2 ON ( af0.advertiser_id = co2.advertiser_id AND af0.campaign_id = co2.id)
                      |
                      |
                      |
                      |) ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
                     |""".stripMargin


    result should equal (expected)(after being whiteSpaceNormalised)
  }

  test("successfully generate dim join conditions on partition col if partition col is not requested for reseller case") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                              "field": "Day",
                              "alias": null,
                              "value": null
                             },
                             {
                               "field": "Advertiser Status",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Campaign Name",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Impressions",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "CTR",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                              {"field": "Reseller ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ],
                           "forceDimensionDriven": false
                         }"""

    val request: ReportingRequest = ReportingRequest.deserializeAsync(jsonString.getBytes(StandardCharsets.UTF_8), ResellerSchema).toOption.get
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    
    val expected =
      s"""
         |SELECT "Day", "Advertiser Status", "Campaign Name", impressions AS "Impressions", CTR AS "CTR"
         |FROM (SELECT to_char(af0.stats_date, 'YYYY-MM-DD') "Day", ao1."Advertiser Status" "Advertiser Status", co2.campaign_name "Campaign Name", SUM(impressions) AS impressions, (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS CTR, SUM(clicks) AS clicks
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
         |                   advertiser_id, campaign_id, stats_date, SUM(CASE WHEN ((clicks >= 1) AND (clicks <= 800)) THEN clicks ELSE 0 END) AS clicks, SUM(impressions) AS impressions
         |            FROM ad_fact1 FactAlias
         |            WHERE (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY advertiser_id, campaign_id, stats_date
         |
         |           ) af0
         |                     INNER JOIN
         |           (SELECT  DECODE(status, 'ON', 'ON', 'OFF') AS "Advertiser Status", id
         |            FROM advertiser_oracle
         |            WHERE (managed_by = 12345)
         |             )
         |           ao1 ON (af0.advertiser_id = ao1.id)
         |           INNER JOIN
         |           (SELECT /*+ CampaignHint */ advertiser_id, campaign_name, id
         |            FROM campaign_oracle
         |
         |             )
         |           co2 ON ( af0.advertiser_id = co2.advertiser_id AND af0.campaign_id = co2.id)
         |
         |          GROUP BY to_char(af0.stats_date, 'YYYY-MM-DD'), ao1."Advertiser Status", co2.campaign_name
         |)
       """.stripMargin

    result should equal (expected)(after being whiteSpaceNormalised)
  }

  test("successfully generate dim join conditions on partition col if partition col is not requested") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                              "field": "Day",
                              "alias": null,
                              "value": null
                             },
                             {
                               "field": "Advertiser Status",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Campaign Name",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Impressions",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "CTR",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ],
                           "forceDimensionDriven": false
                         }"""

    val request: ReportingRequest = ReportingRequest.deserializeAsync(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema).toOption.get
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    
    val expected =
      s"""
         |SELECT "Day", "Advertiser Status", "Campaign Name", impressions AS "Impressions", CTR AS "CTR"
         |FROM (SELECT to_char(af0.stats_date, 'YYYY-MM-DD') "Day", ao1."Advertiser Status" "Advertiser Status", co2.campaign_name "Campaign Name", SUM(impressions) AS impressions, (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS CTR, SUM(clicks) AS clicks
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
         |                   advertiser_id, campaign_id, stats_date, SUM(CASE WHEN ((clicks >= 1) AND (clicks <= 800)) THEN clicks ELSE 0 END) AS clicks, SUM(impressions) AS impressions
         |            FROM ad_fact1 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY advertiser_id, campaign_id, stats_date
         |
         |           ) af0
         |                     LEFT OUTER JOIN
         |           (SELECT  DECODE(status, 'ON', 'ON', 'OFF') AS "Advertiser Status", id
         |            FROM advertiser_oracle
         |            WHERE (id = 12345)
         |             )
         |           ao1 ON (af0.advertiser_id = ao1.id)
         |           LEFT OUTER JOIN
         |           (SELECT /*+ CampaignHint */ advertiser_id, campaign_name, id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           co2 ON ( af0.advertiser_id = co2.advertiser_id AND af0.campaign_id = co2.id)
         |
         |          GROUP BY to_char(af0.stats_date, 'YYYY-MM-DD'), ao1."Advertiser Status", co2.campaign_name
         |)
       """.stripMargin

    result should equal (expected)(after being whiteSpaceNormalised)
  }

  test("AD Page default: Supporting dim test") {
    val jsonString =
      s"""{
                          "cube": "performance_stats",
                          "selectFields": [
                            {"field": "Campaign ID"},
                            {"field": "Ad Group Name"},
                            {"field": "Ad Group ID"},
                            {"field": "Impressions"},
                            {"field": "Campaign Name"},
                            {"field": "Ad Title"},
                            {"field": "Ad ID"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                         "sortBy": [
                               {"field": "Ad ID", "order": "Desc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    val expected =
       s"""SELECT *
         |FROM (SELECT ado3.campaign_id "Campaign ID", ago2.name "Ad Group Name", ado3.ad_group_id "Ad Group ID", coalesce(af0."impressions", 1) "Impressions", co1.campaign_name "Campaign Name", ado3.title "Ad Title", ado3.id "Ad ID"
         |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_ad_stats 4) */
         |                   ad_id, campaign_id, ad_group_id, SUM(impressions) AS "impressions"
         |            FROM ad_fact1 FactAlias
         |            WHERE (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY ad_id, campaign_id, ad_group_id
         |
         |           ) af0
         |           RIGHT OUTER JOIN
         |               ( (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  id, ad_group_id, campaign_id, title, advertiser_id
         |            FROM ad_dim_oracle INNER JOIN ( SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT /*+  INDEX(ad_dim_oracle AD_ID)  */ advertiser_id ado3_advertiser_id, id ado3_id
         |            FROM ad_dim_oracle
         |            WHERE (advertiser_id = 12345)
         |            ORDER BY 2 DESC  ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100 ) adoi4
         |            ON( ad_dim_oracle.advertiser_id = adoi4.ado3_advertiser_id AND ad_dim_oracle.id = adoi4.ado3_id )
         |            WHERE (advertiser_id = 12345)
         |            ORDER BY 1 DESC  ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100) ado3
         |           INNER JOIN
         |            (SELECT  campaign_id, name, id, advertiser_id
         |            FROM ad_group_oracle
         |
         |             ) ago2
         |              ON( ado3.advertiser_id = ago2.advertiser_id AND ado3.ad_group_id = ago2.id )
         |               INNER JOIN
         |            (SELECT /*+ CampaignHint */ campaign_name, id, advertiser_id
         |            FROM campaign_oracle
         |
         |             ) co1
         |              ON( ago2.advertiser_id = co1.advertiser_id AND ago2.campaign_id = co1.id )
         |               )  ON (af0.ad_id = ado3.id)
         |
         |
         |
         |) ORDER BY "Ad ID" DESC
         |""".stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  ignore("successfully generate query for forced fact driven query specialized to use subquery instead of join") {
    val jsonString =
      s"""{ "cube": "performance_stats",
        |   "selectFields": [
        |      {
        |         "field": "Day"
        |      },
        |      {
        |         "field": "Average CPC"
        |      },
        |      {
        |         "field": "Average CPC Cents"
        |      },
        |      {
        |         "field": "Average Position"
        |      },
        |      {
        |         "field": "Impressions"
        |      },
        |      {
        |         "field": "Max Bid"
        |      },
        |      {
        |         "field": "Spend"
        |      },
        |      {
        |         "field": "CTR"
        |      }
        |   ],
        |   "filterExpressions": [
        |      {
        |         "field": "Reseller ID",
        |         "operator": "=",
        |         "value": "12345"
        |      },
        |      {
        |         "field": "Day",
        |         "operator": "Between",
        |         "from": "$fromDate",
        |         "to": "$toDate"
        |      }
        |   ],
        |   "paginationStartIndex": 0,
        |   "rowsPerPage": 200,
        |   "forceDimensionDriven":false,
        |   "includeRowCount": false
        |}
      """.stripMargin

    val request: ReportingRequest = getReportingRequestSyncWithFactBias(jsonString, ResellerSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    val expected =
      s"""
         |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT "Day", CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END AS "Average CPC", (CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END) * 100 AS "Average CPC Cents", avg_pos AS "Average Position", impressions AS "Impressions", max_bid AS "Max Bid", spend AS "Spend", CTR AS "CTR"
         |FROM (SELECT to_char(af0.stats_date, 'YYYY-MM-DD') "Day", (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(CASE WHEN ((avg_pos >= 0.1) AND (avg_pos <= 500)) THEN avg_pos ELSE 0.0 END * impressions) / (SUM(impressions)) END) AS avg_pos, SUM(impressions) AS impressions, MAX(max_bid) AS max_bid, SUM(spend) AS spend, (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS CTR, SUM(clicks) AS clicks
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
         |                   advertiser_id, stats_date, (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(CASE WHEN ((avg_pos >= 0.1) AND (avg_pos <= 500)) THEN avg_pos ELSE 0.0 END * impressions) / (SUM(impressions)) END) AS avg_pos, SUM(impressions) AS impressions, SUM(CASE WHEN ((clicks >= 1) AND (clicks <= 800)) THEN clicks ELSE 0 END) AS clicks, SUM(spend) AS spend, MAX(max_bid) AS max_bid
         |            FROM ad_fact1 FactAlias
         |            WHERE (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY advertiser_id, stats_date
         |
         |           ) af0
         |                     INNER JOIN
         |           (SELECT  id
         |            FROM advertiser_oracle
         |            WHERE (managed_by = 12345)
         |             )
         |           ao1 ON (af0.advertiser_id = ao1.id)
         |
 |          GROUP BY to_char(af0.stats_date, 'YYYY-MM-DD')
         |)
         |   ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }
  test("successfully generate query with new partitioning Scheme") {
    val jsonString = s"""{
                           "cube": "k_stats_new",
                           "selectFields": [
                             {"field": "Keyword ID"},
                             {"field": "Month"},
                             {"field": "Ad Group ID"},
                             {"field": "Week"},
                             {"field": "Day"},
                             {"field": "Impressions"},
                             {"field": "Clicks"},
                             {"field": "CTR"}
                           ],
                           "filterExpressions": [
                             {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                             {"field": "Hour", "operator": "between", "from": "0", "to": "23"},
                             {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                             {"field": "Source", "operator": "=", "value": "1"}
                           ],
                           "paginationStartIndex":0,
                           "rowsPerPage":100,
                           "forceDimensionDriven": true
                         }"""

    val request: ReportingRequest = ReportingRequest
      .deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema).toOption.get
      .copy(additionalParameters = Map(Parameter.Debug -> DebugValue(value = true), Parameter.TimeZone->TimeZoneValue("PST")))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))
    assert(requestModel.toOption.get.isDebugEnabled, requestModel.errorMessage("Debug should be enabled!"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    val expected =
      s"""|SELECT *
         |FROM (SELECT t1.id "Keyword ID", ksf0."Month" "Month", t1.parent_id "Ad Group ID", ksf0."Week" "Week", to_char(ksf0.stats_date, 'YYYY-MM-DD') "Day", coalesce(ksf0."impressions", 1) "Impressions", coalesce(ksf0."clicks", 0) "Clicks", ROUND(ksf0."CTR", 10) "CTR"
         |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) */
         |                   stats_date, ad_group_id, keyword_id, TRUNC(stats_date, 'MM') AS "Month", TRUNC(stats_date, 'IW') AS "Week", SUM(CASE WHEN ((clicks >= 1) AND (clicks <= 800)) THEN clicks ELSE 0 END) AS "clicks", SUM(impressions) AS "impressions", (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS "CTR"
         |            FROM k_stats_fact1 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY stats_date, ad_group_id, keyword_id, TRUNC(stats_date, 'MM'), TRUNC(stats_date, 'IW')
         |
         |           ) ksf0
         |           RIGHT OUTER JOIN
         |                (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  parent_id, id, advertiser_id
         |            FROM targetingattribute
         |            WHERE (advertiser_id = 12345)
         |             ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100) t1
         |            ON (ksf0.keyword_id = t1.id)
         |
         |
         |)
       """.stripMargin


    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Test for Customer Bug fix in old :Fact metric filters in select") {
    val jsonString =
      s"""{
                          "cube": "performance_stats",
                          "selectFields": [
                            {"field": "Campaign ID"},
                            {"field": "Impressions"},
                            {"field": "Campaign Status"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                            {"field": "Impressions", "operator": "=", "value": "12345"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "includeRowCount": true,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    val expected =
      s"""SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
         |FROM (SELECT co1.id "Campaign ID", coalesce(af0."impressions", 1) "Impressions", co1."Campaign Status" "Campaign Status", Count(*) OVER() TOTALROWS
         |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_ad_stats 4) */
         |                   campaign_id, SUM(impressions) AS "impressions"
         |            FROM ad_fact1 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY campaign_id
         |            HAVING (SUM(impressions) = 12345)
         |           ) af0
         |           INNER JOIN
         |                (SELECT /*+ CampaignHint */ DECODE(status, 'ON', 'ON', 'OFF') AS "Campaign Status", id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             ) co1
         |            ON (af0.campaign_id = co1.id)
         |
 |)
         |   ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100
       """.stripMargin


    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("AD Page default: Multiple Sort On Dim Cols") {
    val jsonString =
      s"""{
                          "cube": "performance_stats",
                          "selectFields": [
                            {"field": "Campaign ID"},
                            {"field": "Ad Group ID"},
                            {"field": "Impressions"},
                            {"field": "Ad Title"},
                            {"field": "Ad ID"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                         "sortBy": [
                               {"field": "Ad ID", "order": "Desc"},
                               {"field": "Ad Title", "order": "Desc"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    val expected =
      s"""|SELECT *
         |FROM (SELECT ado1.campaign_id "Campaign ID", ado1.ad_group_id "Ad Group ID", coalesce(af0."impressions", 1) "Impressions", ado1.title "Ad Title", ado1.id "Ad ID"
         |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_ad_stats 4) */
         |                   ad_id, campaign_id, ad_group_id, SUM(impressions) AS "impressions"
         |            FROM ad_fact1 FactAlias
         |            WHERE (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY ad_id, campaign_id, ad_group_id
         |
         |           ) af0
         |           RIGHT OUTER JOIN
         |                (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  id, title, ad_group_id, campaign_id, advertiser_id
         |            FROM ad_dim_oracle
         |            WHERE (advertiser_id = 12345)
         |            ORDER BY 1 DESC , 2 DESC NULLS LAST ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100) ado1
         |            ON (af0.ad_id = ado1.id)
         |
         |
         |)
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  ignore("test NoopRollup expression for generated query") {
    val jsonString =
      s"""{ "cube": "performance_stats",
          |   "selectFields": [
          |      {
          |         "field": "Day"
          |      },
          |      {
          |         "field": "Average CPC"
          |      },
          |      {
          |         "field": "Average Position"
          |      },
          |      {
          |         "field": "Impressions"
          |      },
          |      {
          |         "field": "Max Bid"
          |      },
          |      {
          |         "field": "Spend"
          |      },
          |      {
          |         "field": "CTR"
          |      }
          |   ],
          |   "filterExpressions": [
          |      {
          |         "field": "Reseller ID",
          |         "operator": "=",
          |         "value": "12345"
          |      },
          |      {
          |         "field": "Day",
          |         "operator": "Between",
          |         "from": "$fromDate",
          |         "to": "$toDate"
          |      }
          |   ],
          |   "paginationStartIndex": 0,
          |   "rowsPerPage": 200,
          |   "forceDimensionDriven":false,
          |   "includeRowCount": false
          |}
      """.stripMargin

    val request: ReportingRequest = getReportingRequestSyncWithFactBias(jsonString, ResellerSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    val expected =
      s"""
         |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT "Day", CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END AS "Average CPC", avg_pos AS "Average Position", impressions AS "Impressions", max_bid AS "Max Bid", spend AS "Spend", CTR AS "CTR"
         |FROM (SELECT to_char(af0.stats_date, 'YYYY-MM-DD') "Day", (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(CASE WHEN ((avg_pos >= 0.1) AND (avg_pos <= 500)) THEN avg_pos ELSE 0.0 END * impressions) / (SUM(impressions)) END) AS avg_pos, SUM(impressions) AS impressions, MAX(max_bid) AS max_bid, SUM(spend) AS spend, (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS CTR, SUM(clicks) AS clicks
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
         |                   advertiser_id, stats_date, (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(CASE WHEN ((avg_pos >= 0.1) AND (avg_pos <= 500)) THEN avg_pos ELSE 0.0 END * impressions) / (SUM(impressions)) END) AS avg_pos, SUM(impressions) AS impressions, SUM(CASE WHEN ((clicks >= 1) AND (clicks <= 800)) THEN clicks ELSE 0 END) AS clicks, SUM(spend) AS spend, MAX(max_bid) AS max_bid
         |            FROM ad_fact1 FactAlias
         |            WHERE (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY advertiser_id, stats_date
         |
         |           ) af0
         |                     INNER JOIN
         |           (SELECT  id
         |            FROM advertiser_oracle
         |            WHERE (managed_by = 12345)
         |             )
         |           ao1 ON (af0.advertiser_id = ao1.id)
         |
 |          GROUP BY to_char(af0.stats_date, 'YYYY-MM-DD')
         |)
         |   ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Fact View Model Query Test") {
    val jsonString =
      s"""{ "cube": "keyword_view_test",
         |   "selectFields": [
         |      {
         |         "field": "Ad ID"
         |      },
         |      {
         |         "field": "Day"
         |      },
         |      {
         |         "field": "Average CPC"
         |      },
         |      {
         |         "field": "Average Position"
         |      },
         |      {
         |         "field": "Impressions"
         |      },
         |      {
         |         "field": "Max Bid"
         |      },
         |      {
         |         "field": "Spend"
         |      },
         |      {
         |         "field": "CTR"
         |      }
         |   ],
         |   "filterExpressions": [
         |      {
         |         "field": "Advertiser ID",
         |         "operator": "=",
         |         "value": "12345"
         |      },
         |      {
         |         "field": "Day",
         |         "operator": "Between",
         |         "from": "$fromDate",
         |         "to": "$toDate"
         |      }
         |   ],
         |   "paginationStartIndex": 0,
         |   "rowsPerPage": 200,
         |   "forceDimensionDriven":false,
         |   "includeRowCount": false
         |}
      """.stripMargin

    val request: ReportingRequest = getReportingRequestSyncWithFactBias(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    val expected =
      s"""
         |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
         |FROM (SELECT ksnpo0.ad_id "Ad ID", to_char(ksnpo0.stats_date, 'YYYY-MM-DD') "Day", ROUND(ksnpo0."Average CPC", 10) "Average CPC", coalesce(ROUND(CASE WHEN ((ksnpo0."avg_pos" >= 0.1) AND (ksnpo0."avg_pos" <= 500)) THEN ksnpo0."avg_pos" ELSE 0.0 END, 10), 0.0) "Average Position", coalesce(ksnpo0."impressions", 1) "Impressions", coalesce(ROUND(ksnpo0."max_bid", 10), 0.0) "Max Bid", coalesce(ROUND(ksnpo0."spend", 10), 0.0) "Spend", ROUND(ksnpo0."CTR", 10) "CTR"
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_campaign_k_stats 4) */
         |                   ad_id, stats_date, SUM(impressions) AS "impressions", (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(CASE WHEN ((avg_pos >= 0.1) AND (avg_pos <= 500)) THEN avg_pos ELSE 0.0 END * impressions) / (SUM(impressions)) END) AS "avg_pos", SUM(spend) AS "spend", MAX(max_bid) AS "max_bid", (SUM(spend) / SUM(clicks)) AS "Average CPC", (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS "CTR"
         |            FROM k_stats_new_partitioning_one
         |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY ad_id, stats_date
         |
         |           ) ksnpo0
         |
         |
         |
         |) ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Best Candidates test for campaign adjustment in a_stats Fact View") {
    val jsonString = s"""{
                           "cube": "a_stats",
                           "selectFields": [
                             {
                               "field": "Campaign ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Advertiser ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Spend",
                               "alias": null,
                               "value": null
                             }
                           ],
                          "filterExpressions": [
                             {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                             {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ],
                           "paginationStartIndex":0,
                           "rowsPerPage":100
                          }"""

    val requestOption = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(requestOption.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val factBest = queryPipelineTry.get.factBestCandidate
    assert(factBest.isDefined)
    assert(factBest.get.fact.isInstanceOf[ViewTable])
    assert(factBest.get.fact.asInstanceOf[ViewTable].name == "campaign_adjustment_view")
    factBest.get.fact.asInstanceOf[ViewTable].postValidate(pubfact5())
  }
  test("Best Candidates test for account adjustment in a_stats Fact View") {
    val jsonString = s"""{
                           "cube": "a_stats",
                           "selectFields": [
                             {
                               "field": "Advertiser ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Spend",
                               "alias": null,
                               "value": null
                             }
                           ],
                          "filterExpressions": [
                             {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                             {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ],
                           "paginationStartIndex":0,
                           "rowsPerPage":100
                          }"""

    val requestOption = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(requestOption.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val factBest = queryPipelineTry.get.factBestCandidate
    assert(factBest.isDefined)
    assert(factBest.get.fact.isInstanceOf[ViewTable])
    assert(factBest.get.fact.asInstanceOf[ViewTable].name == "account_adjustment_view")
  }

  test("succesfully generate query with DayColumn annotation on Day column which is of IntType") {
    val jsonString = s"""{
                           "cube": "publisher_stats_int",
                           "selectFields": [
                             {
                               "field": "Publisher ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Spend",
                               "alias": null,
                               "value": null
                             }
                           ],
                          "filterExpressions": [
                             {"field": "Publisher ID", "operator": "=", "value": "12345"},
                             {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ],
                           "paginationStartIndex":0,
                           "rowsPerPage":100
                          }"""

    val requestOption = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), PublisherSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(requestOption.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    val expected = s"""SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
                      |FROM (SELECT vps0.publisher_id "Publisher ID", coalesce(ROUND(vps0."spend", 10), 0.0) "Spend"
                      |      FROM (SELECT
                      |                   publisher_id, SUM(spend) AS "spend"
                      |            FROM v_publisher_stats
                      |            WHERE (publisher_id = 12345) AND (date_sid >= to_number(to_char(trunc(to_date('$fromDate', 'YYYY-MM-DD')), 'YYYYMMDD')) AND date_sid <= to_number(to_char(trunc(to_date('$toDate', 'YYYY-MM-DD')), 'YYYYMMDD')))
                      |            GROUP BY publisher_id
                      |
                     |           ) vps0
                      |
                     |)
                      |   ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100""".stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("succesfully generate query with DayColumn annotation on Day column which is of StrType") {
    val jsonString = s"""{
                           "cube": "publisher_stats_str",
                           "selectFields": [
                             {
                               "field": "Publisher ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Spend",
                               "alias": null,
                               "value": null
                             }
                           ],
                          "filterExpressions": [
                             {"field": "Publisher ID", "operator": "=", "value": "12345"},
                             {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ],
                           "paginationStartIndex":0,
                           "rowsPerPage":100
                          }"""

    val requestOption = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), PublisherSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(requestOption.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    val expected = s"""SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
                     |FROM (SELECT vps0.publisher_id "Publisher ID", coalesce(ROUND(vps0."spend", 10), 0.0) "Spend"
                     |      FROM (SELECT
                     |                   publisher_id, SUM(spend) AS "spend"
                     |            FROM v_publisher_stats
                     |            WHERE (publisher_id = 12345) AND (date_sid >= to_char(trunc(to_date('$fromDate', 'YYYY-MM-DD')), 'YYYYMMDD') AND date_sid <= to_char(trunc(to_date('$toDate', 'YYYY-MM-DD')), 'YYYYMMDD'))
                     |            GROUP BY publisher_id
                     |
                     |           ) vps0
                     |
                     |)
                     |   ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100""".stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

 test("successfully generate fact driven query with outer filter") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Campaign ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Ad Group ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Ad Group Status",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Advertiser Status",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Campaign Name",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Impressions",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "CTR",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                             {"operator": "outer", "outerFilters": [
                                  {"field": "Ad Group ID", "operator": "isnull"},
                                  {"field": "Ad Group Status", "operator": "=", "value":"ON"}
                                  ]
                             },
                              {"field": "Reseller ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ],
                           "forceDimensionDriven": false
                         }"""

    val request: ReportingRequest = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), ResellerSchema).toOption.get
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = s"""
                      |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
                      |FROM (SELECT ago3.campaign_id "Campaign ID", ago3.id "Ad Group ID", ago3."Ad Group Status" "Ad Group Status", ao1."Advertiser Status" "Advertiser Status", co2.campaign_name "Campaign Name", coalesce(af0."impressions", 1) "Impressions", ROUND(af0."CTR", 10) "CTR"
                      |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
                      |                   advertiser_id, campaign_id, ad_group_id, SUM(impressions) AS "impressions", (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS "CTR"
                      |            FROM ad_fact1 FactAlias
                      |            WHERE (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
                      |            GROUP BY advertiser_id, campaign_id, ad_group_id
                      |
                      |           ) af0
                      |           INNER JOIN
                      |           (SELECT  DECODE(status, 'ON', 'ON', 'OFF') AS "Advertiser Status", id
                      |            FROM advertiser_oracle
                      |            WHERE (managed_by = 12345)
                      |             )
                      |           ao1 ON (af0.advertiser_id = ao1.id)
                      |           INNER JOIN
                      |           (SELECT /*+ CampaignHint */ advertiser_id, campaign_name, id
                      |            FROM campaign_oracle
                      |
                      |             )
                      |           co2 ON ( af0.advertiser_id = co2.advertiser_id AND af0.campaign_id = co2.id)
                      |           INNER JOIN
                      |           (SELECT  advertiser_id, campaign_id, DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", id
                      |            FROM ad_group_oracle
                      |
                      |             )
                      |           ago3 ON ( af0.advertiser_id = ago3.advertiser_id AND af0.ad_group_id = ago3.id)
                      |
                      |) WHERE ( "Ad Group ID"   IS NULL) AND ( "Ad Group Status"   = 'ON')
                      |   ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
                      |""".stripMargin


    result should equal (expected)(after being whiteSpaceNormalised)
  }

  test("successfully generate dim driven dim only query with outer filters and order by") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             { "field": "Advertiser ID" },
                             { "field": "Campaign ID" },
                             { "field": "Campaign Name" },
                             { "field": "Ad Group ID" },
                             { "field": "Ad Group Status" },
                             { "field" : "Source", "value" : "2", "alias" : "Source"}
                           ],
                          "filterExpressions": [
                             {"operator": "outer", "outerFilters": [
                                  {"field": "Ad Group ID", "operator": "isnull"}
                                  ]
                             },
                             {"field": "Advertiser Status", "operator": "=", "value": "ON"},
                             {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                             {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ],"sortBy": [
                              {"field": "Campaign ID", "order": "Asc"}
                           ],
                           "paginationStartIndex":0,
                           "rowsPerPage":100,
                           "forceDimensionDriven": false
                          }"""

    val requestOption = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(requestOption.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = """
                     |SELECT  *
                     |      FROM (SELECT ao0.id "Advertiser ID", co1.id "Campaign ID", co1.campaign_name "Campaign Name", ago2.id "Ad Group ID", ago2."Ad Group Status" "Ad Group Status", '2' AS "Source", ROWNUM as ROW_NUMBER
                     |            FROM
                     |               ( (SELECT  campaign_id, advertiser_id, DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", id
                     |            FROM ad_group_oracle
                     |            WHERE (advertiser_id = 12345)
                     |            ORDER BY 1 ASC  ) ago2
                     |          INNER JOIN
                     |            (SELECT /*+ CampaignHint */ id, advertiser_id, campaign_name
                     |            FROM campaign_oracle
                     |            WHERE (advertiser_id = 12345)
                     |             ) co1
                     |              ON( ago2.advertiser_id = co1.advertiser_id AND ago2.campaign_id = co1.id )
                     |               INNER JOIN
                     |            (SELECT  id
                     |            FROM advertiser_oracle
                     |            WHERE (id = 12345) AND (DECODE(status, 'ON', 'ON', 'OFF') = 'ON')
                     |             ) ao0
                     |              ON( co1.advertiser_id = ao0.id )
                     |               )
                     |
                     |           )
                     |            WHERE ( "Ad Group ID"   IS NULL) AND ROW_NUMBER >= 1 AND ROW_NUMBER <= 100
                     |           """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Fact Driven Multidimensional query with outer filters and dim sortBy ") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Keyword ID"},
                            {"field": "Keyword Value"},
                            {"field": "Campaign Name"},
                            {"field": "Ad Group ID"},
                            {"field": "Ad Group Status"},
                            {"field": "Ad Group Name"},
                            {"field": "Ad Title"},
                            {"field": "Impressions"},
                            {"field": "CTR"}
                          ],
                          "filterExpressions": [
                             {"operator": "outer", "outerFilters": [
                                  {"field": "Ad Group ID", "operator": "isnull"},
                                  {"field": "Ad Group Status", "operator": "=", "value":"ON"}
                                  ]
                             },
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "sortBy": [
                            {"field": "Campaign Name", "order": "Asc"}
                          ],
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected =
      s"""
         |SELECT "Keyword ID", "Keyword Value", "Campaign Name", "Ad Group ID", "Ad Group Status", "Ad Group Name", "Ad Title", impressions AS "Impressions", CTR AS "CTR"
         |FROM (SELECT f0.keyword_id "Keyword ID", t4.value "Keyword Value", co1.campaign_name "Campaign Name", f0.ad_group_id "Ad Group ID", ago2."Ad Group Status" "Ad Group Status", ago2.name "Ad Group Name", ado3.title "Ad Title", SUM(impressions) AS impressions, (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS CTR, SUM(clicks) AS clicks
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_campaign_k_stats 4) */
         |                   ad_group_id, advertiser_id, ad_id, campaign_id, keyword_id, SUM(CASE WHEN ((clicks >= 1) AND (clicks <= 800)) THEN clicks ELSE 0 END) AS clicks, SUM(impressions) AS impressions
         |            FROM fact1 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY ad_group_id, advertiser_id, ad_id, campaign_id, keyword_id
         |
         |           ) f0
         |                     LEFT OUTER JOIN
         |           (SELECT /*+ CampaignHint */ campaign_name, id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           co1 ON ( f0.advertiser_id = co1.advertiser_id AND f0.campaign_id = co1.id)
         |           LEFT OUTER JOIN
         |           (SELECT  campaign_id, name, DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", id, advertiser_id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           ago2 ON ( f0.advertiser_id = ago2.advertiser_id AND f0.ad_group_id = ago2.id)
         |           LEFT OUTER JOIN
         |           (SELECT  ad_group_id, campaign_id, title, id, advertiser_id
         |            FROM ad_dim_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           ado3 ON ( f0.advertiser_id = ado3.advertiser_id AND f0.ad_id = ado3.id)
         |           LEFT OUTER JOIN
         |           (SELECT  parent_id, value, id, advertiser_id
         |            FROM targetingattribute
         |            WHERE (advertiser_id = 12345)
         |             )
         |           t4 ON ( f0.advertiser_id = t4.advertiser_id AND f0.keyword_id = t4.id)
         |
 |          GROUP BY f0.keyword_id, t4.value, co1.campaign_name, f0.ad_group_id, ago2."Ad Group Status", ago2.name, ado3.title
         |) WHERE ( "Ad Group ID"   IS NULL) AND ( "Ad Group Status"   = 'ON')
         |   ORDER BY "Campaign Name" ASC NULLS LAST
         |""".stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Group by over derived expression with outer filters") {
    val jsonString = s"""{
                           "cube": "k_stats",
                           "selectFields": [
                             {"field": "Keyword ID"},
                             {"field": "Campaign ID"},
                             {"field": "Month"},
                             {"field": "Ad Group ID"},
                             {"field": "Ad Group Status"},
                             {"field": "Week"},
                             {"field": "Day"},
                             {"field": "Impressions"},
                             {"field": "Clicks"},
                             {"field": "CTR"}
                           ],
                           "filterExpressions": [
                             {"operator": "outer", "outerFilters": [
                                  {"field": "Ad Group ID", "operator": "isnull"},
                                  {"field": "Ad Group Status", "operator": "=", "value":"ON"}
                                  ]
                             },
                             {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                             {"field": "Advertiser ID", "operator": "=", "value": "12345"}],
                           "paginationStartIndex":0,
                           "rowsPerPage":100
                         }"""

    val request: ReportingRequest = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema).toOption.get
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = s"""
                      |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
                      |FROM (SELECT f0.keyword_id "Keyword ID", ago1.campaign_id "Campaign ID", f0."Month" "Month", ago1.id "Ad Group ID", ago1."Ad Group Status" "Ad Group Status", f0."Week" "Week", to_char(f0.stats_date, 'YYYY-MM-DD') "Day", coalesce(f0."impressions", 1) "Impressions", coalesce(f0."clicks", 0) "Clicks", ROUND(f0."CTR", 10) "CTR"
                      |      FROM (SELECT /*+ PARALLEL_INDEX(cb_campaign_k_stats 4) CONDITIONAL_HINT1 CONDITIONAL_HINT2 CONDITIONAL_HINT4 */
                      |                   stats_date, ad_group_id, campaign_id, keyword_id, TRUNC(stats_date, 'MM') AS "Month", TRUNC(stats_date, 'IW') AS "Week", SUM(CASE WHEN ((clicks >= 1) AND (clicks <= 800)) THEN clicks ELSE 0 END) AS "clicks", SUM(impressions) AS "impressions", (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS "CTR"
                      |            FROM fact2 FactAlias
                      |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
                      |            GROUP BY stats_date, ad_group_id, campaign_id, keyword_id, TRUNC(stats_date, 'MM'), TRUNC(stats_date, 'IW')
                      |
                      |           ) f0
                      |           LEFT OUTER JOIN
                      |           (SELECT  DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", campaign_id, id, advertiser_id
                      |            FROM ad_group_oracle
                      |            WHERE (advertiser_id = 12345)
                      |             )
                      |           ago1 ON (f0.ad_group_id = ago1.id)
                      |
                      |) WHERE ( "Ad Group ID"   IS NULL) AND ( "Ad Group Status"   = 'ON')
                      |   ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100
                      |""".stripMargin


    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("PowerEditor: Use case1") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Campaign ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Campaign Name",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Ad Group ID",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                             {"operator": "outer", "outerFilters": [
                                  {"field": "Ad Group ID", "operator": "isnull"}
                                  ]
                             },
                              {"field": "Campaign Status", "operator": "=", "value": "ON"},
                              {"field": "Reseller ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ],
                           "forceDimensionDriven": true
                         }"""

    val request: ReportingRequest = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), ResellerSchema).toOption.get
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))


    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = s"""
                      |SELECT  *
                      |      FROM (SELECT co1.id "Campaign ID", co1.campaign_name "Campaign Name", ago2.id "Ad Group ID", ROWNUM as ROW_NUMBER
                      |            FROM
                      |               ( (SELECT  advertiser_id, campaign_id, id
                      |            FROM ad_group_oracle
                      |
                      |             ) ago2
                      |          INNER JOIN
                      |            (SELECT /*+ CampaignHint */ advertiser_id, campaign_name, id
                      |            FROM campaign_oracle
                      |            WHERE (DECODE(status, 'ON', 'ON', 'OFF') = 'ON')
                      |             ) co1
                      |              ON( ago2.advertiser_id = co1.advertiser_id AND ago2.campaign_id = co1.id )
                      |               INNER JOIN
                      |            (SELECT  id
                      |            FROM advertiser_oracle
                      |            WHERE (managed_by = 12345)
                      |             ) ao0
                      |              ON( co1.advertiser_id = ao0.id )
                      |               )
                      |
                      |           )
                      |            WHERE ( "Ad Group ID"   IS NULL) AND ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
                      |""".stripMargin


    result should equal (expected)(after being whiteSpaceNormalised)
  }

  test("PowerEditor : Use case Dim Only Query Ad Level") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Ad Group ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Campaign Name",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Ad Title",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                             {"operator": "outer", "outerFilters": [
                                  {"field": "Ad Group ID", "operator": "isnull"}
                                  ]
                             },
                              {"field": "Campaign Status", "operator": "=", "value": "ON"},
                              {"field": "Reseller ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ],
                           "forceDimensionDriven": true
                         }"""

    val request: ReportingRequest = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), ResellerSchema).toOption.get
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = s"""
                      |SELECT  *
                      |      FROM (SELECT ado2.ad_group_id "Ad Group ID", co1.campaign_name "Campaign Name", ado2.title "Ad Title", ROWNUM as ROW_NUMBER
                      |            FROM
                      |               ( (SELECT  campaign_id, id, title, ad_group_id, advertiser_id
                      |            FROM ad_dim_oracle
                      |
                      |             ) ado2
                      |          INNER JOIN
                      |            (SELECT /*+ CampaignHint */ advertiser_id, campaign_name, id
                      |            FROM campaign_oracle
                      |            WHERE (DECODE(status, 'ON', 'ON', 'OFF') = 'ON')
                      |             ) co1
                      |              ON( ado2.advertiser_id = co1.advertiser_id AND ado2.campaign_id = co1.id )
                      |               INNER JOIN
                      |            (SELECT  id
                      |            FROM advertiser_oracle
                      |            WHERE (managed_by = 12345)
                      |             ) ao0
                      |              ON( co1.advertiser_id = ao0.id )
                      |               )
                      |
                      |           )
                      |            WHERE ( "Ad Group ID"   IS NULL) AND ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
                      |""".stripMargin


    result should equal (expected)(after being whiteSpaceNormalised)
  }

  test("where clause: ensure duplicate filter mappings are not propagated into the where clause") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Keyword ID"},
                              {"field": "Impressions"},
                              {"field": "Device ID"},
                              {"field": "Network Type"},
                              {"field": "Pricing Type"},
                              {"field": "Campaign Status"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Source", "operator": "=", "value": "1" },
                              {"field": "Source Name", "operator": "In", "values": [ "Native", "Search" ] }
                          ],
                          "sortBy": [
                              {"field": "Campaign Status", "order": "ASC"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    val expected =
      s"""
         |SELECT *
         |FROM (SELECT t3.id "Keyword ID", coalesce(f0."impressions", 1) "Impressions", COALESCE(f0.device_id, 'UNKNOWN') "Device ID", COALESCE(f0.network_type, 'NONE') "Network Type", COALESCE(f0.pricing_type, 'NONE') "Pricing Type", co1."Campaign Status" "Campaign Status"
         |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) CONDITIONAL_HINT1 CONDITIONAL_HINT2 CONDITIONAL_HINT3 */
         |                   CASE WHEN (device_id IN (1)) THEN 'Desktop' WHEN (device_id IN (2)) THEN 'Tablet' WHEN (device_id IN (3)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END device_id, DECODE(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_SYNDICATION', 'Content Syndication', 'EXTERNAL', 'Yahoo Partners', 'INTERNAL', 'Yahoo Properties', 'NONE') network_type, CASE WHEN (pricing_type IN (1)) THEN 'CPC' WHEN (pricing_type IN (6)) THEN 'CPV' WHEN (pricing_type IN (2)) THEN 'CPA' WHEN (pricing_type IN (-10)) THEN 'CPE' WHEN (pricing_type IN (-20)) THEN 'CPF' WHEN (pricing_type IN (7)) THEN 'CPCV' WHEN (pricing_type IN (3)) THEN 'CPM' ELSE 'NONE' END pricing_type, campaign_id, keyword_id, SUM(impressions) AS "impressions"
         |            FROM fact2 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_source IN (1,2)) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY CASE WHEN (device_id IN (1)) THEN 'Desktop' WHEN (device_id IN (2)) THEN 'Tablet' WHEN (device_id IN (3)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END, DECODE(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_SYNDICATION', 'Content Syndication', 'EXTERNAL', 'Yahoo Partners', 'INTERNAL', 'Yahoo Properties', 'NONE'), CASE WHEN (pricing_type IN (1)) THEN 'CPC' WHEN (pricing_type IN (6)) THEN 'CPV' WHEN (pricing_type IN (2)) THEN 'CPA' WHEN (pricing_type IN (-10)) THEN 'CPE' WHEN (pricing_type IN (-20)) THEN 'CPF' WHEN (pricing_type IN (7)) THEN 'CPCV' WHEN (pricing_type IN (3)) THEN 'CPM' ELSE 'NONE' END, campaign_id, keyword_id
         |
         |           ) f0
         |           RIGHT OUTER JOIN
         |               ( (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  id, parent_id, advertiser_id
         |            FROM targetingattribute
         |            WHERE (advertiser_id = 12345)
         |             ) WHERE ROWNUM <= 120) D ) WHERE ROW_NUMBER >= 21 AND ROW_NUMBER <= 120) t3
         |           INNER JOIN
         |            (SELECT  id, campaign_id, advertiser_id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 12345)
         |             ) ago2
         |              ON( t3.advertiser_id = ago2.advertiser_id AND t3.parent_id = ago2.id )
         |               INNER JOIN
         |            (SELECT /*+ CampaignHint */ DECODE(status, 'ON', 'ON', 'OFF') AS "Campaign Status", id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             ) co1
         |              ON( ago2.advertiser_id = co1.advertiser_id AND ago2.campaign_id = co1.id )
         |               )  ON (f0.keyword_id = t3.id)
         |
 |)
         |   ORDER BY "Campaign Status" ASC NULLS LAST
      """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("test using alias to join dimension table") {
    val jsonString =
      s"""{
                          "cube": "performance_stats",
                          "selectFields": [
                            {"field": "Address"},
                            {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Restaurant ID", "operator": "=", "value": "12345"},
                            {"field": "Advertiser ID", "operator": "=", "value": "12345"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    
    val expected =
      s"""|SELECT *
          |FROM (SELECT ro1.address "Address", coalesce(af0."impressions", 1) "Impressions"
          |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_ad_stats 4) */
          |                   advertiser_id, SUM(impressions) AS "impressions"
          |            FROM ad_fact1 FactAlias
          |            WHERE (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
          |            GROUP BY advertiser_id
          |
          |           ) af0
          |           RIGHT OUTER JOIN
          |                (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  address, id
          |            FROM restaurant_oracle
          |            WHERE (id = 12345)
          |             ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100) ro1
          |            ON (af0.advertiser_id = ro1.id)
          |
          |)
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Successfully generated Outer Group By Query with dim non id field and fact field") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Campaign Name",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Spend",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ]
                           }""".stripMargin

    val request = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    queryPipelineTry.get.bestDimCandidates.foreach{db=> assert(db.hasPKRequested == false)}

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery
    assert(query.aliasColumnMap.map(_._1).toSet == Set("Spend", "Campaign Name"))


    val expected =
      s"""
         |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT "Campaign Name", spend AS "Spend"
         |FROM (SELECT co1.campaign_name "Campaign Name", SUM(spend) AS spend
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
         |                   advertiser_id, campaign_id, SUM(spend) AS spend
         |            FROM ad_fact1 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY advertiser_id, campaign_id
         |
         |           ) af0
         |                     LEFT OUTER JOIN
         |           (SELECT /*+ CampaignHint */ campaign_name, id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           co1 ON ( af0.advertiser_id = co1.advertiser_id AND af0.campaign_id = co1.id)
         |
 |          GROUP BY co1.campaign_name
         |)
         |   ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
       """.stripMargin

    result should equal (expected)(after being whiteSpaceNormalised)
  }

  test("Successfully generated Outer Group By Query with dim non id field and derived fact field having dim source col") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Campaign Name",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Source"
                             },
                             {
                               "field": "N Spend",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ]
                           }""".stripMargin

    val request = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    queryPipelineTry.get.bestDimCandidates.foreach{db=> assert(db.hasPKRequested == false)}

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery
    assert(query.aliasColumnMap.map(_._1).toSet == Set("N Spend", "Campaign Name", "Source"))

    val expected =
      s"""
         |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT "Campaign Name", "Source", DECODE(stats_source, 1, spend, 0.0) AS "N Spend"
         |FROM (SELECT co1.campaign_name "Campaign Name", af0.stats_source "Source", SUM(spend) AS spend, stats_source AS stats_source
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
         |                   advertiser_id, campaign_id, stats_source, SUM(spend) AS spend
         |            FROM ad_fact1 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY advertiser_id, campaign_id, stats_source
         |
         |           ) af0
         |                     LEFT OUTER JOIN
         |           (SELECT /*+ CampaignHint */ campaign_name, id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           co1 ON ( af0.advertiser_id = co1.advertiser_id AND af0.campaign_id = co1.id)
         |
 |          GROUP BY co1.campaign_name, af0.stats_source, stats_source
         |)
         |   ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
       """.stripMargin

    result should equal (expected)(after being whiteSpaceNormalised)
  }

  test("Successfully generated timeseries Outer Group By Query with dim non id field and fact field") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Day",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Campaign Name",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Spend",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ]
                           }""".stripMargin

    val request = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    queryPipelineTry.get.bestDimCandidates.foreach{db=> assert(db.hasPKRequested == false)}

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    

    val expected =
      s"""
         |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT "Day", "Campaign Name", spend AS "Spend"
         |FROM (SELECT to_char(af0.stats_date, 'YYYY-MM-DD') "Day", co1.campaign_name "Campaign Name", SUM(spend) AS spend
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
         |                   advertiser_id, campaign_id, stats_date, SUM(spend) AS spend
         |            FROM ad_fact1 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY advertiser_id, campaign_id, stats_date
         |
         |           ) af0
         |                     LEFT OUTER JOIN
         |           (SELECT /*+ CampaignHint */ campaign_name, id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           co1 ON ( af0.advertiser_id = co1.advertiser_id AND af0.campaign_id = co1.id)
         |
 |          GROUP BY to_char(af0.stats_date, 'YYYY-MM-DD'), co1.campaign_name
         |)
         |   ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
       """.stripMargin

    result should equal (expected)(after being whiteSpaceNormalised)
  }

  test("Successfully generated Outer Group By Query with 2 dimension non id fields") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Campaign Name",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Advertiser Currency",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Spend",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ]
                           }""".stripMargin

    val request = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    queryPipelineTry.get.bestDimCandidates.foreach{db=> assert(db.hasPKRequested == false)}

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery
    assert(query.aliasColumnMap.map(_._1).toSet == Set("Spend", "Advertiser Currency", "Campaign Name"))


    val expected =
      s"""
         |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT "Campaign Name", "Advertiser Currency", "spend" AS "Spend"
         |FROM (SELECT co2.campaign_name "Campaign Name", ao1.currency "Advertiser Currency", SUM(spend) AS spend
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
         |                   advertiser_id, campaign_id, SUM(spend) AS "spend"
         |            FROM ad_fact1 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY advertiser_id, campaign_id
         |
         |           ) af0
         |                     LEFT OUTER JOIN
         |           (SELECT  currency, id
         |            FROM advertiser_oracle
         |            WHERE (id = 12345)
         |             )
         |           ao1 ON (af0.advertiser_id = ao1.id)
         |           LEFT OUTER JOIN
         |           (SELECT /*+ CampaignHint */ advertiser_id, campaign_name, id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           co2 ON (af0.campaign_id = co2.id)
         |
 |          GROUP BY "Campaign Name", "Advertiser Currency"
         |)
         |   ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
       """.stripMargin
  }

  test("Should not generate Outer Group By Query context with 2 dimension non id fields and one fact higher level ID field than best dims") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Campaign Name",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Advertiser Currency",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Ad Group ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Spend",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ]
                           }""".stripMargin

    val request = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    queryPipelineTry.get.bestDimCandidates.filter(_.dim.name=="adgroup").foreach{db=> assert(db.hasPKRequested == true, "Should not trigger outer group by")}

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery
    assert(query.aliasColumnMap.map(_._1).toSet == Set("Spend", "Advertiser Currency", "Ad Group ID", "Campaign Name"))


    val expected =
      s"""
         |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
         |FROM (SELECT co2.campaign_name "Campaign Name", ao1.currency "Advertiser Currency", af0.ad_group_id "Ad Group ID", coalesce(ROUND(af0."spend", 10), 0.0) "Spend"
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
         |                   advertiser_id, campaign_id, ad_group_id, SUM(spend) AS "spend"
         |            FROM ad_fact1 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY advertiser_id, campaign_id, ad_group_id
         |
         |           ) af0
         |           LEFT OUTER JOIN
         |           (SELECT  currency, id
         |            FROM advertiser_oracle
         |            WHERE (id = 12345)
         |             )
         |           ao1 ON (af0.advertiser_id = ao1.id)
         |           LEFT OUTER JOIN
         |           (SELECT /*+ CampaignHint */ advertiser_id, campaign_name, id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           co2 ON ( af0.advertiser_id = co2.advertiser_id AND af0.campaign_id = co2.id)
         |
 |)
         |   ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
         |
       """.stripMargin
    result should equal (expected)(after being whiteSpaceNormalised)
  }

  test("Successfully generated Outer Group By Query with 2 dimension non id fields and and two fact transitively dependent cols") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Campaign Name",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Advertiser Currency",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Count",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Custom",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Avg",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Max",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Duplicate Spend",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Min",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Average CPC Cents",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Average CPC",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Spend",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$toDate", "to": "$toDate"}
                           ]
                           }""".stripMargin

    val request = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    queryPipelineTry.get.bestDimCandidates.foreach{db=> assert(db.hasPKRequested == false)}

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery
    assert(query.aliasColumnMap.map(_._1).toSet == Set("Custom", "Duplicate Spend", "Max", "Min", "Avg", "Spend","Advertiser Currency"
      , "Average CPC Cents", "Count", "Average CPC", "Campaign Name"))


    val expected =
      s"""
         |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT "Campaign Name", "Advertiser Currency", Count AS "Count", custom_col AS "Custom", avg_col AS "Avg", max_col AS "Max", spend AS "Duplicate Spend", min_col AS "Min", (CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END) * 100 AS "Average CPC Cents", CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END AS "Average CPC", spend AS "Spend"
         |FROM (SELECT co2.campaign_name "Campaign Name", ao1.currency "Advertiser Currency", COUNT(*) AS Count, (SUM(clicks * max_bid)) AS custom_col, AVG(avg_col) AS avg_col, MAX(max_col) AS max_col, SUM(spend) AS spend, MIN(min_col) AS min_col, MAX(max_bid) AS max_bid, SUM(clicks) AS clicks
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
         |                   advertiser_id, campaign_id, SUM(CASE WHEN ((clicks >= 1) AND (clicks <= 800)) THEN clicks ELSE 0 END) AS clicks, MAX(max_bid) AS max_bid, SUM(spend) AS spend, MIN(min_col) AS min_col, MAX(max_col) AS max_col, AVG(CASE WHEN ((avg_col >= 0) AND (avg_col <= 100000)) THEN avg_col ELSE 0 END) AS avg_col, COUNT(*) AS Count
         |            FROM ad_fact1 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_date >= trunc(to_date('$toDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY advertiser_id, campaign_id
         |
         |           ) af0
         |                     LEFT OUTER JOIN
         |           (SELECT  currency, id
         |            FROM advertiser_oracle
         |            WHERE (id = 12345)
         |             )
         |           ao1 ON (af0.advertiser_id = ao1.id)
         |           LEFT OUTER JOIN
         |           (SELECT /*+ CampaignHint */ advertiser_id, campaign_name, id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           co2 ON ( af0.advertiser_id = co2.advertiser_id AND af0.campaign_id = co2.id)
         |
 |          GROUP BY co2.campaign_name, ao1.currency
         |)
         |   ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
         |
       """
       .stripMargin
    

    result should equal (expected)(after being whiteSpaceNormalised)
  }

  test("Successfully generated Outer Group By Query with mutlifield fact and dim filters") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Campaign Name",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Advertiser Currency",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Business Name",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Business Name 2",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Average CPC Cents",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Average CPC",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Spend",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "N Spend",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$toDate", "to": "$toDate"},
                              {"field": "Spend", "operator": "==", "compareTo": "N Spend"},
                              {"field": "Business Name", "operator": "==", "compareTo": "Business Name 2"}
                           ]
                           }""".stripMargin

    val request = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    queryPipelineTry.get.bestDimCandidates.foreach{db=> assert(db.hasPKRequested == false)}

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString


    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery
    assert(query.aliasColumnMap.map(_._1).toSet == Set("Business Name 2", "Business Name", "N Spend", "Spend","Advertiser Currency"
      , "Average CPC Cents", "Average CPC", "Campaign Name"))


    val expected =
      s"""
         |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT "Campaign Name", "Advertiser Currency", "Business Name", "Business Name 2", (CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END) * 100 AS "Average CPC Cents", CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END AS "Average CPC", spend AS "Spend", DECODE(stats_source, 1, spend, 0.0) AS "N Spend"
         |FROM (SELECT co2.campaign_name "Campaign Name", ao1.currency "Advertiser Currency", af0."Business Name" "Business Name", af0."Business Name 2" "Business Name 2", SUM(spend) AS spend, SUM(clicks) AS clicks, af0.stats_source stats_source
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
         |                   advertiser_id, campaign_id, DECODE(stats_source, 1, Native, 2, Search, Unknown) AS "Business Name", DECODE(stats_source, 1, Expensive, 2, Cheap, Unknown) AS "Business Name 2", SUM(CASE WHEN ((clicks >= 1) AND (clicks <= 800)) THEN clicks ELSE 0 END) AS clicks, SUM(spend) AS spend, stats_source
         |            FROM ad_fact1 FactAlias
         |            WHERE (advertiser_id = 12345) AND (DECODE(stats_source, 1, Native, 2, Search, Unknown) = DECODE(stats_source, 1, Native, 2, Search, Unknown)) AND (stats_date >= trunc(to_date('$toDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY advertiser_id, campaign_id, DECODE(stats_source, 1, Native, 2, Search, Unknown), DECODE(stats_source, 1, Expensive, 2, Cheap, Unknown), stats_source
         |            HAVING (SUM(spend) = SUM(DECODE(stats_source, 1, spend, 0.0)))
         |           ) af0
         |                     LEFT OUTER JOIN
         |           (SELECT  currency, id
         |            FROM advertiser_oracle
         |            WHERE (id = 12345)
         |             )
         |           ao1 ON (af0.advertiser_id = ao1.id)
         |           LEFT OUTER JOIN
         |           (SELECT /*+ CampaignHint */ advertiser_id, campaign_name, id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           co2 ON ( af0.advertiser_id = co2.advertiser_id AND af0.campaign_id = co2.id)
         |
 |          GROUP BY co2.campaign_name, ao1.currency, af0."Business Name", af0."Business Name 2", af0.stats_source
         |)
         |   ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
         |
       """
        .stripMargin


    result should equal (expected)(after being whiteSpaceNormalised)
  }

  test("Successfully generated Outer Group By Query with Lowest level FK col is requested") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Campaign Status",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Advertiser Name"
                             },
                             {
                               "field": "Advertiser ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Spend",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$toDate", "to": "$toDate"}
                           ]
                           }""".stripMargin

    val request = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery
    assert(query.aliasColumnMap.map(_._1).toSet == Set("Spend", "Advertiser ID", "Advertiser Name", "Campaign Status"))


    val expected =
      s"""
         |
         |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT "Campaign Status", "Advertiser Name", "Advertiser ID", spend AS "Spend"
         |FROM (SELECT co2."Campaign Status" "Campaign Status", ao1.name "Advertiser Name", co2.advertiser_id "Advertiser ID", SUM(spend) AS spend
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
         |                   advertiser_id, campaign_id, SUM(spend) AS spend
         |            FROM ad_fact1 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_date >= trunc(to_date('$toDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY advertiser_id, campaign_id
         |
         |           ) af0
         |                     LEFT OUTER JOIN
         |           (SELECT  name, id
         |            FROM advertiser_oracle
         |            WHERE (id = 12345)
         |             )
         |           ao1 ON (af0.advertiser_id = ao1.id)
         |           LEFT OUTER JOIN
         |           (SELECT /*+ CampaignHint */ advertiser_id, DECODE(status, 'ON', 'ON', 'OFF') AS "Campaign Status", id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           co2 ON ( af0.advertiser_id = co2.advertiser_id AND af0.campaign_id = co2.id)
         |
 |          GROUP BY co2."Campaign Status", ao1.name, co2.advertiser_id
         |)
         |   ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
         |
         |
       """
        .stripMargin
    

    result should equal (expected)(after being whiteSpaceNormalised)
  }

  test("Successfully generated Outer Group By Query if fk col one level less than Highest dim candidate level is requested") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Ad Status",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Ad User Count Flag",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Ad Impressions Flag",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Campaign Name"
                             },
                             {
                               "field": "Pricing Type",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Spend",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "User Count",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Impressions",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$toDate", "to": "$toDate"},
                              {"field": "User Count", "operator": "==", "compareTo": "Impressions"}
                           ]
                           }""".stripMargin

    val request = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery
    assert(query.aliasColumnMap.map(_._1).toSet == Set("Ad Status", "Pricing Type", "User Count", "Ad User Count Flag", "Ad Impressions Flag", "Impressions", "Campaign Name", "Spend"))

    val expected =
      s"""
         |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT "Ad Status", "Ad User Count Flag", "Ad Impressions Flag", "Campaign Name", "Pricing Type", spend AS "Spend", user_count AS "User Count", impressions AS "Impressions"
         |FROM (SELECT ado2."Ad Status" "Ad Status", ado2.user_count "Ad User Count Flag", ado2.impressions "Ad Impressions Flag", co1.campaign_name "Campaign Name", af0.price_type "Pricing Type", SUM(af0.spend) AS spend, SUM(af0.user_count) AS user_count, SUM(af0.impressions) AS impressions
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
         |                   CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END price_type, advertiser_id, ad_id, campaign_id, SUM(impressions) AS impressions, SUM(spend) AS spend, SUM(user_count) AS user_count
         |            FROM ad_fact1 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_date >= trunc(to_date('$toDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END, advertiser_id, ad_id, campaign_id
         |            HAVING (SUM(user_count) = SUM(impressions))
         |           ) af0
         |                     LEFT OUTER JOIN
         |           (SELECT /*+ CampaignHint */ campaign_name, id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           co1 ON ( af0.advertiser_id = co1.advertiser_id AND af0.campaign_id = co1.id)
         |           LEFT OUTER JOIN
         |           (SELECT  campaign_id, DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Status", id, user_count, impressions, advertiser_id
         |            FROM ad_dim_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           ado2 ON ( af0.advertiser_id = ado2.advertiser_id AND af0.ad_id = ado2.id)
         |
 |          GROUP BY ado2."Ad Status", ado2.user_count, ado2.impressions, co1.campaign_name, af0.price_type
         |)
         |   ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
       """.stripMargin

    result should equal (expected)(after being whiteSpaceNormalised)
  }

  test("Successfully generated Outer Group By Query if OracleCustomRollup col is requested") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Campaign Name"
                             },
                             {
                               "field": "Average CPC",
                               "alias": null,
                               "value": null
                             },
                             {
                              "field": "Spend",
                              "alias": null,
                              "value": null
                              }
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$toDate", "to": "$toDate"}
                           ]
                           }""".stripMargin

    val request = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery
    assert(query.aliasColumnMap.map(_._1).toSet == Set("Spend", "Average CPC", "Campaign Name"))



    val expected =
      s"""
         |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT "Campaign Name", CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END AS "Average CPC", spend AS "Spend"
         |FROM (SELECT co1.campaign_name "Campaign Name", SUM(spend) AS spend, SUM(clicks) AS clicks
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
         |                   advertiser_id, campaign_id, SUM(CASE WHEN ((clicks >= 1) AND (clicks <= 800)) THEN clicks ELSE 0 END) AS clicks, SUM(spend) AS spend
         |            FROM ad_fact1 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_date >= trunc(to_date('$toDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY advertiser_id, campaign_id
         |
         |           ) af0
         |                     LEFT OUTER JOIN
         |           (SELECT /*+ CampaignHint */ campaign_name, id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           co1 ON ( af0.advertiser_id = co1.advertiser_id AND af0.campaign_id = co1.id)
         |
 |          GROUP BY co1.campaign_name
         |)
         |   ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
       """
        .stripMargin
    result should equal (expected)(after being whiteSpaceNormalised)
  }

  test("Successfully generated Outer Group By Query if OracleCustomRollup col with Derived Expression having rollups is requested") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Campaign Name"
                             },
                             {
                               "field": "Average Position",
                               "alias": null,
                               "value": null
                             },
                             {
                              "field": "Spend",
                              "alias": null,
                              "value": null
                              }
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$toDate", "to": "$toDate"}
                           ]
                           }""".stripMargin

    val request = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery]
    val queryCols = query.aliasColumnMap.map(_._1).toSet

    assert(queryCols == Set("Spend", "Average Position", "Campaign Name"))

    val result = query.asString
    


    val expected =
      s"""
         |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT "Campaign Name", avg_pos AS "Average Position", spend AS "Spend"
         |FROM (SELECT co1.campaign_name "Campaign Name", (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(CASE WHEN ((avg_pos >= 0.1) AND (avg_pos <= 500)) THEN avg_pos ELSE 0.0 END * impressions) / (SUM(impressions)) END) AS avg_pos, SUM(spend) AS spend, SUM(impressions) AS impressions
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
         |                   advertiser_id, campaign_id, (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(CASE WHEN ((avg_pos >= 0.1) AND (avg_pos <= 500)) THEN avg_pos ELSE 0.0 END * impressions) / (SUM(impressions)) END) AS avg_pos, SUM(impressions) AS impressions, SUM(spend) AS spend
         |            FROM ad_fact1 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_date >= trunc(to_date('$toDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY advertiser_id, campaign_id
         |
         |           ) af0
         |                     LEFT OUTER JOIN
         |           (SELECT /*+ CampaignHint */ campaign_name, id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           co1 ON ( af0.advertiser_id = co1.advertiser_id AND af0.campaign_id = co1.id)
         |
 |          GROUP BY co1.campaign_name
         |)
         |   ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
         |
       """
        .stripMargin

    result should equal (expected)(after being whiteSpaceNormalised)
  }

  test("Successfully generated Outer Group By Query if OracleCustomRollup col with Derived Expression having CustomRollup and DerCol are requested") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Campaign Name"
                             },
                             {
                               "field": "Average Position",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Average CPC"
                             },
                             {
                              "field": "Spend",
                              "alias": null,
                              "value": null
                              }
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$toDate", "to": "$toDate"}
                           ]
                           }""".stripMargin

    val request = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery
    assert(query.aliasColumnMap.map(_._1).toSet == Set("Spend", "Average Position", "Average CPC", "Campaign Name"))

    val expected =
      s"""
         |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT "Campaign Name", avg_pos AS "Average Position", CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END AS "Average CPC", spend AS "Spend"
         |FROM (SELECT co1.campaign_name "Campaign Name", (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(CASE WHEN ((avg_pos >= 0.1) AND (avg_pos <= 500)) THEN avg_pos ELSE 0.0 END * impressions) / (SUM(impressions)) END) AS avg_pos, SUM(spend) AS spend, SUM(impressions) AS impressions, SUM(clicks) AS clicks
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
         |                   advertiser_id, campaign_id, (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(CASE WHEN ((avg_pos >= 0.1) AND (avg_pos <= 500)) THEN avg_pos ELSE 0.0 END * impressions) / (SUM(impressions)) END) AS avg_pos, SUM(impressions) AS impressions, SUM(CASE WHEN ((clicks >= 1) AND (clicks <= 800)) THEN clicks ELSE 0 END) AS clicks, SUM(spend) AS spend
         |            FROM ad_fact1 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_date >= trunc(to_date('$toDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY advertiser_id, campaign_id
         |
         |           ) af0
         |                     LEFT OUTER JOIN
         |           (SELECT /*+ CampaignHint */ campaign_name, id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           co1 ON ( af0.advertiser_id = co1.advertiser_id AND af0.campaign_id = co1.id)
         |
 |          GROUP BY co1.campaign_name
         |)
         |   ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
       """
        .stripMargin

    result should equal (expected)(after being whiteSpaceNormalised)
  }

  test("Successfully generated Outer Group By Query if column is derived from dim column") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Campaign Name"
                             },
                             {
                               "field": "Advertiser ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "N Average CPC"
                             },
                             {
                              "field": "Spend",
                              "alias": null,
                              "value": null
                              }
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$toDate", "to": "$toDate"}
                           ]
                           }""".stripMargin

    val request = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery
    assert(query.aliasColumnMap.map(_._1).toSet == Set("Advertiser ID", "N Average CPC", "Campaign Name", "Spend"))

    val expected =
      s"""
         |
         |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT "Campaign Name", "Advertiser ID", CASE WHEN DECODE(stats_source, 1, clicks, 0.0) = 0 THEN 0.0 ELSE DECODE(stats_source, 1, spend, 0.0) / DECODE(stats_source, 1, clicks, 0.0) END AS "N Average CPC", spend AS "Spend"
         |FROM (SELECT co1.campaign_name "Campaign Name", co1.advertiser_id "Advertiser ID", SUM(spend) AS spend, SUM(clicks) AS clicks, af0.stats_source stats_source
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
         |                   advertiser_id, campaign_id, SUM(CASE WHEN ((clicks >= 1) AND (clicks <= 800)) THEN clicks ELSE 0 END) AS clicks, stats_source, SUM(spend) AS spend
         |            FROM ad_fact1 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_date >= trunc(to_date('$toDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY advertiser_id, campaign_id, stats_source
         |
         |           ) af0
         |                     LEFT OUTER JOIN
         |           (SELECT /*+ CampaignHint */ advertiser_id, campaign_name, id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           co1 ON ( af0.advertiser_id = co1.advertiser_id AND af0.campaign_id = co1.id)
         |
 |          GROUP BY co1.campaign_name, co1.advertiser_id, af0.stats_source
         |)
         |   ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
         |
       """
        .stripMargin


    result should equal (expected)(after being whiteSpaceNormalised)
  }

  test("Successfully generated Outer Group By Query if NoopRollupp column requeted") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Campaign Name"
                             },
                             {
                               "field": "Impression Share",
                               "alias": null,
                               "value": null
                             },
                             {
                              "field": "Spend",
                              "alias": null,
                              "value": null
                              }
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$toDate", "to": "$toDate"}
                           ]
                           }""".stripMargin

    val request = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery
    assert(query.aliasColumnMap.map(_._1).toSet == Set("Campaign Name", "Impression Share", "Spend"))

    val expected =
      s"""
         |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT "Campaign Name", "impression_share_rounded" AS "Impression Share", spend AS "Spend"
         |FROM (SELECT co1.campaign_name "Campaign Name", SUM(spend) AS spend, SUM(impressions) AS impressions, SUM(s_impressions) AS s_impressions, af0.show_flag show_flag, (ROUND((DECODE(MAX(show_flag), 1, ROUND(CASE WHEN SUM(s_impressions) = 0 THEN 0.0 ELSE SUM(impressions) / (SUM(s_impressions)) END, 4), NULL)), 5)) AS "impression_share_rounded"
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
         |                   advertiser_id, campaign_id, SUM(impressions) AS impressions, SUM(s_impressions) AS s_impressions, show_flag, SUM(spend) AS spend
         |            FROM ad_fact1 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_date >= trunc(to_date('$toDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY advertiser_id, campaign_id, show_flag
         |
         |           ) af0
         |                     LEFT OUTER JOIN
         |           (SELECT /*+ CampaignHint */ campaign_name, id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           co1 ON ( af0.advertiser_id = co1.advertiser_id AND af0.campaign_id = co1.id)
         |
 |          GROUP BY co1.campaign_name, af0.show_flag
         |)
         |   ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
       """
        .stripMargin

    result should equal (expected)(after being whiteSpaceNormalised)
  }


  test("successfully generate fact driven query with filter on FK and dimension attribute without including attribute in output") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Advertiser ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Impressions",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Average CPC",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                              {"field": "Ad Group Status", "operator": "=", "value": "ON"},
                              {"field": "Campaign ID", "operator": "In", "values": ["22222"]},
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ]
                         }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))


    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    
    val expected =
      s"""
         |SELECT "Advertiser ID", impressions AS "Impressions", CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END AS "Average CPC"
         |FROM (SELECT af0.advertiser_id "Advertiser ID", SUM(impressions) AS impressions, SUM(clicks) AS clicks, SUM(spend) AS spend
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
         |                   advertiser_id, campaign_id, ad_group_id, SUM(CASE WHEN ((clicks >= 1) AND (clicks <= 800)) THEN clicks ELSE 0 END) AS clicks, SUM(spend) AS spend, SUM(impressions) AS impressions
         |            FROM ad_fact1 FactAlias
         |            WHERE (advertiser_id = 12345) AND (campaign_id IN (22222)) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY advertiser_id, campaign_id, ad_group_id
         |
         |           ) af0
         |           INNER JOIN
         |           (SELECT  id, advertiser_id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 12345) AND (campaign_id IN (22222)) AND (DECODE(status, 'ON', 'ON', 'OFF') = 'ON')
         |             )
         |           ago1 ON ( af0.advertiser_id = ago1.advertiser_id AND af0.ad_group_id = ago1.id)
         |
         |          GROUP BY af0.advertiser_id
         |)
       """.stripMargin

    result should equal (expected)(after being whiteSpaceNormalised)
  }

  test("successfully generate fact driven query with filter on FK and dimension attribute without including attribute in output with attribute col as schema required field") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Advertiser ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Impressions",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Average CPC",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                              {"field": "Ad Group Status", "operator": "=", "value": "ON"},
                              {"field": "Campaign ID", "operator": "In", "values": ["22222"]},
                              {"field": "Reseller ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ]
                         }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString, ResellerSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))


    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = s"""
                      |SELECT "Advertiser ID", impressions AS "Impressions", CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END AS "Average CPC"
                      |FROM (SELECT af0.advertiser_id "Advertiser ID", SUM(impressions) AS impressions, SUM(clicks) AS clicks, SUM(spend) AS spend
                      |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
                      |                   advertiser_id, campaign_id, ad_group_id, SUM(CASE WHEN ((clicks >= 1) AND (clicks <= 800)) THEN clicks ELSE 0 END) AS clicks, SUM(spend) AS spend, SUM(impressions) AS impressions
                      |            FROM ad_fact1 FactAlias
                      |            WHERE (campaign_id IN (22222)) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
                      |            GROUP BY advertiser_id, campaign_id, ad_group_id
                      |
                      |           ) af0
                      |                     INNER JOIN
                      |           (SELECT  id
                      |            FROM advertiser_oracle
                      |            WHERE (managed_by = 12345)
                      |             )
                      |           ao1 ON (af0.advertiser_id = ao1.id)
                      |           INNER JOIN
                      |           (SELECT  advertiser_id, id
                      |            FROM ad_group_oracle
                      |            WHERE (campaign_id IN (22222)) AND (DECODE(status, 'ON', 'ON', 'OFF') = 'ON')
                      |             )
                      |           ago2 ON ( af0.advertiser_id = ago2.advertiser_id AND af0.ad_group_id = ago2.id)
                      |
 |          GROUP BY af0.advertiser_id
                      |)
                      |""".stripMargin

    result should equal (expected)(after being whiteSpaceNormalised)
  }

  test("Duplicate registration of the generator") {
    val failRegistry = new QueryGeneratorRegistry
    val dummyOracleQueryGenerator = new QueryGenerator[WithOracleEngine] {
      override def generate(queryContext: QueryContext): Query = { null }
      override def engine: Engine = OracleEngine
    }
    val dummyFalseQueryGenerator = new QueryGenerator[WithDruidEngine] {
      override def generate(queryContext: QueryContext): Query = { null }
      override def engine: Engine = DruidEngine
    }
    failRegistry.register(OracleEngine, dummyOracleQueryGenerator)
    failRegistry.register(DruidEngine, dummyFalseQueryGenerator)

    OracleQueryGenerator.register(failRegistry,DefaultPartitionColumnRenderer)
  }

  test("succesfully generate uncommon filter types") {
    val jsonString = s"""{
                           "cube": "publisher_stats_int2",
                           "selectFields": [
                             {
                               "field": "Publisher ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Spend",
                               "alias": null,
                               "value": null
                             }
                           ],
                          "filterExpressions": [
                             {"field": "Publisher ID", "operator": "=", "value": "12345"},
                             {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                             {"field": "Hour", "operator": "between", "from": "0", "to": "23"}
                          ],
                           "paginationStartIndex":0,
                           "rowsPerPage":100
                          }"""

    val requestOption = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), PublisherSchema)
    val registry = defaultRegistry
    val requestModel = getRequestModel(requestOption.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    val expected = s"""SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
                      |FROM (SELECT vps0.publisher_id "Publisher ID", coalesce(ROUND(vps0."spend", 10), 0.0) "Spend"
                      |      FROM (SELECT
                      |                   publisher_id, SUM(spend) AS "spend"
                      |            FROM v_publisher_stats2
                      |            WHERE (publisher_id = 12345) AND (date_sid >= to_number(to_char(trunc(to_date('$fromDate', 'YYYY-MM-DD')), 'YYYYMMDD')) AND date_sid <= to_number(to_char(trunc(to_date('$toDate', 'YYYY-MM-DD')), 'YYYYMMDD')))
                      |            GROUP BY publisher_id
                      |            HAVING (SUM(clicks) <> 777) AND (SUM(impressions) IS NOT NULL)
                     |           ) vps0
                      |
                     |)
                      |   ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100""".stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("successfully generate dim only oracle query with union all for sync multi engine query for druid + oracle") {
    import DefaultQueryPipelineFactoryTest._
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
                              {"field": "Clicks"},
                              {"field": "Advertiser Currency"},
                              {"field": "Campaign Device ID"},
                              {"field": "Campaign ID"},
                              { "field" : "Country WOEID", "value" : "2", "alias" : "Country WOEID"}
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
                          "rowsPerPage":10
                          }"""
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry, revision = Option(1))
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
    val resultSql = pipeline.queryChain.subsequentQueryList.head.asString

    val expected =
      s"""
         | (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT  *
         |      FROM (SELECT ago2.advertiser_id "Advertiser ID", ago2."Ad Group Status" "Ad Group Status", ago2.id "Ad Group ID", ao0.currency "Advertiser Currency", COALESCE(co1.device_id, 'UNKNOWN') "Campaign Device ID", ago2.campaign_id "Campaign ID", '2' AS "Country WOEID"
         |            FROM
         |               ( (SELECT  advertiser_id, campaign_id, DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 213) AND (id IN (10))
         |             ) ago2
         |          INNER JOIN
         |            (SELECT /*+ CampaignHint */ advertiser_id, CASE WHEN (device_id IN (1)) THEN 'Desktop' WHEN (device_id IN (2)) THEN 'Tablet' WHEN (device_id IN (3)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END AS device_id, id
         |            FROM campaign_oracle
         |
         |             ) co1
         |              ON( ago2.advertiser_id = co1.advertiser_id AND ago2.campaign_id = co1.id )
         |               INNER JOIN
         |            (SELECT  currency, id
         |            FROM advertiser_oracle
         |
         |             ) ao0
         |              ON( co1.advertiser_id = ao0.id )
         |               )
         |
 |           )
         |            ) D )) UNION ALL (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  *
         |      FROM (SELECT ago2.advertiser_id "Advertiser ID", ago2."Ad Group Status" "Ad Group Status", ago2.id "Ad Group ID", ao0.currency "Advertiser Currency", COALESCE(co1.device_id, 'UNKNOWN') "Campaign Device ID", ago2.campaign_id "Campaign ID", '2' AS "Country WOEID"
         |            FROM
         |               ( (SELECT  advertiser_id, campaign_id, DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 213) AND (id NOT IN (10))
         |             ) ago2
         |          INNER JOIN
         |            (SELECT /*+ CampaignHint */ advertiser_id, CASE WHEN (device_id IN (1)) THEN 'Desktop' WHEN (device_id IN (2)) THEN 'Tablet' WHEN (device_id IN (3)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END AS device_id, id
         |            FROM campaign_oracle
         |
         |             ) co1
         |              ON( ago2.advertiser_id = co1.advertiser_id AND ago2.campaign_id = co1.id )
         |               INNER JOIN
         |            (SELECT  currency, id
         |            FROM advertiser_oracle
         |
         |             ) ao0
         |              ON( co1.advertiser_id = ao0.id )
         |               )
         |
 |           )
         |            ) WHERE ROWNUM <= 10) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 10)
       """.stripMargin
    resultSql should equal (expected)(after being whiteSpaceNormalised)
  }


  test("Do not include the NOT IN clause if requested max rows < in filter size") {
    import DefaultQueryPipelineFactoryTest._
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
                          "rowsPerPage":9
                          }"""
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry, revision = Option(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[MultiEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[MultiEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
    val result = pipeline.withDruidCallback {
      rl =>
        (1 to 10).toList.foreach {
          i =>
            val row = rl.newRow
            row.addValue("Ad Group ID", 10+i)
            row.addValue("Impressions", 100)
            row.addValue("Clicks", 1)
            rl.addRow(row)
        }

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
    val resultSql = pipeline.queryChain.subsequentQueryList.head.asString

    val expected =
      s"""
         | SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  *
         |      FROM (SELECT ago2.advertiser_id "Advertiser ID", ago2."Ad Group Status" "Ad Group Status", ago2.id "Ad Group ID", ao0.currency "Advertiser Currency", COALESCE(co1.device_id, 'UNKNOWN') "Campaign Device ID", ago2.campaign_id "Campaign ID"
         |            FROM
         |               ( (SELECT  advertiser_id, campaign_id, DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 213) AND (id IN (12,19,15,11,13,16,17,14,20,18))
         |             ) ago2
         |          INNER JOIN
         |            (SELECT /*+ CampaignHint */ advertiser_id, CASE WHEN (device_id IN (1)) THEN 'Desktop' WHEN (device_id IN (2)) THEN 'Tablet' WHEN (device_id IN (3)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END AS device_id, id
         |            FROM campaign_oracle
         |
         |             ) co1
         |              ON( ago2.advertiser_id = co1.advertiser_id AND ago2.campaign_id = co1.id )
         |               INNER JOIN
         |            (SELECT  currency, id
         |            FROM advertiser_oracle
         |
         |             ) ao0
         |              ON( co1.advertiser_id = ao0.id )
         |               )
         |
         |           )
         |            ) WHERE ROWNUM <= 10) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 10
       """.stripMargin
    resultSql should equal (expected)(after being whiteSpaceNormalised)
  }

  test("Generate dim only query without union all for any middle page(not last)") {
    import DefaultQueryPipelineFactoryTest._
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
                          "paginationStartIndex":31,
                          "rowsPerPage":10
                          }"""
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry, revision = Option(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[MultiEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[MultiEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
    val result = pipeline.withDruidCallback {
      rl =>
        (1 to 45).toList.foreach {
          i =>
            val row = rl.newRow
            row.addValue("Ad Group ID", 10+i)
            row.addValue("Impressions", 100)
            row.addValue("Clicks", 1)
            rl.addRow(row)
        }

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
    val resultSql = pipeline.queryChain.subsequentQueryList.head.asString

    val expected =
      s"""
         | SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  *
         |      FROM (SELECT ago2.advertiser_id "Advertiser ID", ago2."Ad Group Status" "Ad Group Status", ago2.id "Ad Group ID", ao0.currency "Advertiser Currency", COALESCE(co1.device_id, 'UNKNOWN') "Campaign Device ID", ago2.campaign_id "Campaign ID"
         |            FROM
         |               ( (SELECT  advertiser_id, campaign_id, DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 213) AND (id IN (45,34,12,51,19,23,40,15,11,44,33,22,55,26,50,37,13,46,24,35,16,48,21,54,43,32,49,36,39,17,25,14,47,31,53,42,20,27,38,18,30,29,41,52,28))
         |             ) ago2
         |          INNER JOIN
         |            (SELECT /*+ CampaignHint */ advertiser_id, CASE WHEN (device_id IN (1)) THEN 'Desktop' WHEN (device_id IN (2)) THEN 'Tablet' WHEN (device_id IN (3)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END AS device_id, id
         |            FROM campaign_oracle
         |
         |             ) co1
         |              ON( ago2.advertiser_id = co1.advertiser_id AND ago2.campaign_id = co1.id )
         |               INNER JOIN
         |            (SELECT  currency, id
         |            FROM advertiser_oracle
         |
         |             ) ao0
         |              ON( co1.advertiser_id = ao0.id )
         |               )
         |
         |           )
         |            ) WHERE ROWNUM <= 45) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 45
       """.stripMargin
    resultSql should equal (expected)(after being whiteSpaceNormalised)
  }

  test("Generate dim only query wit union all for last page)") {
    import DefaultQueryPipelineFactoryTest._
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
                          "rowsPerPage":20
                          }"""
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry, revision = Option(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[MultiEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[MultiEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
    val result = pipeline.withDruidCallback {
      rl =>
        (1 to 15).toList.foreach {
          i =>
            val row = rl.newRow
            row.addValue("Ad Group ID", 10+i)
            row.addValue("Impressions", 100)
            row.addValue("Clicks", 1)
            rl.addRow(row)
        }

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
    val resultSql = pipeline.queryChain.subsequentQueryList.head.asString

    val expected =
      s"""
         |(SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT  *
         |      FROM (SELECT ago2.advertiser_id "Advertiser ID", ago2."Ad Group Status" "Ad Group Status", ago2.id "Ad Group ID", ao0.currency "Advertiser Currency", COALESCE(co1.device_id, 'UNKNOWN') "Campaign Device ID", ago2.campaign_id "Campaign ID"
         |            FROM
         |               ( (SELECT  advertiser_id, campaign_id, DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 213) AND (id IN (12,19,23,15,11,22,13,24,16,21,17,25,14,20,18))
         |             ) ago2
         |          INNER JOIN
         |            (SELECT /*+ CampaignHint */ advertiser_id, CASE WHEN (device_id IN (1)) THEN 'Desktop' WHEN (device_id IN (2)) THEN 'Tablet' WHEN (device_id IN (3)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END AS device_id, id
         |            FROM campaign_oracle
         |
         |             ) co1
         |              ON( ago2.advertiser_id = co1.advertiser_id AND ago2.campaign_id = co1.id )
         |               INNER JOIN
         |            (SELECT  currency, id
         |            FROM advertiser_oracle
         |
         |             ) ao0
         |              ON( co1.advertiser_id = ao0.id )
         |               )
         |
         |           )
         |            ) D )) UNION ALL (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  *
         |      FROM (SELECT ago2.advertiser_id "Advertiser ID", ago2."Ad Group Status" "Ad Group Status", ago2.id "Ad Group ID", ao0.currency "Advertiser Currency", COALESCE(co1.device_id, 'UNKNOWN') "Campaign Device ID", ago2.campaign_id "Campaign ID"
         |            FROM
         |               ( (SELECT  advertiser_id, campaign_id, DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 213) AND (id NOT IN (12,19,23,15,11,22,13,24,16,21,17,25,14,20,18))
         |             ) ago2
         |          INNER JOIN
         |            (SELECT /*+ CampaignHint */ advertiser_id, CASE WHEN (device_id IN (1)) THEN 'Desktop' WHEN (device_id IN (2)) THEN 'Tablet' WHEN (device_id IN (3)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END AS device_id, id
         |            FROM campaign_oracle
         |
         |             ) co1
         |              ON( ago2.advertiser_id = co1.advertiser_id AND ago2.campaign_id = co1.id )
         |               INNER JOIN
         |            (SELECT  currency, id
         |            FROM advertiser_oracle
         |
         |             ) ao0
         |              ON( co1.advertiser_id = ao0.id )
         |               )
         |
         |           )
         |            ) WHERE ROWNUM <= 20) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 20)
       """.stripMargin
    resultSql should equal (expected)(after being whiteSpaceNormalised)
  }

  test("Generate dim only query wit union all for last page, verify rowCount false works") {
    import DefaultQueryPipelineFactoryTest._
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
                          "includeRowCount" : false,
                          "forceDimensionDriven": true,
                          "rowsPerPage":20
                          }"""
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry, revision = Option(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[MultiEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[MultiEngineQuery].drivingQuery.isInstanceOf[DruidQuery[_]])
    val result = pipeline.withDruidCallback {
      rl =>
        (1 to 15).toList.foreach {
          i =>
            val row = rl.newRow
            row.addValue("Ad Group ID", 10+i)
            row.addValue("Impressions", 100)
            row.addValue("Clicks", 1)
            rl.addRow(row)
        }

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
    val resultSql = pipeline.queryChain.subsequentQueryList.head.asString

    val expected =
      s"""
         |(SELECT * FROM (SELECT D.* FROM (SELECT  *
         |      FROM (SELECT ago2.advertiser_id "Advertiser ID", ago2."Ad Group Status" "Ad Group Status", ago2.id "Ad Group ID", ao0.currency "Advertiser Currency", COALESCE(co1.device_id, 'UNKNOWN') "Campaign Device ID", ago2.campaign_id "Campaign ID"
         |            FROM
         |               ( (SELECT  advertiser_id, campaign_id, DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 213) AND (id IN (12,19,23,15,11,22,13,24,16,21,17,25,14,20,18))
         |             ) ago2
         |          INNER JOIN
         |            (SELECT /*+ CampaignHint */ advertiser_id, CASE WHEN (device_id IN (1)) THEN 'Desktop' WHEN (device_id IN (2)) THEN 'Tablet' WHEN (device_id IN (3)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END AS device_id, id
         |            FROM campaign_oracle
         |
         |             ) co1
         |              ON( ago2.advertiser_id = co1.advertiser_id AND ago2.campaign_id = co1.id )
         |               INNER JOIN
         |            (SELECT  currency, id
         |            FROM advertiser_oracle
         |
         |             ) ao0
         |              ON( co1.advertiser_id = ao0.id )
         |               )
         |
         |           )
         |            ) D )) UNION ALL (SELECT  *
         |      FROM (SELECT ago2.advertiser_id "Advertiser ID", ago2."Ad Group Status" "Ad Group Status", ago2.id "Ad Group ID", ao0.currency "Advertiser Currency", COALESCE(co1.device_id, 'UNKNOWN') "Campaign Device ID", ago2.campaign_id "Campaign ID"
         |            FROM
         |               ( (SELECT  advertiser_id, campaign_id, DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 213) AND (id NOT IN (12,19,23,15,11,22,13,24,16,21,17,25,14,20,18))
         |             ) ago2
         |          INNER JOIN
         |            (SELECT /*+ CampaignHint */ advertiser_id, CASE WHEN (device_id IN (1)) THEN 'Desktop' WHEN (device_id IN (2)) THEN 'Tablet' WHEN (device_id IN (3)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END AS device_id, id
         |            FROM campaign_oracle
         |
         |             ) co1
         |              ON( ago2.advertiser_id = co1.advertiser_id AND ago2.campaign_id = co1.id )
         |               INNER JOIN
         |            (SELECT  currency, id
         |            FROM advertiser_oracle
         |
         |             ) ao0
         |              ON( co1.advertiser_id = ao0.id )
         |               )
         |
         |           )
         |            )
       """.stripMargin
    resultSql should equal (expected)(after being whiteSpaceNormalised)
  }

  test("Greater than filter should work for Oracle Sync") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Impressions", "operator": ">", "value": "1608"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "Fail to get the query pipeline")
    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    print(result)
    val expected =
      s"""
         SELECT *
         |FROM (SELECT co1.id "Campaign ID", coalesce(f0."impressions", 1) "Impressions"
         |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) CONDITIONAL_HINT1 CONDITIONAL_HINT2 CONDITIONAL_HINT3 */
         |                   campaign_id, SUM(impressions) AS "impressions"
         |            FROM fact2 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY campaign_id
         |            HAVING (SUM(impressions) > 1608)
         |           ) f0
         |           INNER JOIN
         |                (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT /*+ CampaignHint */ id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             ) WHERE ROWNUM <= 120) D ) WHERE ROW_NUMBER >= 21 AND ROW_NUMBER <= 120) co1
         |            ON (f0.campaign_id = co1.id)
         |)
      """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Less than filter should work for Oracle Sync") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Impressions", "operator": "<", "value": "1608"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "Fail to get the query pipeline")
    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    print(result)
    val expected =
      s"""
         SELECT *
         |FROM (SELECT co1.id "Campaign ID", coalesce(f0."impressions", 1) "Impressions"
         |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) CONDITIONAL_HINT1 CONDITIONAL_HINT2 CONDITIONAL_HINT3 */
         |                   campaign_id, SUM(impressions) AS "impressions"
         |            FROM fact2 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY campaign_id
         |            HAVING (SUM(impressions) < 1608)
         |           ) f0
         |           INNER JOIN
         |                (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT /*+ CampaignHint */ id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             ) WHERE ROWNUM <= 120) D ) WHERE ROW_NUMBER >= 21 AND ROW_NUMBER <= 120) co1
         |            ON (f0.campaign_id = co1.id)
         |)
      """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("successfully generate dim only oracle query without union all for sync multi engine query for druid + oracle with metric filter") {
    import DefaultQueryPipelineFactoryTest._
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
                              {"field": "Clicks"},
                              {"field": "Advertiser Currency"},
                              {"field": "Campaign Device ID"},
                              {"field": "Campaign ID"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Impressions", "operator": "between", "from": "10", "to": "1000000"}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "ASC"}
                          ],
                          "includeRowCount" : true,
                          "forceDimensionDriven": true,
                          "paginationStartIndex":0,
                          "rowsPerPage":10
                          }"""
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry, revision = Option(1))
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
    val resultSql = pipeline.queryChain.subsequentQueryList.head.asString

    val expected =
      s"""
         |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  *
         |      FROM (SELECT ago2.advertiser_id "Advertiser ID", ago2."Ad Group Status" "Ad Group Status", ago2.id "Ad Group ID", ao0.currency "Advertiser Currency", COALESCE(co1.device_id, 'UNKNOWN') "Campaign Device ID", ago2.campaign_id "Campaign ID"
         |            FROM
         |               ( (SELECT  advertiser_id, campaign_id, DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 213) AND (id IN (10))
         |             ) ago2
         |          INNER JOIN
         |            (SELECT /*+ CampaignHint */ advertiser_id, CASE WHEN (device_id IN (1)) THEN 'Desktop' WHEN (device_id IN (2)) THEN 'Tablet' WHEN (device_id IN (3)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END AS device_id, id
         |            FROM campaign_oracle
         |
         |             ) co1
         |              ON( ago2.advertiser_id = co1.advertiser_id AND ago2.campaign_id = co1.id )
         |               INNER JOIN
         |            (SELECT  currency, id
         |            FROM advertiser_oracle
         |
         |             ) ao0
         |              ON( co1.advertiser_id = ao0.id )
         |               )
         |
 |           )
         |            ) WHERE ROWNUM <= 10) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 10
       """.stripMargin
    resultSql should equal (expected)(after being whiteSpaceNormalised)
  }
  test("successfully generate dim only oracle query with Correct RowNum and pagination") {
    import DefaultQueryPipelineFactoryTest._
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Campaign Name"},
                              {"field": "Impressions"},
                              {"field": "Clicks"},
                              {"field": "Campaign ID"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Campaign Status", "operator": "NOT IN", "values" : ["DELETED"]}
                          ],
                          "sortBy": [
                              {"field": "Campaign Name", "order": "DESC"},
                              {"field": "Campaign ID", "order": "DESC"}
                          ],
                          "includeRowCount" : true,
                          "forceDimensionDriven": true,
                          "paginationStartIndex":2,
                          "rowsPerPage":40
                          }"""
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry, revision = Option(1))
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    assert(pipeline.queryChain.isInstanceOf[MultiEngineQuery])
    assert(pipeline.queryChain.asInstanceOf[MultiEngineQuery].drivingQuery.isInstanceOf[OracleQuery])
    val result = pipeline.withDruidCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Campaign ID", 10)
        row.addValue("Impressions", 100)
        row.addValue("Clicks", 1)
        rl.addRow(row)
    }.withOracleCallback {
      rl =>
        val row = rl.newRow
        row.addValue("Campaign ID", 10)
        row.addValue("Campaign Name", "test_campaign")
        rl.addRow(row)
    }.run()

    assert(result.isSuccess, result)
    val resultSql = pipeline.queryChain.drivingQuery.asString

    val expected =
      s"""
         |SELECT  *
         |      FROM (SELECT co0.campaign_name "Campaign Name", co0.id "Campaign ID", Count(*) OVER() TOTALROWS, ROWNUM as ROW_NUMBER
         |            FROM
         |                (SELECT /*+ CampaignHint */ campaign_name, id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 213) AND (DECODE(status, 'ON', 'ON', 'OFF') NOT IN ('DELETED'))
         |            ORDER BY 1 DESC NULLS LAST, 2 DESC  ) co0
         |
         |
         |           )
         |             WHERE ROW_NUMBER >= 3 AND ROW_NUMBER <= 42
       """.stripMargin
    resultSql should equal (expected)(after being whiteSpaceNormalised)
  }

  test("Skip inSubquery clause for high cardinality dimension filtering") {
    val jsonString = s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Advertiser ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Impressions",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Average CPC",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                              {"field": "Booking Country", "operator": "IN", "values": ["US"]},
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ]
                         }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))


    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    val expected =
      s"""
         |SELECT *
         |FROM (SELECT af0.advertiser_id "Advertiser ID", coalesce(af0."impressions", 1) "Impressions", ROUND((CASE WHEN af0."clicks" = 0 THEN 0.0 ELSE af0."spend" / af0."clicks" END), 10) "Average CPC"
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
         |                   advertiser_id, SUM(impressions) AS "impressions", SUM(CASE WHEN ((clicks >= 1) AND (clicks <= 800)) THEN clicks ELSE 0 END) AS "clicks", SUM(spend) AS "spend"
         |            FROM ad_fact1 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY advertiser_id
         |
         |           ) af0
         |           INNER JOIN
         |           (SELECT  id
         |            FROM advertiser_oracle
         |            WHERE (id = 12345) AND (booking_country IN ('US'))
         |             )
         |           ao1 ON (af0.advertiser_id = ao1.id)
         |
         |)
       """.stripMargin

    result should equal (expected)(after being whiteSpaceNormalised)
  }

  test("Verify Combined queries lose data in Multivalue Dim contexts (Class Name Collapses)") {
    val jsonString: String =
      s"""
         |{
         |  "cube": "class_stats",
         |  "selectFields": [
         |    { "field": "Class ID" },
         |    { "field": "Class Name" },
         |    { "field": "Class Address" },
         |    { "field": "Students" }
         |  ],
         |  "filterExpressions": [
         |    { "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" },
         |    { "field": "Class ID", "operator": "=", "value": "12345" }
         |  ]
         |}
       """.stripMargin
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val resultPipeline = queryPipelineTry.get

    val result = resultPipeline.queryChain.drivingQuery.asInstanceOf[DruidQuery[_]].asString

    /**
      * Create a RowList of 3 rows in Druid & 2 Rows in Oracle, allow Pk to match.
      * Check output.
      *
      * Expectation is that, for each Fact Row returned matching the Dim, both rows will be kept.
      * Current state is that for Statically Mapped columns in Fact, MultiEngine queries collapse the
      * row with unique mapped values since they aren't Pk Aliases.
      */

    val oracleExecutor = new MockOracleQueryExecutor(
      {
        rl =>
          val row1 = rl.newRow
          row1.addValue("Class ID", 12345L)
          row1.addValue("Class Address", "8675 309th St.")
          rl.addRow(row1)

          val row2 = rl.newRow
          row2.addValue("Class ID", 12345L)
          row2.addValue("Class Address", "8675 301st Ave.")
          rl.addRow(row2)
      }
    )

    val druidExecutor = new MockDruidQueryExecutor(
      {
        rl =>
          val row1 = rl.newRow
          row1.addValue("Class ID", 12345L)
          row1.addValue("Class Name", "Classy")
          row1.addValue("Students", 55)
          rl.addRow(row1)

          val row2 = rl.newRow
          row2.addValue("Class ID", 12345L)
          row2.addValue("Class Name", "Classier")
          row2.addValue("Students", 22)
          rl.addRow(row2)

          val row3 = rl.newRow
          row3.addValue("Class ID", 12345L)
          row3.addValue("Class Name", "Classiest")
          row3.addValue("Students", 11)
          rl.addRow(row3)
      }
    )

    val irlFn = (q : Query) => new DimDrivenPartialRowList(RowGrouping("Class ID", List("Class Name")), q)

    val queryExecutorContext: QueryExecutorContext = new QueryExecutorContext
    queryExecutorContext.register(oracleExecutor)
    queryExecutorContext.register(druidExecutor)

    //Non-merged row results
    val postRowResultTry = resultPipeline.execute(queryExecutorContext)
    assert(postRowResultTry.isSuccess)
    val postRowResult = postRowResultTry.get

    //Post-multiEngineQuery Result using Class ID (Pk) as Join key.
    val queryCastedToMultiEngine = resultPipeline.queryChain.asInstanceOf[MultiEngineQuery]
    val executedMultiEngineQuery = queryCastedToMultiEngine.execute(queryExecutorContext, irlFn, QueryAttributes.empty, new EngineQueryStats)

    val expectedUnmergedRowList = List(
      "Row(Map(Class ID -> 0, Class Name -> 1, Class Address -> 2, Students -> 3),ArrayBuffer(12345, Classy, null, 55))"
    , "Row(Map(Class ID -> 0, Class Name -> 1, Class Address -> 2, Students -> 3),ArrayBuffer(12345, Classier, null, 22))"
    , "Row(Map(Class ID -> 0, Class Name -> 1, Class Address -> 2, Students -> 3),ArrayBuffer(12345, Classiest, null, 11))"
    , "Row(Map(Class ID -> 0, Class Name -> 1, Class Address -> 2, Students -> 3),ArrayBuffer(12345, null, 8675 309th St., null))"
    , "Row(Map(Class ID -> 0, Class Name -> 1, Class Address -> 2, Students -> 3),ArrayBuffer(12345, null, 8675 301st Ave., null))")

    val actualMultiEngineRowList = List(
      "Row(Map(Class ID -> 0, Class Name -> 1, Class Address -> 2, Students -> 3),ArrayBuffer(12345, Classy, 8675 301st Ave., 55))"
      , "Row(Map(Class ID -> 0, Class Name -> 1, Class Address -> 2, Students -> 3),ArrayBuffer(12345, Classier, 8675 301st Ave., 22))"
      , "Row(Map(Class ID -> 0, Class Name -> 1, Class Address -> 2, Students -> 3),ArrayBuffer(12345, Classiest, 8675 301st Ave., 11))"
    )

    /**
      * current logic: If grouping already exists in full, overwrite (reason why the second dim grouping is the only one returned)
      * actual goal: If grouping primary key alias already exists, take all rows under that grouping, index the areas to overwrite Dim information, and do so.
      */


    assert(executedMultiEngineQuery.rowList.length == 3)
    assert(executedMultiEngineQuery.rowList.forall(row => {
      actualMultiEngineRowList.contains(row.toString)
    }))
    assert(postRowResult.rowList.forall(row => expectedUnmergedRowList.contains(row.toString)))



  }

  test("Test dimension join when dim primary key has alias") {
    val jsonString = s"""{
                           "cube": "class_stats_2",
                           "selectFields": [
                             {
                               "field": "Class ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Class Name",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Class Address",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Students",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                              {"field": "Class ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ]
                         }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))


    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    val expected =
      s"""
         |SELECT *
         |FROM (SELECT fcso0.class_id "Class ID", fcso0.class_name "Class Name", cc1.address "Class Address", fcso0."num_students" "Students"
         |      FROM (SELECT
         |                   CASE WHEN (class_name IN (1)) THEN 'Classy' WHEN (class_name IN (2)) THEN 'Classier' WHEN (class_name IN (3)) THEN 'Classiest' ELSE 'Unknown' END class_name, class_id, SUM(num_students) AS "num_students"
         |            FROM f_class_stats_ora FactAlias
         |            WHERE (class_id = 12345) AND (date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY CASE WHEN (class_name IN (1)) THEN 'Classy' WHEN (class_name IN (2)) THEN 'Classier' WHEN (class_name IN (3)) THEN 'Classiest' ELSE 'Unknown' END, class_id
         |
         |           ) fcso0
         |           LEFT OUTER JOIN
         |           (SELECT  address, id_alias
         |            FROM combined_class
         |            WHERE (id_alias = 12345)
         |             )
         |           cc1 ON (fcso0.class_id = cc1.id_alias)
         |
         |)
         |""".stripMargin

    result should equal (expected)(after being whiteSpaceNormalised)
  }

  test("successfully generate fact driven query for minute grain with datetime between filter") {
    val jsonString = s"""{
                          "cube": "k_stats_minute",
                          "selectFields": [
                              {"field": "Keyword ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Ad Group Status"},
                              {"field": "Campaign Status"},
                              {"field": "Count"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "datetimebetween", "from": "$fromDateTime", "to": "$toDateTime", "format": "$iso8601Format"}
                          ],
                          "sortBy": [
                              {"field": "Campaign Status", "order": "ASC"}
                          ],
                          "forceFactDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected =
      s"""SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
         |FROM (SELECT f0.keyword_id "Keyword ID", ago2.campaign_id "Campaign ID", coalesce(f0."impressions", 1) "Impressions", ago2."Ad Group Status" "Ad Group Status", co1."Campaign Status" "Campaign Status", f0."Count" "Count"
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_campaign_k_stats 4) CONDITIONAL_HINT1 CONDITIONAL_HINT2 CONDITIONAL_HINT4 */
         |                   ad_group_id, campaign_id, keyword_id, SUM(impressions) AS "impressions", COUNT(*) AS "Count"
         |            FROM fact2 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= TO_UTC_TIMESTAMP_TZ('$fromDateTime') AND stats_date <= TO_UTC_TIMESTAMP_TZ('$toDateTime'))
         |            GROUP BY ad_group_id, campaign_id, keyword_id
         |
         |           ) f0
         |           LEFT OUTER JOIN
         |           (SELECT /*+ CampaignHint */ DECODE(status, 'ON', 'ON', 'OFF') AS "Campaign Status", id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           co1 ON (f0.campaign_id = co1.id)
         |           LEFT OUTER JOIN
         |           (SELECT  campaign_id, DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", id, advertiser_id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           ago2 ON (f0.ad_group_id = ago2.id)
         |
         |)
         |   ORDER BY "Campaign Status" ASC NULLS LAST) WHERE ROWNUM <= 120) D ) WHERE ROW_NUMBER >= 21 AND ROW_NUMBER <= 120
         |   """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("successfully generate dimension driven query for minute grain with datetime between filter") {
    val jsonString = s"""{
                          "cube": "k_stats_minute",
                          "selectFields": [
                              {"field": "Keyword ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Ad Group Status"},
                              {"field": "Campaign Status"},
                              {"field": "Count"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "datetimebetween", "from": "$fromDateTime", "to": "$toDateTime", "format": "$iso8601Format"}
                          ],
                          "sortBy": [
                              {"field": "Campaign Status", "order": "ASC"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected =
      s"""
         |SELECT *
         |FROM (SELECT t3.id "Keyword ID", ago2.campaign_id "Campaign ID", coalesce(f0."impressions", 1) "Impressions", ago2."Ad Group Status" "Ad Group Status", co1."Campaign Status" "Campaign Status", f0."Count" "Count"
         |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) CONDITIONAL_HINT1 CONDITIONAL_HINT2 CONDITIONAL_HINT3 */
         |                   ad_group_id, campaign_id, keyword_id, SUM(impressions) AS "impressions", COUNT(*) AS "Count"
         |            FROM fact2 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= TO_UTC_TIMESTAMP_TZ('$fromDateTime') AND stats_date <= TO_UTC_TIMESTAMP_TZ('$toDateTime'))
         |            GROUP BY ad_group_id, campaign_id, keyword_id
         |
         |           ) f0
         |           RIGHT OUTER JOIN
         |               ( (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  parent_id, id, advertiser_id
         |            FROM targetingattribute
         |            WHERE (advertiser_id = 12345)
         |             ) WHERE ROWNUM <= 120) D ) WHERE ROW_NUMBER >= 21 AND ROW_NUMBER <= 120) t3
         |          INNER JOIN
         |            (SELECT  campaign_id, DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", id, advertiser_id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 12345)
         |             ) ago2
         |              ON( t3.advertiser_id = ago2.advertiser_id AND t3.parent_id = ago2.id )
         |               INNER JOIN
         |            (SELECT /*+ CampaignHint */ DECODE(status, 'ON', 'ON', 'OFF') AS "Campaign Status", id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             ) co1
         |              ON( ago2.advertiser_id = co1.advertiser_id AND ago2.campaign_id = co1.id )
         |               )  ON (f0.keyword_id = t3.id)
         |
         |)
         |   ORDER BY "Campaign Status" ASC NULLS LAST
      """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("successfully generate fact driven query for day grain with datetime between filter") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Keyword ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Ad Group Status"},
                              {"field": "Campaign Status"},
                              {"field": "Count"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "datetimebetween", "from": "$fromDateTime", "to": "$toDateTime", "format": "$iso8601Format"}
                          ],
                          "sortBy": [
                              {"field": "Campaign Status", "order": "ASC"}
                          ],
                          "forceFactDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected =
      s"""SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
         |FROM (SELECT f0.keyword_id "Keyword ID", ago2.campaign_id "Campaign ID", coalesce(f0."impressions", 1) "Impressions", ago2."Ad Group Status" "Ad Group Status", co1."Campaign Status" "Campaign Status", f0."Count" "Count"
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_campaign_k_stats 4) CONDITIONAL_HINT1 CONDITIONAL_HINT2 CONDITIONAL_HINT4 */
         |                   ad_group_id, campaign_id, keyword_id, SUM(impressions) AS "impressions", COUNT(*) AS "Count"
         |            FROM fact2 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= to_date('$fromDate', 'YYYY-MM-DD') AND stats_date <= to_date('$toDate', 'YYYY-MM-DD'))
         |            GROUP BY ad_group_id, campaign_id, keyword_id
         |
         |           ) f0
         |           LEFT OUTER JOIN
         |           (SELECT /*+ CampaignHint */ DECODE(status, 'ON', 'ON', 'OFF') AS "Campaign Status", id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           co1 ON (f0.campaign_id = co1.id)
         |           LEFT OUTER JOIN
         |           (SELECT  campaign_id, DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", id, advertiser_id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           ago2 ON (f0.ad_group_id = ago2.id)
         |
         |)
         |   ORDER BY "Campaign Status" ASC NULLS LAST) WHERE ROWNUM <= 120) D ) WHERE ROW_NUMBER >= 21 AND ROW_NUMBER <= 120
         |   """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("successfully generate dimension driven query for day grain with datetime between filter") {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                              {"field": "Keyword ID"},
                              {"field": "Campaign ID"},
                              {"field": "Impressions"},
                              {"field": "Ad Group Status"},
                              {"field": "Campaign Status"},
                              {"field": "Count"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "datetimebetween", "from": "$fromDateTime", "to": "$toDateTime", "format": "$iso8601Format"}
                          ],
                          "sortBy": [
                              {"field": "Campaign Status", "order": "ASC"}
                          ],
                          "forceDimensionDriven": true,
                          "paginationStartIndex":20,
                          "rowsPerPage":100
                          }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
      .copy(additionalParameters = Map(Parameter.Debug -> DebugValue(value = true), Parameter.TimeZone->TimeZoneValue("America/Los_Angeles")))
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected =
      s"""
         |SELECT *
         |FROM (SELECT t3.id "Keyword ID", ago2.campaign_id "Campaign ID", coalesce(f0."impressions", 1) "Impressions", ago2."Ad Group Status" "Ad Group Status", co1."Campaign Status" "Campaign Status", f0."Count" "Count"
         |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) CONDITIONAL_HINT1 CONDITIONAL_HINT2 CONDITIONAL_HINT3 */
         |                   ad_group_id, campaign_id, keyword_id, SUM(impressions) AS "impressions", COUNT(*) AS "Count"
         |            FROM fact2 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= to_date('$fromDate', 'YYYY-MM-DD') AND stats_date <= to_date('$toDate', 'YYYY-MM-DD'))
         |            GROUP BY ad_group_id, campaign_id, keyword_id
         |
         |           ) f0
         |           RIGHT OUTER JOIN
         |               ( (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  parent_id, id, advertiser_id
         |            FROM targetingattribute
         |            WHERE (advertiser_id = 12345)
         |             ) WHERE ROWNUM <= 120) D ) WHERE ROW_NUMBER >= 21 AND ROW_NUMBER <= 120) t3
         |          INNER JOIN
         |            (SELECT  campaign_id, DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", id, advertiser_id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 12345)
         |             ) ago2
         |              ON( t3.advertiser_id = ago2.advertiser_id AND t3.parent_id = ago2.id )
         |               INNER JOIN
         |            (SELECT /*+ CampaignHint */ DECODE(status, 'ON', 'ON', 'OFF') AS "Campaign Status", id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             ) co1
         |              ON( ago2.advertiser_id = co1.advertiser_id AND ago2.campaign_id = co1.id )
         |               )  ON (f0.keyword_id = t3.id)
         |
         |)
         |   ORDER BY "Campaign Status" ASC NULLS LAST
      """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }
}
