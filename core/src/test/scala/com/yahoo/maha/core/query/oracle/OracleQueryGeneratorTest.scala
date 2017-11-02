// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.oracle

import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core._
import com.yahoo.maha.core.fact.Fact.ViewTable
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.request._
import java.nio.charset.StandardCharsets


/**
 * Created by jians on 11/12/15.
 */
class OracleQueryGeneratorTest extends BaseOracleQueryGeneratorTest {

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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val select = """SELECT to_char(f0.campaign_id) "Campaign ID", coalesce(f0."impressions", 1) "Impressions", ao1.name "Advertiser Name", ao1."Advertiser Status" "Advertiser Status", TOTALROWS"""
    assert(result.contains(select), result)
  }

  test("dim fact sync fact driven query with multiple dim join should produce all requested fields in same order as in request") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_with_multi_dim_join.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val select = """SELECT to_char(co2.id) "Campaign ID", coalesce(f0."impressions", 1) "Impressions", ao1.name "Advertiser Name", co2."Campaign Status" "Campaign Status", TOTALROWS"""
    assert(result.contains(select), result)
  }

  test("dim fact async fact driven query should produce all requested fields in same order as in request") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_wo_dim_filters.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val select = """SELECT to_char(f0.campaign_id) "Campaign ID", coalesce(f0."impressions", 1) "Impressions", ao1.name "Advertiser Name", ao1."Advertiser Status" "Advertiser Status""""
    assert(result.contains(select), result)
  }

  test("dim fact sync dimension driven query should produce all requested fields in same order as in request with in Subquery Clause") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_dim_driven_wo_dim_filters.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val select = """SELECT to_char(ago1.campaign_id) "Campaign ID", coalesce(f0."impressions", 1) "Impressions", ago1."Ad Group Status" "Ad Group Status""""
    assert(result.contains(select), result)
    assert(result.contains("campaign_id IN (SELECT id FROM campaign_oracle WHERE (DECODE(status, 'ON', 'ON', 'OFF') IN ('ON'))"),result)
  }

  test("dim fact async fact driven query with dim filters should use INNER JOIN") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_w_dim_filters.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
                              {"field": "Campaign Status"}
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected =
      s"""
        |SELECT *
        |FROM (SELECT to_char(t3.id) "Keyword ID", to_char(ago2.campaign_id) "Campaign ID", coalesce(f0."impressions", 1) "Impressions", ago2."Ad Group Status" "Ad Group Status", co1."Campaign Status" "Campaign Status"
        |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) */
        |                   ad_group_id, campaign_id, keyword_id, SUM(impressions) AS "impressions"
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
        |          LEFT OUTER JOIN
        |            (SELECT  campaign_id, DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", id, advertiser_id
        |            FROM ad_group_oracle
        |            WHERE (advertiser_id = 12345)
        |             ) ago2
        |              ON( t3.advertiser_id = ago2.advertiser_id AND t3.parent_id = ago2.id )
        |               LEFT OUTER JOIN
        |            (SELECT /*+ CampaignHint */ DECODE(status, 'ON', 'ON', 'OFF') AS "Campaign Status", id, advertiser_id
        |            FROM campaign_oracle
        |            WHERE (advertiser_id = 12345)
        |             ) co1
        |              ON( ago2.advertiser_id = co1.advertiser_id AND ago2.campaign_id = co1.id )
        |               )  ON (f0.keyword_id = t3.id)
        |
        |
        |
        |) ORDER BY "Campaign Status" ASC NULLS LAST
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected =
      s"""
         |SELECT *
         |FROM (SELECT to_char(t3.id) "Keyword ID", coalesce(f0."impressions", 1) "Impressions", COALESCE(f0.device_id, 'UNKNOWN') "Device ID", COALESCE(f0.network_type, 'NONE') "Network Type", COALESCE(f0.pricing_type, 'NONE') "Pricing Type", co1."Campaign Status" "Campaign Status"
         |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) */
         |                   CASE WHEN (device_id IN (1)) THEN 'Desktop' WHEN (device_id IN (2)) THEN 'Tablet' WHEN (device_id IN (3)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END device_id, DECODE(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_SYNDICATION', 'Content Syndication', 'EXTERNAL', 'Yahoo Partners', 'INTERNAL', 'Yahoo Properties', 'NONE') network_type, CASE WHEN (pricing_type IN (1)) THEN 'CPC' WHEN (pricing_type IN (6)) THEN 'CPV' WHEN (pricing_type IN (2)) THEN 'CPA' WHEN (pricing_type IN (-10)) THEN 'CPE' WHEN (pricing_type IN (-20)) THEN 'CPF' WHEN (pricing_type IN (7)) THEN 'CPCV' WHEN (pricing_type IN (3)) THEN 'CPM' ELSE 'NONE' END pricing_type, campaign_id, keyword_id, SUM(impressions) AS "impressions"
         |            FROM fact2 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_source IN (1,2)) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY CASE WHEN (device_id IN (1)) THEN 'Desktop' WHEN (device_id IN (2)) THEN 'Tablet' WHEN (device_id IN (3)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END, DECODE(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_SYNDICATION', 'Content Syndication', 'EXTERNAL', 'Yahoo Partners', 'INTERNAL', 'Yahoo Properties', 'NONE'), CASE WHEN (pricing_type IN (1)) THEN 'CPC' WHEN (pricing_type IN (6)) THEN 'CPV' WHEN (pricing_type IN (2)) THEN 'CPA' WHEN (pricing_type IN (-10)) THEN 'CPE' WHEN (pricing_type IN (-20)) THEN 'CPF' WHEN (pricing_type IN (7)) THEN 'CPCV' WHEN (pricing_type IN (3)) THEN 'CPM' ELSE 'NONE' END, campaign_id, keyword_id
         |
         |           ) f0
         |           RIGHT OUTER JOIN
         |               ( (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  id, advertiser_id
         |            FROM targetingattribute
         |            WHERE (advertiser_id = 12345)
         |             ) WHERE ROWNUM <= 120) D ) WHERE ROW_NUMBER >= 21 AND ROW_NUMBER <= 120) t3
         |          LEFT OUTER JOIN
         |            (SELECT  id, campaign_id, advertiser_id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 12345)
         |             ) ago2
         |              ON( t3.advertiser_id = ago2.advertiser_id AND t3.parent_id = ago2.id )
         |               LEFT OUTER JOIN
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Non-hash partitioned dimension with singleton snapshot failed"))
    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("/*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) */"), "Query should contain dimension hint")
  }

  test("dim fact async fact driven query with hint annotation should have static hint comment in the final sql string") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_w_hint.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("/*+ PARALLEL_INDEX(cb_campaign_k_stats 4) */"), "Query should contain dimension hint")
  }

  test("dim fact sync dimension driven query with dimension id filters should generate full SQL with in subquery clause") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_dim_driven_w_dim_id_filters.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("NOT IN ('OFF')"), "Query should contain NOT IN")
    assert(result.contains("RIGHT OUTER JOIN"), "Query should be ROJ")
    assert(result.contains("/*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) */"), "Query should contain dim driven hint")
  }

  test("dim fact sync fact driven query should have static hint") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_w_hint.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    assert(result.contains("LEFT OUTER JOIN"), "Query should use LEFT OUTER JOIN")
    assert(result.contains("/*+ PARALLEL_INDEX(cb_campaign_k_stats 4) */"), "Query should contain dim driven hint")
  }

  test("dim fact sync fact driven query with request DecType fields that contains max and min should return query with max and min range") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "dim_fact_fact_driven_dec_max_min.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val where = """coalesce(f0."impressions", 1) "Total Impressions", coalesce(f0."impressions", 1) "Impressions""""
    assert(result.contains(where), result)
  }

  test("Fact Driven Multidimensional query with dim sortBy ") {
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected =
      s"""SELECT *
        |FROM (SELECT to_char(f0.keyword_id) "Keyword ID", t4.value "Keyword Value", co1.campaign_name "Campaign Name", ago2.name "Ad Group Name", ado3.title "Ad Title", coalesce(f0."impressions", 1) "Impressions", ROUND(f0."CTR", 10) "CTR"
        |      FROM (SELECT /*+ PARALLEL_INDEX(cb_campaign_k_stats 4) */
        |                   ad_group_id, ad_id, campaign_id, keyword_id, SUM(impressions) AS "impressions", (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS "CTR"
        |            FROM fact1 FactAlias
        |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
        |            GROUP BY ad_group_id, ad_id, campaign_id, keyword_id
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
        |           (SELECT  ad_group_id, campaign_id, title, id, advertiser_id
        |            FROM ad_dim_oracle
        |            WHERE (advertiser_id = 12345)
        |             )
        |           ado3 ON (f0.ad_id = ado3.id)
        |           LEFT OUTER JOIN
        |           (SELECT  parent_id, value, id, advertiser_id
        |            FROM targetingattribute
        |            WHERE (advertiser_id = 12345)
        |             )
        |           t4 ON (f0.keyword_id = t4.id)
        |
        |
        |
        |) ORDER BY "Campaign Name" ASC NULLS LAST
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected =
      s"""
         |SELECT *
         |FROM (SELECT to_char(t3.id) "Keyword ID", to_char(ago2.campaign_id) "Campaign ID", ago2.name "Ad Group Name", to_char(t3.parent_id) "Ad Group ID", t3.value "Keyword Value", coalesce(f0."impressions", 1) "Impressions", co1.campaign_name "Campaign Name", ROUND(f0."CTR", 10) "CTR"
         |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) */
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
         |          LEFT OUTER JOIN
         |            (SELECT  campaign_id, name, id, advertiser_id
         |            FROM ad_group_oracle
         |
         |             ) ago2
         |              ON( t3.advertiser_id = ago2.advertiser_id AND t3.parent_id = ago2.id )
         |               LEFT OUTER JOIN
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected =
      s"""SELECT *
         |FROM (SELECT to_char(t4.id) "Keyword ID", to_char(ago3.campaign_id) "Campaign ID", ago3.name "Ad Group Name", to_char(t4.parent_id) "Ad Group ID", t4.value "Keyword Value", coalesce(f0."impressions", 1) "Impressions", co2.campaign_name "Campaign Name", ROUND(f0."CTR", 10) "CTR", ao1.name "Advertiser Name"
         |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) */
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
         |          LEFT OUTER JOIN
         |            (SELECT  advertiser_id, campaign_id, name, id
         |            FROM ad_group_oracle
         |
         |             ) ago3
         |              ON( t4.advertiser_id = ago3.advertiser_id AND t4.parent_id = ago3.id )
         |               LEFT OUTER JOIN
         |            (SELECT /*+ CampaignHint */ advertiser_id, campaign_name, id
         |            FROM campaign_oracle
         |
         |             ) co2
         |              ON( ago3.advertiser_id = co2.advertiser_id AND ago3.campaign_id = co2.id )
         |               LEFT OUTER JOIN
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)

    assert(queryPipelineTry.isSuccess)
    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = s"""SELECT *
                      |FROM (SELECT to_char(t3.id) "Keyword ID", coalesce(f0."impressions", 1) "Impressions", co1.campaign_name "Campaign Name", ROUND(f0."CTR", 10) "CTR"
                      |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) */
                      |                   keyword_id, campaign_id, SUM(impressions) AS "impressions", (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS "CTR"
                      |            FROM fact2 FactAlias
                      |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
                      |            GROUP BY keyword_id, campaign_id
                      |
                      |           ) f0
                      |           RIGHT OUTER JOIN
                      |               ( (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  id, advertiser_id
                      |            FROM targetingattribute
                      |            WHERE (advertiser_id = 12345)
                      |             ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100) t3
                      |          LEFT OUTER JOIN
                      |            (SELECT  id, campaign_id, advertiser_id
                      |            FROM ad_group_oracle
                      |
                      |             ) ago2
                      |              ON( t3.advertiser_id = ago2.advertiser_id AND t3.parent_id = ago2.id )
                      |               LEFT OUTER JOIN
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected =
      s"""SELECT *
         |FROM (SELECT to_char(t4.id) "Keyword ID", to_char(ago3.campaign_id) "Campaign ID", ago3.name "Ad Group Name", to_char(t4.parent_id) "Ad Group ID", to_char(t4.advertiser_id) "Advertiser ID", t4.value "Keyword Value", coalesce(f0."impressions", 1) "Impressions", co2.campaign_name "Campaign Name", ROUND(f0."CTR", 10) "CTR", ao1.name "Advertiser Name"
         |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) */
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
         |          LEFT OUTER JOIN
         |            (SELECT  name, id, campaign_id, advertiser_id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 12345)
         |             ) ago3
         |              ON( t4.advertiser_id = ago3.advertiser_id AND t4.parent_id = ago3.id )
         |               LEFT OUTER JOIN
         |            (SELECT /*+ CampaignHint */ campaign_name, id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             ) co2
         |              ON( ago3.advertiser_id = co2.advertiser_id AND ago3.campaign_id = co2.id )
         |               LEFT OUTER JOIN
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = s"""
                      |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
                      |FROM (SELECT to_char(f0.keyword_id) "Keyword ID", to_char(f0.campaign_id) "Campaign ID", f0."Month" "Month", to_char(f0.ad_group_id) "Ad Group ID", f0."Week" "Week", to_char(f0.stats_date, 'YYYY-MM-DD') "Day", coalesce(f0."impressions", 1) "Impressions", coalesce(f0."clicks", 0) "Clicks", ROUND(f0."CTR", 10) "CTR"
                      |      FROM (SELECT /*+ PARALLEL_INDEX(cb_campaign_k_stats 4) */
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = s"""
                      |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
                      |FROM (SELECT f0."Month" "Month", to_char(f0.advertiser_id) "Advertiser ID"
                      |      FROM (SELECT /*+ PARALLEL_INDEX(cb_campaign_k_stats 4) */
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
                             }
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Advertiser Status", "operator": "in", "values": ["ON"]}
                           ],
                           "paginationStartIndex":0,
                           "rowsPerPage":100,
                           "forceDimensionDriven": true
                         }"""

    val request: ReportingRequest = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema).toOption.get
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)

    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = """SELECT *
                     |      FROM (SELECT to_char(co1.id) "Campaign ID", to_char(ago2.id) "Ad Group ID", ao0."Advertiser Status" "Advertiser Status", co1.campaign_name "Campaign Name"
                     |            FROM
                     |               ( (SELECT  advertiser_id, campaign_id, id
                     |            FROM ad_group_oracle
                     |            WHERE (advertiser_id = 12345)
                     |             ) ago2
                     |          RIGHT OUTER JOIN
                     |            (SELECT /*+ CampaignHint */ advertiser_id, campaign_name, id
                     |            FROM campaign_oracle
                     |            WHERE (advertiser_id = 12345)
                     |             ) co1
                     |              ON( ago2.advertiser_id = co1.advertiser_id AND ago2.campaign_id = co1.id )
                     |               RIGHT OUTER JOIN
                     |            (SELECT  DECODE(status, 'ON', 'ON', 'OFF') AS "Advertiser Status", id
                     |            FROM advertiser_oracle
                     |            WHERE (id = 12345) AND (DECODE(status, 'ON', 'ON', 'OFF') IN ('ON'))
                     |             ) ao0
                     |              ON( co1.advertiser_id = ao0.id )
                     |               )
                     |
                     |           ) WHERE ROWNUM >= 1 AND ROWNUM <= 100""".stripMargin

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

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)

    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected =
      """
        |SELECT *
        |      FROM (SELECT DISTINCT ao0."Advertiser Status" "Advertiser Status"
        |            FROM
        |                (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  DECODE(status, 'ON', 'ON', 'OFF') AS "Advertiser Status", id
        |            FROM advertiser_oracle
        |            WHERE (id = 12345)
        |             ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100) ao0
        |
        |           )
      """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("successfully generate dim driven dim only query with filters and order by") {
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
                             }
                           ],
                          "filterExpressions": [
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(requestOption.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = """
                     |SELECT *
                     |      FROM (SELECT to_char(co1.id) "Campaign ID", ao0."Advertiser Status" "Advertiser Status", co1.campaign_name "Campaign Name"
                     |            FROM
                     |               ( (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT /*+ CampaignHint */ id, advertiser_id, campaign_name
                     |            FROM campaign_oracle
                     |            WHERE (advertiser_id = 12345)
                     |            ORDER BY 1 ASC  ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100) co1
                     |          LEFT OUTER JOIN
                     |            (SELECT  DECODE(status, 'ON', 'ON', 'OFF') AS "Advertiser Status", id
                     |            FROM advertiser_oracle
                     |            WHERE (id = 12345)
                     |             ) ao0
                     |              ON( co1.advertiser_id = ao0.id )
                     |               )
                     |
                     |           )""".stripMargin

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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = s"""
                      |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
                      |FROM (SELECT to_char(co2.id) "Campaign ID", to_char(af0.ad_group_id) "Ad Group ID", ao1."Advertiser Status" "Advertiser Status", co2.campaign_name "Campaign Name", coalesce(af0."impressions", 1) "Impressions", ROUND(af0."CTR", 10) "CTR"
                      |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
                      |                   advertiser_id, campaign_id, ad_group_id, SUM(impressions) AS "impressions", (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS "CTR"
                      |            FROM ad_fact1 FactAlias
                      |            WHERE (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
                      |            GROUP BY advertiser_id, campaign_id, ad_group_id
                      |
                      |           ) af0
                      |           RIGHT OUTER JOIN
                      |           (SELECT  DECODE(status, 'ON', 'ON', 'OFF') AS "Advertiser Status", id
                      |            FROM advertiser_oracle
                      |            WHERE (managed_by = 12345)
                      |             )
                      |           ao1 ON (af0.advertiser_id = ao1.id)
                      |           LEFT OUTER JOIN
                      |           (SELECT /*+ CampaignHint */ advertiser_id, campaign_name, id
                      |            FROM campaign_oracle
                      |
                      |             )
                      |           co2 ON (af0.campaign_id = co2.id)
                      |
                      |
                      |
                      |) ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
                     |""".stripMargin


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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    val expected =
       s"""SELECT *
         |FROM (SELECT to_char(ado3.campaign_id) "Campaign ID", ago2.name "Ad Group Name", to_char(ado3.ad_group_id) "Ad Group ID", coalesce(af0."impressions", 1) "Impressions", co1.campaign_name "Campaign Name", ado3.title "Ad Title", to_char(ado3.id) "Ad ID"
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
         |          LEFT OUTER JOIN
         |            (SELECT  campaign_id, name, id, advertiser_id
         |            FROM ad_group_oracle
         |
         |             ) ago2
         |              ON( ado3.advertiser_id = ago2.advertiser_id AND ado3.ad_group_id = ago2.id )
         |               LEFT OUTER JOIN
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

  test("successfully generate query for forced fact driven query specialized to use subquery instead of join") {
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    val expected =
      s"""SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
         |FROM (SELECT to_char(af0.stats_date, 'YYYY-MM-DD') "Day", ROUND(af0."Average CPC", 10) "Average CPC", ROUND((af0."Average CPC" * 100), 10) "Average CPC Cents", coalesce(ROUND(CASE WHEN ((af0."avg_pos" >= 0.1) AND (af0."avg_pos" <= 500)) THEN af0."avg_pos" ELSE 0.0 END, 10), 0.0) "Average Position", coalesce(af0."impressions", 1) "Impressions", coalesce(ROUND(af0."max_bid", 10), 0.0) "Max Bid", coalesce(ROUND(af0."spend", 10), 0.0) "Spend", ROUND(af0."CTR", 10) "CTR"
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
         |                   stats_date, SUM(impressions) AS "impressions", (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(CASE WHEN ((avg_pos >= 0.1) AND (avg_pos <= 500)) THEN avg_pos ELSE 0.0 END * impressions) / (SUM(impressions)) END) AS "avg_pos", SUM(spend) AS "spend", MAX(max_bid) AS "max_bid", (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS "CTR", SUM(CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END) AS "Average CPC"
         |            FROM ad_fact1 FactAlias
         |            WHERE (advertiser_id IN (SELECT id FROM advertiser_oracle WHERE (managed_by = 12345))) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY stats_date
         |
         |           ) af0
         |
         |
         |
         |) ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))
    assert(requestModel.toOption.get.isDebugEnabled, requestModel.errorMessage("Debug should be enabled!"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    val expected =
      s"""|SELECT *
         |FROM (SELECT to_char(t1.id) "Keyword ID", ksf0."Month" "Month", to_char(t1.parent_id) "Ad Group ID", ksf0."Week" "Week", to_char(ksf0.stats_date, 'YYYY-MM-DD') "Day", coalesce(ksf0."impressions", 1) "Impressions", coalesce(ksf0."clicks", 0) "Clicks", ROUND(ksf0."CTR", 10) "CTR"
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
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    val expected =
      s"""SELECT *
         |FROM (SELECT to_char(co1.id) "Campaign ID", coalesce(af0."impressions", 1) "Impressions", co1."Campaign Status" "Campaign Status"
         |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_ad_stats 4) */
         |                   campaign_id, SUM(impressions) AS "impressions"
         |            FROM ad_fact1 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY campaign_id
         |            HAVING (SUM(impressions) = 12345)
         |           ) af0
         |           RIGHT OUTER JOIN
         |                (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT /*+ CampaignHint */ DECODE(status, 'ON', 'ON', 'OFF') AS "Campaign Status", id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             ) WHERE ROWNUM <= 100) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 100) co1
         |            ON (af0.campaign_id = co1.id)
         |
         |
         |)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    val expected =
      s"""|SELECT *
         |FROM (SELECT to_char(ado1.campaign_id) "Campaign ID", to_char(ado1.ad_group_id) "Ad Group ID", coalesce(af0."impressions", 1) "Impressions", ado1.title "Ad Title", to_char(ado1.id) "Ad ID"
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

  test("test NoopRollup expression for generated query") {
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    val expected =
      s"""SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
          |FROM (SELECT to_char(af0.stats_date, 'YYYY-MM-DD') "Day", ROUND((CASE WHEN af0."clicks" = 0 THEN 0.0 ELSE af0."spend" / af0."clicks" END), 10) "Average CPC", coalesce(ROUND(CASE WHEN ((af0."avg_pos" >= 0.1) AND (af0."avg_pos" <= 500)) THEN af0."avg_pos" ELSE 0.0 END, 10), 0.0) "Average Position", coalesce(af0."impressions", 1) "Impressions", coalesce(ROUND(af0."max_bid", 10), 0.0) "Max Bid", coalesce(ROUND(af0."spend", 10), 0.0) "Spend", ROUND(af0."CTR", 10) "CTR"
          |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
          |                   stats_date, SUM(impressions) AS "impressions", (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(CASE WHEN ((avg_pos >= 0.1) AND (avg_pos <= 500)) THEN avg_pos ELSE 0.0 END * impressions) / (SUM(impressions)) END) AS "avg_pos", SUM(spend) AS "spend", MAX(max_bid) AS "max_bid", (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS "CTR", SUM(CASE WHEN ((clicks >= 1) AND (clicks <= 800)) THEN clicks ELSE 0 END) AS "clicks"
          |            FROM ad_fact1 FactAlias
          |            WHERE (advertiser_id IN (SELECT id FROM advertiser_oracle WHERE (managed_by = 12345))) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
          |            GROUP BY stats_date
          |
          |           ) af0
          |
          |
          |
          |) ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    val expected =
      s"""
         |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
         |FROM (SELECT to_char(ksnpo0.ad_id) "Ad ID", to_char(ksnpo0.stats_date, 'YYYY-MM-DD') "Day", ROUND(ksnpo0."Average CPC", 10) "Average CPC", coalesce(ROUND(CASE WHEN ((ksnpo0."avg_pos" >= 0.1) AND (ksnpo0."avg_pos" <= 500)) THEN ksnpo0."avg_pos" ELSE 0.0 END, 10), 0.0) "Average Position", coalesce(ksnpo0."impressions", 1) "Impressions", coalesce(ROUND(ksnpo0."max_bid", 10), 0.0) "Max Bid", coalesce(ROUND(ksnpo0."spend", 10), 0.0) "Spend", ROUND(ksnpo0."CTR", 10) "CTR"
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_campaign_k_stats 4) */
         |                   ad_id, stats_date, SUM(impressions) AS "impressions", (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(CASE WHEN ((avg_pos >= 0.1) AND (avg_pos <= 500)) THEN avg_pos ELSE 0.0 END * impressions) / (SUM(impressions)) END) AS "avg_pos", SUM(spend) AS "spend", MAX(max_bid) AS "max_bid", (spend / clicks) AS "Average CPC", (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS "CTR"
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(requestOption.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val factBest = queryPipelineTry.get.factBestCandidate
    assert(factBest.isDefined)
    assert(factBest.get.fact.isInstanceOf[ViewTable])
    assert(factBest.get.fact.asInstanceOf[ViewTable].name == "campaign_adjustment_view")
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(requestOption.toOption.get, registry)
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(requestOption.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    val expected = s"""SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
                      |FROM (SELECT to_char(vps0.publisher_id) "Publisher ID", coalesce(ROUND(vps0."spend", 10), 0.0) "Spend"
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(requestOption.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    val expected = s"""SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
                     |FROM (SELECT to_char(vps0.publisher_id) "Publisher ID", coalesce(ROUND(vps0."spend", 10), 0.0) "Spend"
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = s"""
                      |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
                      |FROM (SELECT to_char(ago3.campaign_id) "Campaign ID", to_char(ago3.id) "Ad Group ID", ago3."Ad Group Status" "Ad Group Status", ao1."Advertiser Status" "Advertiser Status", co2.campaign_name "Campaign Name", coalesce(af0."impressions", 1) "Impressions", ROUND(af0."CTR", 10) "CTR"
                      |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
                      |                   advertiser_id, campaign_id, ad_group_id, SUM(impressions) AS "impressions", (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS "CTR"
                      |            FROM ad_fact1 FactAlias
                      |            WHERE (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
                      |            GROUP BY advertiser_id, campaign_id, ad_group_id
                      |
                      |           ) af0
                      |           RIGHT OUTER JOIN
                      |           (SELECT  DECODE(status, 'ON', 'ON', 'OFF') AS "Advertiser Status", id
                      |            FROM advertiser_oracle
                      |            WHERE (managed_by = 12345)
                      |             )
                      |           ao1 ON (af0.advertiser_id = ao1.id)
                      |           LEFT OUTER JOIN
                      |           (SELECT /*+ CampaignHint */ advertiser_id, campaign_name, id
                      |            FROM campaign_oracle
                      |
                      |             )
                      |           co2 ON (af0.campaign_id = co2.id)
                      |           LEFT OUTER JOIN
                      |           (SELECT  advertiser_id, campaign_id, DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", id
                      |            FROM ad_group_oracle
                      |
                      |             )
                      |           ago3 ON (af0.ad_group_id = ago3.id)
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
                             { "field": "Ad Group Status" }
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(requestOption.toOption.get, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = """
                     |SELECT  *
                     |      FROM (SELECT to_char(ao0.id) "Advertiser ID", to_char(co1.id) "Campaign ID", co1.campaign_name "Campaign Name", to_char(ago2.id) "Ad Group ID", ago2."Ad Group Status" "Ad Group Status"
                     |            FROM
                     |               ( (SELECT  campaign_id, advertiser_id, DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", id
                     |            FROM ad_group_oracle
                     |            WHERE (advertiser_id = 12345)
                     |            ORDER BY 1 ASC  ) ago2
                     |          RIGHT OUTER JOIN
                     |            (SELECT /*+ CampaignHint */ id, advertiser_id, campaign_name
                     |            FROM campaign_oracle
                     |            WHERE (advertiser_id = 12345)
                     |             ) co1
                     |              ON( ago2.advertiser_id = co1.advertiser_id AND ago2.campaign_id = co1.id )
                     |               RIGHT OUTER JOIN
                     |            (SELECT  id
                     |            FROM advertiser_oracle
                     |            WHERE (id = 12345) AND (DECODE(status, 'ON', 'ON', 'OFF') = 'ON')
                     |             ) ao0
                     |              ON( co1.advertiser_id = ao0.id )
                     |               )
                     |
                     |           )
                     |            WHERE ( "Ad Group ID"   IS NULL) AND ROWNUM >= 1 AND ROWNUM <= 100
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    val expected =
      s"""
         |SELECT *
         |FROM (SELECT to_char(f0.keyword_id) "Keyword ID", t4.value "Keyword Value", co1.campaign_name "Campaign Name", to_char(f0.ad_group_id) "Ad Group ID", ago2."Ad Group Status" "Ad Group Status", ago2.name "Ad Group Name", ado3.title "Ad Title", coalesce(f0."impressions", 1) "Impressions", ROUND(f0."CTR", 10) "CTR"
         |      FROM (SELECT /*+ PARALLEL_INDEX(cb_campaign_k_stats 4) */
         |                   ad_group_id, ad_id, campaign_id, keyword_id, SUM(impressions) AS "impressions", (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS "CTR"
         |            FROM fact1 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_source = 2) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY ad_group_id, ad_id, campaign_id, keyword_id
         |
         |           ) f0
         |           LEFT OUTER JOIN
         |           (SELECT /*+ CampaignHint */ campaign_name, id, advertiser_id
         |            FROM campaign_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           co1 ON (f0.campaign_id = co1.id)
         |           LEFT OUTER JOIN
         |           (SELECT  campaign_id, name, DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", id, advertiser_id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           ago2 ON (f0.ad_group_id = ago2.id)
         |           LEFT OUTER JOIN
         |           (SELECT  ad_group_id, campaign_id, title, id, advertiser_id
         |            FROM ad_dim_oracle
         |            WHERE (advertiser_id = 12345)
         |             )
         |           ado3 ON (f0.ad_id = ado3.id)
         |           LEFT OUTER JOIN
         |           (SELECT  parent_id, value, id, advertiser_id
         |            FROM targetingattribute
         |            WHERE (advertiser_id = 12345)
         |             )
         |           t4 ON (f0.keyword_id = t4.id)
         |
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = s"""
                      |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
                      |FROM (SELECT to_char(f0.keyword_id) "Keyword ID", to_char(ago1.campaign_id) "Campaign ID", f0."Month" "Month", to_char(ago1.id) "Ad Group ID", ago1."Ad Group Status" "Ad Group Status", f0."Week" "Week", to_char(f0.stats_date, 'YYYY-MM-DD') "Day", coalesce(f0."impressions", 1) "Impressions", coalesce(f0."clicks", 0) "Clicks", ROUND(f0."CTR", 10) "CTR"
                      |      FROM (SELECT /*+ PARALLEL_INDEX(cb_campaign_k_stats 4) */
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

  test("successfully generate fact driven query with right outer join when schema required fields are not present in the fact and with outer filters") {
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))


    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = s"""
                      |SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT *
                      |FROM (SELECT to_char(ago3.campaign_id) "Campaign ID", to_char(ago3.id) "Ad Group ID", ago3."Ad Group Status" "Ad Group Status", ao1."Advertiser Status" "Advertiser Status", co2.campaign_name "Campaign Name", coalesce(af0."impressions", 1) "Impressions", ROUND(af0."CTR", 10) "CTR"
                      |      FROM (SELECT /*+ PARALLEL_INDEX(cb_ad_stats 4) */
                      |                   advertiser_id, campaign_id, ad_group_id, SUM(impressions) AS "impressions", (SUM(CASE WHEN impressions = 0 THEN 0.0 ELSE clicks / impressions END)) AS "CTR"
                      |            FROM ad_fact1 FactAlias
                      |            WHERE (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
                      |            GROUP BY advertiser_id, campaign_id, ad_group_id
                      |
                      |           ) af0
                      |           RIGHT OUTER JOIN
                      |           (SELECT  DECODE(status, 'ON', 'ON', 'OFF') AS "Advertiser Status", id
                      |            FROM advertiser_oracle
                      |            WHERE (managed_by = 12345)
                      |             )
                      |           ao1 ON (af0.advertiser_id = ao1.id)
                      |           LEFT OUTER JOIN
                      |           (SELECT /*+ CampaignHint */ advertiser_id, campaign_name, id
                      |            FROM campaign_oracle
                      |
                      |             )
                      |           co2 ON (af0.campaign_id = co2.id)
                      |           LEFT OUTER JOIN
                      |           (SELECT  advertiser_id, campaign_id, DECODE(status, 'ON', 'ON', 'OFF') AS "Ad Group Status", id
                      |            FROM ad_group_oracle
                      |
                      |             )
                      |           ago3 ON (af0.ad_group_id = ago3.id)
                      |
                      |) WHERE ( "Ad Group ID"   IS NULL) AND ( "Ad Group Status"   = 'ON')
                      |   ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
                      |
                      |""".stripMargin


    result should equal (expected)(after being whiteSpaceNormalised)
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
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ],
                           "forceDimensionDriven": true
                         }"""

    val request: ReportingRequest = ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), ResellerSchema).toOption.get
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))


    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expected = s"""
                      |SELECT  *
                      |      FROM (SELECT to_char(co0.id) "Campaign ID", co0.campaign_name "Campaign Name", to_char(ago1.id) "Ad Group ID"
                      |            FROM
                      |               ( (SELECT  campaign_id, id, advertiser_id
                      |            FROM ad_group_oracle
                      |            WHERE (advertiser_id = 12345)
                      |             ) ago1
                      |          RIGHT OUTER JOIN
                      |            (SELECT /*+ CampaignHint */ campaign_name, id, advertiser_id
                      |            FROM campaign_oracle
                      |            WHERE (advertiser_id = 12345) AND (DECODE(status, 'ON', 'ON', 'OFF') = 'ON')
                      |             ) co0
                      |              ON( ago1.advertiser_id = co0.advertiser_id AND ago1.campaign_id = co0.id )
                      |               )
                      |
                      |           )
                      |            WHERE ( "Ad Group ID"   IS NULL) AND ROWNUM >= 1 AND ROWNUM <= 200
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
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    val expected =
      s"""
         |SELECT *
         |FROM (SELECT to_char(t3.id) "Keyword ID", coalesce(f0."impressions", 1) "Impressions", COALESCE(f0.device_id, 'UNKNOWN') "Device ID", COALESCE(f0.network_type, 'NONE') "Network Type", COALESCE(f0.pricing_type, 'NONE') "Pricing Type", co1."Campaign Status" "Campaign Status"
         |      FROM (SELECT /*+ PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4) */
         |                   CASE WHEN (device_id IN (1)) THEN 'Desktop' WHEN (device_id IN (2)) THEN 'Tablet' WHEN (device_id IN (3)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END device_id, DECODE(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_SYNDICATION', 'Content Syndication', 'EXTERNAL', 'Yahoo Partners', 'INTERNAL', 'Yahoo Properties', 'NONE') network_type, CASE WHEN (pricing_type IN (1)) THEN 'CPC' WHEN (pricing_type IN (6)) THEN 'CPV' WHEN (pricing_type IN (2)) THEN 'CPA' WHEN (pricing_type IN (-10)) THEN 'CPE' WHEN (pricing_type IN (-20)) THEN 'CPF' WHEN (pricing_type IN (7)) THEN 'CPCV' WHEN (pricing_type IN (3)) THEN 'CPM' ELSE 'NONE' END pricing_type, campaign_id, keyword_id, SUM(impressions) AS "impressions"
         |            FROM fact2 FactAlias
         |            WHERE (advertiser_id = 12345) AND (stats_source IN (1,2)) AND (stats_date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND stats_date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY CASE WHEN (device_id IN (1)) THEN 'Desktop' WHEN (device_id IN (2)) THEN 'Tablet' WHEN (device_id IN (3)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END, DECODE(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_SYNDICATION', 'Content Syndication', 'EXTERNAL', 'Yahoo Partners', 'INTERNAL', 'Yahoo Properties', 'NONE'), CASE WHEN (pricing_type IN (1)) THEN 'CPC' WHEN (pricing_type IN (6)) THEN 'CPV' WHEN (pricing_type IN (2)) THEN 'CPA' WHEN (pricing_type IN (-10)) THEN 'CPE' WHEN (pricing_type IN (-20)) THEN 'CPF' WHEN (pricing_type IN (7)) THEN 'CPCV' WHEN (pricing_type IN (3)) THEN 'CPM' ELSE 'NONE' END, campaign_id, keyword_id
         |
         |           ) f0
         |           RIGHT OUTER JOIN
         |               ( (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  id, advertiser_id
         |            FROM targetingattribute
         |            WHERE (advertiser_id = 12345)
         |             ) WHERE ROWNUM <= 120) D ) WHERE ROW_NUMBER >= 21 AND ROW_NUMBER <= 120) t3
         |          LEFT OUTER JOIN
         |            (SELECT  id, campaign_id, advertiser_id
         |            FROM ad_group_oracle
         |            WHERE (advertiser_id = 12345)
         |             ) ago2
         |              ON( t3.advertiser_id = ago2.advertiser_id AND t3.parent_id = ago2.id )
         |               LEFT OUTER JOIN
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
