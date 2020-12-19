// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.bigquery

import com.yahoo.maha.core._
import com.yahoo.maha.core.query.{QueryGeneratorRegistry, _}
import com.yahoo.maha.core.request.ReportingRequest

class BigqueryQueryGeneratorTest extends BaseBigqueryQueryGeneratorTest {

  lazy val defaultRegistry = getDefaultRegistry()

  test("Registering Bigquery query generation multiple times should fail") {
    intercept[IllegalArgumentException] {
      val dummyQueryGenerator = new QueryGenerator[WithBigqueryEngine] {
        override def generate(queryContext: QueryContext): Query = {
          null
        }
        override def engine: Engine = BigqueryEngine
      }

      val queryGeneratorRegistryTest = new QueryGeneratorRegistry
      queryGeneratorRegistryTest.register(BigqueryEngine, dummyQueryGenerator)
      BigqueryQueryGenerator.register(queryGeneratorRegistryTest, BigqueryPartitionColumnRenderer, TestUDFRegistrationFactory())

      queryGeneratorRegistry.register(BigqueryEngine, dummyQueryGenerator)
    }
  }

  test("Generating Bigquery query") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "bigquery_query_generator_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))
    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    val expected =
      s"""
         |SELECT IFNULL(CAST(mang_day AS STRING), '') AS mang_day, IFNULL(CAST(advertiser_id AS STRING), '') AS advertiser_id, IFNULL(CAST(campaign_id AS STRING), '') AS campaign_id, IFNULL(CAST(ad_group_id AS STRING), '') AS ad_group_id, IFNULL(CAST(keyword_id AS STRING), '') AS keyword_id, IFNULL(CAST(mang_ad_format_name AS STRING), '') AS mang_ad_format_name, IFNULL(CAST(mang_ad_format_sub_type AS STRING), '') AS mang_ad_format_sub_type, IFNULL(CAST(mang_keyword AS STRING), '') AS mang_keyword, IFNULL(CAST(mang_search_term AS STRING), '') AS mang_search_term, IFNULL(CAST(mang_delivered_match_type AS STRING), '') AS mang_delivered_match_type, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions
         |FROM (
         |SELECT FORMAT_DATETIME('%Y-%m-%d', stats_date) mang_day, COALESCE(account_id, 0) advertiser_id, COALESCE(campaign_id, '') campaign_id, COALESCE(ad_group_id, 0) ad_group_id, COALESCE(keyword_id, 0) keyword_id, COALESCE(ad_format_id, 0) mang_ad_format_name, COALESCE(ad_format_sub_type, 0) mang_ad_format_sub_type, getCsvEscapedString(IFNULL(CAST(keyword AS STRING), '')) mang_keyword, COALESCE(search_term, 'None') mang_search_term, COALESCE(delivered_match_type, 0) mang_delivered_match_type, COALESCE(impressions, 1) mang_impressions
         |FROM ( SELECT CASE WHEN (delivered_match_type IN (1)) THEN 'Exact' WHEN (delivered_match_type IN (2)) THEN 'Broad' WHEN (delivered_match_type IN (3)) THEN 'Phrase' ELSE 'UNKNOWN' END delivered_match_type, stats_date, keyword, ad_group_id, search_term, account_id, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (5)) THEN 'Single image' WHEN (ad_format_id IN (6)) THEN 'Single image' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (9)) THEN 'Carousel' WHEN (ad_format_id IN (2)) THEN 'Single image' WHEN (ad_format_id IN (7)) THEN 'Video' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (3)) THEN 'Single image' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (8)) THEN 'Video with HTML Endcard' WHEN (ad_format_id IN (4)) THEN 'Single image' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'Other' END ad_format_id, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'N/A' END ad_format_sub_type, campaign_id, keyword_id, SUM(impressions) impressions
         |FROM `s_stats_fact`
         |WHERE (account_id = 12345) AND (stats_date >= DATE('$fromDate') AND stats_date <= DATE('$toDate'))
         |GROUP BY CASE WHEN (delivered_match_type IN (1)) THEN 'Exact' WHEN (delivered_match_type IN (2)) THEN 'Broad' WHEN (delivered_match_type IN (3)) THEN 'Phrase' ELSE 'UNKNOWN' END, stats_date, keyword, ad_group_id, search_term, account_id, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (5)) THEN 'Single image' WHEN (ad_format_id IN (6)) THEN 'Single image' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (9)) THEN 'Carousel' WHEN (ad_format_id IN (2)) THEN 'Single image' WHEN (ad_format_id IN (7)) THEN 'Video' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (3)) THEN 'Single image' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (8)) THEN 'Video with HTML Endcard' WHEN (ad_format_id IN (4)) THEN 'Single image' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'Other' END, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'N/A' END, campaign_id, keyword_id
         |)
         |ssf0
         |ORDER BY mang_impressions ASC
         |) queryAlias LIMIT 100
       """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Generating Bigquery query with custom rollups") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "bigquery_query_generator_test_custom_rollups.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    val expected =
      s"""
         |SELECT IFNULL(CAST(mang_day AS STRING), '') AS mang_day, IFNULL(CAST(advertiser_id AS STRING), '') AS advertiser_id, IFNULL(CAST(campaign_id AS STRING), '') AS campaign_id, IFNULL(CAST(ad_group_id AS STRING), '') AS ad_group_id, IFNULL(CAST(mang_bid_strategy AS STRING), '') AS mang_bid_strategy, IFNULL(CAST(mang_forecasted_spend AS STRING), '') AS mang_forecasted_spend, IFNULL(CAST(mang_bid_modifier AS STRING), '') AS mang_bid_modifier, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions, IFNULL(CAST(mang_max_rollup_spend AS STRING), '') AS mang_max_rollup_spend, IFNULL(CAST(mang_min_rollup_spend AS STRING), '') AS mang_min_rollup_spend, IFNULL(CAST(mang_avg_rollup_spend AS STRING), '') AS mang_avg_rollup_spend, IFNULL(CAST(mang_custom_rollup_spend AS STRING), '') AS mang_custom_rollup_spend, IFNULL(CAST(mang_noop_rollup_spend AS STRING), '') AS mang_noop_rollup_spend
         |FROM (
         |SELECT FORMAT_DATETIME('YYYYMMdd', mang_day) mang_day, COALESCE(account_id, 0) advertiser_id, COALESCE(campaign_id, 0) campaign_id, COALESCE(ad_group_id, 0) ad_group_id, COALESCE(bid_strategy, 0) mang_bid_strategy, ROUND(COALESCE(mang_forecasted_spend, 0.0), 10) mang_forecasted_spend, ROUND(COALESCE(mang_bid_modifier, 0.0), 10) mang_bid_modifier, COALESCE(actual_impressions, 0) mang_impressions, ROUND(COALESCE(mang_max_rollup_spend, 0.0), 10) mang_max_rollup_spend, ROUND(COALESCE(mang_min_rollup_spend, 0.0), 10) mang_min_rollup_spend, ROUND(COALESCE(mang_avg_rollup_spend, 0.0), 10) mang_avg_rollup_spend, ROUND(COALESCE(mang_custom_rollup_spend, 0.0), 10) mang_custom_rollup_spend, ROUND(COALESCE(mang_noop_rollup_spend, 0.0), 10) mang_noop_rollup_spend
         |FROM ( SELECT CASE WHEN (bid_strategy IN (1)) THEN 'Max Click' WHEN (bid_strategy IN (2)) THEN 'Inflection Point' ELSE 'NONE' END bid_strategy, ad_group_id, account_id, campaign_id, (modified_bid - current_bid) / current_bid * 100 mang_bid_modifier, SUBSTR(load_time, 1, 8) mang_day, SUM(actual_impressions) actual_impressions, (spend * forecasted_clicks / actual_clicks * recommended_bid / modified_bid) mang_noop_rollup_spend, AVG(spend * forecasted_clicks / actual_clicks * recommended_bid / modified_bid) mang_avg_rollup_spend, MAX(spend * forecasted_clicks / actual_clicks * recommended_bid / modified_bid) mang_max_rollup_spend, MIN(spend * forecasted_clicks / actual_clicks * recommended_bid / modified_bid) mang_min_rollup_spend, SUM(spend * forecasted_clicks / actual_clicks * recommended_bid / modified_bid) mang_forecasted_spend, () mang_custom_rollup_spend
         |FROM `bidreco_complete`
         |WHERE (account_id = 12345) AND (status = 'Valid') AND (SUBSTR(load_time, 1, 8) >= DATE('$fromDate') AND SUBSTR(load_time, 1, 8) <= DATE('$toDate'))
         |GROUP BY CASE WHEN (bid_strategy IN (1)) THEN 'Max Click' WHEN (bid_strategy IN (2)) THEN 'Inflection Point' ELSE 'NONE' END, ad_group_id, account_id, campaign_id, (modified_bid - current_bid) / current_bid * 100, SUBSTR(load_time, 1, 8)
         |HAVING (spend IN (1))
         |)
         |bc0
         |ORDER BY mang_impressions ASC
         |) queryAlias LIMIT 100
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("User stats hourly") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "user_stats_hourly.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    val sourceForceFilter: EqualityFilter = EqualityFilter("Source", "2", isForceFilter = true)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))
    assert(queryPipelineTry.toOption.get.factBestCandidate.get.filters.size == 3, requestModel.errorMessage("Building request model failed"))
    assert(queryPipelineTry.toOption.get.factBestCandidate.get.filters.contains(sourceForceFilter), requestModel.errorMessage("Building request model failed"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    val expected =
      s"""
         |SELECT IFNULL(CAST(advertiser_id AS STRING), '') AS advertiser_id, IFNULL(CAST(campaign_id AS STRING), '') AS campaign_id, IFNULL(CAST(ad_group_id AS STRING), '') AS ad_group_id, IFNULL(CAST(mang_day AS STRING), '') AS mang_day, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions, IFNULL(CAST(mang_ad_extn_spend AS STRING), '') AS mang_ad_extn_spend, IFNULL(CAST(mang_hour AS STRING), '') AS mang_hour
         |FROM (
         |SELECT COALESCE(account_id, 0) advertiser_id, COALESCE(campaign_id, 0) campaign_id, COALESCE(ad_group_id, 0) ad_group_id, FORMAT_DATETIME('%Y-%m-%d', stats_date) mang_day, COALESCE(impressions, 1) mang_impressions, ROUND(COALESCE(ad_extn_spend, 0.0), 10) mang_ad_extn_spend, COALESCE(stats_hour, '') mang_hour
         |FROM ( SELECT stats_date, stats_hour, ad_group_id, account_id, campaign_id, SUM(impressions) impressions, SUM(ad_extn_spend) ad_extn_spend
         |FROM `aga_fact`
         |WHERE (account_id = 12345) AND (gender = 'F') AND (stats_source = 2) AND (stats_date >= DATE('$fromDate') AND stats_date <= DATE('$toDate'))
         |GROUP BY stats_date, stats_hour, ad_group_id, account_id, campaign_id
         |)
         |af0
         |) queryAlias LIMIT 200
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Date type columns should be rendered with FORMAT_DATETIME function in outer select") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "bigquery_request_with_date_field.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    assert(result.contains("FORMAT_DATETIME"), "Date type columms should include FORMAT_DATETIME")
  }

  test("Should escape string if escaping is required") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "bigquery_request_with_escaping_required_field.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    assert(result.contains("getCsvEscapedString"), "Should escape string if escaping is required")
  }

  test("Bigquery query should contain UDF definitions ") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "bigquery_query_generator_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery]
    assert(query.udfStatements.nonEmpty, "Bigquery Query should contain udf statements")
  }

  test("Should mangle non-id column aliases") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "bigquery_query_generator_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    assert(result.contains("mang_search_term"), "Should mangle non-id field: Search Term")
    assert(result.contains("mang_day"), "Should mangle non-id field: Day")
    assert(result.contains("mang_keyword"), "Should mangle non-id field: Keyword")
    assert(result.contains("mang_impression"), "Should mangle non-id field: Impression")
    assert(result.contains("mang_delivered_match_type"), "Should mangle non-id field: Delivered Match Type")
  }

  test("Should return empty string for NULL string values") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "bigquery_request_with_string_field.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    assert(result.contains("COALESCE(a1.mang_advertiser_status, '') mang_advertiser_status"), "Should support '' for NULL string")
  }

  test("Query with request DecType fields that contains max and min should return query with max and min range") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "bigquery_request_with_min_max_dec_field.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    assert(result.contains("ROUND(COALESCE(CASE WHEN ((mang_average_position >= 0.1) AND (mang_average_position <= 500)) THEN mang_average_position ELSE 0.0 END, 0.0), 10) mang_average_position"), "Should support “NA” for NULL string")
  }

  test("Query with constant requested fields should have constant columns") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "bigquery_request_with_constant_field.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    assert(result.contains("'2' mang_source"), "No constant field in outer columns")
  }

  test("Fact group by clause should only include dim columns") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "bigquery_request_with_fact_n_factder_field.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    assert(result.contains("GROUP BY landing_page_url, stats_date\n"), "Group by should only include dim columns")
  }

  test("Bigquery multi dimensional query") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "bigquery_request_with_multiple_dimension.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    val expected =
      s"""
         |SELECT IFNULL(CAST(mang_day AS STRING), '') AS mang_day, IFNULL(CAST(mang_destination_url AS STRING), '') AS mang_destination_url, IFNULL(CAST(campaign_id AS STRING), '') AS campaign_id, IFNULL(CAST(mang_campaign_name AS STRING), '') AS mang_campaign_name, IFNULL(CAST(mang_campaign_status AS STRING), '') AS mang_campaign_status, IFNULL(CAST(mang_advertiser_status AS STRING), '') AS mang_advertiser_status, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions
         |FROM (
         |SELECT FORMAT_DATETIME('%Y-%m-%d', stats_date) mang_day, getCsvEscapedString(IFNULL(CAST(landing_page_url AS STRING), '')) mang_destination_url, COALESCE(ssf0.campaign_id, '') campaign_id, getCsvEscapedString(IFNULL(CAST(c2.mang_campaign_name AS STRING), '')) mang_campaign_name, COALESCE(c2.mang_campaign_status, '') mang_campaign_status, COALESCE(a1.mang_advertiser_status, '') mang_advertiser_status, COALESCE(impressions, 1) mang_impressions
         |FROM ( SELECT account_id, landing_page_url, campaign_id, stats_date, SUM(impressions) impressions
         |FROM `s_stats_fact`
         |WHERE (account_id = 12345) AND (stats_date >= DATE('$fromDate') AND stats_date <= DATE('$toDate'))
         |GROUP BY account_id, landing_page_url, campaign_id, stats_date
         |)
         |ssf0
         |LEFT OUTER JOIN (
         |SELECT CASE WHEN status = 'ON' THEN 'ON' ELSE 'OFF' END AS mang_advertiser_status, id a1_id
         |FROM `advertiser_bigquery`
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
         |)
         |a1
         |ON
         |ssf0.account_id = a1.a1_id
         |LEFT OUTER JOIN (
         |SELECT advertiser_id AS advertiser_id, campaign_name AS mang_campaign_name, CASE WHEN status = 'ON' THEN 'ON' ELSE 'OFF' END AS mang_campaign_status, id c2_id
         |FROM `campaign_bigquery`
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c2
         |ON
         |ssf0.campaign_id = c2.c2_id
         |ORDER BY mang_impressions ASC
         |) queryAlias LIMIT 100
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("DateTime type columns should be rendered with getDateTimeFromEpoch udf in outer select") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "ce_stats.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    assert(result.contains("getDateTimeFromEpoch"), "Date type columms should be wrapped in getFormattedDate udf")
  }

  test("NoopRollup expression for generated query") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "bigquery_query_nooprollup_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    val expected =
      s"""
         |SELECT IFNULL(CAST(mang_day AS STRING), '') AS mang_day, IFNULL(CAST(advertiser_id AS STRING), '') AS advertiser_id, IFNULL(CAST(campaign_id AS STRING), '') AS campaign_id, IFNULL(CAST(ad_group_id AS STRING), '') AS ad_group_id, IFNULL(CAST(keyword_id AS STRING), '') AS keyword_id, IFNULL(CAST(mang_keyword AS STRING), '') AS mang_keyword, IFNULL(CAST(mang_ad_format_name AS STRING), '') AS mang_ad_format_name, IFNULL(CAST(mang_ad_format_sub_type AS STRING), '') AS mang_ad_format_sub_type, IFNULL(CAST(mang_search_term AS STRING), '') AS mang_search_term, IFNULL(CAST(mang_delivered_match_type AS STRING), '') AS mang_delivered_match_type, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions, IFNULL(CAST(mang_average_cpc AS STRING), '') AS mang_average_cpc
         |FROM (
         |SELECT FORMAT_DATETIME('%Y-%m-%d', stats_date) mang_day, COALESCE(account_id, 0) advertiser_id, COALESCE(campaign_id, '') campaign_id, COALESCE(ad_group_id, 0) ad_group_id, COALESCE(keyword_id, 0) keyword_id, getCsvEscapedString(IFNULL(CAST(keyword AS STRING), '')) mang_keyword, COALESCE(ad_format_id, 0) mang_ad_format_name, COALESCE(ad_format_sub_type, 0) mang_ad_format_sub_type, COALESCE(search_term, 'None') mang_search_term, COALESCE(delivered_match_type, 0) mang_delivered_match_type, COALESCE(impressions, 1) mang_impressions, ROUND(COALESCE((CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END), 0.0), 10) mang_average_cpc
         |FROM ( SELECT CASE WHEN (delivered_match_type IN (1)) THEN 'Exact' WHEN (delivered_match_type IN (2)) THEN 'Broad' WHEN (delivered_match_type IN (3)) THEN 'Phrase' ELSE 'UNKNOWN' END delivered_match_type, stats_date, keyword, ad_group_id, search_term, account_id, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (5)) THEN 'Single image' WHEN (ad_format_id IN (6)) THEN 'Single image' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (9)) THEN 'Carousel' WHEN (ad_format_id IN (2)) THEN 'Single image' WHEN (ad_format_id IN (7)) THEN 'Video' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (3)) THEN 'Single image' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (8)) THEN 'Video with HTML Endcard' WHEN (ad_format_id IN (4)) THEN 'Single image' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'Other' END ad_format_id, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'N/A' END ad_format_sub_type, campaign_id, keyword_id, SUM(impressions) impressions, SUM(clicks) clicks, SUM(spend) spend
         |FROM `s_stats_fact`
         |WHERE (account_id = 12345) AND (stats_date >= DATE('$fromDate') AND stats_date <= DATE('$toDate'))
         |GROUP BY CASE WHEN (delivered_match_type IN (1)) THEN 'Exact' WHEN (delivered_match_type IN (2)) THEN 'Broad' WHEN (delivered_match_type IN (3)) THEN 'Phrase' ELSE 'UNKNOWN' END, stats_date, keyword, ad_group_id, search_term, account_id, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (5)) THEN 'Single image' WHEN (ad_format_id IN (6)) THEN 'Single image' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (9)) THEN 'Carousel' WHEN (ad_format_id IN (2)) THEN 'Single image' WHEN (ad_format_id IN (7)) THEN 'Video' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (3)) THEN 'Single image' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (8)) THEN 'Video with HTML Endcard' WHEN (ad_format_id IN (4)) THEN 'Single image' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'Other' END, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'N/A' END, campaign_id, keyword_id
         |) ssf0
         |ORDER BY mang_impressions ASC
         |) queryAlias LIMIT 100
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("JoinType for non-FK dim filters for generated query") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "bigquery_query_with_non_fk_dim_filters_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    assert(requestModel.toOption.get.anyDimHasNonFKNonForceFilter == true)

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    assert(result.contains("ssf0\nJOIN ("))
  }

  test("JoinType for fact driven queries generated query") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "bigquery_query_with_wo_non_fk_dim_filters_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    assert(requestModel.toOption.get.anyDimHasNonFKNonForceFilter == false)

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    assert(result.contains("LEFT OUTER JOIN"))
  }

  test("Like filter with Bigquery query") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "bigquery_query_with_dim_like_filter.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    assert(result.contains("(lower(campaign_name) LIKE lower('%yahoo%'))"))
    val expected =
      s"""
         |SELECT IFNULL(CAST(mang_day AS STRING), '') AS mang_day, IFNULL(CAST(advertiser_id AS STRING), '') AS advertiser_id, IFNULL(CAST(campaign_id AS STRING), '') AS campaign_id, IFNULL(CAST(mang_campaign_name AS STRING), '') AS mang_campaign_name, IFNULL(CAST(ad_group_id AS STRING), '') AS ad_group_id, IFNULL(CAST(keyword_id AS STRING), '') AS keyword_id, IFNULL(CAST(mang_keyword AS STRING), '') AS mang_keyword, IFNULL(CAST(mang_search_term AS STRING), '') AS mang_search_term, IFNULL(CAST(mang_delivered_match_type AS STRING), '') AS mang_delivered_match_type, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions, IFNULL(CAST(mang_average_cpc_cents AS STRING), '') AS mang_average_cpc_cents, IFNULL(CAST(mang_average_cpc AS STRING), '') AS mang_average_cpc
         |FROM (
         |SELECT FORMAT_DATETIME('%Y-%m-%d', stats_date) mang_day, COALESCE(account_id, 0) advertiser_id, COALESCE(ssf0.campaign_id, '') campaign_id, getCsvEscapedString(IFNULL(CAST(c1.mang_campaign_name AS STRING), '')) mang_campaign_name, COALESCE(ad_group_id, 0) ad_group_id, COALESCE(keyword_id, 0) keyword_id, getCsvEscapedString(IFNULL(CAST(keyword AS STRING), '')) mang_keyword, COALESCE(search_term, 'None') mang_search_term, COALESCE(delivered_match_type, 0) mang_delivered_match_type, COALESCE(impressions, 1) mang_impressions, ROUND(COALESCE((mang_average_cpc * 100), 0.0), 10) mang_average_cpc_cents, ROUND(COALESCE(mang_average_cpc, 0.0), 10) mang_average_cpc
         |FROM ( SELECT CASE WHEN (delivered_match_type IN (1)) THEN 'Exact' WHEN (delivered_match_type IN (2)) THEN 'Broad' WHEN (delivered_match_type IN (3)) THEN 'Phrase' ELSE 'UNKNOWN' END delivered_match_type, stats_date, keyword, ad_group_id, search_term, account_id, campaign_id, keyword_id, SUM(impressions) impressions, (CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END) mang_average_cpc
         |FROM `s_stats_fact`
         |WHERE (account_id = 12345) AND (stats_date >= DATE('$fromDate') AND stats_date <= DATE('$toDate'))
         |GROUP BY CASE WHEN (delivered_match_type IN (1)) THEN 'Exact' WHEN (delivered_match_type IN (2)) THEN 'Broad' WHEN (delivered_match_type IN (3)) THEN 'Phrase' ELSE 'UNKNOWN' END, stats_date, keyword, ad_group_id, search_term, account_id, campaign_id, keyword_id
         |)
         |ssf0
         |JOIN (
         |SELECT campaign_name AS mang_campaign_name, id c1_id
         |FROM `campaign_bigquery`
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345) AND (lower(campaign_name) LIKE lower('%yahoo%'))
         |)
         |c1
         |ON
         |ssf0.campaign_id = c1.c1_id
         |ORDER BY mang_impressions ASC
         |) queryAlias LIMIT 100
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Like filter with Bigquery query injection testing") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "bigquery_query_with_dim_like_filter_injection_testing.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    assert(result.contains("(lower(campaign_name) LIKE lower('%Server Log Avoidance\t #alert(1) #alert(1) # alert(1) Shortest PoC\t $ while:; do echo \"alert(1)\" | nc -lp80; done%')"))
  }

  test("Missing group by fact cols") {
    val jsonString =
      s"""
         |{
         |  "cube": "bid_reco",
         |  "selectFields": [
         |    { "field": "Advertiser ID" },
         |    { "field": "Ad Group ID" },
         |    { "field": "Campaign ID" },
         |    { "field": "Bid Strategy" },
         |    { "field": "Current Base Bid" },
         |    { "field": "Modified Bid" },
         |    { "field": "Bid Modifier" },
         |    { "field": "Recommended Bid" },
         |    { "field": "Clicks" },
         |    { "field": "Forecasted Clicks" },
         |    { "field": "Impressions" },
         |    { "field": "Forecasted Impressions" },
         |    { "field": "Budget" },
         |    { "field": "Spend" },
         |    { "field": "Forecasted Spend" }
         |  ],
         |  "filterExpressions": [
         |    { "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" },
         |    { "field": "Advertiser ID", "operator": "=", "value": "12345" }
         |  ]
         |}
      """.stripMargin
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    assert(result.contains("GROUP BY modified_bid, CASE WHEN (bid_strategy IN (1)) THEN 'Max Click' WHEN (bid_strategy IN (2)) THEN 'Inflection Point' ELSE 'NONE' END, ad_group_id, account_id, campaign_id, current_bid, (modified_bid - current_bid) / current_bid * 100"))
  }

  test("Verify dim query can generate inner select and group by with static mapping") {
    val jsonString =
      s"""
         |{
         |  "cube": "s_stats",
         |  "selectFields": [
         |    {"field": "Device ID"},
         |    {"field": "Advertiser ID"},
         |    {"field": "Impressions"},
         |    {"field": "Pricing Type"},
         |    {"field": "Network ID"}
         |  ],
         |  "filterExpressions": [
         |    {"field": "Advertiser ID", "operator": "=", "value": "12345"},
         |    {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
         |  ]
         |}
       """.stripMargin

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString

    val expected =
      s"""
         |SELECT IFNULL(CAST(device_id AS STRING), '') AS device_id, IFNULL(CAST(advertiser_id AS STRING), '') AS advertiser_id, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions, IFNULL(CAST(mang_pricing_type AS STRING), '') AS mang_pricing_type, IFNULL(CAST(network_id AS STRING), '') AS network_id
         |FROM (
         |SELECT COALESCE(device_id, 0) device_id, COALESCE(account_id, 0) advertiser_id, COALESCE(impressions, 1) mang_impressions, COALESCE(price_type, 0) mang_pricing_type, COALESCE(network_type, '') network_id
         |FROM ( SELECT CASE WHEN (network_type IN ('TEST_PUBLISHER')) THEN 'Test Publisher' WHEN (network_type IN ('CONTENT_S')) THEN 'Content Secured' WHEN (network_type IN ('EXTERNAL')) THEN 'External Partners' WHEN (network_type IN ('INTERNAL')) THEN 'Internal Properties' ELSE 'NONE' END network_type, CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END price_type, account_id, CASE WHEN (device_id IN (5199520)) THEN 'SmartPhone' WHEN (device_id IN (5199503)) THEN 'Tablet' WHEN (device_id IN (5199421)) THEN 'Desktop' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END device_id, SUM(impressions) impressions
         |FROM `s_stats_fact`
         |WHERE (account_id = 12345) AND (stats_date >= DATE('$fromDate') AND stats_date <= DATE('$toDate'))
         |GROUP BY CASE WHEN (network_type IN ('TEST_PUBLISHER')) THEN 'Test Publisher' WHEN (network_type IN ('CONTENT_S')) THEN 'Content Secured' WHEN (network_type IN ('EXTERNAL')) THEN 'External Partners' WHEN (network_type IN ('INTERNAL')) THEN 'Internal Properties' ELSE 'NONE' END, CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END, account_id, CASE WHEN (device_id IN (5199520)) THEN 'SmartPhone' WHEN (device_id IN (5199503)) THEN 'Tablet' WHEN (device_id IN (5199421)) THEN 'Desktop' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END
         |)
         |ssf0
         |) queryAlias LIMIT 200
      """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Duplicate registration of the generator") {
    val failRegistry = new QueryGeneratorRegistry
    val dummyBigqueryQueryGenerator = new QueryGenerator[WithBigqueryEngine] {
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

    failRegistry.register(OracleEngine, dummyBigqueryQueryGenerator)
    failRegistry.register(DruidEngine, dummyFalseQueryGenerator)

    BigqueryQueryGenerator.register(failRegistry, BigqueryPartitionColumnRenderer, TestUDFRegistrationFactory())
  }

  test("Generating bigquery query with sort on dimension") {
    val jsonString =
      s"""
         |{
         |  "cube": "s_stats",
         |  "selectFields": [
         |    { "field": "Advertiser ID" },
         |    { "field": "Advertiser Name" },
         |    { "field": "Impressions" }
         |  ],
         |  "filterExpressions": [
         |    { "field": "Advertiser ID", "operator": "=", "value": "12345" },
         |    { "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" },
         |    { "field": "Impressions", "operator": ">", "value": "1608" }
         |  ],
         |  "sortBy": [
         |    { "field": "Advertiser Name", "order": "Desc" },
         |    { "field": "Impressions", "order": "DESC" }
         |  ]
         |}
       """.stripMargin

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v0)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString

    val expected =
      s"""
         |SELECT IFNULL(CAST(advertiser_id AS STRING), '') AS advertiser_id, IFNULL(CAST(mang_advertiser_name AS STRING), '') AS mang_advertiser_name, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions
         |FROM (
         |SELECT COALESCE(ssf0.account_id, 0) advertiser_id, COALESCE(a1.mang_advertiser_name, '') mang_advertiser_name, COALESCE(impressions, 1) mang_impressions
         |FROM ( SELECT account_id, SUM(impressions) impressions
         |FROM `s_stats_fact`
         |WHERE (account_id = 12345) AND (stats_date >= DATE('$fromDate') AND stats_date <= DATE('$toDate'))
         |GROUP BY account_id
         |HAVING (impressions > 1608)
         |)
         |ssf0
         |LEFT OUTER JOIN (
         |SELECT name AS mang_advertiser_name, id a1_id
         |FROM `advertiser_bigquery`
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
         |)
         |a1
         |ON
         |ssf0.account_id = a1.a1_id
         |ORDER BY mang_advertiser_name DESC, mang_impressions DESC
         |) queryAlias LIMIT 200
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Generating Bigquery query with greater than filter") {
    val jsonString =
      s"""
         |{
         |  "cube": "s_stats",
         |  "selectFields": [
         |    { "field": "Advertiser Name" },
         |    { "field": "Impressions" }
         |  ],
         |  "filterExpressions": [
         |    { "field": "Advertiser ID", "operator": "=", "value": "12345" },
         |    { "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" },
         |    { "field": "Impressions", "operator": ">", "value": "1608" }
         |  ],
         |  "sortBy": [
         |    { "field": "Advertiser Name", "order": "Desc" },
         |    { "field": "Impressions", "order": "DESC" }
         |  ]
         |}
       """.stripMargin

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v0)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString

    val expected =
      s"""
         |SELECT IFNULL(CAST(mang_advertiser_name AS STRING), '') AS mang_advertiser_name,
         |  IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions
         |FROM (
         |SELECT mang_advertiser_name AS mang_advertiser_name, impressions AS mang_impressions
         |FROM (
         |SELECT COALESCE(a1.mang_advertiser_name, '') mang_advertiser_name, SUM(impressions) AS impressions
         |FROM ( SELECT account_id, SUM(impressions) impressions
         |FROM `s_stats_fact`
         |WHERE (account_id = 12345) AND (stats_date >= DATE('$fromDate') AND stats_date <= DATE('$toDate'))
         |GROUP BY account_id
         |HAVING (impressions > 1608)
         |        )
         |ssf0
         |LEFT OUTER JOIN (
         |SELECT name AS mang_advertiser_name, id a1_id
         |FROM `advertiser_bigquery`
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
         |)
         |a1
         |ON
         |ssf0.account_id = a1.a1_id
         |GROUP BY mang_advertiser_name
         |ORDER BY mang_advertiser_name DESC, impressions DESC) OgbQueryAlias
         |) queryAlias LIMIT 200
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Generating bigquery query with less than filter") {
    val jsonString =
      s"""
         |{
         |  "cube": "s_stats",
         |  "selectFields": [
         |    { "field": "Advertiser ID" },
         |    { "field": "Impressions" }
         |  ],
         |  "filterExpressions": [
         |    { "field": "Advertiser ID", "operator": "=", "value": "12345" },
         |    { "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" },
         |    { "field": "Impressions", "operator": "<", "value": "1608" }
         |  ]
         |}
       """.stripMargin

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString

    val expected =
      s"""
         |SELECT IFNULL(CAST(advertiser_id AS STRING), '') AS advertiser_id, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions
         |FROM (
         |SELECT COALESCE(account_id, 0) advertiser_id, COALESCE(impressions, 1) mang_impressions
         |FROM ( SELECT account_id, SUM(impressions) impressions
         |FROM `s_stats_fact`
         |WHERE (account_id = 12345) AND (stats_date >= DATE('$fromDate') AND stats_date <= DATE('$toDate'))
         |GROUP BY account_id
         |HAVING (impressions < 1608)
         |)
         |ssf0
         |) queryAlias LIMIT 200
         """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Generate fact driven query for day grain with datetime between filter") {
    val jsonString =
      s"""
         |{
         |  "cube": "s_stats",
         |  "selectFields": [
         |    { "field": "Advertiser Name" },
         |    { "field": "Impressions" }
         |  ],
         |  "filterExpressions": [
         |    { "field": "Advertiser ID", "operator": "=", "value": "12345" },
         |    { "field": "Day", "operator": "datetimebetween", "from": "$fromDateTime", "to": "$toDateTime", "format": "$iso8601Format" },
         |    { "field": "Impressions", "operator": ">", "value": "1608" }
         |  ],
         |  "sortBy": [
         |    { "field": "Advertiser Name", "order": "Desc" },
         |    { "field": "Impressions", "order": "DESC" }
         |  ]
         |}
       """.stripMargin

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v0)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString

    val expected =
      s"""
         |SELECT IFNULL(CAST(mang_advertiser_name AS STRING), '') AS mang_advertiser_name, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions
         |FROM (
         |SELECT mang_advertiser_name AS mang_advertiser_name, impressions AS mang_impressions
         |FROM (
         |SELECT COALESCE(a1.mang_advertiser_name, '') mang_advertiser_name, SUM(impressions) AS impressions
         |FROM ( SELECT account_id, SUM(impressions) impressions
         |FROM `s_stats_fact`
         |WHERE (account_id = 12345) AND (stats_date >= DATE('$fromDate') AND stats_date <= DATE('$toDate'))
         |GROUP BY account_id
         |HAVING (impressions > 1608)
         |)
         |ssf0
         |LEFT OUTER JOIN (
         |SELECT name AS mang_advertiser_name, id a1_id
         |FROM `advertiser_bigquery`
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
         |)
         |a1
         |ON
         |ssf0.account_id = a1.a1_id
         |GROUP BY mang_advertiser_name
         |ORDER BY mang_advertiser_name DESC, impressions DESC) OgbQueryAlias
         |) queryAlias LIMIT 200
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Generate fact driven query for minute grain with datetime between filter") {
    val jsonString =
      s"""
         |{
         |  "cube": "s_stats_minute",
         |  "selectFields": [
         |    { "field": "Advertiser Name" },
         |    { "field": "Impressions" }
         |  ],
         |  "filterExpressions": [
         |    { "field": "Advertiser ID", "operator": "=", "value": "12345" },
         |    { "field": "Day", "operator": "datetimebetween", "from": "$fromDateTime", "to": "$toDateTime", "format": "$iso8601Format" },
         |    { "field": "Impressions", "operator": ">", "value": "1608" }
         |  ],
         |  "sortBy": [{ "field": "Advertiser Name", "order": "Desc" },  { "field": "Impressions", "order": "DESC" }]
         |}
       """.stripMargin

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v0)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString

    val expected =
      s"""
         |SELECT IFNULL(CAST(mang_advertiser_name AS STRING), '') AS mang_advertiser_name, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions
         |FROM (
         |SELECT mang_advertiser_name AS mang_advertiser_name, impressions AS mang_impressions
         |FROM (
         |SELECT COALESCE(a1.mang_advertiser_name, '') mang_advertiser_name, SUM(impressions) AS impressions
         |FROM ( SELECT account_id, SUM(impressions) impressions
         |FROM `s_stats_fact`
         |WHERE (account_id = 12345) AND (stats_date >= TIMESTAMP('${fromDateTime.dropRight(1)}') AND stats_date <= TIMESTAMP('${toDateTime.dropRight(1)}'))
         |GROUP BY account_id
         |HAVING (impressions > 1608)
         |)
         |ssf0
         |LEFT OUTER JOIN (
         |SELECT name AS mang_advertiser_name, id a1_id
         |FROM `advertiser_bigquery`
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
         |)
         |a1
         |ON
         |ssf0.account_id = a1.a1_id
         |GROUP BY mang_advertiser_name
         |ORDER BY mang_advertiser_name DESC, impressions DESC) OgbQueryAlias
         |) queryAlias LIMIT 200
         |
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Generating Bigquery query with sort by") {
    val jsonString =
      s"""
         |{
         |  "cube": "s_stats",
         |  "selectFields": [
         |    { "field": "Advertiser ID" },
         |    { "field": "Impressions" },
         |    { "field": "Advertiser Name" }
         |  ],
         |  "filterExpressions": [
         |    { "field": "Advertiser ID", "operator": "=", "value": "12345" },
         |    { "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" }
         |  ],
         |  "sortBy": [
         |    { "field": "Impressions", "order": "ASC" }
         |  ]
         |}
       """.stripMargin

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString

    val expected =
      s"""
         |SELECT IFNULL(CAST(advertiser_id AS STRING), '') AS advertiser_id, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions, IFNULL(CAST(mang_advertiser_name AS STRING), '') AS mang_advertiser_name
         |FROM (
         |SELECT COALESCE(ssf0.account_id, 0) advertiser_id, COALESCE(impressions, 1) mang_impressions, COALESCE(a1.mang_advertiser_name, '') mang_advertiser_name
         |FROM ( SELECT account_id, SUM(impressions) impressions
         |FROM `s_stats_fact`
         |WHERE (account_id = 12345) AND (stats_date >= DATE('$fromDate') AND stats_date <= DATE('$toDate'))
         |GROUP BY account_id
         |)
         |ssf0
         |LEFT OUTER JOIN (
         |SELECT name AS mang_advertiser_name, id a1_id
         |FROM `advertiser_bigquery`
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
         |)
         |a1
         |ON
         |ssf0.account_id = a1.a1_id
         |ORDER BY mang_impressions ASC
         |) queryAlias LIMIT 200
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Generating Bigquery query with dim and fact sort by") {
    val jsonString =
      s"""
         |{
         |  "cube": "s_stats",
         |  "selectFields": [
         |    { "field": "Advertiser ID" },
         |    { "field": "Impressions" },
         |    { "field": "Advertiser Name" }
         |  ],
         |  "filterExpressions": [
         |    { "field": "Advertiser ID", "operator": "=", "value": "12345" },
         |    { "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" }
         |  ],
         |  "sortBy": [
         |    { "field": "Impressions", "order": "ASC" },  { "field": "Advertiser Name", "order": "DESC" }
         |  ]
         |}
       """.stripMargin

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val queryChain = queryPipelineTry.toOption.get.queryChain

    val result =  queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    assert(queryChain.drivingQuery.queryGenVersion.isDefined)
    assert(queryChain.drivingQuery.queryGenVersion.get == Version.v0)

    val expected =
      s"""
         |SELECT IFNULL(CAST(advertiser_id AS STRING), '') AS advertiser_id, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions, IFNULL(CAST(mang_advertiser_name AS STRING), '') AS mang_advertiser_name
         |FROM (
         |SELECT COALESCE(ssf0.account_id, 0) advertiser_id, COALESCE(impressions, 1) mang_impressions, COALESCE(a1.mang_advertiser_name, '') mang_advertiser_name
         |FROM ( SELECT account_id, SUM(impressions) impressions
         |FROM `s_stats_fact`
         |WHERE (account_id = 12345) AND (stats_date >= DATE('$fromDate') AND stats_date <= DATE('$toDate'))
         |GROUP BY account_id
         |)
         |ssf0
         |LEFT OUTER JOIN (
         |SELECT name AS mang_advertiser_name, id a1_id
         |FROM `advertiser_bigquery`
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
         |)
         |a1
         |ON
         |ssf0.account_id = a1.a1_id
         |ORDER BY mang_impressions ASC, mang_advertiser_name DESC
         |) queryAlias LIMIT 200
         |
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Generating bigquery query with outer group containing derived columns of level 2") {
    val jsonString =
      s"""
         |{
         |  "cube": "s_stats",
         |  "selectFields": [
         |    { "field": "Campaign Name" },
         |    { "field": "Average CPC" },
         |    { "field": "Average Position" },
         |    { "field": "Impressions" }
         |  ],
         |  "filterExpressions": [
         |    { "field": "Advertiser ID", "operator": "=", "value": "12345" },
         |    { "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" }
         |  ]
         |}
       """.stripMargin

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v0)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val queryChain = queryPipelineTry.toOption.get.queryChain

    val result =  queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    assert(queryChain.drivingQuery.queryGenVersion.isDefined)
    assert(queryChain.drivingQuery.queryGenVersion.get == Version.v0)
    val expected =
      s"""
         |SELECT IFNULL(CAST(mang_campaign_name AS STRING), '') AS mang_campaign_name, IFNULL(CAST(mang_average_cpc AS STRING), '') AS mang_average_cpc, IFNULL(CAST(mang_average_position AS STRING), '') AS mang_average_position, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions
         |FROM (
         |SELECT mang_campaign_name AS mang_campaign_name, CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END AS mang_average_cpc, avg_pos AS mang_average_position, impressions AS mang_impressions
         |FROM (
         |SELECT getCsvEscapedString(IFNULL(CAST(c1.mang_campaign_name AS STRING), '')) mang_campaign_name, (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(weighted_position * impressions) / (SUM(impressions)) END) AS avg_pos, SUM(impressions) AS impressions, SUM(clicks) AS clicks, SUM(spend) AS spend, SUM(weighted_position) AS weighted_position
         |FROM ( SELECT campaign_id, SUM(impressions) impressions, SUM(clicks) clicks, SUM(spend) spend, SUM(weighted_position) weighted_position
         |FROM `s_stats_fact`
         |WHERE (account_id = 12345) AND (stats_date >= DATE('$fromDate') AND stats_date <= DATE('$toDate'))
         |GROUP BY campaign_id
         |)
         |ssf0
         |LEFT OUTER JOIN (
         |SELECT campaign_name AS mang_campaign_name, id c1_id
         |FROM `campaign_bigquery`
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c1
         |ON
         |ssf0.campaign_id = c1.c1_id
         |GROUP BY mang_campaign_name
         |) OgbQueryAlias
         |) queryAlias LIMIT 200
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Generating Bigquery query with outer group by, 2 dim joins and sorting") {
    val jsonString =
      s"""
         |{
         |  "cube": "s_stats",
         |  "selectFields": [
         |    { "field": "Advertiser ID" },
         |    { "field": "Campaign Name" },
         |    { "field": "Advertiser Name" },
         |    { "field": "Average CPC" },
         |    { "field": "Average Position" },
         |    { "field": "Impressions" }
         |  ],
         |  "filterExpressions": [
         |    { "field": "Advertiser ID", "operator": "=", "value": "12345" },
         |    { "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" }
         |  ],
         |  "sortBy": [
         |    { "field": "Impressions", "order": "Desc" },
         |    { "field": "Advertiser Name", "order": "DESC" }
         |  ]
         |}
       """.stripMargin

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v0)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val queryChain = queryPipelineTry.toOption.get.queryChain

    val result =  queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    assert(queryChain.drivingQuery.queryGenVersion.isDefined)
    assert(queryChain.drivingQuery.queryGenVersion.get == Version.v0)

    val expected =
      s"""
         |SELECT IFNULL(CAST(advertiser_id AS STRING), '') AS advertiser_id, IFNULL(CAST(mang_campaign_name AS STRING), '') AS mang_campaign_name, IFNULL(CAST(mang_advertiser_name AS STRING), '') AS mang_advertiser_name, IFNULL(CAST(mang_average_cpc AS STRING), '') AS mang_average_cpc, IFNULL(CAST(mang_average_position AS STRING), '') AS mang_average_position, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions
         |FROM (
         |SELECT advertiser_id AS advertiser_id, mang_campaign_name AS mang_campaign_name, mang_advertiser_name AS mang_advertiser_name, CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END AS mang_average_cpc, avg_pos AS mang_average_position, impressions AS mang_impressions
         |FROM (
         |SELECT COALESCE(c2.advertiser_id, 0) advertiser_id, getCsvEscapedString(IFNULL(CAST(c2.mang_campaign_name AS STRING), '')) mang_campaign_name, COALESCE(a1.mang_advertiser_name, '') mang_advertiser_name, (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(weighted_position * impressions) / (SUM(impressions)) END) AS avg_pos, SUM(impressions) AS impressions, SUM(clicks) AS clicks, SUM(spend) AS spend, SUM(weighted_position) AS weighted_position
         |FROM ( SELECT account_id, campaign_id, SUM(impressions) impressions, SUM(clicks) clicks, SUM(spend) spend, SUM(weighted_position) weighted_position
         |FROM `s_stats_fact`
         |WHERE (account_id = 12345) AND (stats_date >= DATE('$fromDate') AND stats_date <= DATE('$toDate'))
         |GROUP BY account_id, campaign_id
         |)
         |ssf0
         |LEFT OUTER JOIN (
         |SELECT name AS mang_advertiser_name, id a1_id
         |FROM `advertiser_bigquery`
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
         |)
         |a1
         |ON
         |ssf0.account_id = a1.a1_id
         |LEFT OUTER JOIN (
         |SELECT advertiser_id AS advertiser_id, campaign_name AS mang_campaign_name, id c2_id
         |FROM `campaign_bigquery`
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c2
         |ON
         |ssf0.campaign_id = c2.c2_id
         |GROUP BY advertiser_id, mang_campaign_name, mang_advertiser_name
         |ORDER BY impressions DESC, mang_advertiser_name DESC) OgbQueryAlias
         |) queryAlias LIMIT 200
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Generating Bigquery query with outer group by, Noop rollup cols") {
    val jsonString =
      s"""
         |{
         |  "cube": "performance_stats",
         |  "selectFields": [
         |    { "field": "Campaign Name" },
         |    { "field": "Clicks" },
         |    { "field": "Impressions" },
         |    { "field": "Click Rate" }
         |  ],
         |  "filterExpressions": [
         |    { "field": "Advertiser ID", "operator": "=", "value": "12345" },
         |    { "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" }
         |  ]
         |}
       """.stripMargin

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v0)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val queryChain = queryPipelineTry.toOption.get.queryChain

    val result =  queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    assert(queryChain.drivingQuery.queryGenVersion.isDefined)
    assert(queryChain.drivingQuery.queryGenVersion.get == Version.v0)

    val expected =
      s"""
         |SELECT IFNULL(CAST(mang_campaign_name AS STRING), '') AS mang_campaign_name, IFNULL(CAST(mang_clicks AS STRING), '') AS mang_clicks, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions, IFNULL(CAST(mang_click_rate AS STRING), '') AS mang_click_rate
         |FROM (
         |SELECT mang_campaign_name AS mang_campaign_name, clicks AS mang_clicks, impressions AS mang_impressions, mang_click_rate AS mang_click_rate
         |FROM (
         |SELECT getCsvEscapedString(IFNULL(CAST(c1.mang_campaign_name AS STRING), '')) mang_campaign_name, SUM(clicks) AS clicks, SUM(impressions) AS impressions, (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(clicks) / (SUM(impressions)) END) AS mang_click_rate
         |FROM ( SELECT campaign_id, SUM(clicks) clicks, SUM(impressions) impressions
         |FROM `ad_fact1`
         |WHERE (advertiser_id = 12345) AND (stats_date >= DATE('$fromDate') AND stats_date <= DATE('$toDate'))
         |GROUP BY campaign_id
         |)
         |af0
         |LEFT OUTER JOIN (
         |SELECT campaign_name AS mang_campaign_name, id c1_id
         |FROM `campaign_bigquery`
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c1
         |ON
         |af0.campaign_id = c1.c1_id
         |GROUP BY mang_campaign_name
         |) OgbQueryAlias
         |) queryAlias LIMIT 200
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Generating Bigquery query with outer group by, derived cols") {
    val jsonString =
      s"""
         |{
         |  "cube": "performance_stats",
         |  "selectFields": [
         |    { "field": "Campaign Name" },
         |    { "field": "N Clicks" },
         |    { "field": "Pricing Type" },
         |    { "field": "Static Mapping String" },
         |    { "field": "Impressions" },
         |    { "field": "N Spend" },
         |    { "field": "Month" },
         |    { "field": "Clicks" }
         |  ],
         |  "filterExpressions": [
         |    { "field": "Advertiser ID", "operator": "=", "value": "12345" },
         |    { "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" }
         |  ],
         |  "sortBy": [
         |    { "field": "Campaign Name", "order": "Desc" },
         |    { "field": "Impressions", "order": "DESC" }
         |  ]
         |}
       """.stripMargin

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v0)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))
    val queryChain = queryPipelineTry.toOption.get.queryChain

    val result =  queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    assert(queryChain.drivingQuery.queryGenVersion.isDefined)
    assert(queryChain.drivingQuery.queryGenVersion.get == Version.v0)

    val expected =
      s"""
         |SELECT IFNULL(CAST(mang_campaign_name AS STRING), '') AS mang_campaign_name, IFNULL(CAST(mang_n_clicks AS STRING), '') AS mang_n_clicks, IFNULL(CAST(mang_pricing_type AS STRING), '') AS mang_pricing_type, IFNULL(CAST(mang_static_mapping_string AS STRING), '') AS mang_static_mapping_string, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions, IFNULL(CAST(mang_n_spend AS STRING), '') AS mang_n_spend, IFNULL(CAST(mang_month AS STRING), '') AS mang_month, IFNULL(CAST(mang_clicks AS STRING), '') AS mang_clicks
         |FROM (
         |SELECT mang_campaign_name AS mang_campaign_name, CASE WHEN stats_source = 1 THEN clicks ELSE 0.0 END AS mang_n_clicks, mang_pricing_type AS mang_pricing_type, mang_static_mapping_string AS mang_static_mapping_string, impressions AS mang_impressions, CASE WHEN stats_source = 1 THEN spend ELSE 0.0 END AS mang_n_spend, mang_month, clicks AS mang_clicks
         |FROM (
         |SELECT getCsvEscapedString(IFNULL(CAST(c1.mang_campaign_name AS STRING), '')) mang_campaign_name, CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END mang_pricing_type, CASE WHEN (sm_string IN ('Y')) THEN 'Yes' WHEN (sm_string IN ('N')) THEN 'No' ELSE '' END mang_static_mapping_string, SUM(impressions) AS impressions, mang_month mang_month, SUM(clicks) AS clicks, COALESCE(stats_source, 0) stats_source, SUM(spend) AS spend
         |FROM ( SELECT sm_string, price_type, campaign_id, dateUDF(stats_date, 'M') mang_month, SUM(clicks) clicks, SUM(impressions) impressions, stats_source, SUM(spend) spend
         |FROM `ad_fact1`
         |WHERE (advertiser_id = 12345) AND (stats_date >= DATE('$fromDate') AND stats_date <= DATE('$toDate'))
         |GROUP BY sm_string, price_type, campaign_id, dateUDF(stats_date, 'M'), stats_source
         |)
         |af0
         |LEFT OUTER JOIN (
         |SELECT campaign_name AS mang_campaign_name, id c1_id
         |FROM `campaign_bigquery`
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c1
         |ON
         |af0.campaign_id = c1.c1_id
         |GROUP BY mang_campaign_name, mang_pricing_type, mang_static_mapping_string, mang_month, stats_source
         |ORDER BY mang_campaign_name DESC, impressions DESC) OgbQueryAlias
         |) queryAlias LIMIT 200
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Generating Bigquery query with outer group by, having and partitioning scheme, test part col renderer") {
    val jsonString =
      s"""
         |{
         |  "cube": "bid_reco",
         |  "selectFields": [
         |    { "field": "Campaign Name" },
         |    { "field": "Bid Strategy" },
         |    { "field": "Advertiser Name" },
         |    { "field": "Advertiser ID" },
         |    { "field": "Impressions" },
         |    { "field": "Test Constant Col" },
         |    { "field": "Load Time" }
         |  ],
         |  "filterExpressions": [
         |    { "field": "Advertiser ID", "operator": "=", "value": "12345" },
         |    { "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" },
         |    { "field": "Impressions", "operator": ">", "value": "1608" }
         |  ],
         |  "sortBy": [
         |    { "field": "Campaign Name", "order": "Desc" },
         |    { "field": "Impressions", "order": "DESC" }
         |  ]
         |}
       """.stripMargin

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v0)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val queryChain = queryPipelineTry.toOption.get.queryChain

    val result =  queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    assert(queryChain.drivingQuery.queryGenVersion.isDefined)
    assert(queryChain.drivingQuery.queryGenVersion.get == Version.v0)

    val expected =
      s"""
         |SELECT IFNULL(CAST(mang_campaign_name AS STRING), '') AS mang_campaign_name, IFNULL(CAST(mang_bid_strategy AS STRING), '') AS mang_bid_strategy, IFNULL(CAST(mang_advertiser_name AS STRING), '') AS mang_advertiser_name, IFNULL(CAST(advertiser_id AS STRING), '') AS advertiser_id, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions, IFNULL(CAST(mang_test_constant_col AS STRING), '') AS mang_test_constant_col, IFNULL(CAST(mang_load_time AS STRING), '') AS mang_load_time
         |FROM (
         |SELECT mang_campaign_name AS mang_campaign_name, mang_bid_strategy AS mang_bid_strategy, mang_advertiser_name AS mang_advertiser_name, advertiser_id AS advertiser_id, actual_impressions AS mang_impressions, mang_test_constant_col AS mang_test_constant_col, mang_load_time
         |FROM (
         |SELECT getCsvEscapedString(IFNULL(CAST(c2.mang_campaign_name AS STRING), '')) mang_campaign_name, CASE WHEN (bid_strategy IN (1)) THEN 'Max Click' WHEN (bid_strategy IN (2)) THEN 'Inflection Point' ELSE 'NONE' END mang_bid_strategy, COALESCE(a1.mang_advertiser_name, '') mang_advertiser_name, COALESCE(c2.advertiser_id, 0) advertiser_id, SUM(actual_impressions) AS actual_impressions, ROUND(COALESCE(test_constant_col, 0.0), 10) mang_test_constant_col, COALESCE(load_time, '') mang_load_time
         |FROM ( SELECT bid_strategy, account_id, load_time, 'test_constant_col_value' AS test_constant_col, campaign_id, SUM(actual_impressions) actual_impressions
         |FROM `bidreco_complete`
         |WHERE (account_id = 12345) AND (status = 'Valid') AND (SUBSTR(load_time, 1, 8) >= DATE('$fromDate') AND SUBSTR(load_time, 1, 8) <= DATE('$toDate'))
         |GROUP BY bid_strategy, account_id, load_time, test_constant_col, campaign_id
         |HAVING (actual_impressions > 1608)
         |)
         |bc0
         |LEFT OUTER JOIN (
         |SELECT name AS mang_advertiser_name, id a1_id
         |FROM `advertiser_bigquery`
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
         |)
         |a1
         |ON
         |bc0.account_id = a1.a1_id
         |LEFT OUTER JOIN (
         |SELECT advertiser_id AS advertiser_id, campaign_name AS mang_campaign_name, id c2_id
         |FROM `campaign_bigquery`
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c2
         |ON
         |bc0.campaign_id = c2.c2_id
         |GROUP BY mang_campaign_name, mang_bid_strategy, mang_advertiser_name, advertiser_id, mang_test_constant_col, mang_load_time
         |ORDER BY mang_campaign_name DESC, actual_impressions DESC) OgbQueryAlias
         |) queryAlias LIMIT 200
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Test constant requested fields with Outer Group By") {
    val jsonString =
      s"""
         |{
         |  "cube" : "s_stats",
         |  "selectFields" : [
         |    { "field" : "Day" },
         |    { "field" : "Advertiser ID" },
         |    { "field" : "Campaign Name" },
         |    { "field" : "Impressions" },
         |    { "field" : "Source", "value" : "2", "alias" : "Source" }
         |  ],
         |  "filterExpressions":[
         |    { "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" },
         |    { "field":"Advertiser ID", "operator":"=", "value":"12345" }
         |  ],
         |  "sortBy": [
         |    { "field": "Impressions", "order": "Asc" }
         |  ],
         |  "paginationStartIndex":0,
         |  "rowsPerPage":100
         |}
       """.stripMargin

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v0)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val queryChain = queryPipelineTry.toOption.get.queryChain

    val result =  queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    assert(result.contains("'2' AS mang_source"), "No constant field in outer columns")
  }

  test("NoopRollup columns in Outer Group By should be rendered correctly") {
    val jsonString =
      s"""
         |{
         |  "cube": "bid_reco",
         |  "selectFields": [
         |    { "field": "Campaign Name" },
         |    { "field": "Advertiser Name" },
         |    { "field": "Advertiser ID" },
         |    { "field": "Bid Strategy" },
         |    { "field": "Impressions" },
         |    { "field": "Clicks" },
         |    { "field": "Bid Modifier Fact" },
         |    { "field": "Modified Bid Fact" }
         |  ],
         |  "filterExpressions": [
         |    { "field": "Advertiser ID", "operator": "=", "value": "12345" },
         |    { "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" },
         |    { "field": "Impressions", "operator": ">", "value": "1608" }
         |  ],
         |  "sortBy": [
         |    { "field": "Campaign Name", "order": "Desc" },
         |    { "field": "Impressions", "order": "DESC" }
         |  ]
         |}
       """.stripMargin

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v0)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val queryChain = queryPipelineTry.toOption.get.queryChain

    val result =  queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString

    val expected =
      s"""
         |SELECT IFNULL(CAST(mang_campaign_name AS STRING), '') AS mang_campaign_name, IFNULL(CAST(mang_advertiser_name AS STRING), '') AS mang_advertiser_name, IFNULL(CAST(advertiser_id AS STRING), '') AS advertiser_id, IFNULL(CAST(mang_bid_strategy AS STRING), '') AS mang_bid_strategy, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions, IFNULL(CAST(mang_clicks AS STRING), '') AS mang_clicks, IFNULL(CAST(mang_bid_modifier_fact AS STRING), '') AS mang_bid_modifier_fact, IFNULL(CAST(mang_modified_bid_fact AS STRING), '') AS mang_modified_bid_fact
         |FROM (
         |SELECT mang_campaign_name AS mang_campaign_name, mang_advertiser_name AS mang_advertiser_name, advertiser_id AS advertiser_id, mang_bid_strategy AS mang_bid_strategy, actual_impressions AS mang_impressions, actual_clicks AS mang_clicks, mang_bid_modifier_fact AS mang_bid_modifier_fact, mang_modified_bid_fact AS mang_modified_bid_fact
         |FROM (
         |SELECT getCsvEscapedString(IFNULL(CAST(c2.mang_campaign_name AS STRING), '')) mang_campaign_name, COALESCE(a1.mang_advertiser_name, '') mang_advertiser_name, COALESCE(c2.advertiser_id, 0) advertiser_id, CASE WHEN (bid_strategy IN (1)) THEN 'Max Click' WHEN (bid_strategy IN (2)) THEN 'Inflection Point' ELSE 'NONE' END mang_bid_strategy, SUM(actual_impressions) AS actual_impressions, SUM(actual_clicks) AS actual_clicks, SUM(recommended_bid) AS recommended_bid, (COALESCE(CASE WHEN (getAbyB(recommended_bid, actual_clicks)) = 0 THEN 1 WHEN IS_NAN((getAbyB(recommended_bid, actual_clicks))) THEN 1 ELSE (getAbyB(recommended_bid, actual_clicks)) END, 1)) AS mang_bid_modifier_fact, SUM(COALESCE(CASE WHEN COALESCE(CASE WHEN (getAbyB(recommended_bid, actual_clicks)) = 0 THEN 1 WHEN IS_NAN((getAbyB(recommended_bid, actual_clicks))) THEN 1 ELSE (getAbyB(recommended_bid, actual_clicks)) END, 1) = 0 THEN 1 WHEN IS_NAN(COALESCE(CASE WHEN (getAbyB(recommended_bid, actual_clicks)) = 0 THEN 1 WHEN IS_NAN((getAbyB(recommended_bid, actual_clicks))) THEN 1 ELSE (getAbyB(recommended_bid, actual_clicks)) END, 1)) THEN 1 ELSE COALESCE(CASE WHEN (getAbyB(recommended_bid, actual_clicks)) = 0 THEN 1 WHEN IS_NAN((getAbyB(recommended_bid, actual_clicks))) THEN 1 ELSE (getAbyB(recommended_bid, actual_clicks)) END, 1) END, 1) * (getAbyB(recommended_bid, actual_impressions))) AS mang_modified_bid_fact
         |FROM ( SELECT bid_strategy, account_id, campaign_id, SUM(actual_clicks) actual_clicks, SUM(actual_impressions) actual_impressions, SUM(recommended_bid) recommended_bid
         |FROM `bidreco_complete`
         |WHERE (account_id = 12345) AND (status = 'Valid') AND (SUBSTR(load_time, 1, 8) >= DATE('$fromDate') AND SUBSTR(load_time, 1, 8) <= DATE('$toDate'))
         |GROUP BY bid_strategy, account_id, campaign_id
         |HAVING (actual_impressions > 1608)
         |)
         |bc0
         |LEFT OUTER JOIN (
         |SELECT name AS mang_advertiser_name, id a1_id
         |FROM `advertiser_bigquery`
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
         |)
         |a1
         |ON
         |bc0.account_id = a1.a1_id
         |       LEFT OUTER JOIN (
         |SELECT advertiser_id AS advertiser_id, campaign_name AS mang_campaign_name, id c2_id
         |FROM `campaign_bigquery`
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c2
         |ON
         |bc0.campaign_id = c2.c2_id
         |GROUP BY mang_campaign_name, mang_advertiser_name, advertiser_id, mang_bid_strategy
         |ORDER BY mang_campaign_name DESC, actual_impressions DESC) OgbQueryAlias
         |) queryAlias LIMIT 200
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Generating Bigquery query with dim non id field as filter") {
    val jsonString =
      s"""
         |{
         |  "cube": "s_stats",
         |  "selectFields": [
         |    { "field": "Average CPC" },
         |    { "field": "Average Position" },
         |    { "field": "Impressions" }
         |  ],
         |  "filterExpressions": [
         |    { "field": "Advertiser ID", "operator": "=", "value": "12345" },
         |    { "field": "Campaign Name", "operator": "=", "value": "cmpgn_1" },
         |    { "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" }
         |  ]
         |}
       """.stripMargin

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v0)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val queryChain = queryPipelineTry.toOption.get.queryChain

    val result =  queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    assert(queryChain.drivingQuery.queryGenVersion.isDefined)
    assert(queryChain.drivingQuery.queryGenVersion.get == Version.v0)

    val expected =
      s"""
         |SELECT IFNULL(CAST(mang_average_cpc AS STRING), '') AS mang_average_cpc, IFNULL(CAST(mang_average_position AS STRING), '') AS mang_average_position, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions
         |FROM (
         |SELECT CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END AS mang_average_cpc, avg_pos AS mang_average_position, impressions AS mang_impressions
         |FROM (
         |SELECT (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(weighted_position * impressions) / (SUM(impressions)) END) AS avg_pos, SUM(impressions) AS impressions, SUM(clicks) AS clicks, SUM(spend) AS spend, SUM(weighted_position) AS weighted_position
         |FROM ( SELECT campaign_id, SUM(impressions) impressions, SUM(clicks) clicks, SUM(spend) spend, SUM(weighted_position) weighted_position
         |FROM `s_stats_fact`
         |WHERE (account_id = 12345) AND (stats_date >= DATE('$fromDate') AND stats_date <= DATE('$toDate'))
         |GROUP BY campaign_id
         |)
         |ssf0
         |JOIN (
         |SELECT id c1_id
         |FROM `campaign_bigquery`
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345) AND (lower(campaign_name) = lower('cmpgn_1'))
         |)
         |c1
         |ON
         |ssf0.campaign_id = c1.c1_id
         |) OgbQueryAlias
         |) queryAlias LIMIT 200
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Multiple filters on same ID column") {
    val jsonString =
      s"""
         |{
         |  "cube": "performance_stats",
         |  "selectFields": [
         |    { "field": "Advertiser ID" },
         |    { "field": "Impressions" },
         |    { "field": "Pricing Type" }
         |  ],
         |  "filterExpressions": [
         |    { "field": "Advertiser ID", "operator": "=", "value": "12345" },
         |    { "field": "Advertiser ID", "operator": "IsNotNull" },
         |    { "field": "Advertiser ID", "operator": "<>", "value": "-3" },
         |    { "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" }
         |  ]
         |}
       """.stripMargin

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v0)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString

    val expected =
      s"""
         |SELECT IFNULL(CAST(advertiser_id AS STRING), '') AS advertiser_id, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions, IFNULL(CAST(mang_pricing_type AS STRING), '') AS mang_pricing_type
         |FROM (
         |SELECT COALESCE(advertiser_id, 0) advertiser_id, COALESCE(impressions, 1) mang_impressions, COALESCE(price_type, 0) mang_pricing_type
         |FROM ( SELECT CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END price_type, advertiser_id, SUM(impressions) impressions
         |FROM `ad_fact1`
         |WHERE (advertiser_id <> -3) AND (advertiser_id = 12345) AND (advertiser_id IS NOT NULL) AND (stats_date >= DATE('$fromDate') AND stats_date <= DATE('$toDate'))
         |GROUP BY CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END, advertiser_id
         |)
         |af0
         |) queryAlias LIMIT 200
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Multiple filters on same column OGB") {
    val jsonString =
      s"""
         |{
         |  "cube": "performance_stats",
         |  "selectFields": [
         |    { "field": "Average CPC" },
         |    { "field": "Average Position" },
         |    { "field": "Impressions" }
         |  ],
         |  "filterExpressions": [
         |    { "field": "Advertiser ID", "operator": "=", "value": "12345" },
         |    { "field": "Advertiser ID", "operator": "IsNotNull" },
         |    { "field": "Campaign Name", "operator": "=", "value": "cmpgn_1" },
         |    { "field": "Campaign Name", "operator": "IsNotNull" },
         |    { "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" }
         |  ]
         |}
       """.stripMargin

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v0)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val queryChain = queryPipelineTry.toOption.get.queryChain

    val result =  queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString
    assert(queryChain.drivingQuery.queryGenVersion.isDefined)
    assert(queryChain.drivingQuery.queryGenVersion.get == Version.v0)

    val expected =
      s"""
         |SELECT IFNULL(CAST(mang_average_cpc AS STRING), '') AS mang_average_cpc, IFNULL(CAST(mang_average_position AS STRING), '') AS mang_average_position, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions
         |FROM (
         |SELECT CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END AS mang_average_cpc, avg_pos AS mang_average_position, impressions AS mang_impressions
         |FROM (
         |SELECT (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(weighted_position * impressions) / (SUM(impressions)) END) AS avg_pos, SUM(impressions) AS impressions, SUM(clicks) AS clicks, SUM(spend) AS spend, SUM(weighted_position) AS weighted_position
         |FROM ( SELECT campaign_id, SUM(impressions) impressions, SUM(clicks) clicks, SUM(spend) spend, SUM(weighted_position) weighted_position
         |FROM `ad_fact1`
         |WHERE (advertiser_id = 12345) AND (advertiser_id IS NOT NULL) AND (stats_date >= DATE('$fromDate') AND stats_date <= DATE('$toDate'))
         |GROUP BY campaign_id
         |)
         |af0
         |JOIN (
         |SELECT id c1_id
         |FROM `campaign_bigquery`
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345) AND (advertiser_id IS NOT NULL) AND (campaign_name IS NOT NULL) AND (lower(campaign_name) = lower('cmpgn_1'))
         |)
         |c1
         |ON
         |af0.campaign_id = c1.c1_id
         |) OgbQueryAlias
         |) queryAlias LIMIT 200
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Not Like filter Bigquery") {
    val jsonString =
      s"""
         |{
         |  "cube": "s_stats",
         |  "selectFields": [
         |    { "field": "Campaign ID" },
         |    { "field": "Advertiser ID" },
         |    { "field": "Impressions" },
         |    { "field": "Clicks" }
         |  ],
         |  "filterExpressions": [
         |    { "field": "Advertiser ID", "operator": "=", "value": "12345" },
         |    { "field": "Campaign Name", "operator": "Not Like", "value": "cmpgn1" },
         |    { "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" }
         |  ]
         |}
       """.stripMargin

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v0)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString

    val expected =
      s"""
         |SELECT IFNULL(CAST(campaign_id AS STRING), '') AS campaign_id, IFNULL(CAST(advertiser_id AS STRING), '') AS advertiser_id, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions, IFNULL(CAST(mang_clicks AS STRING), '') AS mang_clicks
         |FROM (
         |SELECT COALESCE(ssf0.campaign_id, '') campaign_id, COALESCE(account_id, 0) advertiser_id, COALESCE(impressions, 1) mang_impressions, COALESCE(mang_clicks, 0) mang_clicks
         |FROM ( SELECT account_id, campaign_id, SUM(clicks) mang_clicks, SUM(impressions) impressions
         |FROM `s_stats_fact`
         |WHERE (account_id = 12345) AND (stats_date >= DATE('$fromDate') AND stats_date <= DATE('$toDate'))
         |GROUP BY account_id, campaign_id
         |)
         |ssf0
         |JOIN (
         |SELECT id c1_id
         |FROM `campaign_bigquery`
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345) AND (lower(campaign_name) NOT LIKE lower('%cmpgn1%'))
         |)
         |c1
         |ON
         |ssf0.campaign_id = c1.c1_id
         |) queryAlias LIMIT 200
      """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Query with both aliases") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "bigquery_query_with_both_aliases_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v0)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Failed to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString

    val expected =
      s"""
         |SELECT IFNULL(CAST(mang_day AS STRING), '') AS mang_day, IFNULL(CAST(advertiser_id AS STRING), '') AS advertiser_id, IFNULL(CAST(campaign_id AS STRING), '') AS campaign_id, IFNULL(CAST(ad_group_id AS STRING), '') AS ad_group_id, IFNULL(CAST(keyword_id AS STRING), '') AS keyword_id, IFNULL(CAST(mang_ad_format_name AS STRING), '') AS mang_ad_format_name, IFNULL(CAST(mang_ad_format_sub_type AS STRING), '') AS mang_ad_format_sub_type, IFNULL(CAST(mang_ad_format_type AS STRING), '') AS mang_ad_format_type, IFNULL(CAST(mang_keyword AS STRING), '') AS mang_keyword, IFNULL(CAST(mang_search_term AS STRING), '') AS mang_search_term, IFNULL(CAST(mang_delivered_match_type AS STRING), '') AS mang_delivered_match_type, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions
         |FROM (
         |SELECT FORMAT_DATETIME('%Y-%m-%d', stats_date) mang_day, COALESCE(account_id, 0) advertiser_id, COALESCE(campaign_id, '') campaign_id, COALESCE(ad_group_id, 0) ad_group_id, COALESCE(keyword_id, 0) keyword_id, COALESCE(ad_format_id, 0) mang_ad_format_name, COALESCE(ad_format_sub_type, 0) mang_ad_format_sub_type, COALESCE(ad_format_type, 0) mang_ad_format_type, getCsvEscapedString(IFNULL(CAST(keyword AS STRING), '')) mang_keyword, COALESCE(search_term, 'None') mang_search_term, COALESCE(delivered_match_type, 0) mang_delivered_match_type, COALESCE(impressions, 1) mang_impressions
         |FROM ( SELECT CASE WHEN (delivered_match_type IN (1)) THEN 'Exact' WHEN (delivered_match_type IN (2)) THEN 'Broad' WHEN (delivered_match_type IN (3)) THEN 'Phrase' ELSE 'UNKNOWN' END delivered_match_type, stats_date, keyword, ad_group_id, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'N/A' END ad_format_type, search_term, account_id, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (5)) THEN 'Single image' WHEN (ad_format_id IN (6)) THEN 'Single image' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (9)) THEN 'Carousel' WHEN (ad_format_id IN (2)) THEN 'Single image' WHEN (ad_format_id IN (7)) THEN 'Video' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (3)) THEN 'Single image' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (8)) THEN 'Video with HTML Endcard' WHEN (ad_format_id IN (4)) THEN 'Single image' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'Other' END ad_format_id, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'N/A' END ad_format_sub_type, campaign_id, keyword_id, SUM(impressions) impressions
         |FROM `s_stats_fact`
         |WHERE (account_id = 12345) AND (ad_format_id IN (4,5,6,2,3)) AND (stats_date >= DATE('$fromDate') AND stats_date <= DATE('$toDate'))
         |GROUP BY CASE WHEN (delivered_match_type IN (1)) THEN 'Exact' WHEN (delivered_match_type IN (2)) THEN 'Broad' WHEN (delivered_match_type IN (3)) THEN 'Phrase' ELSE 'UNKNOWN' END, stats_date, keyword, ad_group_id, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'N/A' END, search_term, account_id, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (5)) THEN 'Single image' WHEN (ad_format_id IN (6)) THEN 'Single image' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (9)) THEN 'Carousel' WHEN (ad_format_id IN (2)) THEN 'Single image' WHEN (ad_format_id IN (7)) THEN 'Video' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (3)) THEN 'Single image' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (8)) THEN 'Video with HTML Endcard' WHEN (ad_format_id IN (4)) THEN 'Single image' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'Other' END, campaign_id, keyword_id
         |)
         |ssf0
         |ORDER BY mang_impressions ASC
         |) queryAlias LIMIT 100
       """.stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Query with both aliases with filter and sort by") {
    val jsonString =
      s"""
         |{
         |  "cube": "s_stats",
         |  "selectFields": [
         |    { "field": "Device ID" },
         |    { "field": "Advertiser ID" },
         |    { "field": "Ad Format Name" },
         |    { "field": "Ad Format Sub Type" },
         |    { "field": "Impressions" },
         |    { "field": "Pricing Type" },
         |    { "field": "Network ID" }
         |  ],
         |  "filterExpressions": [
         |    { "field": "Advertiser ID", "operator": "=", "value": "12345" },
         |    { "field": "Ad Format Name", "operator": "=", "value": "Product Ad" },
         |    { "field": "Ad Format Sub Type", "operator": "<>", "value": "DPA Single Image Ad" },
         |    { "field": "Ad Format Sub Type", "operator": "=", "value": "DPA Collection Ad" },
         |    { "field": "Campaign Name", "operator": "IsNotNull" },
         |    { "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" }
         |  ],
         |  "sortBy": [
         |    { "field": "Advertiser ID", "order": "Asc" },
         |    { "field": "Ad Format Name", "order": "Asc" }
         |  ]
         |}
       """.stripMargin

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v0)
    assert(queryPipelineTry.isSuccess, "query with both aliases should not fail")
    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[BigqueryQuery].asString

    val expected =
      s"""
         |SELECT IFNULL(CAST(device_id AS STRING), '') AS device_id, IFNULL(CAST(advertiser_id AS STRING), '') AS advertiser_id, IFNULL(CAST(mang_ad_format_name AS STRING), '') AS mang_ad_format_name, IFNULL(CAST(mang_ad_format_sub_type AS STRING), '') AS mang_ad_format_sub_type, IFNULL(CAST(mang_impressions AS STRING), '') AS mang_impressions, IFNULL(CAST(mang_pricing_type AS STRING), '') AS mang_pricing_type, IFNULL(CAST(network_id AS STRING), '') AS network_id
         |FROM (
         |SELECT device_id AS device_id, advertiser_id AS advertiser_id, mang_ad_format_name AS mang_ad_format_name, mang_ad_format_sub_type AS mang_ad_format_sub_type, impressions AS mang_impressions, mang_pricing_type AS mang_pricing_type, network_id AS network_id
         |FROM (
         |SELECT CASE WHEN (device_id IN (5199520)) THEN 'SmartPhone' WHEN (device_id IN (5199503)) THEN 'Tablet' WHEN (device_id IN (5199421)) THEN 'Desktop' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END device_id, COALESCE(account_id, 0) advertiser_id, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (5)) THEN 'Single image' WHEN (ad_format_id IN (6)) THEN 'Single image' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (9)) THEN 'Carousel' WHEN (ad_format_id IN (2)) THEN 'Single image' WHEN (ad_format_id IN (7)) THEN 'Video' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (3)) THEN 'Single image' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (8)) THEN 'Video with HTML Endcard' WHEN (ad_format_id IN (4)) THEN 'Single image' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'Other' END mang_ad_format_name, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'N/A' END mang_ad_format_sub_type, SUM(impressions) AS impressions, CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END mang_pricing_type, CASE WHEN (network_type IN ('TEST_PUBLISHER')) THEN 'Test Publisher' WHEN (network_type IN ('CONTENT_S')) THEN 'Content Secured' WHEN (network_type IN ('EXTERNAL')) THEN 'External Partners' WHEN (network_type IN ('INTERNAL')) THEN 'Internal Properties' ELSE '' END network_id
         |FROM ( SELECT device_id, network_type, price_type, account_id, ad_format_id, campaign_id, SUM(impressions) impressions
         |FROM `s_stats_fact`
         |WHERE (ad_format_id <> 100) AND (ad_format_id = 35) AND (ad_format_id = 97) AND (account_id = 12345) AND (stats_date >= DATE('$fromDate') AND stats_date <= DATE('$toDate'))
         |GROUP BY device_id, network_type, price_type, account_id, ad_format_id, campaign_id
         |)
         |ssf0
         |JOIN (
         |SELECT id c1_id
         |FROM `campaign_bigquery`
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345) AND (campaign_name IS NOT NULL)
         |)
         |c1
         |ON
         |ssf0.campaign_id = c1.c1_id
         |GROUP BY device_id, advertiser_id, mang_ad_format_name, mang_ad_format_sub_type, mang_pricing_type, network_id
         |ORDER BY advertiser_id ASC, mang_ad_format_name ASC) OgbQueryAlias
         |) queryAlias LIMIT 200
       """.stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }
}

