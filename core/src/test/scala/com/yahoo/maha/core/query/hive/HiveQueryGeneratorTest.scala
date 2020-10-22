// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.hive

import java.nio.charset.StandardCharsets

import com.yahoo.maha.core.CoreSchema.AdvertiserSchema
import com.yahoo.maha.core._
import com.yahoo.maha.core.query.{QueryGeneratorRegistry, _}
import com.yahoo.maha.core.request.ReportingRequest

/**
 * Created by shengyao on 12/21/15.
 */
class HiveQueryGeneratorTest extends BaseHiveQueryGeneratorTest {

  lazy val defaultRegistry = getDefaultRegistry()

  test("registering Hive query generation multiple times should fail") {
    intercept[IllegalArgumentException] {
      val dummyQueryGenerator = new QueryGenerator[WithHiveEngine] {
        override def generate(queryContext: QueryContext): Query = {
          null
        }

        override def engine: Engine = HiveEngine
      }

      val queryGeneratorRegistryTest = new QueryGeneratorRegistry
      queryGeneratorRegistryTest.register(HiveEngine, dummyQueryGenerator)
      HiveQueryGenerator.register(queryGeneratorRegistryTest, DefaultPartitionColumnRenderer, TestUDFRegistrationFactory())

      queryGeneratorRegistry.register(HiveEngine, dummyQueryGenerator)
    }
  }

  test("generating hive query") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_query_generator_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
  }

  test("generating hive query with custom rollups") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_query_generator_test_custom_rollups.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))


    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

  }

  test("user stats hourly") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "user_stats_hourly.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    val sourceForceFilter: EqualityFilter = EqualityFilter("Source", "2", isForceFilter = true)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    assert(queryPipelineTry.toOption.get.factBestCandidate.get.filters.size == 3, requestModel.errorMessage("Building request model failed"))
    assert(queryPipelineTry.toOption.get.factBestCandidate.get.filters.contains(sourceForceFilter), requestModel.errorMessage("Building request model failed"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

  }

  test("Date type columns should be rendered with getFormattedDate udf in outer select") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_request_with_date_field.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(result.contains("getFormattedDate"), "Date type columms should be wrapped in getFormattedDate udf")
  }

  test("Should escape string if escaping is required") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_request_with_escaping_required_field.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(result.contains("getCsvEscapedString"), "Should escape string if escaping is required")
  }

  test("Hive query should contain UDF definitions ") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_query_generator_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery]
    assert(query.udfStatements.nonEmpty, "Hive Query should contain udf statements")
  }

  test("should support execution parameters") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_query_generator_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery]
  }

  test("Concatenated cols should be wrapped in CONCAT_WS()") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_query_generator_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(result.contains("CONCAT_WS"), "Concatenated cols should be wrapped in CONCAT_WS()")
  }

  test("Should mangle non-id column aliases") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_query_generator_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(result.contains("mang_search_term"), "Should mangle non-id field: Search Term")
    assert(result.contains("mang_day"), "Should mangle non-id field: Day")
    assert(result.contains("mang_keyword"), "Should mangle non-id field: Keyword")
    assert(result.contains("mang_impression"), "Should mangle non-id field: Impression")
    assert(result.contains("mang_delivered_match_type"), "Should mangle non-id field: Delivered Match Type")

  }

  test("Should support “NA” for NULL string") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_request_with_string_field.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    //assert(result.contains("COALESCE(outergroupby.mang_advertiser_status, \"NA\") mang_advertiser_status"), "Should support “NA” for NULL string")
    assert(result.contains("COALESCE(a1.mang_advertiser_status, \"NA\") mang_advertiser_status"), "Should support “NA” for NULL string")
  }

  test("Query with request DecType fields that contains max and min should return query with max and min range") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_request_with_min_max_dec_field.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(result.contains("ROUND(COALESCE(CASE WHEN ((mang_average_position >= 0.1) AND (mang_average_position <= 500)) THEN mang_average_position ELSE 0.0 END, 0.0), 10) mang_average_position"), "Should support “NA” for NULL string")
  }

  test("Query with constant requested fields should have constant columns") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_request_with_constant_field.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(result.contains("NVL(CAST(mang_source AS STRING), '')"), "No constant field in concatenated columns")
    assert(result.contains("'2' mang_source"), "No constant field in outer columns")
  }

  test("Fact group by clause should only include dim columns") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_request_with_fact_n_factder_field.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(result.contains("GROUP BY landing_page_url, stats_date\n"), "Group by should only include dim columns")

  }

  test("Hive multi dimensional query") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_request_with_multiple_dimension.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

  }

  test("DateTime type columns should be rendered with getDateTimeFromEpoch udf in outer select") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "ce_stats.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(result.contains("getDateTimeFromEpoch"), "Date type columms should be wrapped in getFormattedDate udf")
  }

  test("test NoopRollup expression for generated query") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_query_nooprollup_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(CAST(mang_day AS STRING), ''), NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(campaign_id AS STRING), ''), NVL(CAST(ad_group_id AS STRING), ''), NVL(CAST(keyword_id AS STRING), ''), NVL(CAST(mang_keyword AS STRING), ''), NVL(CAST(mang_search_term AS STRING), ''), NVL(CAST(mang_delivered_match_type AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''), NVL(CAST(mang_average_cpc AS STRING), ''))
         |FROM(
         |SELECT getFormattedDate(stats_date) mang_day, COALESCE(account_id, 0L) advertiser_id, COALESCE(campaign_id, 0L) campaign_id, COALESCE(ad_group_id, 0L) ad_group_id, COALESCE(keyword_id, 0L) keyword_id, getCsvEscapedString(CAST(NVL(keyword, '') AS STRING)) mang_keyword, COALESCE(search_term, "None") mang_search_term, COALESCE(delivered_match_type, 0L) mang_delivered_match_type, COALESCE(impressions, 1L) mang_impressions, ROUND(COALESCE((CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END), 0L), 10) mang_average_cpc
         |FROM(SELECT CASE WHEN (delivered_match_type IN (1)) THEN 'Exact' WHEN (delivered_match_type IN (2)) THEN 'Broad' WHEN (delivered_match_type IN (3)) THEN 'Phrase' ELSE 'UNKNOWN' END delivered_match_type, stats_date, keyword, ad_group_id, search_term, account_id, campaign_id, keyword_id, SUM(impressions) impressions, SUM(clicks) clicks, SUM(spend) spend
         |FROM s_stats_fact
         |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY CASE WHEN (delivered_match_type IN (1)) THEN 'Exact' WHEN (delivered_match_type IN (2)) THEN 'Broad' WHEN (delivered_match_type IN (3)) THEN 'Phrase' ELSE 'UNKNOWN' END, stats_date, keyword, ad_group_id, search_term, account_id, campaign_id, keyword_id
         |
         |       )
         |ssf0
         |
         |ORDER BY mang_impressions ASC) queryAlias LIMIT 100
         |
        """.stripMargin


    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("test joinType for non fk dim filters for generated query") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_query_with_non_fk_dim_filters_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    assert(requestModel.toOption.get.anyDimHasNonFKNonForceFilter == true)

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    assert(result.contains("ssf0\nJOIN ("))
  }

  test("test joinType for fact driven queries generated query") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_query_with_wo_non_fk_dim_filters_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    assert(requestModel.toOption.get.anyDimHasNonFKNonForceFilter == false)

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    assert(result.contains("LEFT OUTER JOIN"))
  }

  test("test like filter with hive query gem") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_query_with_dim_like_filter.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    assert(result.contains("(lower(campaign_name) LIKE lower('%yahoo%'))"))
    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(CAST(mang_day AS STRING), ''), NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(campaign_id AS STRING), ''), NVL(CAST(mang_campaign_name AS STRING), ''), NVL(CAST(ad_group_id AS STRING), ''), NVL(CAST(keyword_id AS STRING), ''), NVL(CAST(mang_keyword AS STRING), ''), NVL(CAST(mang_search_term AS STRING), ''), NVL(CAST(mang_delivered_match_type AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''), NVL(CAST(mang_average_cpc_cents AS STRING), ''), NVL(CAST(mang_average_cpc AS STRING), ''))
         |FROM(
         |SELECT getFormattedDate(stats_date) mang_day, COALESCE(account_id, 0L) advertiser_id, COALESCE(ssf0.campaign_id, 0L) campaign_id, getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)) mang_campaign_name, COALESCE(ad_group_id, 0L) ad_group_id, COALESCE(keyword_id, 0L) keyword_id, getCsvEscapedString(CAST(NVL(keyword, '') AS STRING)) mang_keyword, COALESCE(search_term, "None") mang_search_term, COALESCE(delivered_match_type, 0L) mang_delivered_match_type, COALESCE(impressions, 1L) mang_impressions, ROUND(COALESCE((mang_average_cpc * 100), 0L), 10) mang_average_cpc_cents, ROUND(COALESCE(mang_average_cpc, 0L), 10) mang_average_cpc
         |FROM(SELECT CASE WHEN (delivered_match_type IN (1)) THEN 'Exact' WHEN (delivered_match_type IN (2)) THEN 'Broad' WHEN (delivered_match_type IN (3)) THEN 'Phrase' ELSE 'UNKNOWN' END delivered_match_type, stats_date, keyword, ad_group_id, search_term, account_id, campaign_id, keyword_id, SUM(impressions) impressions, (CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END) mang_average_cpc
         |FROM s_stats_fact
         |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY CASE WHEN (delivered_match_type IN (1)) THEN 'Exact' WHEN (delivered_match_type IN (2)) THEN 'Broad' WHEN (delivered_match_type IN (3)) THEN 'Phrase' ELSE 'UNKNOWN' END, stats_date, keyword, ad_group_id, search_term, account_id, campaign_id, keyword_id
         |
 |       )
         |ssf0
         |JOIN (
         |SELECT campaign_name AS mang_campaign_name, id c1_id
         |FROM campaing_hive
         |""".stripMargin

    whiteSpaceNormalised.normalized(result) should startWith(whiteSpaceNormalised.normalized(expected))
  }

  test("test like filter with hive query injection testing") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_query_with_dim_like_filter_injection_testing.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    assert(result.contains("(lower(campaign_name) LIKE lower('%Server Log Avoidance\t #alert(1) #alert(1) # alert(1) Shortest PoC\t $ while:; do echo \"alert(1)\" | nc -lp80; done%')"))
  }

  test("Missing group by fact cols") {
    val jsonString =
      s"""
         |{
         |"cube": "bid_reco",
         |"selectFields": [
         |{ "field": "Advertiser ID" }
         |,
         |{ "field": "Ad Group ID" }
         |,
         |{ "field": "Campaign ID" }
         |,
         |{ "field": "Bid Strategy" }
         |,
         |{ "field": "Current Base Bid" }
         |,
         |{ "field": "Modified Bid" }
         |,
         |{ "field": "Bid Modifier" }
         |,
         |{ "field": "Recommended Bid" }
         |,
         |{ "field": "Clicks" }
         |,
         |{ "field": "Forecasted Clicks" }
         |,
         |{ "field": "Impressions" }
         |,
         |{ "field": "Forecasted Impressions" }
         |,
         |{ "field": "Budget" }
         |,
         |{ "field": "Spend" }
         |,
         |{ "field": "Forecasted Spend" }
         |],
         |"filterExpressions": [
         |{ "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" }
         |,
         |{ "field": "Advertiser ID", "operator": "=", "value": "12345" }
         |]
         |}
      """.stripMargin
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString


    assert(result.contains("GROUP BY modified_bid, CASE WHEN (bid_strategy IN (1)) THEN 'Max Click' WHEN (bid_strategy IN (2)) THEN 'Inflection Point' ELSE 'NONE' END, ad_group_id, account_id, campaign_id, current_bid, (modified_bid - current_bid) / current_bid * 100"))
  }

  test("HiveQueryGeneratorTest: should fail to generate Hive query for Outer Filters") {
    val jsonString =
      s"""
         |{
         |"cube": "bid_reco",
         |"selectFields": [
         |{ "field": "Advertiser ID" }
         |,
         |{ "field": "Ad Group ID" }
         |,
         |{ "field": "Campaign ID" }
         |,
         |{ "field": "Bid Strategy" }
         |,
         |{ "field": "Current Base Bid" }
         |,
         |{ "field": "Modified Bid" }
         |,
         |{ "field": "Bid Modifier" }
         |,
         |{ "field": "Recommended Bid" }
         |,
         |{ "field": "Clicks" }
         |,
         |{ "field": "Forecasted Clicks" }
         |,
         |{ "field": "Impressions" }
         |,
         |{ "field": "Forecasted Impressions" }
         |,
         |{ "field": "Budget" }
         |,
         |{ "field": "Spend" }
         |,
         |{ "field": "Forecasted Spend" }
         |],
         |"filterExpressions": [
         |                     {"operator": "outer", "outerFilters": [
         |                          {"field": "Ad Group ID", "operator": "isnull"}
         |                          ]
         |                     },
         |{ "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" }
         |,
         |{ "field": "Advertiser ID", "operator": "=", "value": "12345" }
         |]
         |}
      """.stripMargin
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isFailure, queryPipelineTry.errorMessage("Outer Filters on Hive Query should fail to get the query pipeline"))
  }

  test("verify dim query can generate inner select and group by with static mapping") {
    val jsonString =
      s"""{
                          "cube": "s_stats",
                          "selectFields": [
                              {"field": "Device ID"},
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"},
                              {"field": "Network ID"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Ad ID", "operator": "==", "compareTo": "Ad Group ID"}
                          ]
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(CAST(device_id AS STRING), ''), NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''), NVL(CAST(mang_pricing_type AS STRING), ''), NVL(CAST(network_id AS STRING), ''))
         |FROM(
         |SELECT COALESCE(device_id, 0L) device_id, COALESCE(account_id, 0L) advertiser_id, COALESCE(impressions, 1L) mang_impressions, COALESCE(price_type, 0L) mang_pricing_type, COALESCE(network_type, "NA") network_id
         |FROM(SELECT CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END device_id, decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE') network_type, CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END price_type, account_id, SUM(impressions) impressions
         |FROM s_stats_fact
         |WHERE (account_id = 12345) AND (ad_id = ad_group_id) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END, decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE'), CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END, account_id
         |
 |       )
         |ssf0
         |) queryAlias LIMIT 200
      """.stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("verify regex_extract query") {
    val jsonString =
      s"""{
                          "cube": "s_stats",
                          "selectFields": [
                              {"field": "Click Exp ID"},
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ]
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "query should not fail")
    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(CAST(click_exp_id AS STRING), ''), NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''))
         |FROM(
         |SELECT COALESCE(click_exp_id, "NA") click_exp_id, COALESCE(account_id, 0L) advertiser_id, COALESCE(impressions, 1L) mang_impressions
         |FROM(SELECT account_id, CASE WHEN LENGTH(regexp_extract(internal_bucket_id, '(cl-)(.*?)(,|$$)', 2)) > 0 THEN regexp_extract(internal_bucket_id, '(cl-)(.*?)(,|$$)', 2) ELSE '-3' END click_exp_id, SUM(impressions) impressions
         |FROM s_stats_fact
         |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY account_id, CASE WHEN LENGTH(regexp_extract(internal_bucket_id, '(cl-)(.*?)(,|$$)', 2)) > 0 THEN regexp_extract(internal_bucket_id, '(cl-)(.*?)(,|$$)', 2) ELSE '-3' END
         |
 |       )
         |ssf0
         |) queryAlias LIMIT 200
      """.stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Verify metric FieldEquality generates a valid query.") {
    val jsonString =
      s"""{
                          "cube": "s_stats",
                          "selectFields": [
                              {"field": "Device ID"},
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"},
                              {"field": "Network ID"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Max Bid", "operator": "==", "compareTo": "Spend"}
                          ]
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(CAST(device_id AS STRING), ''), NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''), NVL(CAST(mang_pricing_type AS STRING), ''), NVL(CAST(network_id AS STRING), ''))
         |FROM(
         |SELECT COALESCE(device_id, 0L) device_id, COALESCE(account_id, 0L) advertiser_id, COALESCE(impressions, 1L) mang_impressions, COALESCE(price_type, 0L) mang_pricing_type, COALESCE(network_type, "NA") network_id
         |FROM(SELECT decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE') network_type, CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END price_type, account_id, CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END device_id, SUM(impressions) impressions
         |FROM s_stats_fact
         |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE'), CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END, account_id, CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END
         |HAVING (MAX(max_bid) = SUM(spend))
         |       )
         |ssf0
         |) queryAlias LIMIT 200
      """.stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }
/*
  test("where clause: ensure duplicate filter mappings are not propagated into the where clause") {
    //currently needs to remove duplicate filter entries, as resolved in base column level
    val jsonString =
      s"""{
                          "cube": "s_stats",
                          "selectFields": [
                              {"field": "Device ID"},
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"},
                              {"field": "Network ID"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Source", "operator": "=", "value": "1"},
                              {"field": "Source Name", "operator": "=", "value": "2"}
                          ]
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(CAST(device_id AS STRING), ''), NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''), NVL(CAST(mang_pricing_type AS STRING), ''), NVL(CAST(network_id AS STRING), ''))
         |FROM(
         |SELECT COALESCE(device_id, 0L) device_id, COALESCE(account_id, 0L) advertiser_id, COALESCE(impressions, 1L) mang_impressions, COALESCE(price_type, 0L) mang_pricing_type, COALESCE(network_type, "NA") network_id
         |FROM(SELECT CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END device_id, decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE') network_type, CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END price_type, account_id, SUM(impressions) impressions
         |FROM s_stats_fact
         |WHERE (account_id = 12345) AND (stats_source = 2) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END, decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE'), CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END, account_id
         |
         |       )
         |ssf0
         |) queryAlias LIMIT 200
      """.stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }
*/
  test("Duplicate registration of the generator") {
    val failRegistry = new QueryGeneratorRegistry
    val dummyHiveQueryGenerator = new QueryGenerator[WithHiveEngine] {
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
    failRegistry.register(OracleEngine, dummyHiveQueryGenerator)
    failRegistry.register(DruidEngine, dummyFalseQueryGenerator)

    HiveQueryGenerator.register(failRegistry, DefaultPartitionColumnRenderer, TestUDFRegistrationFactory())
  }

  test("generating hive query with greater than filter") {
    val jsonString =
      s"""{
                          "cube": "s_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Impressions", "operator": ">", "value": "1608"}
                          ]
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))


    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    val expected =
      s"""SELECT CONCAT_WS(",",NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''))
          FROM(
          SELECT COALESCE(account_id, 0L) advertiser_id, COALESCE(impressions, 1L) mang_impressions
            FROM(SELECT account_id, SUM(impressions) impressions
            FROM s_stats_fact
            WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
            GROUP BY account_id
            HAVING (SUM(impressions) > 1608)
              )
          ssf0
      ) queryAlias LIMIT 200""".stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)

  }

  test("generating hive query with less than filter") {
    val jsonString =
      s"""{
                          "cube": "s_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Impressions", "operator": "<", "value": "1608"}
                          ]
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    val expected =
      s"""SELECT CONCAT_WS(",",NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''))
          FROM(
          SELECT COALESCE(account_id, 0L) advertiser_id, COALESCE(impressions, 1L) mang_impressions
            FROM(SELECT account_id, SUM(impressions) impressions
            FROM s_stats_fact
            WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
            GROUP BY account_id
            HAVING (SUM(impressions) < 1608)
              )
          ssf0
      ) queryAlias LIMIT 200""".stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)

  }

  test("generating hive query with dim col aggregate function") {
    val jsonString =
      s"""{
                          "cube": "s_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Keyword ID"},
                              {"field": "Impressions"},
                              {"field": "Keyword Count"},
                              {"field": "Keyword Count Scaled"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ]
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))


    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString



    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(keyword_id AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''), NVL(CAST(mang_keyword_count AS STRING), ''), NVL(CAST(mang_keyword_count_scaled AS STRING), ''))
         |FROM(
         |SELECT COALESCE(account_id, 0L) advertiser_id, COALESCE(keyword_id, 0L) keyword_id, COALESCE(impressions, 1L) mang_impressions, COALESCE(mang_keyword_count, 0L) mang_keyword_count, COALESCE(mang_keyword_count_scaled, 0L) mang_keyword_count_scaled
         |FROM(SELECT account_id, keyword_id, (COUNT(distinct keyword_id)) mang_keyword_count, (COUNT(keyword_id * stats_source * 10)) mang_keyword_count_scaled, SUM(impressions) impressions
         |FROM s_stats_fact
         |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY account_id, keyword_id
         |
         |       )
         |ssf0
         |) queryAlias LIMIT 200
       """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)

  }
  test("Multiple filters on same ID column") {
    val jsonString =
      s"""{
                          "cube": "s_stats",
                          "selectFields": [
                              {"field": "Device ID"},
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"},
                              {"field": "Network ID"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Advertiser ID", "operator": "IsNotNull"},
                              {"field": "Advertiser ID", "operator": "<>", "value": "-3"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ]
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(CAST(device_id AS STRING), ''), NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''), NVL(CAST(mang_pricing_type AS STRING), ''), NVL(CAST(network_id AS STRING), ''))
         |FROM(
         |SELECT COALESCE(device_id, 0L) device_id, COALESCE(account_id, 0L) advertiser_id, COALESCE(impressions, 1L) mang_impressions, COALESCE(price_type, 0L) mang_pricing_type, COALESCE(network_type, "NA") network_id
         |FROM(SELECT decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE') network_type, CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END price_type, account_id, CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END device_id, SUM(impressions) impressions
         |FROM s_stats_fact
         |WHERE (account_id <> -3) AND (account_id = 12345) AND (account_id IS NOT NULL) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE'), CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END, account_id, CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END
         |
         |       )
         |ssf0
         |
         |)
         |        queryAlias LIMIT 200
         |""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Multiple filters on same column") {
    val jsonString =
      s"""{
                          "cube": "s_stats",
                          "selectFields": [
                              {"field": "Device ID"},
                              {"field": "Advertiser ID"},
                              {"field": "Campaign ID"},
                              {"field": "Campaign Name"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"},
                              {"field": "Network ID"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Advertiser ID", "operator": "IsNotNull"},
                              {"field": "Advertiser ID", "operator": "<>", "value": "-3"},
                              {"field": "Campaign Name", "operator": "<>", "value": "-3"},
                              {"field": "Campaign Name", "operator": "IsNotNull"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ]
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(CAST(device_id AS STRING), ''), NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(campaign_id AS STRING), ''), NVL(CAST(mang_campaign_name AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''), NVL(CAST(mang_pricing_type AS STRING), ''), NVL(CAST(network_id AS STRING), ''))
         |FROM(
         |SELECT COALESCE(device_id, 0L) device_id, COALESCE(account_id, 0L) advertiser_id, COALESCE(ssf0.campaign_id, 0L) campaign_id, getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)) mang_campaign_name, COALESCE(impressions, 1L) mang_impressions, COALESCE(price_type, 0L) mang_pricing_type, COALESCE(network_type, "NA") network_id
         |FROM(SELECT CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END device_id, decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE') network_type, CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END price_type, account_id, campaign_id, SUM(impressions) impressions
         |FROM s_stats_fact
         |WHERE (account_id <> -3) AND (account_id = 12345) AND (account_id IS NOT NULL) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END, decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE'), CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END, account_id, campaign_id
         |
         |       )
         |ssf0
         |JOIN (
         |SELECT campaign_name AS mang_campaign_name, id c1_id
         |FROM campaing_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id <> -3) AND (advertiser_id = 12345) AND (advertiser_id IS NOT NULL) AND (campaign_name <> '-3') AND (campaign_name IS NOT NULL)
         |)
         |c1
         |ON
         |ssf0.campaign_id = c1.c1_id
         |
         |)
         |        queryAlias LIMIT 200
         |""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Not Like filter Hive") {
    val jsonString =
      s"""{
                          "cube": "s_stats",
                          "selectFields": [
                              {"field": "Campaign ID"},
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Clicks"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Campaign Name", "operator": "Not Like", "value": "cmpgn1"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ]
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(CAST(campaign_id AS STRING), ''), NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''), NVL(CAST(mang_clicks AS STRING), ''))
         |FROM(
         |SELECT COALESCE(ssf0.campaign_id, 0L) campaign_id, COALESCE(account_id, 0L) advertiser_id, COALESCE(impressions, 1L) mang_impressions, COALESCE(mang_clicks, 0L) mang_clicks
         |FROM(SELECT account_id, campaign_id, SUM(clicks) mang_clicks, SUM(impressions) impressions
         |FROM s_stats_fact
         |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY account_id, campaign_id
         |
         |       )
         |ssf0
         |JOIN (
         |SELECT id c1_id
         |FROM campaing_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345) AND (lower(campaign_name) NOT LIKE lower('%cmpgn1%'))
         |)
         |c1
         |ON
         |ssf0.campaign_id = c1.c1_id
         |
         |) queryAlias LIMIT 200
      """.stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  /*
  // Outer Group By
  test("Successfully generated Outer Group By Query with dim non id field and fact field") {
    val jsonString =
      s"""{
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

    val result = generateHiveQuery(jsonString)
    val expected =
      s"""SELECT CONCAT_WS(",",NVL(mang_campaign_name, ''), NVL(mang_spend, ''))
    |FROM(
    |SELECT getCsvEscapedString(CAST(NVL(outergroupby.mang_campaign_name, '') AS STRING)) mang_campaign_name, CAST(ROUND(COALESCE(spend, 0.0), 10) as STRING) mang_spend
    |FROM(
    |SELECT c1.mang_campaign_name,SUM(spend) spend
    |FROM(SELECT campaign_id, SUM(spend) spend
    |FROM ad_fact1
    |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
    |GROUP BY campaign_id
    |
    |       )
    |af0
    |LEFT OUTER JOIN (
    |SELECT campaign_name AS mang_campaign_name, id c1_id
    |FROM campaing_hive
    |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
    |)
    |c1
    |ON
    |af0.campaign_id = c1.c1_id
    |
    |GROUP BY c1.mang_campaign_name) outergroupby
    |)""".stripMargin

    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Successfully generated Outer Group By Query with dim non id field and derived fact field having dim source col") {
    val jsonString =
      s"""{
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

    val result = generateHiveQuery(jsonString)
    val expected =
      s"""SELECT CONCAT_WS(",",NVL(mang_campaign_name, ''), NVL(mang_source, ''), NVL(mang_n_spend, ''))
    FROM(
    SELECT getCsvEscapedString(CAST(NVL(outergroupby.mang_campaign_name, '') AS STRING)) mang_campaign_name, CAST(COALESCE(stats_source, 0L) as STRING) mang_source, CAST(ROUND(COALESCE(decodeUDF(stats_source, 1, spend, 0.0), 0L), 10) as STRING) mang_n_spend
    FROM(
    SELECT c1.mang_campaign_name,af0.stats_source,SUM(spend) spend
    FROM(SELECT campaign_id, stats_source, SUM(spend) spend
    FROM ad_fact1
    WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
    GROUP BY campaign_id, stats_source

           )
    af0
    LEFT OUTER JOIN (
    SELECT campaign_name AS mang_campaign_name, id c1_id
    FROM campaing_hive
    WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
    )
    c1
    ON
    af0.campaign_id = c1.c1_id

    GROUP BY c1.mang_campaign_name,af0.stats_source) outergroupby
    )""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Successfully generated timeseries Outer Group By Query with dim non id field and fact field") {
    val jsonString =
      s"""{
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
    val result = generateHiveQuery(jsonString)
    val expected =
      s"""SELECT CONCAT_WS(",",NVL(mang_day, ''), NVL(mang_campaign_name, ''), NVL(mang_spend, ''))
      FROM(
      SELECT getFormattedDate(stats_date) mang_day, getCsvEscapedString(CAST(NVL(outergroupby.mang_campaign_name, '') AS STRING)) mang_campaign_name, CAST(ROUND(COALESCE(spend, 0.0), 10) as STRING) mang_spend
      FROM(
      SELECT af0.stats_date,c1.mang_campaign_name,SUM(spend) spend
      FROM(SELECT campaign_id, stats_date, SUM(spend) spend
      FROM ad_fact1
      WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
      GROUP BY campaign_id, stats_date

             )
      af0
      LEFT OUTER JOIN (
      SELECT campaign_name AS mang_campaign_name, id c1_id
      FROM campaing_hive
      WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
      )
      c1
      ON
      af0.campaign_id = c1.c1_id

      GROUP BY af0.stats_date,c1.mang_campaign_name) outergroupby
      )""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Successfully generated Outer Group By Query with 2 dimension non id fields") {
    val jsonString =
      s"""{
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
    val result = generateHiveQuery(jsonString)
    val expected = s"""SELECT CONCAT_WS(",",NVL(mang_campaign_name, ''), NVL(mang_advertiser_currency, ''), NVL(mang_spend, ''))
      FROM(
      SELECT getCsvEscapedString(CAST(NVL(outergroupby.mang_campaign_name, '') AS STRING)) mang_campaign_name, COALESCE(outergroupby.mang_advertiser_currency, "NA") mang_advertiser_currency, CAST(ROUND(COALESCE(spend, 0.0), 10) as STRING) mang_spend
      FROM(
      SELECT c2.mang_campaign_name,a1.mang_advertiser_currency,SUM(spend) spend
      FROM(SELECT advertiser_id, campaign_id, SUM(spend) spend
      FROM ad_fact1
      WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
      GROUP BY advertiser_id, campaign_id

             )
      af0
      LEFT OUTER JOIN (
      SELECT currency AS mang_advertiser_currency, id a1_id
      FROM advertiser_hive
      WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
      )
      a1
      ON
      af0.advertiser_id = a1.a1_id
             LEFT OUTER JOIN (
      SELECT advertiser_id AS advertiser_id, campaign_name AS mang_campaign_name, id c2_id
      FROM campaing_hive
      WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
      )
      c2
      ON
      af0.campaign_id = c2.c2_id

      GROUP BY c2.mang_campaign_name,a1.mang_advertiser_currency) outergroupby
      )""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Should not generate Outer Group By Query context with 2 dimension non id fields and one fact higher level ID field than best dims") {
    val jsonString =
      s"""{
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
    val result = generateHiveQuery(jsonString)
    assert(!result.contains("outergroupby"))
  }

  test("Successfully generated Outer Group By Query with 2 dimension non id fields and and two fact transitively dependent cols") {
    val jsonString =
      s"""{
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
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ]
                           }""".stripMargin
    val result = generateHiveQuery(jsonString)
    val expected = s"""SELECT CONCAT_WS(",",NVL(mang_campaign_name, ''), NVL(mang_advertiser_currency, ''), NVL(mang_average_cpc_cents, ''), NVL(mang_average_cpc, ''), NVL(mang_spend, ''))
      |FROM(
      |SELECT getCsvEscapedString(CAST(NVL(outergroupby.mang_campaign_name, '') AS STRING)) mang_campaign_name, COALESCE(outergroupby.mang_advertiser_currency, "NA") mang_advertiser_currency, CAST(ROUND(COALESCE((CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END) * 100, 0L), 10) as STRING) mang_average_cpc_cents, CAST(ROUND(COALESCE(CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END, 0L), 10) as STRING) mang_average_cpc, CAST(ROUND(COALESCE(spend, 0.0), 10) as STRING) mang_spend
      |FROM(
      |SELECT c2.mang_campaign_name,a1.mang_advertiser_currency,SUM(clicks) clicks,SUM(spend) spend
      |FROM(SELECT advertiser_id, campaign_id, SUM(spend) spend, SUM(clicks) clicks
      |FROM ad_fact1
      |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
      |GROUP BY advertiser_id, campaign_id
      |
      |       )
      |af0
      |LEFT OUTER JOIN (
      |SELECT currency AS mang_advertiser_currency, id a1_id
      |FROM advertiser_hive
      |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
      |)
      |a1
      |ON
      |af0.advertiser_id = a1.a1_id
      |      LEFT OUTER JOIN (
      |SELECT advertiser_id AS advertiser_id, campaign_name AS mang_campaign_name, id c2_id
      |FROM campaing_hive
      |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
      |)
      |c2
      |ON
      |af0.campaign_id = c2.c2_id
      |
      |GROUP BY c2.mang_campaign_name,a1.mang_advertiser_currency) outergroupby
      |)""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Successfully generated Outer Group By Query if fk col one level less than Highest dim candidate level is requested") {
    val jsonString =
      s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Ad Status",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Campaign Name"
                             },
                             {
                               "field": "Campaign ID",
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
    val result = generateHiveQuery(jsonString)
    val expected = s"""SELECT CONCAT_WS(",",NVL(mang_ad_status, ''), NVL(mang_campaign_name, ''), NVL(campaign_id, ''), NVL(mang_spend, ''))
      |FROM(
      |SELECT COALESCE(outergroupby.mang_ad_status, "NA") mang_ad_status, getCsvEscapedString(CAST(NVL(outergroupby.mang_campaign_name, '') AS STRING)) mang_campaign_name, CAST(COALESCE(campaign_id, 0L) as STRING) campaign_id, CAST(ROUND(COALESCE(spend, 0.0), 10) as STRING) mang_spend
      |FROM(
      |SELECT a2.mang_ad_status,c1.mang_campaign_name,af0.campaign_id,SUM(spend) spend
      |FROM(SELECT campaign_id, ad_id, SUM(spend) spend
      |FROM ad_fact1
      |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
      |GROUP BY campaign_id, ad_id
      |
      |       )
      |af0
      |LEFT OUTER JOIN (
      |SELECT campaign_name AS mang_campaign_name, id c1_id
      |FROM campaing_hive
      |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
      |)
      |c1
      |ON
      |af0.campaign_id = c1.c1_id
      |      LEFT OUTER JOIN (
      |SELECT campaign_id AS campaign_id, decodeUDF(status, 'ON', 'ON', 'OFF') AS mang_ad_status, id a2_id
      |FROM ad_dim_hive
      |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
      |)
      |a2
      |ON
      |af0.ad_id = a2.a2_id
      |
      |GROUP BY a2.mang_ad_status,c1.mang_campaign_name,af0.campaign_id) outergroupby
      |)""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Successfully generated Outer Group By Query if CustomRollup col is requested") {
    val jsonString =
      s"""{
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
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ]
                           }""".stripMargin
    val result = generateHiveQuery(jsonString)
    val expected = s"""SELECT CONCAT_WS(",",NVL(mang_campaign_name, ''), NVL(mang_average_cpc, ''), NVL(mang_spend, ''))
      |FROM(
      |SELECT getCsvEscapedString(CAST(NVL(outergroupby.mang_campaign_name, '') AS STRING)) mang_campaign_name, CAST(ROUND(COALESCE(CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END, 0L), 10) as STRING) mang_average_cpc, CAST(ROUND(COALESCE(spend, 0.0), 10) as STRING) mang_spend
      |FROM(
      |SELECT c1.mang_campaign_name,SUM(clicks) clicks,SUM(spend) spend
      |FROM(SELECT campaign_id, SUM(spend) spend, SUM(clicks) clicks
      |FROM ad_fact1
      |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
      |GROUP BY campaign_id
      |
      |       )
      |af0
      |LEFT OUTER JOIN (
      |SELECT campaign_name AS mang_campaign_name, id c1_id
      |FROM campaing_hive
      |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
      |)
      |c1
      |ON
      |af0.campaign_id = c1.c1_id
      |
      |GROUP BY c1.mang_campaign_name) outergroupby
      |)""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Successfully generated Outer Group By Query if CustomRollup col with Derived Expression having rollups is requested") {
    val jsonString =
      s"""{
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
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ]
                           }""".stripMargin

    val result = generateHiveQuery(jsonString)
    val expected = s"""SELECT CONCAT_WS(",",NVL(mang_campaign_name, ''), NVL(mang_average_position, ''), NVL(mang_spend, ''))
      |FROM(
      |SELECT getCsvEscapedString(CAST(NVL(outergroupby.mang_campaign_name, '') AS STRING)) mang_campaign_name, CAST(ROUND(COALESCE(CASE WHEN ((avg_pos >= 0.1) AND (avg_pos <= 500)) THEN avg_pos ELSE 0.0 END, 0.0), 10) as STRING) mang_average_position, CAST(ROUND(COALESCE(spend, 0.0), 10) as STRING) mang_spend
      |FROM(
      |SELECT c1.mang_campaign_name,(CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(CASE WHEN ((avg_pos >= 0.1) AND (avg_pos <= 500)) THEN avg_pos ELSE 0.0 END * impressions) / (SUM(impressions)) END) avg_pos,SUM(spend) spend
      |FROM(SELECT campaign_id, SUM(spend) spend, SUM(impressions) impressions, (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(CASE WHEN ((avg_pos >= 0.1) AND (avg_pos <= 500)) THEN avg_pos ELSE 0.0 END * impressions) / (SUM(impressions)) END) avg_pos
      |FROM ad_fact1
      |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
      |GROUP BY campaign_id
      |
      |       )
      |af0
      |LEFT OUTER JOIN (
      |SELECT campaign_name AS mang_campaign_name, id c1_id
      |FROM campaing_hive
      |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
      |)
      |c1
      |ON
      |af0.campaign_id = c1.c1_id
      |
      |GROUP BY c1.mang_campaign_name) outergroupby
      |)""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Successfully generated Outer Group By Query if OracleCustomRollup col with Derived Expression having CustomRollup and DerCol are requested") {
    val jsonString =
      s"""{
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
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ]
                           }""".stripMargin
    val result = generateHiveQuery(jsonString)
    val expected = s"""SELECT CONCAT_WS(",",NVL(mang_campaign_name, ''), NVL(mang_average_position, ''), NVL(mang_average_cpc, ''), NVL(mang_spend, ''))
      |FROM(
      |SELECT getCsvEscapedString(CAST(NVL(outergroupby.mang_campaign_name, '') AS STRING)) mang_campaign_name, CAST(ROUND(COALESCE(CASE WHEN ((avg_pos >= 0.1) AND (avg_pos <= 500)) THEN avg_pos ELSE 0.0 END, 0.0), 10) as STRING) mang_average_position, CAST(ROUND(COALESCE(CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END, 0L), 10) as STRING) mang_average_cpc, CAST(ROUND(COALESCE(spend, 0.0), 10) as STRING) mang_spend
      |FROM(
      |SELECT c1.mang_campaign_name,(CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(CASE WHEN ((avg_pos >= 0.1) AND (avg_pos <= 500)) THEN avg_pos ELSE 0.0 END * impressions) / (SUM(impressions)) END) avg_pos,SUM(clicks) clicks,SUM(spend) spend
      |FROM(SELECT campaign_id, SUM(spend) spend, SUM(impressions) impressions, (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(CASE WHEN ((avg_pos >= 0.1) AND (avg_pos <= 500)) THEN avg_pos ELSE 0.0 END * impressions) / (SUM(impressions)) END) avg_pos, SUM(clicks) clicks
      |FROM ad_fact1
      |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
      |GROUP BY campaign_id
      |
      |)
      |af0
      |LEFT OUTER JOIN (
      |SELECT campaign_name AS mang_campaign_name, id c1_id
      |FROM campaing_hive
      |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
      |)
      |c1
      |ON
      |af0.campaign_id = c1.c1_id
      |
      |GROUP BY c1.mang_campaign_name) outergroupby
      |)""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Successfully generated Outer Group By Query if column is derived from dim column") {
    val jsonString =
      s"""{
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
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ]
                           }""".stripMargin
    val result = generateHiveQuery(jsonString)
    val expected = s"""SELECT CONCAT_WS(",",NVL(mang_campaign_name, ''), NVL(advertiser_id, ''), NVL(mang_n_average_cpc, ''), NVL(mang_spend, ''))
      |FROM(
      |SELECT getCsvEscapedString(CAST(NVL(outergroupby.mang_campaign_name, '') AS STRING)) mang_campaign_name, CAST(COALESCE(advertiser_id, 0L) as STRING) advertiser_id, CAST(ROUND(COALESCE(CASE WHEN decodeUDF(stats_source, 1, clicks, 0.0) = 0 THEN 0.0 ELSE decodeUDF(stats_source, 1, spend, 0.0) / decodeUDF(stats_source, 1, clicks, 0.0) END, 0L), 10) as STRING) mang_n_average_cpc, CAST(ROUND(COALESCE(spend, 0.0), 10) as STRING) mang_spend
      |FROM(
      |SELECT c1.mang_campaign_name,af0.advertiser_id,af0.stats_source,SUM(clicks) clicks,SUM(spend) spend
      |FROM(SELECT advertiser_id, campaign_id, SUM(spend) spend, stats_source, SUM(clicks) clicks
      |FROM ad_fact1
      |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
      |GROUP BY advertiser_id, campaign_id, stats_source
      |
      |       )
      |af0
      |LEFT OUTER JOIN (
      |SELECT campaign_name AS mang_campaign_name, id c1_id
      |FROM campaing_hive
      |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
      |)
      |c1
      |ON
      |af0.campaign_id = c1.c1_id
      |
      |GROUP BY c1.mang_campaign_name,af0.advertiser_id,af0.stats_source) outergroupby
      |)""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Successfully generated Outer Group By Query if NoopRollupp column requested") {
    val jsonString =
      s"""{
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
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ]
                           }""".stripMargin
    val result = generateHiveQuery(jsonString)
    val expected = s"""SELECT CONCAT_WS(",",NVL(mang_campaign_name, ''), NVL(mang_impression_share, ''), NVL(mang_spend, ''))
      |FROM(
      |SELECT getCsvEscapedString(CAST(NVL(outergroupby.mang_campaign_name, '') AS STRING)) mang_campaign_name, CAST(COALESCE(impression_share, 0L) as STRING) mang_impression_share, CAST(ROUND(COALESCE(spend, 0.0), 10) as STRING) mang_spend
      |FROM(
      |SELECT c1.mang_campaign_name,(decodeUDF(MAX(show_flag), 1, ROUND(CASE WHEN SUM(s_impressions) = 0 THEN 0.0 ELSE SUM(impressions) / (SUM(s_impressions)) END, 4), NULL)) impression_share,SUM(spend) spend
      |FROM(SELECT campaign_id, SUM(spend) spend, show_flag, SUM(s_impressions) s_impressions, SUM(impressions) impressions
      |FROM ad_fact1
      |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
      |GROUP BY campaign_id, show_flag
      |
      |       )
      |af0
      |LEFT OUTER JOIN (
      |SELECT campaign_name AS mang_campaign_name, id c1_id
      |FROM campaing_hive
      |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
      |)
      |c1
      |ON
      |af0.campaign_id = c1.c1_id
      |
      |GROUP BY c1.mang_campaign_name) outergroupby
      |)""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Successfully generated Outer Group By Query if NoopRollupp derived column is requested for non-derived source fields") {
      val jsonString =
        s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Ad Status",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Campaign Name"
                             },
                             {
                               "field": "Campaign ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Spend",
                               "alias": null,
                               "value": null
                             },
                             {
                                "field": "Engagement Rate",
                                "alias": null,
                                "value": null
                             }
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ]
                           }""".stripMargin
      val result = generateHiveQuery(jsonString)
      val expected = s"""SELECT CONCAT_WS(",",NVL(mang_ad_status, ''), NVL(mang_campaign_name, ''), NVL(campaign_id, ''), NVL(mang_spend, ''), NVL(mang_engagement_rate, ''))
FROM(
SELECT COALESCE(outergroupby.mang_ad_status, "NA") mang_ad_status, getCsvEscapedString(CAST(NVL(outergroupby.mang_campaign_name, '') AS STRING)) mang_campaign_name, CAST(COALESCE(campaign_id, 0L) as STRING) campaign_id, CAST(ROUND(COALESCE(spend, 0.0), 10) as STRING) mang_spend, CAST(ROUND(COALESCE(100 * mathUDF(engagement_count, impressions), 0L), 10) as STRING) mang_engagement_rate
FROM(
SELECT a2.mang_ad_status,c1.mang_campaign_name,af0.campaign_id,SUM(spend) spend,SUM(engagement_count) engagement_count,SUM(impressions) impressions
FROM(SELECT ad_id, campaign_id, SUM(spend) spend, SUM(engagement_count) engagement_count, SUM(impressions) impressions
FROM ad_fact1
WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
GROUP BY ad_id, campaign_id

       )
af0
LEFT OUTER JOIN (
SELECT campaign_name AS mang_campaign_name, id c1_id
FROM campaing_hive
WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
)
c1
ON
af0.campaign_id = c1.c1_id
       LEFT OUTER JOIN (
SELECT campaign_id AS campaign_id, decodeUDF(status, 'ON', 'ON', 'OFF') AS mang_ad_status, id a2_id
FROM ad_dim_hive
WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
)
a2
ON
af0.ad_id = a2.a2_id

GROUP BY a2.mang_ad_status,c1.mang_campaign_name,af0.campaign_id) outergroupby
)""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }


  test("Successfully generated Outer Group By Query if aggregate derived column (eg UDAF) is requested") {
    val jsonString =
      s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Ad Status",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Campaign Name"
                             },
                             {
                               "field": "Campaign ID",
                               "alias": null,
                               "value": null
                             },
                             {
                               "field": "Spend",
                               "alias": null,
                               "value": null
                             },
                             {
                                "field": "Engagement Rate",
                                "alias": null,
                                "value": null
                             },
                             {
                                "field": "Paid Engagement Rate",
                                "alias": null,
                                "value": null
                             }
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ]
                           }""".stripMargin
    val result = generateHiveQuery(jsonString)
    val expected = s"""SELECT CONCAT_WS(",",NVL(mang_ad_status, ''), NVL(mang_campaign_name, ''), NVL(campaign_id, ''), NVL(mang_spend, ''), NVL(mang_engagement_rate, ''), NVL(mang_paid_engagement_rate, ''))
FROM(
SELECT COALESCE(outergroupby.mang_ad_status, "NA") mang_ad_status, getCsvEscapedString(CAST(NVL(outergroupby.mang_campaign_name, '') AS STRING)) mang_campaign_name, CAST(COALESCE(campaign_id, 0L) as STRING) campaign_id, CAST(ROUND(COALESCE(spend, 0.0), 10) as STRING) mang_spend, CAST(ROUND(COALESCE(100 * mathUDF(engagement_count, impressions), 0L), 10) as STRING) mang_engagement_rate, CAST(ROUND(COALESCE(mang_paid_engagement_rate, 0L), 10) as STRING) mang_paid_engagement_rate
FROM(
SELECT a2.mang_ad_status,c1.mang_campaign_name,af0.campaign_id,SUM(spend) spend,SUM(engagement_count) engagement_count,SUM(impressions) impressions,(100 * mathUDAF(engagement_count, 0, 0, clicks, impressions)) mang_paid_engagement_rate
FROM(SELECT ad_id, campaign_id, SUM(spend) spend, SUM(engagement_count) engagement_count, SUM(clicks) clicks, SUM(impressions) impressions
FROM ad_fact1
WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
GROUP BY ad_id, campaign_id

       )
af0
LEFT OUTER JOIN (
SELECT campaign_name AS mang_campaign_name, id c1_id
FROM campaing_hive
WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
)
c1
ON
af0.campaign_id = c1.c1_id
       LEFT OUTER JOIN (
SELECT campaign_id AS campaign_id, decodeUDF(status, 'ON', 'ON', 'OFF') AS mang_ad_status, id a2_id
FROM ad_dim_hive
WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
)
a2
ON
af0.ad_id = a2.a2_id

GROUP BY a2.mang_ad_status,c1.mang_campaign_name,af0.campaign_id) outergroupby
)""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }
*/

  def generateHiveQuery(requestJson: String): String = {
    val requestRaw = ReportingRequest.deserializeAsync(requestJson.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    val registry = defaultRegistry
    val request = ReportingRequest.forceHive(requestRaw.toOption.get)
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    result
  }
}
