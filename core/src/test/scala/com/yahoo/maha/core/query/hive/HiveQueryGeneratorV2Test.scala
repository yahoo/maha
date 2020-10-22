// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.hive

import java.nio.charset.StandardCharsets

import com.yahoo.maha.core.CoreSchema.AdvertiserSchema
import com.yahoo.maha.core._
import com.yahoo.maha.core.query.{QueryGeneratorRegistry, _}
import com.yahoo.maha.core.request.ReportingRequest

/**
 * Created by pranavbhole on 10/16/18.
  * To test sort by feature of hive query generator
  * Now v2 is promoted to v0
 */
class HiveQueryGeneratorV2Test extends BaseHiveQueryGeneratorTest {

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
         |ORDER BY mang_impressions ASC) queryAlias LIMIT 100
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
         |SELECT CONCAT_WS(",",NVL(CAST(device_id AS STRING), ''), NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''), NVL(CAST(mang_pricing_type AS STRING), ''), NVL(CAST(network_id AS STRING), ''))
         |FROM(
         |SELECT COALESCE(device_id, 0L) device_id, COALESCE(account_id, 0L) advertiser_id, COALESCE(impressions, 1L) mang_impressions, COALESCE(price_type, 0L) mang_pricing_type, COALESCE(network_type, "NA") network_id
         |FROM(SELECT decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE') network_type, CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END price_type, account_id, CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END device_id, SUM(impressions) impressions
         |FROM s_stats_fact
         |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE'), CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END, account_id, CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END
         |
         |       )
         |ssf0
         |) queryAlias LIMIT 200
      """.stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

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

  test("generating hive query with sort on dimension") {
    val jsonString =
      s"""{
                          "cube": "s_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Advertiser Name"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Impressions", "operator": ">", "value": "1608"}
                          ],
                          "sortBy": [{"field": "Advertiser Name", "order": "Desc"},  {"field": "Impressions", "order": "DESC"}]
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString


        val expected =
          s"""
             |SELECT CONCAT_WS(",",NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(mang_advertiser_name AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''))
             |FROM(
             |SELECT COALESCE(ssf0.account_id, 0L) advertiser_id, COALESCE(a1.mang_advertiser_name, 'NA') mang_advertiser_name, COALESCE(impressions, 1L) mang_impressions
             |FROM(SELECT account_id, SUM(impressions) impressions
             |FROM s_stats_fact
             |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
             |GROUP BY account_id
             |HAVING (SUM(impressions) > 1608)
             |       )
             |ssf0
             |LEFT OUTER JOIN (
             |SELECT name AS mang_advertiser_name, id a1_id
             |FROM advertiser_hive
             |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
             |)
             |a1
             |ON
             |ssf0.account_id = a1.a1_id
             |
             |ORDER BY mang_advertiser_name DESC, mang_impressions DESC) queryAlias LIMIT 200
             """.stripMargin

        result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("generating hive query with greater than filter") {
    val jsonString =
      s"""{
                          "cube": "s_stats",
                          "selectFields": [
                              {"field": "Advertiser Name"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Impressions", "operator": ">", "value": "1608"}
                          ],
                          "sortBy": [{"field": "Advertiser Name", "order": "Desc"},  {"field": "Impressions", "order": "DESC"}]
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(mang_advertiser_name,'') AS STRING),CAST(NVL(mang_impressions,'') AS STRING))
         |FROM(
         |SELECT mang_advertiser_name AS mang_advertiser_name, impressions AS mang_impressions
         |FROM(
         |SELECT COALESCE(a1.mang_advertiser_name, 'NA') mang_advertiser_name, SUM(impressions) AS impressions
         |FROM(SELECT account_id, SUM(impressions) impressions
         |FROM s_stats_fact
         |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY account_id
         |HAVING (SUM(impressions) > 1608)
         |       )
         |ssf0
         |LEFT OUTER JOIN (
         |SELECT name AS mang_advertiser_name, id a1_id
         |FROM advertiser_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
         |)
         |a1
         |ON
         |ssf0.account_id = a1.a1_id
         |
         |GROUP BY COALESCE(a1.mang_advertiser_name, 'NA')
         |ORDER BY mang_advertiser_name DESC, impressions DESC) OgbQueryAlias
         |) queryAlias LIMIT 200
         |
       """.stripMargin

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

  test("successfully generate fact driven query for day grain with datetime between filter") {
    val jsonString =
      s"""{
                          "cube": "s_stats",
                          "selectFields": [
                              {"field": "Advertiser Name"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "datetimebetween", "from": "$fromDateTime", "to": "$toDateTime", "format": "$iso8601Format"},
                              {"field": "Impressions", "operator": ">", "value": "1608"}
                          ],
                          "sortBy": [{"field": "Advertiser Name", "order": "Desc"},  {"field": "Impressions", "order": "DESC"}]
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(mang_advertiser_name,'') AS STRING),CAST(NVL(mang_impressions,'') AS STRING))
         |FROM(
         |SELECT mang_advertiser_name AS mang_advertiser_name, impressions AS mang_impressions
         |FROM(
         |SELECT COALESCE(a1.mang_advertiser_name, 'NA') mang_advertiser_name, SUM(impressions) AS impressions
         |FROM(SELECT account_id, SUM(impressions) impressions
         |FROM s_stats_fact
         |WHERE (account_id = 12345) AND (stats_date >= cast('$fromDate' as date) AND stats_date <= cast('$toDate' as date))
         |GROUP BY account_id
         |HAVING (SUM(impressions) > 1608)
         |       )
         |ssf0
         |LEFT OUTER JOIN (
         |SELECT name AS mang_advertiser_name, id a1_id
         |FROM advertiser_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
         |)
         |a1
         |ON
         |ssf0.account_id = a1.a1_id
         |
         |GROUP BY COALESCE(a1.mang_advertiser_name, 'NA')
         |ORDER BY mang_advertiser_name DESC, impressions DESC) OgbQueryAlias
         |) queryAlias LIMIT 200
         |
       """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)

  }

  test("successfully generate fact driven query for minute grain with datetime between filter") {
    val jsonString =
      s"""{
                          "cube": "s_stats_minute",
                          "selectFields": [
                              {"field": "Advertiser Name"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "datetimebetween", "from": "$fromDateTime", "to": "$toDateTime", "format": "$iso8601Format"},
                              {"field": "Impressions", "operator": ">", "value": "1608"}
                          ],
                          "sortBy": [{"field": "Advertiser Name", "order": "Desc"},  {"field": "Impressions", "order": "DESC"}]
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(mang_advertiser_name,'') AS STRING),CAST(NVL(mang_impressions,'') AS STRING))
         |FROM(
         |SELECT mang_advertiser_name AS mang_advertiser_name, impressions AS mang_impressions
         |FROM(
         |SELECT COALESCE(a1.mang_advertiser_name, 'NA') mang_advertiser_name, SUM(impressions) AS impressions
         |FROM(SELECT account_id, SUM(impressions) impressions
         |FROM s_stats_fact
         |WHERE (account_id = 12345) AND (stats_date >= cast('${fromDateTime.dropRight(1)}' as timestamp) AND stats_date <= cast('${toDateTime.dropRight(1)}' as timestamp))
         |GROUP BY account_id
         |HAVING (SUM(impressions) > 1608)
         |       )
         |ssf0
         |LEFT OUTER JOIN (
         |SELECT name AS mang_advertiser_name, id a1_id
         |FROM advertiser_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
         |)
         |a1
         |ON
         |ssf0.account_id = a1.a1_id
         |
         |GROUP BY COALESCE(a1.mang_advertiser_name, 'NA')
         |ORDER BY mang_advertiser_name DESC, impressions DESC) OgbQueryAlias
         |) queryAlias LIMIT 200
         |
       """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)

  }

  test("generating hive query with sort by") {
    val jsonString =
      s"""{
                          "cube": "s_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Name"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "ASC"}
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
         |SELECT CONCAT_WS(",",NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''), NVL(CAST(mang_advertiser_name AS STRING), ''))
         |FROM(
         |SELECT COALESCE(ssf0.account_id, 0L) advertiser_id, COALESCE(impressions, 1L) mang_impressions, COALESCE(a1.mang_advertiser_name, "NA") mang_advertiser_name
         |FROM(SELECT account_id, SUM(impressions) impressions
         |FROM s_stats_fact
         |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY account_id
         |
         |       )
         |ssf0
         |LEFT OUTER JOIN (
         |SELECT name AS mang_advertiser_name, id a1_id
         |FROM advertiser_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
         |)
         |a1
         |ON
         |ssf0.account_id = a1.a1_id
         |
         |ORDER BY mang_impressions ASC) queryAlias LIMIT 200
       """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)

  }

  test("generating hive query with dim and fact sort by") {
    val jsonString =
      s"""{
                          "cube": "s_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Advertiser Name"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              {"field": "Impressions", "order": "ASC"},  {"field": "Advertiser Name", "order": "DESC"}
                           ]
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryChain = queryPipelineTry.toOption.get.queryChain

    val result =  queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(queryChain.drivingQuery.queryGenVersion.isDefined)
    assert(queryChain.drivingQuery.queryGenVersion.get == Version.v0)

    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''), NVL(CAST(mang_advertiser_name AS STRING), ''))
         |FROM(
         |SELECT COALESCE(ssf0.account_id, 0L) advertiser_id, COALESCE(impressions, 1L) mang_impressions, COALESCE(a1.mang_advertiser_name, "NA") mang_advertiser_name
         |FROM(SELECT account_id, SUM(impressions) impressions
         |FROM s_stats_fact
         |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY account_id
         |
         |       )
         |ssf0
         |LEFT OUTER JOIN (
         |SELECT name AS mang_advertiser_name, id a1_id
         |FROM advertiser_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
         |)
         |a1
         |ON
         |ssf0.account_id = a1.a1_id
         |
         |ORDER BY mang_impressions ASC, mang_advertiser_name DESC) queryAlias LIMIT 200
       """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)

  }


  test("generating hive query with outer group containing derived columns of level 2") {
    val jsonString =
      s"""{
                          "cube": "s_stats",
                          "selectFields": [
                              {"field": "Campaign Name"},
                              {"field": "Average CPC"},
                              {"field": "Average Position"},
                              {"field": "Impressions"}
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

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryChain = queryPipelineTry.toOption.get.queryChain

    val result =  queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(queryChain.drivingQuery.queryGenVersion.isDefined)
    assert(queryChain.drivingQuery.queryGenVersion.get == Version.v2)
    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(mang_campaign_name,'') AS STRING),CAST(NVL(mang_average_cpc,'') AS STRING),CAST(NVL(mang_average_position,'') AS STRING),CAST(NVL(mang_impressions,'') AS STRING))
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END AS mang_average_cpc, avg_pos AS mang_average_position, impressions AS mang_impressions
         |FROM(
         |SELECT getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)) mang_campaign_name, (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(weighted_position * impressions) / (SUM(impressions)) END) AS avg_pos, SUM(impressions) AS impressions, SUM(clicks) AS clicks, SUM(spend) AS spend, SUM(weighted_position) AS weighted_position
         |FROM(SELECT campaign_id, SUM(impressions) impressions, SUM(clicks) clicks, SUM(spend) spend, SUM(weighted_position) weighted_position
         |FROM s_stats_fact
         |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY campaign_id
         |
         |       )
         |ssf0
         |LEFT OUTER JOIN (
         |SELECT campaign_name AS mang_campaign_name, id c1_id
         |FROM campaing_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c1
         |ON
         |ssf0.campaign_id = c1.c1_id
         |
         |GROUP BY getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING))
         |) OgbQueryAlias
         |) queryAlias LIMIT 200
         |
       """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("generating hive query with outer group by, 2 dim joins and sorting") {
    val jsonString =
      s"""{
                          "cube": "s_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Campaign Name"},
                              {"field": "Advertiser Name"},
                              {"field": "Average CPC"},
                              {"field": "Average Position"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                          {"field": "Impressions", "order": "Desc"},  {"field": "Advertiser Name", "order": "DESC"}]
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryChain = queryPipelineTry.toOption.get.queryChain

    val result =  queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(queryChain.drivingQuery.queryGenVersion.isDefined)
    assert(queryChain.drivingQuery.queryGenVersion.get == Version.v2)

    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(advertiser_id,'') AS STRING),CAST(NVL(mang_campaign_name,'') AS STRING),CAST(NVL(mang_advertiser_name,'') AS STRING),CAST(NVL(mang_average_cpc,'') AS STRING),CAST(NVL(mang_average_position,'') AS STRING),CAST(NVL(mang_impressions,'') AS STRING))
         |FROM(
         |SELECT advertiser_id AS advertiser_id, mang_campaign_name AS mang_campaign_name, mang_advertiser_name AS mang_advertiser_name, CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END AS mang_average_cpc, avg_pos AS mang_average_position, impressions AS mang_impressions
         |FROM(
         |SELECT COALESCE(c2.advertiser_id, 0L) advertiser_id, getCsvEscapedString(CAST(NVL(c2.mang_campaign_name, '') AS STRING)) mang_campaign_name, COALESCE(a1.mang_advertiser_name, 'NA') mang_advertiser_name, (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(weighted_position * impressions) / (SUM(impressions)) END) AS avg_pos, SUM(impressions) AS impressions, SUM(clicks) AS clicks, SUM(spend) AS spend, SUM(weighted_position) AS weighted_position
         |FROM(SELECT account_id, campaign_id, SUM(impressions) impressions, SUM(clicks) clicks, SUM(spend) spend, SUM(weighted_position) weighted_position
         |FROM s_stats_fact
         |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY account_id, campaign_id
         |
         |       )
         |ssf0
         |LEFT OUTER JOIN (
         |SELECT name AS mang_advertiser_name, id a1_id
         |FROM advertiser_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
         |)
         |a1
         |ON
         |ssf0.account_id = a1.a1_id
         |       LEFT OUTER JOIN (
         |SELECT advertiser_id AS advertiser_id, campaign_name AS mang_campaign_name, id c2_id
         |FROM campaing_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c2
         |ON
         |ssf0.campaign_id = c2.c2_id
         |
         |GROUP BY COALESCE(c2.advertiser_id, 0L), getCsvEscapedString(CAST(NVL(c2.mang_campaign_name, '') AS STRING)), COALESCE(a1.mang_advertiser_name, 'NA')
         |ORDER BY impressions DESC, mang_advertiser_name DESC) OgbQueryAlias
         |) queryAlias LIMIT 200
         |
       """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("generating hive query with outer group by,  Noop rollup cols") {
    val jsonString =
      s"""{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Campaign Name"},
                              {"field": "Clicks"},
                              {"field": "Impressions"},
                              {"field": "Click Rate"}
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

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryChain = queryPipelineTry.toOption.get.queryChain

    val result =  queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(queryChain.drivingQuery.queryGenVersion.isDefined)
    assert(queryChain.drivingQuery.queryGenVersion.get == Version.v2)

    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(mang_campaign_name,'') AS STRING),CAST(NVL(mang_clicks,'') AS STRING),CAST(NVL(mang_impressions,'') AS STRING),CAST(NVL(mang_click_rate,'') AS STRING))
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, clicks AS mang_clicks, impressions AS mang_impressions, mang_click_rate AS mang_click_rate
         |FROM(
         |SELECT getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)) mang_campaign_name, SUM(clicks) AS clicks, SUM(impressions) AS impressions, (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(clicks) / (SUM(impressions)) END) AS mang_click_rate
         |FROM(SELECT campaign_id, SUM(clicks) clicks, SUM(impressions) impressions
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
         |GROUP BY getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING))
         |) OgbQueryAlias
         |) queryAlias LIMIT 200
       """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }


  test("generating hive query with outer group by, derived cols") {
    val jsonString =
      s"""{
                          "cube": "performance_stats",
                          "selectFields": [
                              {"field": "Campaign Name"},
                              {"field": "N Clicks"},
                              {"field": "Pricing Type"},
                              {"field": "Static Mapping String"},
                              {"field": "Impressions"},
                              {"field": "N Spend"},
                              {"field": "Month"},
                              {"field": "Clicks"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                          {"field": "Campaign Name", "order": "Desc"},  {"field": "Impressions", "order": "DESC"}]
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryChain = queryPipelineTry.toOption.get.queryChain

    val result =  queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(queryChain.drivingQuery.queryGenVersion.isDefined)
    assert(queryChain.drivingQuery.queryGenVersion.get == Version.v2)

    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(mang_campaign_name,'') AS STRING),CAST(NVL(mang_n_clicks,'') AS STRING),CAST(NVL(mang_pricing_type,'') AS STRING),CAST(NVL(mang_static_mapping_string,'') AS STRING),CAST(NVL(mang_impressions,'') AS STRING),CAST(NVL(mang_n_spend,'') AS STRING),CAST(NVL(mang_month,'') AS STRING),CAST(NVL(mang_clicks,'') AS STRING))
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, decodeUDF(stats_source, 1, clicks, 0.0) AS mang_n_clicks, mang_pricing_type AS mang_pricing_type, mang_static_mapping_string AS mang_static_mapping_string, impressions AS mang_impressions, decodeUDF(stats_source, 1, spend, 0.0) AS mang_n_spend, mang_month, clicks AS mang_clicks
         |FROM(
         |SELECT getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)) mang_campaign_name, CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END mang_pricing_type, CASE WHEN (sm_string IN ('Y')) THEN 'Yes' WHEN (sm_string IN ('N')) THEN 'No' ELSE 'NA' END mang_static_mapping_string, SUM(impressions) AS impressions, getFormattedDate(mang_month) mang_month, SUM(clicks) AS clicks, COALESCE(stats_source, 0L) stats_source, SUM(spend) AS spend
         |FROM(SELECT sm_string, price_type, campaign_id, dateUDF(stats_date, 'M') mang_month, SUM(clicks) clicks, SUM(impressions) impressions, stats_source, SUM(spend) spend
         |FROM ad_fact1
         |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY sm_string, price_type, campaign_id, dateUDF(stats_date, 'M'), stats_source
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
         |GROUP BY getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)), CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END, CASE WHEN (sm_string IN ('Y')) THEN 'Yes' WHEN (sm_string IN ('N')) THEN 'No' ELSE 'NA' END, getFormattedDate(mang_month), COALESCE(stats_source, 0L)
         |ORDER BY mang_campaign_name DESC, impressions DESC) OgbQueryAlias
         |) queryAlias LIMIT 200
         |
       """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("generating hive query with outer group by, having and partitioning scheme, test part col renderer") {
    val jsonString =
      s"""{
                          "cube": "bid_reco",
                          "selectFields": [
                              {"field": "Campaign Name"},
                              {"field": "Bid Strategy"},
                              {"field": "Advertiser Name"},
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Test Constant Col"},
                              {"field": "Load Time"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Impressions", "operator": ">", "value": "1608"}
                          ],
                          "sortBy": [
                             {"field": "Campaign Name", "order": "Desc"},  {"field": "Impressions", "order": "DESC"}]
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryChain = queryPipelineTry.toOption.get.queryChain

    val result =  queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(queryChain.drivingQuery.queryGenVersion.isDefined)
    assert(queryChain.drivingQuery.queryGenVersion.get == Version.v2)

    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(mang_campaign_name,'') AS STRING),CAST(NVL(mang_bid_strategy,'') AS STRING),CAST(NVL(mang_advertiser_name,'') AS STRING),CAST(NVL(advertiser_id,'') AS STRING),CAST(NVL(mang_impressions,'') AS STRING),CAST(NVL(mang_test_constant_col,'') AS STRING),CAST(NVL(mang_load_time,'') AS STRING))
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, mang_bid_strategy AS mang_bid_strategy, mang_advertiser_name AS mang_advertiser_name, advertiser_id AS advertiser_id, actual_impressions AS mang_impressions, mang_test_constant_col AS mang_test_constant_col, mang_load_time
         |FROM(
         |SELECT getCsvEscapedString(CAST(NVL(c2.mang_campaign_name, '') AS STRING)) mang_campaign_name, CASE WHEN (bid_strategy IN (1)) THEN 'Max Click' WHEN (bid_strategy IN (2)) THEN 'Inflection Point' ELSE 'NONE' END mang_bid_strategy, COALESCE(a1.mang_advertiser_name, 'NA') mang_advertiser_name, COALESCE(c2.advertiser_id, 0L) advertiser_id, SUM(actual_impressions) AS actual_impressions, ROUND(COALESCE(test_constant_col, 0L), 10) mang_test_constant_col, COALESCE(mang_load_time, 'NA') mang_load_time
         |FROM(SELECT bid_strategy, account_id, load_time, 'test_constant_col_value' AS test_constant_col, campaign_id, SUM(actual_impressions) actual_impressions
         |FROM bidreco_complete
         |WHERE (account_id = 12345) AND (status = 'Valid') AND (factPartCol = 123)
         |GROUP BY bid_strategy, account_id, load_time, test_constant_col, campaign_id
         |HAVING (SUM(actual_impressions) > 1608)
         |       )
         |bc0
         |LEFT OUTER JOIN (
         |SELECT name AS mang_advertiser_name, id a1_id
         |FROM advertiser_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
         |)
         |a1
         |ON
         |bc0.account_id = a1.a1_id
         |       LEFT OUTER JOIN (
         |SELECT advertiser_id AS advertiser_id, campaign_name AS mang_campaign_name, id c2_id
         |FROM campaing_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c2
         |ON
         |bc0.campaign_id = c2.c2_id
         |
         |GROUP BY getCsvEscapedString(CAST(NVL(c2.mang_campaign_name, '') AS STRING)), CASE WHEN (bid_strategy IN (1)) THEN 'Max Click' WHEN (bid_strategy IN (2)) THEN 'Inflection Point' ELSE 'NONE' END, COALESCE(a1.mang_advertiser_name, 'NA'), COALESCE(c2.advertiser_id, 0L), ROUND(COALESCE(test_constant_col, 0L), 10), COALESCE(mang_load_time, 'NA')
         |ORDER BY mang_campaign_name DESC, actual_impressions DESC) OgbQueryAlias
         |) queryAlias LIMIT 200
       """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("generating hive query with outer group by, count dims") {
    val jsonString =
      s"""{
                          "cube": "s_stats",
                          "selectFields": [
                              {"field": "Campaign Name"},
                              {"field": "Impressions"},
                              {"field": "Keyword Count"}
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

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryChain = queryPipelineTry.toOption.get.queryChain

    val result =  queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(queryChain.drivingQuery.queryGenVersion.isDefined)
    assert(queryChain.drivingQuery.queryGenVersion.get == Version.v2)

    assert(result.contains(s"""(COUNT(distinct keyword_id)) mang_keyword_count"""))
  }

  test("Test constant requested fields with Outer Group By") {
    val jsonString =
      s"""{
              "cube" : "s_stats",
              "selectFields" : [
                  { "field" : "Day" },
                  { "field" : "Advertiser ID" },
                  { "field" : "Campaign Name" },
                  { "field" : "Impressions" },
                  { "field" : "Source", "value" : "2", "alias" : "Source"}
              ],
              "filterExpressions":[
                  { "field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate" },
                  { "field":"Advertiser ID", "operator":"=", "value":"12345" }
              ],
              "sortBy": [
                  { "field": "Impressions", "order": "Asc" }
              ],
              "paginationStartIndex":0,
              "rowsPerPage":100
      }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryChain = queryPipelineTry.toOption.get.queryChain

    val result =  queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(result.contains("'2' AS mang_source"), "No constant field in outer columns")
  }

  test("NoopRollup columns in Outer Group By should be rendered correctly") {
    val jsonString =
      s"""{
                          "cube": "bid_reco",
                          "selectFields": [
                              {"field": "Campaign Name"},
                              {"field": "Advertiser Name"},
                              {"field": "Advertiser ID"},
                              {"field": "Bid Strategy"},
                              {"field": "Impressions"},
                              {"field": "Clicks"},
                              {"field": "Bid Modifier Fact"},
                              {"field": "Modified Bid Fact"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Impressions", "operator": ">", "value": "1608"}
                          ],
                          "sortBy": [
                             {"field": "Campaign Name", "order": "Desc"},  {"field": "Impressions", "order": "DESC"}]
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryChain = queryPipelineTry.toOption.get.queryChain

    val result =  queryChain.drivingQuery.asInstanceOf[HiveQuery].asString


    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(mang_campaign_name,'') AS STRING),CAST(NVL(mang_advertiser_name,'') AS STRING),CAST(NVL(advertiser_id,'') AS STRING),CAST(NVL(mang_bid_strategy,'') AS STRING),CAST(NVL(mang_impressions,'') AS STRING),CAST(NVL(mang_clicks,'') AS STRING),CAST(NVL(mang_bid_modifier_fact,'') AS STRING),CAST(NVL(mang_modified_bid_fact,'') AS STRING))
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, mang_advertiser_name AS mang_advertiser_name, advertiser_id AS advertiser_id, mang_bid_strategy AS mang_bid_strategy, actual_impressions AS mang_impressions, actual_clicks AS mang_clicks, mang_bid_modifier_fact AS mang_bid_modifier_fact, mang_modified_bid_fact AS mang_modified_bid_fact
         |FROM(
         |SELECT getCsvEscapedString(CAST(NVL(c2.mang_campaign_name, '') AS STRING)) mang_campaign_name, COALESCE(a1.mang_advertiser_name, 'NA') mang_advertiser_name, COALESCE(c2.advertiser_id, 0L) advertiser_id, CASE WHEN (bid_strategy IN (1)) THEN 'Max Click' WHEN (bid_strategy IN (2)) THEN 'Inflection Point' ELSE 'NONE' END mang_bid_strategy, SUM(actual_impressions) AS actual_impressions, SUM(actual_clicks) AS actual_clicks, SUM(recommended_bid) AS recommended_bid, (coalesce(CASE WHEN (getAbyB(recommended_bid, actual_clicks)) = 0 THEN 1 WHEN IS_NAN((getAbyB(recommended_bid, actual_clicks))) THEN 1 ELSE (getAbyB(recommended_bid, actual_clicks)) END, 1)) AS mang_bid_modifier_fact, SUM(coalesce(CASE WHEN coalesce(CASE WHEN (getAbyB(recommended_bid, actual_clicks)) = 0 THEN 1 WHEN IS_NAN((getAbyB(recommended_bid, actual_clicks))) THEN 1 ELSE (getAbyB(recommended_bid, actual_clicks)) END, 1) = 0 THEN 1 WHEN IS_NAN(coalesce(CASE WHEN (getAbyB(recommended_bid, actual_clicks)) = 0 THEN 1 WHEN IS_NAN((getAbyB(recommended_bid, actual_clicks))) THEN 1 ELSE (getAbyB(recommended_bid, actual_clicks)) END, 1)) THEN 1 ELSE coalesce(CASE WHEN (getAbyB(recommended_bid, actual_clicks)) = 0 THEN 1 WHEN IS_NAN((getAbyB(recommended_bid, actual_clicks))) THEN 1 ELSE (getAbyB(recommended_bid, actual_clicks)) END, 1) END, 1) * (getAbyB(recommended_bid, actual_impressions))) AS mang_modified_bid_fact
         |FROM(SELECT bid_strategy, account_id, campaign_id, SUM(actual_clicks) actual_clicks, SUM(actual_impressions) actual_impressions, SUM(recommended_bid) recommended_bid
         |FROM bidreco_complete
         |WHERE (account_id = 12345) AND (status = 'Valid') AND (factPartCol = 123)
         |GROUP BY bid_strategy, account_id, campaign_id
         |HAVING (SUM(actual_impressions) > 1608)
         |       )
         |bc0
         |LEFT OUTER JOIN (
         |SELECT name AS mang_advertiser_name, id a1_id
         |FROM advertiser_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
         |)
         |a1
         |ON
         |bc0.account_id = a1.a1_id
         |       LEFT OUTER JOIN (
         |SELECT advertiser_id AS advertiser_id, campaign_name AS mang_campaign_name, id c2_id
         |FROM campaing_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c2
         |ON
         |bc0.campaign_id = c2.c2_id
         |
         |GROUP BY getCsvEscapedString(CAST(NVL(c2.mang_campaign_name, '') AS STRING)), COALESCE(a1.mang_advertiser_name, 'NA'), COALESCE(c2.advertiser_id, 0L), CASE WHEN (bid_strategy IN (1)) THEN 'Max Click' WHEN (bid_strategy IN (2)) THEN 'Inflection Point' ELSE 'NONE' END
         |ORDER BY mang_campaign_name DESC, actual_impressions DESC) OgbQueryAlias
         |) queryAlias LIMIT 200
       """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("generating hive query with dim non id field as filter") {
    val jsonString =
      s"""{
                          "cube": "s_stats",
                          "selectFields": [
                              {"field": "Average CPC"},
                              {"field": "Average Position"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Campaign Name", "operator": "=", "value": "cmpgn_1"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ]
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryChain = queryPipelineTry.toOption.get.queryChain

    val result =  queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(queryChain.drivingQuery.queryGenVersion.isDefined)
    assert(queryChain.drivingQuery.queryGenVersion.get == Version.v2)

    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(mang_average_cpc,'') AS STRING),CAST(NVL(mang_average_position,'') AS STRING),CAST(NVL(mang_impressions,'') AS STRING))
         |FROM(
         |SELECT CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END AS mang_average_cpc, avg_pos AS mang_average_position, impressions AS mang_impressions
         |FROM(
         |SELECT (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(weighted_position * impressions) / (SUM(impressions)) END) AS avg_pos, SUM(impressions) AS impressions, SUM(clicks) AS clicks, SUM(spend) AS spend, SUM(weighted_position) AS weighted_position
         |FROM(SELECT campaign_id, SUM(impressions) impressions, SUM(clicks) clicks, SUM(spend) spend, SUM(weighted_position) weighted_position
         |FROM s_stats_fact
         |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY campaign_id
         |
 |       )
         |ssf0
         |JOIN (
         |SELECT id c1_id
         |FROM campaing_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345) AND (lower(campaign_name) = lower('cmpgn_1'))
         |)
         |c1
         |ON
         |ssf0.campaign_id = c1.c1_id
         |
         |
 |) OgbQueryAlias
         |) queryAlias LIMIT 200
       """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
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


    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
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
}
