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
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
  }

  test("generating hive query with custom rollups") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_query_generator_test_custom_rollups.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))


    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

  }

  test("user stats hourly") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "user_stats_hourly.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
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
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(result.contains("getFormattedDate"), "Date type columms should be wrapped in getFormattedDate udf")
  }

  test("Should escape string if escaping is required") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_request_with_escaping_required_field.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(result.contains("getCsvEscapedString"), "Should escape string if escaping is required")
  }

  test("Hive query should contain UDF definitions ") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_query_generator_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery]
    assert(query.udfStatements.nonEmpty, "Hive Query should contain udf statements")
  }

  test("should support execution parameters") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_query_generator_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery]
  }

  test("Concatenated cols should be wrapped in CONCAT_WS()") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_query_generator_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(result.contains("CONCAT_WS"), "Concatenated cols should be wrapped in CONCAT_WS()")
  }

  test("Should mangle non-id column aliases") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_query_generator_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
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
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
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
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(result.contains("CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(CASE WHEN ((avg_pos >= 0.1) AND (avg_pos <= 500)) THEN avg_pos ELSE 0.0 END * impressions) / (SUM(impressions)) END) mang_average_position"), "Should support “NA” for NULL string")
  }

  test("Query with constant requested fields should have constant columns") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_request_with_constant_field.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(result.contains("NVL(mang_source, '')"), "No constant field in concatenated columns")
    assert(result.contains("'2' mang_source"), "No constant field in outer columns")
  }

  test("Fact group by clause should only include dim columns") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_request_with_fact_n_factder_field.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(result.contains("GROUP BY landing_page_url, stats_date\n"), "Group by should only include dim columns")

  }

  test("Hive multi dimensional query") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_request_with_multiple_dimension.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

  }

  test("DateTime type columns should be rendered with getDateTimeFromEpoch udf in outer select") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "ce_stats.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(result.contains("getDateTimeFromEpoch"), "Date type columms should be wrapped in getFormattedDate udf")
  }

  test("test NoopRollup expression for generated query") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_query_nooprollup_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(mang_day, ''), NVL(advertiser_id, ''), NVL(campaign_id, ''), NVL(ad_group_id, ''), NVL(keyword_id, ''), NVL(mang_keyword, ''), NVL(mang_search_term, ''), NVL(mang_delivered_match_type, ''), NVL(mang_impressions, ''), NVL(mang_average_cpc, ''))
         |FROM(
         |SELECT getFormattedDate(stats_date) mang_day, CAST(COALESCE(account_id, 0L) as STRING) advertiser_id, CAST(COALESCE(campaign_id, 0L) as STRING) campaign_id, CAST(COALESCE(ad_group_id, 0L) as STRING) ad_group_id, CAST(COALESCE(keyword_id, 0L) as STRING) keyword_id, getCsvEscapedString(CAST(NVL(keyword, '') AS STRING)) mang_keyword, COALESCE(search_term, "None") mang_search_term, CAST(COALESCE(delivered_match_type, 0L) as STRING) mang_delivered_match_type, CAST(COALESCE(impressions, 0L) as STRING) mang_impressions, CAST(ROUND(COALESCE((CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END), 0L), 10) as STRING) mang_average_cpc
         |FROM(SELECT CASE WHEN (delivered_match_type IN (1)) THEN 'Exact' WHEN (delivered_match_type IN (2)) THEN 'Broad' WHEN (delivered_match_type IN (3)) THEN 'Phrase' ELSE 'UNKNOWN' END delivered_match_type, stats_date, keyword, ad_group_id, search_term, account_id, campaign_id, keyword_id, SUM(impressions) impressions, SUM(clicks) clicks, SUM(spend) spend
         |FROM s_stats_fact
         |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY CASE WHEN (delivered_match_type IN (1)) THEN 'Exact' WHEN (delivered_match_type IN (2)) THEN 'Broad' WHEN (delivered_match_type IN (3)) THEN 'Phrase' ELSE 'UNKNOWN' END, stats_date, keyword, ad_group_id, search_term, account_id, campaign_id, keyword_id
         |
          |       )
         |ssf0
         |)
         |
        """.stripMargin


    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("test joinType for non fk dim filters for generated query") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_query_with_non_fk_dim_filters_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    assert(requestModel.toOption.get.anyDimHasNonFKNonForceFilter == true)

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    assert(result.contains("ssf0\nJOIN ("))
  }

  test("test joinType for fact driven queries generated query") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_query_with_wo_non_fk_dim_filters_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    assert(requestModel.toOption.get.anyDimHasNonFKNonForceFilter == false)

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    assert(result.contains("LEFT OUTER JOIN"))
  }

  test("test like filter with hive query gem") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_query_with_dim_like_filter.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    assert(result.contains("(lower(campaign_name) LIKE lower('%yahoo%'))"))
    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(mang_day, ''), NVL(advertiser_id, ''), NVL(campaign_id, ''), NVL(mang_campaign_name, ''), NVL(ad_group_id, ''), NVL(keyword_id, ''), NVL(mang_keyword, ''), NVL(mang_search_term, ''), NVL(mang_delivered_match_type, ''), NVL(mang_impressions, ''), NVL(mang_average_cpc_cents, ''), NVL(mang_average_cpc, ''))
         |FROM(
         |SELECT getFormattedDate(stats_date) mang_day, CAST(COALESCE(account_id, 0L) as STRING) advertiser_id, CAST(COALESCE(ssf0.campaign_id, 0L) as STRING) campaign_id, getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)) mang_campaign_name, CAST(COALESCE(ad_group_id, 0L) as STRING) ad_group_id, CAST(COALESCE(keyword_id, 0L) as STRING) keyword_id, getCsvEscapedString(CAST(NVL(keyword, '') AS STRING)) mang_keyword, COALESCE(search_term, "None") mang_search_term, CAST(COALESCE(delivered_match_type, 0L) as STRING) mang_delivered_match_type, CAST(COALESCE(impressions, 0L) as STRING) mang_impressions, CAST(ROUND(COALESCE((mang_average_cpc * 100), 0L), 10) as STRING) mang_average_cpc_cents, CAST(ROUND(COALESCE(mang_average_cpc, 0L), 10) as STRING) mang_average_cpc
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
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
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
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
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
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
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
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(device_id, ''), NVL(advertiser_id, ''), NVL(mang_impressions, ''), NVL(mang_pricing_type, ''), NVL(network_id, ''))
         |FROM(
         |SELECT CAST(COALESCE(device_id, 0L) as STRING) device_id, CAST(COALESCE(account_id, 0L) as STRING) advertiser_id, CAST(COALESCE(impressions, 0L) as STRING) mang_impressions, CAST(COALESCE(price_type, 0L) as STRING) mang_pricing_type, COALESCE(network_type, "NA") network_id
         |FROM(SELECT decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE') network_type, CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END price_type, account_id, CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END device_id, SUM(impressions) impressions
         |FROM s_stats_fact
         |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE'), CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END, account_id, CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END
         |
         |       )
         |ssf0
         |)
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
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(device_id, ''), NVL(advertiser_id, ''), NVL(mang_impressions, ''), NVL(mang_pricing_type, ''), NVL(network_id, ''))
         |FROM(
         |SELECT CAST(COALESCE(device_id, 0L) as STRING) device_id, CAST(COALESCE(account_id, 0L) as STRING) advertiser_id, CAST(COALESCE(impressions, 0L) as STRING) mang_impressions, CAST(COALESCE(price_type, 0L) as STRING) mang_pricing_type, COALESCE(network_type, "NA") network_id
         |FROM(SELECT CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END device_id, decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE') network_type, CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END price_type, account_id, SUM(impressions) impressions
         |FROM s_stats_fact
         |WHERE (account_id = 12345) AND (stats_source = 2) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END, decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE'), CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END, account_id
         |
         |       )
         |ssf0
         |)
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
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))


    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    val expected =
      s"""SELECT CONCAT_WS(",",NVL(advertiser_id, ''), NVL(mang_impressions, ''))
          FROM(
            SELECT CAST(COALESCE(account_id, 0L) as STRING) advertiser_id, CAST(COALESCE(impressions, 0L) as STRING) mang_impressions
            FROM(SELECT account_id, SUM(impressions) impressions
            FROM s_stats_fact
            WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
            GROUP BY account_id
            HAVING (SUM(impressions) > 1608)
              )
          ssf0
      )""".stripMargin

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
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))


    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    val expected =
      s"""SELECT CONCAT_WS(",",NVL(advertiser_id, ''), NVL(mang_impressions, ''))
          FROM(
            SELECT CAST(COALESCE(account_id, 0L) as STRING) advertiser_id, CAST(COALESCE(impressions, 0L) as STRING) mang_impressions
            FROM(SELECT account_id, SUM(impressions) impressions
            FROM s_stats_fact
            WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
            GROUP BY account_id
            HAVING (SUM(impressions) < 1608)
              )
          ssf0
      )""".stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)

  }

  def generateHiveQuery(requestJson: String): String = {
    val requestRaw = ReportingRequest.deserializeAsync(requestJson.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    val registry = defaultRegistry
    val request = ReportingRequest.forceHive(requestRaw.toOption.get)
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineV2(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    println(result)
    result
  }
}
