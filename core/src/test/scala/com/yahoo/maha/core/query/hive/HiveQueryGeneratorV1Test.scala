// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.hive

import java.nio.charset.StandardCharsets

import com.yahoo.maha.core.CoreSchema.AdvertiserSchema
import com.yahoo.maha.core._
import com.yahoo.maha.core.query.{QueryGeneratorRegistry, _}
import com.yahoo.maha.core.registry.Registry
import com.yahoo.maha.core.request.ReportingRequest
import org.mockito.Matchers._
import org.mockito.Mockito._

import scala.util.Try

/**
 * Created by shengyao on 12/21/15.
  *
  * Verification tests for old hive outer group by tests running against new hive OGB query generator v2
 */
class HiveQueryGeneratorV1Test extends BaseHiveQueryGeneratorTest {

  val defaultRegistry = getDefaultRegistry()

  require(defaultRegistry.factMap.nonEmpty)

  test("registering Hive query generation multiple times should fail") {
    intercept[IllegalArgumentException] {
      val dummyQueryGenerator = new QueryGenerator[WithHiveEngine] {
        override def generate(queryContext: QueryContext): Query = {
          null
        }

        override def engine: Engine = HiveEngine
      }

      val queryGeneratorRegistryTest = new QueryGeneratorRegistry
      queryGeneratorRegistryTest.register(HiveEngine, dummyQueryGenerator, Version.v2)
      HiveQueryGeneratorV2.register(queryGeneratorRegistryTest, DefaultPartitionColumnRenderer, TestUDFRegistrationFactory())
    }
  }

  test("test register with query generator for a different engine") {
    intercept[IllegalArgumentException] {
      val queryGeneratorRegistryTest = new QueryGeneratorRegistry
      val dummyOracleQueryGenerator = new QueryGenerator[WithOracleEngine] {
        override def generate(queryContext: QueryContext): Query = {
          null
        }

        override def engine: Engine = OracleEngine
      }
      queryGeneratorRegistryTest.register(HiveEngine, dummyOracleQueryGenerator, Version.v2)
      HiveQueryGeneratorV2.register(queryGeneratorRegistryTest, DefaultPartitionColumnRenderer, TestUDFRegistrationFactory())
    }
  }

  test("Invalid query context") {
    intercept[UnsupportedOperationException] {
      val hiveQueryGeneratorV2 = new HiveQueryGeneratorV2(DefaultPartitionColumnRenderer, TestUDFRegistrationFactory())
      val queryContext = mock(classOf[QueryContext])
      hiveQueryGeneratorV2.generate(queryContext)
    }
  }

  test("generating hive query") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_query_generator_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))


    val resultQuery = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery]
    assert(Version.v2.equals(resultQuery.queryGenVersion.get), "Expected v2 queryGenVersion")

  }

  test("generating hive query with custom rollups") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_query_generator_test_custom_rollups.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
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

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
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

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
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

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
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

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
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

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
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

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
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

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
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

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString



    val expected =
      s"""
        |SELECT mang_day AS "Day", mang_destination_url AS "Destination URL", mang_advertiser_status AS "Advertiser Status", impressions AS "Impressions"
        |FROM(
        |SELECT getFormattedDate(stats_date) mang_day, getCsvEscapedString(CAST(NVL(landing_page_url, '') AS STRING)) mang_destination_url, COALESCE(a1.mang_advertiser_status, "NA") mang_advertiser_status, SUM(impressions) AS impressions
        |FROM(SELECT account_id, landing_page_url, stats_date, SUM(impressions) impressions
        |FROM s_stats_fact_underlying
        |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
        |GROUP BY account_id, landing_page_url, stats_date
        |
        |       )
        |ssfu0
        |LEFT OUTER JOIN (
        |SELECT decodeUDF(status, 'ON', 'ON', 'OFF') AS mang_advertiser_status, id a1_id
        |FROM advertiser_hive
        |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
        |)
        |a1
        |ON
        |ssfu0.account_id = a1.a1_id
        |
        |GROUP BY getFormattedDate(stats_date), getCsvEscapedString(CAST(NVL(landing_page_url, '') AS STRING)), COALESCE(a1.mang_advertiser_status, "NA")
        |ORDER BY impressions ASC)
        |
      """.stripMargin

  }

  test("Query with request DecType fields that contains max and min should return query with max and min range") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_request_with_min_max_dec_field.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
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

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(result.contains("NVL(CAST(mang_source AS STRING), ''))"), "No constant field in concatenated columns")
    assert(result.contains("'2' mang_source"), "No constant field in outer columns")
  }

  test("Fact group by clause should only include dim columns") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_request_with_fact_n_factder_field.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
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

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
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

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
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

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(CAST(mang_day AS STRING), ''), NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(campaign_id AS STRING), ''), NVL(CAST(ad_group_id AS STRING), ''), NVL(CAST(keyword_id AS STRING), ''), NVL(CAST(mang_keyword AS STRING), ''), NVL(CAST(mang_ad_format_name AS STRING), ''), NVL(CAST(mang_ad_format_sub_type AS STRING), ''), NVL(CAST(mang_search_term AS STRING), ''), NVL(CAST(mang_delivered_match_type AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''), NVL(CAST(mang_average_cpc AS STRING), ''))
         |FROM(
         |SELECT getFormattedDate(stats_date) mang_day, COALESCE(account_id, 0L) advertiser_id, COALESCE(campaign_id, 0L) campaign_id, COALESCE(ad_group_id, 0L) ad_group_id, COALESCE(keyword_id, 0L) keyword_id, getCsvEscapedString(CAST(NVL(keyword, '') AS STRING)) mang_keyword, COALESCE(ad_format_id, 0L) mang_ad_format_name, COALESCE(ad_format_sub_type, 0L) mang_ad_format_sub_type, COALESCE(search_term, 'None') mang_search_term, COALESCE(delivered_match_type, 0L) mang_delivered_match_type, COALESCE(impressions, 1L) mang_impressions, ROUND(COALESCE((CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END), 0L), 10) mang_average_cpc
         |FROM(SELECT CASE WHEN (delivered_match_type IN (1)) THEN 'Exact' WHEN (delivered_match_type IN (2)) THEN 'Broad' WHEN (delivered_match_type IN (3)) THEN 'Phrase' ELSE 'UNKNOWN' END delivered_match_type, stats_date, keyword, ad_group_id, search_term, account_id, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (5)) THEN 'Single image' WHEN (ad_format_id IN (6)) THEN 'Single image' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (9)) THEN 'Carousel' WHEN (ad_format_id IN (2)) THEN 'Single image' WHEN (ad_format_id IN (7)) THEN 'Video' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (3)) THEN 'Single image' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (8)) THEN 'Video with HTML Endcard' WHEN (ad_format_id IN (4)) THEN 'Single image' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'Other' END ad_format_id, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'N/A' END ad_format_sub_type, campaign_id, keyword_id, SUM(impressions) impressions, SUM(clicks) clicks, SUM(spend) spend
         |FROM s_stats_fact_underlying
         |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY CASE WHEN (delivered_match_type IN (1)) THEN 'Exact' WHEN (delivered_match_type IN (2)) THEN 'Broad' WHEN (delivered_match_type IN (3)) THEN 'Phrase' ELSE 'UNKNOWN' END, stats_date, keyword, ad_group_id, search_term, account_id, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (5)) THEN 'Single image' WHEN (ad_format_id IN (6)) THEN 'Single image' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (9)) THEN 'Carousel' WHEN (ad_format_id IN (2)) THEN 'Single image' WHEN (ad_format_id IN (7)) THEN 'Video' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (3)) THEN 'Single image' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (8)) THEN 'Video with HTML Endcard' WHEN (ad_format_id IN (4)) THEN 'Single image' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'Other' END, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'N/A' END, campaign_id, keyword_id
         |
 |       )
         |ssfu0
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

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    assert(result.contains("ssfu0\nJOIN ("))
  }

  test("test joinType for fact driven queries generated query") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_query_with_wo_non_fk_dim_filters_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    assert(requestModel.toOption.get.anyDimHasNonFKNonForceFilter == false)

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
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

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString


    assert(result.contains("(lower(campaign_name) LIKE lower('%yahoo%'))"))
    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(CAST(mang_day AS STRING), ''), NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(campaign_id AS STRING), ''), NVL(CAST(mang_campaign_name AS STRING), ''), NVL(CAST(ad_group_id AS STRING), ''), NVL(CAST(keyword_id AS STRING), ''), NVL(CAST(mang_keyword AS STRING), ''), NVL(CAST(mang_search_term AS STRING), ''), NVL(CAST(mang_delivered_match_type AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''), NVL(CAST(mang_average_cpc_cents AS STRING), ''), NVL(CAST(mang_average_cpc AS STRING), ''))
         |FROM(
         |SELECT getFormattedDate(stats_date) mang_day, COALESCE(account_id, 0L) advertiser_id, COALESCE(ssfu0.campaign_id, 0L) campaign_id, getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)) mang_campaign_name, COALESCE(ad_group_id, 0L) ad_group_id, COALESCE(keyword_id, 0L) keyword_id, getCsvEscapedString(CAST(NVL(keyword, '') AS STRING)) mang_keyword, COALESCE(search_term, 'None') mang_search_term, COALESCE(delivered_match_type, 0L) mang_delivered_match_type, COALESCE(impressions, 1L) mang_impressions, ROUND(COALESCE((mang_average_cpc * 100), 0L), 10) mang_average_cpc_cents, ROUND(COALESCE(mang_average_cpc, 0L), 10) mang_average_cpc
         |FROM(SELECT CASE WHEN (delivered_match_type IN (1)) THEN 'Exact' WHEN (delivered_match_type IN (2)) THEN 'Broad' WHEN (delivered_match_type IN (3)) THEN 'Phrase' ELSE 'UNKNOWN' END delivered_match_type, stats_date, keyword, ad_group_id, search_term, account_id, campaign_id, keyword_id, SUM(impressions) impressions, (CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END) mang_average_cpc
         |FROM s_stats_fact_underlying
         |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY CASE WHEN (delivered_match_type IN (1)) THEN 'Exact' WHEN (delivered_match_type IN (2)) THEN 'Broad' WHEN (delivered_match_type IN (3)) THEN 'Phrase' ELSE 'UNKNOWN' END, stats_date, keyword, ad_group_id, search_term, account_id, campaign_id, keyword_id
         |
         |       )
         |ssfu0
         |JOIN (
         |SELECT campaign_name AS mang_campaign_name, id c1_id
         |FROM campaing_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345) AND (lower(campaign_name) LIKE lower('%yahoo%'))
         |)
         |c1
         |ON
         |ssfu0.campaign_id = c1.c1_id
         |
         |ORDER BY mang_impressions ASC)
         |
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

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
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

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString


    assert(result.contains("GROUP BY modified_bid, CASE WHEN (bid_strategy IN (1)) THEN 'Max Click' WHEN (bid_strategy IN (2)) THEN 'Inflection Point' ELSE 'NONE' END, ad_group_id, account_id, campaign_id, current_bid, (modified_bid - current_bid) / current_bid * 100"))
  }

  test("HiveQueryGeneratorv2Test: should fail to generate Hive query for Outer Filters") {
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

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isFailure, queryPipelineTry.errorMessage("Outer Filters on Hive Query should fail to get the query pipeline"))
    assert(queryPipelineTry.failed.get.getMessage.contains("Failed to find best candidate"))
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


    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(CAST(device_id AS STRING), ''), NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''), NVL(CAST(mang_pricing_type AS STRING), ''), NVL(CAST(network_id AS STRING), ''))
         |FROM(
         |SELECT COALESCE(device_id, 0L) device_id, COALESCE(account_id, 0L) advertiser_id, COALESCE(impressions, 1L) mang_impressions, COALESCE(price_type, 0L) mang_pricing_type, COALESCE(network_type, 'NA') network_id
         |FROM(SELECT decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE') network_type, CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END price_type, account_id, CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END device_id, SUM(impressions) impressions
         |FROM s_stats_fact_underlying
         |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE'), CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END, account_id, CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END
         |
         |       )
         |ssfu0
         |) queryAlias LIMIT 200
      """.stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Query containing fields with MAX rollup should generate successfully") {
    val jsonString =
      s"""{
                          "cube": "s_stats",
                          "selectFields": [
                              {"field": "Device ID"},
                              {"field": "Campaign Name"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"},
                              {"field": "Max Price Type"}
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

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, "Querypipeline containing fields with MAX rollup should generate successfully")
    //println(queryPipelineTry.get.queryChain.drivingQuery.asString)
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


    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(CAST(device_id AS STRING), ''), NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''), NVL(CAST(mang_pricing_type AS STRING), ''), NVL(CAST(network_id AS STRING), ''))
         |FROM(
         |SELECT COALESCE(device_id, 0L) device_id, COALESCE(account_id, 0L) advertiser_id, COALESCE(impressions, 1L) mang_impressions, COALESCE(price_type, 0L) mang_pricing_type, COALESCE(network_type, 'NA') network_id
         |FROM(SELECT CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END device_id, decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE') network_type, CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END price_type, account_id, SUM(impressions) impressions
         |FROM s_stats_fact
         |WHERE (account_id = 12345) AND (stats_source = 2) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END, decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE'), CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END, account_id
         |
         |       )
         |ssf0
         |)  queryAlias LIMIT 200
      """.stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }
*/
  test("fact only query context should be switched to CombinedQueryContext") {
    val hiveQueryGeneratorV2  = spy(new HiveQueryGeneratorV2(DefaultPartitionColumnRenderer, TestUDFRegistrationFactory()))
    val queryContext = mock(classOf[FactQueryContext])
    try {
      hiveQueryGeneratorV2.generate(queryContext)
    } catch {
      case e: Exception => // Ignore
    }

    verify(hiveQueryGeneratorV2).generate(any(classOf[CombinedQueryContext]))
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

    HiveQueryGeneratorV2.register(failRegistry, DefaultPartitionColumnRenderer, TestUDFRegistrationFactory())
  }


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

    val result = generateHiveQuery(jsonString, defaultRegistry)

    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(mang_campaign_name,'') AS STRING),CAST(NVL(mang_spend,'') AS STRING))
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, spend AS mang_spend
         |FROM(
         |SELECT getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)) mang_campaign_name, SUM(spend) AS spend
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
         |GROUP BY getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING))
         |) OgbQueryAlias
         |) queryAlias LIMIT 200
         """.stripMargin
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

    val result = generateHiveQuery(jsonString, defaultRegistry)
    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(mang_campaign_name,'') AS STRING),CAST(NVL(mang_source,'') AS STRING),CAST(NVL(mang_n_spend,'') AS STRING))
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, mang_source AS mang_source, decodeUDF(stats_source, 1, spend, 0.0) AS mang_n_spend
         |FROM(
         |SELECT getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)) mang_campaign_name, COALESCE(stats_source, 0L) mang_source, SUM(spend) AS spend, stats_source AS stats_source
         |FROM(SELECT campaign_id, stats_source, SUM(spend) spend
         |FROM ad_fact1
         |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY campaign_id, stats_source
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
         |GROUP BY getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)), COALESCE(stats_source, 0L), stats_source
         |) OgbQueryAlias
         |) queryAlias LIMIT 200
         |
       """.stripMargin
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
    val result = generateHiveQuery(jsonString, defaultRegistry)
    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(mang_day,'') AS STRING),CAST(NVL(mang_campaign_name,'') AS STRING),CAST(NVL(mang_spend,'') AS STRING))
         |FROM(
         |SELECT mang_day AS mang_day, mang_campaign_name AS mang_campaign_name, spend AS mang_spend
         |FROM(
         |SELECT getFormattedDate(stats_date) mang_day, getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)) mang_campaign_name, SUM(spend) AS spend
         |FROM(SELECT campaign_id, stats_date, SUM(spend) spend
         |FROM ad_fact1
         |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY campaign_id, stats_date
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
         |GROUP BY getFormattedDate(stats_date), getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING))
         |) OgbQueryAlias
         |) queryAlias LIMIT 200
       """.stripMargin
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
    val result = generateHiveQuery(jsonString, defaultRegistry)
    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(mang_campaign_name,'') AS STRING),CAST(NVL(mang_advertiser_currency,'') AS STRING),CAST(NVL(mang_spend,'') AS STRING))
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, mang_advertiser_currency AS mang_advertiser_currency, spend AS mang_spend
         |FROM(
         |SELECT getCsvEscapedString(CAST(NVL(c2.mang_campaign_name, '') AS STRING)) mang_campaign_name, COALESCE(a1.mang_advertiser_currency, 'NA') mang_advertiser_currency, SUM(spend) AS spend
         |FROM(SELECT advertiser_id, campaign_id, SUM(spend) spend
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
         |       LEFT OUTER JOIN (
         |SELECT advertiser_id AS advertiser_id, campaign_name AS mang_campaign_name, id c2_id
         |FROM campaing_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c2
         |ON
         |af0.campaign_id = c2.c2_id
         |
         |GROUP BY getCsvEscapedString(CAST(NVL(c2.mang_campaign_name, '') AS STRING)), COALESCE(a1.mang_advertiser_currency, 'NA')
         |) OgbQueryAlias
         |) queryAlias LIMIT 200
       """.stripMargin
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
    val result = generateHiveQuery(jsonString, defaultRegistry)
    assert(!result.contains("OgbQueryAlias"))


    val expected =
      """
        |
      """.stripMargin
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
    val result = generateHiveQuery(jsonString, defaultRegistry)
    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(mang_campaign_name,'') AS STRING),CAST(NVL(mang_advertiser_currency,'') AS STRING),CAST(NVL(mang_average_cpc_cents,'') AS STRING),CAST(NVL(mang_average_cpc,'') AS STRING),CAST(NVL(mang_spend,'') AS STRING))
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, mang_advertiser_currency AS mang_advertiser_currency, (CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END) * 100 AS mang_average_cpc_cents, CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END AS mang_average_cpc, spend AS mang_spend
         |FROM(
         |SELECT getCsvEscapedString(CAST(NVL(c2.mang_campaign_name, '') AS STRING)) mang_campaign_name, COALESCE(a1.mang_advertiser_currency, 'NA') mang_advertiser_currency, SUM(spend) AS spend, SUM(clicks) AS clicks
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
         |       LEFT OUTER JOIN (
         |SELECT advertiser_id AS advertiser_id, campaign_name AS mang_campaign_name, id c2_id
         |FROM campaing_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c2
         |ON
         |af0.campaign_id = c2.c2_id
         |
         |GROUP BY getCsvEscapedString(CAST(NVL(c2.mang_campaign_name, '') AS STRING)), COALESCE(a1.mang_advertiser_currency, 'NA')
         |) OgbQueryAlias
         |) queryAlias LIMIT 200
       """.stripMargin
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
    val result = generateHiveQuery(jsonString, defaultRegistry)
    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(mang_ad_status,'') AS STRING),CAST(NVL(mang_campaign_name,'') AS STRING),CAST(NVL(campaign_id,'') AS STRING),CAST(NVL(mang_spend,'') AS STRING))
         |FROM(
         |SELECT mang_ad_status AS mang_ad_status, mang_campaign_name AS mang_campaign_name, campaign_id AS campaign_id, spend AS mang_spend
         |FROM(
         |SELECT COALESCE(a2.mang_ad_status, 'NA') mang_ad_status, getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)) mang_campaign_name, COALESCE(a2.campaign_id, 0L) campaign_id, SUM(spend) AS spend
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
         |       LEFT OUTER JOIN (
         |SELECT campaign_id AS campaign_id, decodeUDF(status, 'ON', 'ON', 'OFF') AS mang_ad_status, id a2_id
         |FROM ad_dim_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |a2
         |ON
         |af0.ad_id = a2.a2_id
         |
         |GROUP BY COALESCE(a2.mang_ad_status, 'NA'), getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)), COALESCE(a2.campaign_id, 0L)
         |) OgbQueryAlias
         |) queryAlias LIMIT 200
       """.stripMargin
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
    val result = generateHiveQuery(jsonString, defaultRegistry)
    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(mang_campaign_name,'') AS STRING),CAST(NVL(mang_average_cpc,'') AS STRING),CAST(NVL(mang_spend,'') AS STRING))
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END AS mang_average_cpc, spend AS mang_spend
         |FROM(
         |SELECT getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)) mang_campaign_name, SUM(spend) AS spend, SUM(clicks) AS clicks
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
         |GROUP BY getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING))
         |) OgbQueryAlias
         |) queryAlias LIMIT 200
       """.stripMargin
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

    val result = generateHiveQuery(jsonString, defaultRegistry)
    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(mang_campaign_name,'') AS STRING),CAST(NVL(mang_average_position,'') AS STRING),CAST(NVL(mang_spend,'') AS STRING))
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, avg_pos AS mang_average_position, spend AS mang_spend
         |FROM(
         |SELECT getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)) mang_campaign_name, (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(weighted_position * impressions) / (SUM(impressions)) END) AS avg_pos, SUM(spend) AS spend, SUM(impressions) AS impressions, SUM(weighted_position) AS weighted_position
         |FROM(SELECT campaign_id, SUM(spend) spend, SUM(impressions) impressions, SUM(weighted_position) weighted_position
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
    val result = generateHiveQuery(jsonString, defaultRegistry)
    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(mang_campaign_name,'') AS STRING),CAST(NVL(mang_average_position,'') AS STRING),CAST(NVL(mang_average_cpc,'') AS STRING),CAST(NVL(mang_spend,'') AS STRING))
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, avg_pos AS mang_average_position, CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END AS mang_average_cpc, spend AS mang_spend
         |FROM(
         |SELECT getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)) mang_campaign_name, (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(weighted_position * impressions) / (SUM(impressions)) END) AS avg_pos, SUM(spend) AS spend, SUM(clicks) AS clicks, SUM(impressions) AS impressions, SUM(weighted_position) AS weighted_position
         |FROM(SELECT campaign_id, SUM(spend) spend, SUM(clicks) clicks, SUM(impressions) impressions, SUM(weighted_position) weighted_position
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
    val result = generateHiveQuery(jsonString, defaultRegistry)
    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(mang_campaign_name,'') AS STRING),CAST(NVL(advertiser_id,'') AS STRING),CAST(NVL(mang_n_average_cpc,'') AS STRING),CAST(NVL(mang_spend,'') AS STRING))
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, advertiser_id AS advertiser_id, CASE WHEN decodeUDF(stats_source, 1, clicks, 0.0) = 0 THEN 0.0 ELSE decodeUDF(stats_source, 1, spend, 0.0) / decodeUDF(stats_source, 1, clicks, 0.0) END AS mang_n_average_cpc, spend AS mang_spend
         |FROM(
         |SELECT getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)) mang_campaign_name, COALESCE(advertiser_id, 0L) advertiser_id, SUM(spend) AS spend, SUM(clicks) AS clicks, COALESCE(stats_source, 0L) stats_source
         |FROM(SELECT advertiser_id, campaign_id, SUM(spend) spend, SUM(clicks) clicks, stats_source
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
         |GROUP BY getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)), COALESCE(advertiser_id, 0L), COALESCE(stats_source, 0L)
         |) OgbQueryAlias
         |) queryAlias LIMIT 200
       """.stripMargin
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
    val result = generateHiveQuery(jsonString, defaultRegistry)
    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(mang_campaign_name,'') AS STRING),CAST(NVL(mang_impression_share,'') AS STRING),CAST(NVL(mang_spend,'') AS STRING))
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, impression_share_rounded AS mang_impression_share, spend AS mang_spend
         |FROM(
         |SELECT getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)) mang_campaign_name, SUM(spend) AS spend, SUM(impressions) AS impressions, SUM(s_impressions) AS s_impressions, COALESCE(show_flag, 0L) show_flag, (ROUND((decodeUDF(MAX(show_flag), 1, ROUND(CASE WHEN SUM(s_impressions) = 0 THEN 0.0 ELSE SUM(impressions) / (SUM(s_impressions)) END, 4), NULL)), 5)) AS impression_share_rounded
         |FROM(SELECT campaign_id, SUM(spend) spend, SUM(impressions) impressions, SUM(s_impressions) s_impressions, show_flag
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
         |GROUP BY getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)), COALESCE(show_flag, 0L)
         |) OgbQueryAlias
         |) queryAlias LIMIT 200
       """.stripMargin
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
      val result = generateHiveQuery(jsonString, defaultRegistry)
      val expected =
        s"""
           |SELECT CONCAT_WS(',', CAST(NVL(mang_ad_status,'') AS STRING),CAST(NVL(mang_campaign_name,'') AS STRING),CAST(NVL(campaign_id,'') AS STRING),CAST(NVL(mang_spend,'') AS STRING),CAST(NVL(mang_engagement_rate,'') AS STRING))
           |FROM(
           |SELECT mang_ad_status AS mang_ad_status, mang_campaign_name AS mang_campaign_name, campaign_id AS campaign_id, spend AS mang_spend, 100 * mathUDF(engagement_count, impressions) AS mang_engagement_rate
           |FROM(
           |SELECT COALESCE(a2.mang_ad_status, 'NA') mang_ad_status, getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)) mang_campaign_name, COALESCE(a2.campaign_id, 0L) campaign_id, SUM(spend) AS spend, SUM(engagement_count) AS engagement_count, SUM(impressions) AS impressions
           |FROM(SELECT ad_id, campaign_id, SUM(spend) spend, SUM(engagement_count) engagement_count, SUM(impressions) impressions
           |FROM ad_fact1
           |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
           |GROUP BY ad_id, campaign_id
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
           |       LEFT OUTER JOIN (
           |SELECT campaign_id AS campaign_id, decodeUDF(status, 'ON', 'ON', 'OFF') AS mang_ad_status, id a2_id
           |FROM ad_dim_hive
           |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
           |)
           |a2
           |ON
           |af0.ad_id = a2.a2_id
           |
           |GROUP BY COALESCE(a2.mang_ad_status, 'NA'), getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)), COALESCE(a2.campaign_id, 0L)
           |) OgbQueryAlias
           |) queryAlias LIMIT 200
         """.stripMargin
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
    val result = generateHiveQuery(jsonString, defaultRegistry)

    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(mang_ad_status,'') AS STRING),CAST(NVL(mang_campaign_name,'') AS STRING),CAST(NVL(campaign_id,'') AS STRING),CAST(NVL(mang_spend,'') AS STRING),CAST(NVL(mang_engagement_rate,'') AS STRING),CAST(NVL(mang_paid_engagement_rate,'') AS STRING))
         |FROM(
         |SELECT mang_ad_status AS mang_ad_status, mang_campaign_name AS mang_campaign_name, campaign_id AS campaign_id, spend AS mang_spend, 100 * mathUDF(engagement_count, impressions) AS mang_engagement_rate, 100 * mathUDAF(engagement_count, 0, 0, clicks, impressions) AS mang_paid_engagement_rate
         |FROM(
         |SELECT COALESCE(a2.mang_ad_status, 'NA') mang_ad_status, getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)) mang_campaign_name, COALESCE(a2.campaign_id, 0L) campaign_id, SUM(spend) AS spend, SUM(clicks) AS clicks, SUM(engagement_count) AS engagement_count, SUM(impressions) AS impressions
         |FROM(SELECT ad_id, campaign_id, SUM(spend) spend, SUM(clicks) clicks, SUM(engagement_count) engagement_count, SUM(impressions) impressions
         |FROM ad_fact1
         |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY ad_id, campaign_id
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
         |       LEFT OUTER JOIN (
         |SELECT campaign_id AS campaign_id, decodeUDF(status, 'ON', 'ON', 'OFF') AS mang_ad_status, id a2_id
         |FROM ad_dim_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |a2
         |ON
         |af0.ad_id = a2.a2_id
         |
         |GROUP BY COALESCE(a2.mang_ad_status, 'NA'), getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)), COALESCE(a2.campaign_id, 0L)
         |) OgbQueryAlias
         |) queryAlias LIMIT 200
         |
       """.stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Successfully generated Outer Group By Query with id cols requested from dims") {
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
                                "field": "Ad Group ID"
                             },
                             {
                                "field": "Advertiser ID"
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
    val result = generateHiveQuery(jsonString, defaultRegistry)

    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(mang_ad_status,'') AS STRING),CAST(NVL(ad_group_id,'') AS STRING),CAST(NVL(advertiser_id,'') AS STRING),CAST(NVL(mang_campaign_name,'') AS STRING),CAST(NVL(campaign_id,'') AS STRING),CAST(NVL(mang_spend,'') AS STRING),CAST(NVL(mang_engagement_rate,'') AS STRING),CAST(NVL(mang_paid_engagement_rate,'') AS STRING))
         |FROM(
         |SELECT mang_ad_status AS mang_ad_status, ad_group_id AS ad_group_id, advertiser_id AS advertiser_id, mang_campaign_name AS mang_campaign_name, campaign_id AS campaign_id, spend AS mang_spend, 100 * mathUDF(engagement_count, impressions) AS mang_engagement_rate, 100 * mathUDAF(engagement_count, 0, 0, clicks, impressions) AS mang_paid_engagement_rate
         |FROM(
         |SELECT COALESCE(a2.mang_ad_status, 'NA') mang_ad_status, COALESCE(ad_group_id, 0L) ad_group_id, COALESCE(advertiser_id, 0L) advertiser_id, getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)) mang_campaign_name, COALESCE(a2.campaign_id, 0L) campaign_id, SUM(spend) AS spend, SUM(clicks) AS clicks, SUM(engagement_count) AS engagement_count, SUM(impressions) AS impressions
         |FROM(SELECT advertiser_id, ad_id, campaign_id, ad_group_id, SUM(spend) spend, SUM(clicks) clicks, SUM(engagement_count) engagement_count, SUM(impressions) impressions
         |FROM ad_fact1
         |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY advertiser_id, ad_id, campaign_id, ad_group_id
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
         |       LEFT OUTER JOIN (
         |SELECT campaign_id AS campaign_id, decodeUDF(status, 'ON', 'ON', 'OFF') AS mang_ad_status, id a2_id
         |FROM ad_dim_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |a2
         |ON
         |af0.ad_id = a2.a2_id
         |
         |GROUP BY COALESCE(a2.mang_ad_status, 'NA'), COALESCE(ad_group_id, 0L), COALESCE(advertiser_id, 0L), getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)), COALESCE(a2.campaign_id, 0L)
         |) OgbQueryAlias
         |) queryAlias LIMIT 200
         |
         |
       """.stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }


  def getHiveReportingRequest(requestJson:String) : ReportingRequest = {
    val requestRaw = ReportingRequest.deserializeAsync(requestJson.getBytes(StandardCharsets.UTF_8), AdvertiserSchema)
    assert(requestRaw.isSuccess, "Failed to deserializeAsync RR")
    ReportingRequest.forceHive(requestRaw.toOption.get)
  }

  def generateHiveQuery(requestJson: String, registry:Registry): String = {
    val requestModel = getRequestModel(getHiveReportingRequest(requestJson), registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry: Try[QueryPipeline] = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.failed.errorMessage(s"Fail to get the query pipeline $queryPipelineTry"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    //println(s"got json $requestJson")

    result
  }

  test("Successfully generated Outer Group By Query with dim non id field as filter") {
    val jsonString =
      s"""{
                           "cube": "performance_stats",
                           "selectFields": [
                             {
                               "field": "Spend",
                               "alias": null,
                               "value": null
                             }
                           ],
                           "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Campaign Name", "operator": "=", "value": "cmpgn_1"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ]
                           }""".stripMargin

    val result = generateHiveQuery(jsonString, defaultRegistry)

    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(mang_spend,'') AS STRING))
         |FROM(
         |SELECT spend AS mang_spend
         |FROM(
         |SELECT SUM(spend) AS spend
         |FROM(SELECT campaign_id, SUM(spend) spend
         |FROM ad_fact1
         |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY campaign_id
         |
 |       )
         |af0
         |JOIN (
         |SELECT id c1_id
         |FROM campaing_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345) AND (lower(campaign_name) = lower('cmpgn_1'))
         |)
         |c1
         |ON
         |af0.campaign_id = c1.c1_id
         |
         |
 |) OgbQueryAlias
         |) queryAlias LIMIT 200
       """.stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Successfully generated Outer Group By Query with dim non id field as filter and derived fact field having dim source col") {
    val jsonString =
      s"""{
                           "cube": "performance_stats",
                           "selectFields": [
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
                              {"field": "Campaign Name", "operator": "=", "value": "cmpgn_1"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                           ]
                           }""".stripMargin

    val result = generateHiveQuery(jsonString, defaultRegistry)
    val expected =
      s"""
         |SELECT CONCAT_WS(',', CAST(NVL(mang_source,'') AS STRING),CAST(NVL(mang_n_spend,'') AS STRING))
         |FROM(
         |SELECT mang_source AS mang_source, decodeUDF(stats_source, 1, spend, 0.0) AS mang_n_spend
         |FROM(
         |SELECT COALESCE(stats_source, 0L) mang_source, SUM(spend) AS spend, stats_source AS stats_source
         |FROM(SELECT campaign_id, stats_source, SUM(spend) spend
         |FROM ad_fact1
         |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY campaign_id, stats_source
         |
 |       )
         |af0
         |JOIN (
         |SELECT id c1_id
         |FROM campaing_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345) AND (lower(campaign_name) = lower('cmpgn_1'))
         |)
         |c1
         |ON
         |af0.campaign_id = c1.c1_id
         |
         |GROUP BY COALESCE(stats_source, 0L), stats_source
         |) OgbQueryAlias
         |) queryAlias LIMIT 200
       """.stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
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
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(CAST(device_id AS STRING), ''), NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''), NVL(CAST(mang_pricing_type AS STRING), ''), NVL(CAST(network_id AS STRING), ''))
         |FROM(
         |SELECT COALESCE(device_id, 0L) device_id, COALESCE(account_id, 0L) advertiser_id, COALESCE(impressions, 1L) mang_impressions, COALESCE(price_type, 0L) mang_pricing_type, COALESCE(network_type, 'NA') network_id
         |FROM(SELECT decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE') network_type, CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END price_type, account_id, CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END device_id, SUM(impressions) impressions
         |FROM s_stats_fact_underlying
         |WHERE (account_id <> -3) AND (account_id = 12345) AND (account_id IS NOT NULL) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE'), CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END, account_id, CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END
         |
         |       )
         |ssfu0
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
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(CAST(device_id AS STRING), ''), NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(campaign_id AS STRING), ''), NVL(CAST(mang_campaign_name AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''), NVL(CAST(mang_pricing_type AS STRING), ''), NVL(CAST(network_id AS STRING), ''))
         |FROM(
         |SELECT COALESCE(device_id, 0L) device_id, COALESCE(account_id, 0L) advertiser_id, COALESCE(ssfu0.campaign_id, 0L) campaign_id, getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)) mang_campaign_name, COALESCE(impressions, 1L) mang_impressions, COALESCE(price_type, 0L) mang_pricing_type, COALESCE(network_type, 'NA') network_id
         |FROM(SELECT CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END device_id, decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE') network_type, CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END price_type, account_id, campaign_id, SUM(impressions) impressions
         |FROM s_stats_fact_underlying
         |WHERE (account_id <> -3) AND (account_id = 12345) AND (account_id IS NOT NULL) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END, decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE'), CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END, account_id, campaign_id
         |
         |       )
         |ssfu0
         |JOIN (
         |SELECT campaign_name AS mang_campaign_name, id c1_id
         |FROM campaing_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id <> -3) AND (advertiser_id = 12345) AND (advertiser_id IS NOT NULL) AND (campaign_name <> '-3') AND (campaign_name IS NOT NULL)
         |)
         |c1
         |ON
         |ssfu0.campaign_id = c1.c1_id
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


    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v2)
    assert(queryPipelineTry.isSuccess, "dim fact sync dimension driven query with requested fields in multiple dimensions should not fail")
    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(CAST(campaign_id AS STRING), ''), NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''), NVL(CAST(mang_clicks AS STRING), ''))
         |FROM(
         |SELECT COALESCE(ssfu0.campaign_id, 0L) campaign_id, COALESCE(account_id, 0L) advertiser_id, COALESCE(impressions, 1L) mang_impressions, COALESCE(mang_clicks, 0L) mang_clicks
         |FROM(SELECT account_id, campaign_id, SUM(clicks) mang_clicks, SUM(impressions) impressions
         |FROM s_stats_fact_underlying
         |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY account_id, campaign_id
         |
         |       )
         |ssfu0
         |JOIN (
         |SELECT id c1_id
         |FROM campaing_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345) AND (lower(campaign_name) NOT LIKE lower('%cmpgn1%'))
         |)
         |c1
         |ON
         |ssfu0.campaign_id = c1.c1_id
         |
         |) queryAlias LIMIT 200
      """.stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Query with both aliases") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "hive_query_with_both_aliases_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v0)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString


    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(CAST(mang_day AS STRING), ''), NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(campaign_id AS STRING), ''), NVL(CAST(ad_group_id AS STRING), ''), NVL(CAST(keyword_id AS STRING), ''), NVL(CAST(mang_ad_format_name AS STRING), ''), NVL(CAST(mang_ad_format_sub_type AS STRING), ''), NVL(CAST(mang_ad_format_type AS STRING), ''), NVL(CAST(mang_keyword AS STRING), ''), NVL(CAST(mang_search_term AS STRING), ''), NVL(CAST(mang_delivered_match_type AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''))
         |FROM(
         |SELECT getFormattedDate(stats_date) mang_day, COALESCE(account_id, 0L) advertiser_id, COALESCE(campaign_id, 0L) campaign_id, COALESCE(ad_group_id, 0L) ad_group_id, COALESCE(keyword_id, 0L) keyword_id, COALESCE(ad_format_id, 0L) mang_ad_format_name, COALESCE(ad_format_sub_type, 0L) mang_ad_format_sub_type, COALESCE(ad_format_type, 0L) mang_ad_format_type, getCsvEscapedString(CAST(NVL(keyword, '') AS STRING)) mang_keyword, COALESCE(search_term, "None") mang_search_term, COALESCE(delivered_match_type, 0L) mang_delivered_match_type, COALESCE(impressions, 1L) mang_impressions
         |FROM(SELECT CASE WHEN (delivered_match_type IN (1)) THEN 'Exact' WHEN (delivered_match_type IN (2)) THEN 'Broad' WHEN (delivered_match_type IN (3)) THEN 'Phrase' ELSE 'UNKNOWN' END delivered_match_type, stats_date, keyword, ad_group_id, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'N/A' END ad_format_type, search_term, account_id, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (5)) THEN 'Single image' WHEN (ad_format_id IN (6)) THEN 'Single image' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (9)) THEN 'Carousel' WHEN (ad_format_id IN (2)) THEN 'Single image' WHEN (ad_format_id IN (7)) THEN 'Video' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (3)) THEN 'Single image' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (8)) THEN 'Video with HTML Endcard' WHEN (ad_format_id IN (4)) THEN 'Single image' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'Other' END ad_format_id,  CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'N/A' END ad_format_sub_type, campaign_id, keyword_id, SUM(impressions) impressions
         |FROM s_stats_fact_underlying
         |WHERE (account_id = 12345) AND (ad_format_id IN (4,5,6,2,3)) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY CASE WHEN (delivered_match_type IN (1)) THEN 'Exact' WHEN (delivered_match_type IN (2)) THEN 'Broad' WHEN (delivered_match_type IN (3)) THEN 'Phrase' ELSE 'UNKNOWN' END, stats_date, keyword, ad_group_id, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'N/A' END, search_term, account_id, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (5)) THEN 'Single image' WHEN (ad_format_id IN (6)) THEN 'Single image' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (9)) THEN 'Carousel' WHEN (ad_format_id IN (2)) THEN 'Single image' WHEN (ad_format_id IN (7)) THEN 'Video' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (3)) THEN 'Single image' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (8)) THEN 'Video with HTML Endcard' WHEN (ad_format_id IN (4)) THEN 'Single image' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'Other' END, campaign_id, keyword_id
         |
         |       )
         |ssfu0
         |
         |ORDER BY mang_impressions ASC) queryAlias LIMIT 100
         |
         |""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Query with both aliases with filter and sort by") {
    val jsonString =
      s"""{
                          "cube": "s_stats",
                          "selectFields": [
                              {"field": "Device ID"},
                              {"field": "Advertiser ID"},
                              {"field": "Ad Format Name"},
                              {"field": "Ad Format Sub Type"},
                              {"field": "Impressions"},
                              {"field": "Pricing Type"},
                              {"field": "Network ID"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Ad Format Name", "operator": "=", "value": "Product Ad"},
                              {"field": "Ad Format Sub Type", "operator": "<>", "value": "DPA Single Image Ad"},
                              {"field": "Ad Format Sub Type", "operator": "=", "value": "DPA Collection Ad"},
                              {"field": "Campaign Name", "operator": "IsNotNull"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "sortBy": [
                              { "field": "Advertiser ID", "order": "Asc"},
                              { "field": "Ad Format Name", "order": "Asc"}
                          ]
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = defaultRegistry
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v0)
    assert(queryPipelineTry.isSuccess, "query with both aliases should not fail")
    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString

    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(CAST(device_id AS STRING), ''), NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(mang_ad_format_name AS STRING), ''), NVL(CAST(mang_ad_format_sub_type AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''), NVL(CAST(mang_pricing_type AS STRING), ''), NVL(CAST(network_id AS STRING), ''))
         |FROM(
         |SELECT COALESCE(device_id, 0L) device_id, COALESCE(account_id, 0L) advertiser_id, COALESCE(ad_format_id, 0L) mang_ad_format_name, COALESCE(ad_format_sub_type, 0L) mang_ad_format_sub_type, COALESCE(impressions, 1L) mang_impressions, COALESCE(price_type, 0L) mang_pricing_type, COALESCE(network_type, "NA") network_id
         |FROM(SELECT CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END device_id, decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE') network_type, CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END price_type, account_id, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (5)) THEN 'Single image' WHEN (ad_format_id IN (6)) THEN 'Single image' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (9)) THEN 'Carousel' WHEN (ad_format_id IN (2)) THEN 'Single image' WHEN (ad_format_id IN (7)) THEN 'Video' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (3)) THEN 'Single image' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (8)) THEN 'Video with HTML Endcard' WHEN (ad_format_id IN (4)) THEN 'Single image' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'Other' END ad_format_id, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'N/A' END ad_format_sub_type, campaign_id, SUM(impressions) impressions
         |FROM s_stats_fact_underlying
         |WHERE (ad_format_id <> 100) AND (ad_format_id = 35) AND (ad_format_id = 97) AND (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY CASE WHEN (device_id IN (11)) THEN 'Desktop' WHEN (device_id IN (22)) THEN 'Tablet' WHEN (device_id IN (33)) THEN 'SmartPhone' WHEN (device_id IN (-1)) THEN 'UNKNOWN' ELSE 'UNKNOWN' END, decodeUDF(network_type, 'TEST_PUBLISHER', 'Test Publisher', 'CONTENT_S', 'Content Secured', 'EXTERNAL', 'External Partners', 'INTERNAL', 'Internal Properties', 'NONE'), CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END, account_id, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (5)) THEN 'Single image' WHEN (ad_format_id IN (6)) THEN 'Single image' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (9)) THEN 'Carousel' WHEN (ad_format_id IN (2)) THEN 'Single image' WHEN (ad_format_id IN (7)) THEN 'Video' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (3)) THEN 'Single image' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (8)) THEN 'Video with HTML Endcard' WHEN (ad_format_id IN (4)) THEN 'Single image' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'Other' END, CASE WHEN (ad_format_id IN (101)) THEN 'DPA Carousel Ad' WHEN (ad_format_id IN (97)) THEN 'DPA Collection Ad' WHEN (ad_format_id IN (98)) THEN 'DPA View More' WHEN (ad_format_id IN (35)) THEN 'Product Ad' WHEN (ad_format_id IN (99)) THEN 'DPA Extended Carousel' WHEN (ad_format_id IN (100)) THEN 'DPA Single Image Ad' ELSE 'N/A' END, campaign_id
         |
 |       )
         |ssfu0
         |JOIN (
         |SELECT id c1_id
         |FROM campaing_hive
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345) AND (campaign_name IS NOT NULL)
         |)
         |c1
         |ON
         |ssfu0.campaign_id = c1.c1_id
         |
         |ORDER BY advertiser_id ASC, mang_ad_format_name ASC)
         |        queryAlias LIMIT 200
         |""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("generating hive query with COL Function on FACT Cols") {
    val jsonString =
      s"""{
                          "cube": "s_stats_minute",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Device ID"},
                              {"field": "Test Metric COL"},
                              {"field": "Test Mod Metric COL"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [

                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Impressions", "operator": ">", "value": "1608"}
                          ],
                         "sortBy": [
                           { "field": "Impressions", "order": "Desc" },
                           { "field": "Advertiser ID", "order": "Asc"}
                         ],
                         "additionalParameters": {
                            "AdditionalColumnInfo":
                            [
                              {"field": "spend", "value": "123"},
                              {"field": "impressions", "value": "potatoes"}
                            ]
                         }
          }"""
    val request: ReportingRequest = ReportingRequest.deserializeWithAdditionalParameters(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema).toOption.get

    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))
    val queryPipelineTry = generatePipeline(requestModel.toOption.get, Version.v0)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))


    val result = queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    assert(result.contains("""ROUND(COALESCE((CASE WHEN SUM(spend) > 0 THEN SUM(clicks) / 10 ELSE SUM(impressions) END), 0L), 10) mang_test_metric_col""")) //DON'T change existing functions
    assert(result.contains("""ROUND(COALESCE((CASE WHEN SUM(123) > 0 THEN SUM(clicks) / 10 ELSE SUM(potatoes) END), 0L), 10) mang_test_mod_metric_col""")) //DO change new function
  }

  test("Test specified timezone in additionalParameter should be applied in hive v0 query") {
    val jsonString =
      s"""{
                          "cube": "s_stats_timezone",
                          "selectFields": [
                              {"field": "Day"},
                              {"field": "Hour"},
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
                          ],
                          "additionalParameters": {"TimeZone": "America/New_York"}
                          }"""

    val request: ReportingRequest = getReportingRequestAsyncWithAdditionalParameters(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get, Version.v0)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[HiveQuery].asString
    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(CAST(mang_day AS STRING), ''), NVL(CAST(mang_hour AS STRING), ''), NVL(CAST(advertiser_id AS STRING), ''), NVL(CAST(mang_impressions AS STRING), ''))
         |FROM(
         |SELECT getFormattedDate(mang_day) mang_day, getFormattedDate(mang_hour) mang_hour, COALESCE(account_id, 0L) advertiser_id, COALESCE(impressions, 1L) mang_impressions
         |FROM(SELECT account_id, from_unixtime(unix_timestamp(from_utc_timestamp(from_unixtime(unix_timestamp(utc_hour, 'yyyyMMddHH'), 'yyyy-MM-dd HH:mm:ss'), 'America/New_York')), 'yyyyMMddHH') mang_hour, from_unixtime(unix_timestamp(from_utc_timestamp(from_unixtime(unix_timestamp(utc_hour, 'yyyyMMddHH'), 'yyyy-MM-dd HH:mm:ss'), 'America/New_York')), 'yyyyMMdd') mang_day, SUM(impressions) impressions
         |FROM s_stats_timezone_fact
         |WHERE (account_id = 12345) AND (from_unixtime(unix_timestamp(from_utc_timestamp(from_unixtime(unix_timestamp(utc_hour, 'yyyyMMddHH'), 'yyyy-MM-dd HH:mm:ss'), 'America/New_York')), 'yyyyMMdd') >= '$fromDateHive' AND from_unixtime(unix_timestamp(from_utc_timestamp(from_unixtime(unix_timestamp(utc_hour, 'yyyyMMddHH'), 'yyyy-MM-dd HH:mm:ss'), 'America/New_York')), 'yyyyMMdd') <= '$toDateHive')
         |GROUP BY account_id, from_unixtime(unix_timestamp(from_utc_timestamp(from_unixtime(unix_timestamp(utc_hour, 'yyyyMMddHH'), 'yyyy-MM-dd HH:mm:ss'), 'America/New_York')), 'yyyyMMddHH'), from_unixtime(unix_timestamp(from_utc_timestamp(from_unixtime(unix_timestamp(utc_hour, 'yyyyMMddHH'), 'yyyy-MM-dd HH:mm:ss'), 'America/New_York')), 'yyyyMMdd')
         |
         |       )
         |sstf0
         |
         |) queryAlias LIMIT 200
         |""".stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

}