// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.presto

import com.yahoo.maha.core._
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.request.ReportingRequest


class PrestoQueryGeneratorV1Test extends BasePrestoQueryGeneratorTest {

  test("registering Presto query generation multiple times should fail") {
    intercept[IllegalArgumentException] {
      val dummyQueryGenerator = new QueryGenerator[WithPrestoEngine] {
        override def generate(queryContext: QueryContext): Query = { null }
        override def engine: Engine = PrestoEngine
      }
      queryGeneratorRegistry.register(PrestoEngine, dummyQueryGenerator)
    }
  }

  test("generating presto query") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "presto_query_generator_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))


    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString

    val expected = s"""SELECT CAST(mang_day as VARCHAR) AS mang_day, CAST(advertiser_id as VARCHAR) AS advertiser_id, CAST(campaign_id as VARCHAR) AS campaign_id, CAST(mang_campaign_name as VARCHAR) AS mang_campaign_name, CAST(ad_group_id as VARCHAR) AS ad_group_id, CAST(keyword_id as VARCHAR) AS keyword_id, CAST(mang_keyword as VARCHAR) AS mang_keyword, CAST(mang_search_term as VARCHAR) AS mang_search_term, CAST(mang_delivered_match_type as VARCHAR) AS mang_delivered_match_type, CAST(mang_impressions as VARCHAR) AS mang_impressions, CAST(mang_ad_group_start_date_full as VARCHAR) AS mang_ad_group_start_date_full, CAST(mang_clicks as VARCHAR) AS mang_clicks, CAST(mang_average_cpc as VARCHAR) AS mang_average_cpc
                      |FROM(
                      |SELECT getFormattedDate(stats_date) mang_day, COALESCE(account_id, 0) advertiser_id, COALESCE(CAST(ssfu0.campaign_id as VARCHAR), 'NA') campaign_id, getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)) mang_campaign_name, COALESCE(ad_group_id, 0) ad_group_id, COALESCE(keyword_id, 0) keyword_id, getCsvEscapedString(CAST(COALESCE(keyword, '') AS VARCHAR)) mang_keyword, COALESCE(CAST(search_term as VARCHAR), 'None') mang_search_term, COALESCE(CAST(delivered_match_type as varchar), 'NA') mang_delivered_match_type, COALESCE(impressions, 1) mang_impressions, COALESCE(CAST(mang_ad_group_start_date_full as VARCHAR), 'NA') mang_ad_group_start_date_full, COALESCE(mang_clicks, 0) mang_clicks, ROUND(COALESCE((CASE WHEN clicks = 0 THEN 0.0 ELSE CAST(spend AS DOUBLE) / clicks END), 0), 10) mang_average_cpc
                      |FROM(SELECT CASE WHEN (delivered_match_type IN (1)) THEN 'Exact' WHEN (delivered_match_type IN (2)) THEN 'Broad' WHEN (delivered_match_type IN (3)) THEN 'Phrase' ELSE 'UNKNOWN' END delivered_match_type, stats_date, keyword, ad_group_id, search_term, account_id, campaign_id, keyword_id, getDateFromEpoch(start_time, 'YYYY-MM-dd HH:mm:ss') mang_ad_group_start_date_full, SUM(clicks) mang_clicks, SUM(impressions) impressions, SUM(spend) spend
                      |FROM s_stats_fact_underlying
        WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
        GROUP BY CASE WHEN (delivered_match_type IN (1)) THEN 'Exact' WHEN (delivered_match_type IN (2)) THEN 'Broad' WHEN (delivered_match_type IN (3)) THEN 'Phrase' ELSE 'UNKNOWN' END, stats_date, keyword, ad_group_id, search_term, account_id, campaign_id, keyword_id, getDateFromEpoch(start_time, 'YYYY-MM-dd HH:mm:ss')
HAVING (SUM(clicks) >= 0 AND SUM(clicks) <= 100000)
       )
ssfu0
LEFT OUTER JOIN (
SELECT campaign_name AS mang_campaign_name, id c1_id
FROM campaign_presto_underlying
WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
)
c1
ON
CAST(ssfu0.campaign_id AS VARCHAR) = CAST(c1.c1_id AS VARCHAR)
ORDER BY mang_impressions ASC
       ) queryAlias LIMIT 100""".stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("generating presto query with greater than filter") {
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

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))


    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString

    val expected = s"""SELECT CAST(advertiser_id as VARCHAR) AS advertiser_id, CAST(mang_impressions as VARCHAR) AS mang_impressions
    FROM(
      SELECT COALESCE(account_id, 0) advertiser_id, COALESCE(impressions, 1) mang_impressions
        FROM(SELECT account_id, SUM(impressions) impressions
          FROM s_stats_fact_underlying
          WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
    GROUP BY account_id
    HAVING (SUM(impressions) > 1608)
    )
    ssfu0
    ) queryAlias LIMIT 200""".stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("generating presto query with less than filter") {
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
                              {"field": "Impressions", "operator": "<", "value": "1608"},
                              {"field": "Ad Group ID", "operator": "==", "compareTo": "Advertiser ID"}
                          ]
                          }"""
    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString

    val expected = s"""SELECT CAST(advertiser_id as VARCHAR) AS advertiser_id, CAST(mang_impressions as VARCHAR) AS mang_impressions
    FROM(
      SELECT COALESCE(account_id, 0) advertiser_id, COALESCE(impressions, 1) mang_impressions
        FROM(SELECT account_id, SUM(impressions) impressions
          FROM s_stats_fact_underlying
          WHERE (ad_group_id = account_id) AND (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
    GROUP BY account_id
    HAVING (SUM(impressions) < 1608)
    )
    ssfu0
    ) queryAlias LIMIT 200""".stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Verify metric Presto column comparison") {
    val jsonString =
      s"""{
                          "cube": "s_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Impressions"},
                              {"field": "Network ID"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Impressions", "operator": "<", "value": "1608"},
                              {"field": "Max Bid", "operator": "==", "compareTo": "Spend"}
                          ]
                          }"""
    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    val expected = s"""SELECT CAST(advertiser_id as VARCHAR) AS advertiser_id, CAST(mang_impressions as VARCHAR) AS mang_impressions, CAST(network_id as VARCHAR) AS network_id
                      |FROM(
                      |SELECT COALESCE(account_id, 0) advertiser_id, COALESCE(impressions, 1) mang_impressions, COALESCE(CAST(network_type as VARCHAR), 'NA') network_id
                      |FROM(SELECT CASE WHEN (network_type IN ('TEST_PUBLISHER')) THEN 'Test Publisher' WHEN (network_type IN ('CONTENT_S')) THEN 'Content Secured' WHEN (network_type IN ('EXTERNAL')) THEN 'External Partners' WHEN (network_type IN ('INTERNAL')) THEN 'Internal Properties' ELSE 'NONE' END network_type, account_id, SUM(impressions) impressions
                      |FROM s_stats_fact_underlying
                      |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
                      |GROUP BY CASE WHEN (network_type IN ('TEST_PUBLISHER')) THEN 'Test Publisher' WHEN (network_type IN ('CONTENT_S')) THEN 'Content Secured' WHEN (network_type IN ('EXTERNAL')) THEN 'External Partners' WHEN (network_type IN ('INTERNAL')) THEN 'Internal Properties' ELSE 'NONE' END, account_id
                      |HAVING (SUM(impressions) < 1608) AND (MAX(max_bid) = SUM(spend))
                      |       )
                      |ssfu0
                      |) queryAlias LIMIT 200""".stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("generating presto query with custom rollups") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "presto_query_generator_test_custom_rollups.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString

    val expected = s"""FROM(SELECT CASE WHEN (bid_strategy IN (1)) THEN 'Max Click' WHEN (bid_strategy IN (2)) THEN 'Inflection Point' ELSE 'NONE' END bid_strategy, ad_group_id, account_id, campaign_id, (modified_bid - current_bid) / current_bid * 100 mang_bid_modifier, SUBSTRING(load_time, 1, 8) mang_day, SUM(actual_impressions) actual_impressions, (spend * forecasted_clicks / actual_clicks * recommended_bid / modified_bid) mang_noop_rollup_spend, AVG(spend * forecasted_clicks / actual_clicks * recommended_bid / modified_bid) mang_avg_rollup_spend, MAX(spend * forecasted_clicks / actual_clicks * recommended_bid / modified_bid) mang_max_rollup_spend, MIN(spend * forecasted_clicks / actual_clicks * recommended_bid / modified_bid) mang_min_rollup_spend, SUM(spend * forecasted_clicks / actual_clicks * recommended_bid / modified_bid) mang_forecasted_spend, () mang_custom_rollup_spend
""".stripMargin

    assert(result.contains(expected), "Result should have all requested fields.")
  }

  test("generating presto query with underlying table name") {
    val jsonString = scala.io.Source.fromFile(getBaseDir + "presto_query_generator_underlying_test.json")
      .getLines().mkString.replace("{from_date}", fromDate).replace("{to_date}", toDate)
    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    
    assert(result != null && result.length > 0 && result.contains("campaign_presto_underlying"))
  }

  test("Duplicate registration of the generator") {
    val failRegistry = new QueryGeneratorRegistry
    val dummyPrestoQueryGeneratorV1 = new QueryGenerator[WithPrestoEngine] {
      override def generate(queryContext: QueryContext): Query = { null }
      override def engine: Engine = OracleEngine
    }
    val dummyFalseQueryGenerator = new QueryGenerator[WithDruidEngine] {
      override def generate(queryContext: QueryContext): Query = { null }
      override def engine: Engine = DruidEngine
    }
    failRegistry.register(PrestoEngine, dummyPrestoQueryGeneratorV1)
    failRegistry.register(DruidEngine, dummyFalseQueryGenerator)

    PrestoQueryGeneratorV1.register(failRegistry,DefaultPartitionColumnRenderer, TestPrestoUDFRegistrationFactory())
  }

  test("generating presto query with greater than filter and sort by") {
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
                          ],
                         "sortBy": [
                           { "field": "Impressions", "order": "Desc" }
                         ]
          }"""
    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString

    val expected =
      s"""
         |SELECT CAST(advertiser_id as VARCHAR) AS advertiser_id, CAST(mang_impressions as VARCHAR) AS mang_impressions
         |FROM(
         |SELECT COALESCE(account_id, 0) advertiser_id, COALESCE(impressions, 1) mang_impressions
         |FROM(SELECT account_id, SUM(impressions) impressions
         |FROM s_stats_fact_underlying
         |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY account_id
         |HAVING (SUM(impressions) > 1608)
         |       )
         |ssfu0
         |
         |ORDER BY mang_impressions DESC
         |          )
         |        queryAlias LIMIT 200
       """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("generating presto query with greater than filter and multiple sort bys") {
    val jsonString =
      s"""{
                          "cube": "s_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Count"},
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
                         ]
          }"""
    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString

    val expected =
      s"""
         |SELECT CAST(advertiser_id as VARCHAR) AS advertiser_id, CAST(mang_count as VARCHAR) AS mang_count, CAST(mang_impressions as VARCHAR) AS mang_impressions
         |FROM(
         |SELECT COALESCE(account_id, 0) advertiser_id, COALESCE(Count, 0) mang_count, COALESCE(impressions, 1) mang_impressions
         |FROM(SELECT account_id, SUM(impressions) impressions, COUNT(*) Count
         |FROM s_stats_fact_underlying
         |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY account_id
         |HAVING (SUM(impressions) > 1608)
         |       )
         |ssfu0
         |
         |ORDER BY mang_impressions DESC, advertiser_id ASC
         |          )
         |        queryAlias LIMIT 200
       """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("Query with constant requested fields should have constant columns") {
    val jsonString =
      s"""{
              "cube" : "s_stats",
              "selectFields" : [
                  { "field" : "Day" },
                  { "field" : "Advertiser ID" },
                  { "field" : "Campaign ID" },
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
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    assert(result.contains("'2' mang_source"), "No constant field in outer columns")
  }

  test("generating presto query with sort on dimension") {
    val jsonString =
      s"""{
                          "cube": "s_stats",
                          "selectFields": [
                              {"field": "Advertiser ID"},
                              {"field": "Advertiser Name"},
                              {"field": "Impressions"},
                              {"field": "Average Position"}
                          ],
                          "filterExpressions": [
                              {"field": "Advertiser ID", "operator": "=", "value": "12345"},
                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                              {"field": "Impressions", "operator": ">", "value": "1608"}
                          ],
                          "sortBy": [{"field": "Advertiser Name", "order": "Desc"},  {"field": "Impressions", "order": "DESC"}]
                          }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

        val expected =
          s"""
             |SELECT CAST(advertiser_id as VARCHAR) AS advertiser_id, CAST(mang_advertiser_name as VARCHAR) AS mang_advertiser_name, CAST(mang_impressions as VARCHAR) AS mang_impressions, CAST(mang_average_position as VARCHAR) AS mang_average_position
             |FROM(
             |SELECT COALESCE(ssfu0.account_id, 0) advertiser_id, COALESCE(CAST(a1.mang_advertiser_name as VARCHAR), 'NA') mang_advertiser_name, COALESCE(impressions, 1) mang_impressions, ROUND(COALESCE(CASE WHEN ((mang_average_position >= 0.1) AND (mang_average_position <= 500)) THEN mang_average_position ELSE 0.0 END, 0.0), 10) mang_average_position
             |FROM(SELECT account_id, SUM(impressions) impressions, (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE CAST(SUM(weighted_position * impressions) AS DOUBLE) / (SUM(impressions)) END) mang_average_position
             |FROM s_stats_fact_underlying
             |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
             |GROUP BY account_id
             |HAVING (SUM(impressions) > 1608)
             |       )
             |ssfu0
             |LEFT OUTER JOIN (
             |SELECT name AS mang_advertiser_name, id a1_id
             |FROM advertiser_presto
             |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
             |)
             |a1
             |ON
             |ssfu0.account_id = a1.a1_id
             |
             |ORDER BY mang_advertiser_name DESC, mang_impressions DESC
             |          )
             |        queryAlias LIMIT 200
             """.stripMargin

        result should equal (expected) (after being whiteSpaceNormalised)
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

    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    val expected =
      s"""
         |SELECT CAST(mang_campaign_name as VARCHAR) AS mang_campaign_name, CAST(mang_spend as VARCHAR) AS mang_spend
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, spend AS mang_spend
         |FROM(
         |SELECT getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)) mang_campaign_name, SUM(spend) AS spend
         |FROM(SELECT campaign_id, SUM(spend) spend
         |FROM ad_fact1
         |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY campaign_id
         |
         |)
         |af0
         |LEFT OUTER JOIN (
         |SELECT campaign_name AS mang_campaign_name, id c1_id
         |FROM campaign_presto_underlying
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c1
         |ON
         |af0.campaign_id = c1.c1_id
         |
         |GROUP BY getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR))
         |) OgbQueryAlias
         |)
         |        queryAlias LIMIT 200
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

    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    val expected =
      s"""
         |SELECT CAST(mang_campaign_name as VARCHAR) AS mang_campaign_name, CAST(mang_source as VARCHAR) AS mang_source, CAST(mang_n_spend as VARCHAR) AS mang_n_spend
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, mang_source AS mang_source, decodeUDF(stats_source, 1, spend, 0.0) AS mang_n_spend
         |FROM(
         |SELECT getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)) mang_campaign_name, COALESCE(stats_source, 0) mang_source, SUM(spend) AS spend, stats_source AS stats_source
         |FROM(SELECT campaign_id, stats_source, SUM(spend) spend
         |FROM ad_fact1
         |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY campaign_id, stats_source
         |
 |       )
         |af0
         |LEFT OUTER JOIN (
         |SELECT campaign_name AS mang_campaign_name, id c1_id
         |FROM campaign_presto_underlying
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c1
         |ON
         |af0.campaign_id = c1.c1_id
         |
         |GROUP BY getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)), COALESCE(stats_source, 0), stats_source
         |) OgbQueryAlias
         |)
         |        queryAlias LIMIT 200
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

    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    val expected =
      s"""
         |SELECT CAST(mang_day as VARCHAR) AS mang_day, CAST(mang_campaign_name as VARCHAR) AS mang_campaign_name, CAST(mang_spend as VARCHAR) AS mang_spend
         |FROM(
         |SELECT mang_day AS mang_day, mang_campaign_name AS mang_campaign_name, spend AS mang_spend
         |FROM(
         |SELECT getFormattedDate(stats_date) mang_day, getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)) mang_campaign_name, SUM(spend) AS spend
         |FROM(SELECT campaign_id, stats_date, SUM(spend) spend
         |FROM ad_fact1
         |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY campaign_id, stats_date
         |
 |       )
         |af0
         |LEFT OUTER JOIN (
         |SELECT campaign_name AS mang_campaign_name, id c1_id
         |FROM campaign_presto_underlying
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c1
         |ON
         |af0.campaign_id = c1.c1_id
         |
         |GROUP BY getFormattedDate(stats_date), getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR))
         |) OgbQueryAlias
         |)
         |        queryAlias LIMIT 200
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

    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    val expected =
      s"""
         |SELECT CAST(mang_campaign_name as VARCHAR) AS mang_campaign_name, CAST(mang_advertiser_currency as VARCHAR) AS mang_advertiser_currency, CAST(mang_spend as VARCHAR) AS mang_spend
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, mang_advertiser_currency AS mang_advertiser_currency, spend AS mang_spend
         |FROM(
         |SELECT getCsvEscapedString(CAST(COALESCE(c2.mang_campaign_name, '') AS VARCHAR)) mang_campaign_name, COALESCE(CAST(a1.mang_advertiser_currency as VARCHAR), 'NA') mang_advertiser_currency, SUM(spend) AS spend
         |FROM(SELECT advertiser_id, campaign_id, SUM(spend) spend
         |FROM ad_fact1
         |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY advertiser_id, campaign_id
         |
 |       )
         |af0
         |LEFT OUTER JOIN (
         |SELECT currency AS mang_advertiser_currency, id a1_id
         |FROM advertiser_presto
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
         |)
         |a1
         |ON
         |af0.advertiser_id = a1.a1_id
         |       LEFT OUTER JOIN (
         |SELECT advertiser_id AS advertiser_id, campaign_name AS mang_campaign_name, id c2_id
         |FROM campaign_presto_underlying
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c2
         |ON
         |af0.campaign_id = c2.c2_id
         |
         |GROUP BY getCsvEscapedString(CAST(COALESCE(c2.mang_campaign_name, '') AS VARCHAR)), COALESCE(CAST(a1.mang_advertiser_currency as VARCHAR), 'NA')
         |) OgbQueryAlias
         |)
         |        queryAlias LIMIT 200
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

    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    assert(!result.contains("OgbQueryAlias"))
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

    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    val expected =
      s"""
         |SELECT CAST(mang_campaign_name as VARCHAR) AS mang_campaign_name, CAST(mang_advertiser_currency as VARCHAR) AS mang_advertiser_currency, CAST(mang_average_cpc_cents as VARCHAR) AS mang_average_cpc_cents, CAST(mang_average_cpc as VARCHAR) AS mang_average_cpc, CAST(mang_spend as VARCHAR) AS mang_spend
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, mang_advertiser_currency AS mang_advertiser_currency, (CASE WHEN clicks = 0 THEN 0.0 ELSE CAST(spend AS DOUBLE) / clicks END) * 100 AS mang_average_cpc_cents, CASE WHEN clicks = 0 THEN 0.0 ELSE CAST(spend AS DOUBLE) / clicks END AS mang_average_cpc, spend AS mang_spend
         |FROM(
         |SELECT getCsvEscapedString(CAST(COALESCE(c2.mang_campaign_name, '') AS VARCHAR)) mang_campaign_name, COALESCE(CAST(a1.mang_advertiser_currency as VARCHAR), 'NA') mang_advertiser_currency, SUM(spend) AS spend, SUM(clicks) AS clicks
         |FROM(SELECT advertiser_id, campaign_id, SUM(spend) spend, SUM(clicks) clicks
         |FROM ad_fact1
         |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY advertiser_id, campaign_id
         |
 |       )
         |af0
         |LEFT OUTER JOIN (
         |SELECT currency AS mang_advertiser_currency, id a1_id
         |FROM advertiser_presto
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
         |)
         |a1
         |ON
         |af0.advertiser_id = a1.a1_id
         |       LEFT OUTER JOIN (
         |SELECT advertiser_id AS advertiser_id, campaign_name AS mang_campaign_name, id c2_id
         |FROM campaign_presto_underlying
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c2
         |ON
         |af0.campaign_id = c2.c2_id
         |
         |GROUP BY getCsvEscapedString(CAST(COALESCE(c2.mang_campaign_name, '') AS VARCHAR)), COALESCE(CAST(a1.mang_advertiser_currency as VARCHAR), 'NA')
         |) OgbQueryAlias
         |)
         |        queryAlias LIMIT 200
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

    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    val expected =
      s"""
         |SELECT CAST(mang_ad_status as VARCHAR) AS mang_ad_status, CAST(mang_campaign_name as VARCHAR) AS mang_campaign_name, CAST(campaign_id as VARCHAR) AS campaign_id, CAST(mang_spend as VARCHAR) AS mang_spend
         |FROM(
         |SELECT mang_ad_status AS mang_ad_status, mang_campaign_name AS mang_campaign_name, campaign_id AS campaign_id, spend AS mang_spend
         |FROM(
         |SELECT COALESCE(CAST(a2.mang_ad_status as VARCHAR), 'NA') mang_ad_status, getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)) mang_campaign_name, COALESCE(a2.campaign_id, 0) campaign_id, SUM(spend) AS spend
         |FROM(SELECT campaign_id, ad_id, SUM(spend) spend
         |FROM ad_fact1
         |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY campaign_id, ad_id
         |
 |       )
         |af0
         |LEFT OUTER JOIN (
         |SELECT campaign_name AS mang_campaign_name, id c1_id
         |FROM campaign_presto_underlying
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c1
         |ON
         |af0.campaign_id = c1.c1_id
         |       LEFT OUTER JOIN (
         |SELECT campaign_id AS campaign_id, decodeUDF(status, 'ON', 'ON', 'OFF') AS mang_ad_status, id a2_id
         |FROM ad_dim_presto
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |a2
         |ON
         |af0.ad_id = a2.a2_id
         |
         |GROUP BY COALESCE(CAST(a2.mang_ad_status as VARCHAR), 'NA'), getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)), COALESCE(a2.campaign_id, 0)
         |) OgbQueryAlias
         |)
         |        queryAlias LIMIT 200
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

    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    val expected =
      s"""
         |SELECT CAST(mang_campaign_name as VARCHAR) AS mang_campaign_name, CAST(mang_average_cpc as VARCHAR) AS mang_average_cpc, CAST(mang_spend as VARCHAR) AS mang_spend
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, CASE WHEN clicks = 0 THEN 0.0 ELSE CAST(spend AS DOUBLE) / clicks END AS mang_average_cpc, spend AS mang_spend
         |FROM(
         |SELECT getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)) mang_campaign_name, SUM(spend) AS spend, SUM(clicks) AS clicks
         |FROM(SELECT campaign_id, SUM(spend) spend, SUM(clicks) clicks
         |FROM ad_fact1
         |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY campaign_id
         |
 |       )
         |af0
         |LEFT OUTER JOIN (
         |SELECT campaign_name AS mang_campaign_name, id c1_id
         |FROM campaign_presto_underlying
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c1
         |ON
         |af0.campaign_id = c1.c1_id
         |
         |GROUP BY getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR))
         |) OgbQueryAlias
         |)
         |        queryAlias LIMIT 200
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

    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    val expected =
      s"""
         |SELECT CAST(mang_campaign_name as VARCHAR) AS mang_campaign_name, CAST(mang_average_position as VARCHAR) AS mang_average_position, CAST(mang_spend as VARCHAR) AS mang_spend
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, avg_pos AS mang_average_position, spend AS mang_spend
         |FROM(
         |SELECT getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)) mang_campaign_name, (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE CAST(SUM(weighted_position * impressions) AS DOUBLE) / (SUM(impressions)) END) AS avg_pos, SUM(spend) AS spend, SUM(impressions) AS impressions, SUM(weighted_position) AS weighted_position
         |FROM(SELECT campaign_id, SUM(spend) spend, SUM(impressions) impressions, SUM(weighted_position) weighted_position
         |FROM ad_fact1
         |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY campaign_id
         |
 |       )
         |af0
         |LEFT OUTER JOIN (
         |SELECT campaign_name AS mang_campaign_name, id c1_id
         |FROM campaign_presto_underlying
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c1
         |ON
         |af0.campaign_id = c1.c1_id
         |
         |GROUP BY getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR))
         |) OgbQueryAlias
         |)
         |        queryAlias LIMIT 200
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

    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    val expected =
      s"""
         |SELECT CAST(mang_campaign_name as VARCHAR) AS mang_campaign_name, CAST(mang_average_position as VARCHAR) AS mang_average_position, CAST(mang_average_cpc as VARCHAR) AS mang_average_cpc, CAST(mang_spend as VARCHAR) AS mang_spend
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, avg_pos AS mang_average_position, CASE WHEN clicks = 0 THEN 0.0 ELSE CAST(spend AS DOUBLE) / clicks END AS mang_average_cpc, spend AS mang_spend
         |FROM(
         |SELECT getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)) mang_campaign_name, (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE CAST(SUM(weighted_position * impressions) AS DOUBLE) / (SUM(impressions)) END) AS avg_pos, SUM(spend) AS spend, SUM(clicks) AS clicks, SUM(impressions) AS impressions, SUM(weighted_position) AS weighted_position
         |FROM(SELECT campaign_id, SUM(spend) spend, SUM(clicks) clicks, SUM(impressions) impressions, SUM(weighted_position) weighted_position
         |FROM ad_fact1
         |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY campaign_id
         |
 |       )
         |af0
         |LEFT OUTER JOIN (
         |SELECT campaign_name AS mang_campaign_name, id c1_id
         |FROM campaign_presto_underlying
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c1
         |ON
         |af0.campaign_id = c1.c1_id
         |
         |GROUP BY getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR))
         |) OgbQueryAlias
         |)
         |        queryAlias LIMIT 200
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

    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    val expected =
      s"""
         |SELECT CAST(mang_campaign_name as VARCHAR) AS mang_campaign_name, CAST(advertiser_id as VARCHAR) AS advertiser_id, CAST(mang_n_average_cpc as VARCHAR) AS mang_n_average_cpc, CAST(mang_spend as VARCHAR) AS mang_spend
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, advertiser_id AS advertiser_id, CASE WHEN decodeUDF(stats_source, 1, clicks, 0.0) = 0 THEN 0.0 ELSE CAST(decodeUDF(stats_source, 1, spend, 0.0) AS DOUBLE) / decodeUDF(stats_source, 1, clicks, 0.0) END AS mang_n_average_cpc, spend AS mang_spend
         |FROM(
         |SELECT getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)) mang_campaign_name, COALESCE(advertiser_id, 0) advertiser_id, SUM(spend) AS spend, SUM(clicks) AS clicks, COALESCE(stats_source, 0) stats_source
         |FROM(SELECT advertiser_id, campaign_id, SUM(spend) spend, SUM(clicks) clicks, stats_source
         |FROM ad_fact1
         |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY advertiser_id, campaign_id, stats_source
         |
 |       )
         |af0
         |LEFT OUTER JOIN (
         |SELECT campaign_name AS mang_campaign_name, id c1_id
         |FROM campaign_presto_underlying
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c1
         |ON
         |af0.campaign_id = c1.c1_id
         |
         |GROUP BY getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)), COALESCE(advertiser_id, 0), COALESCE(stats_source, 0)
         |) OgbQueryAlias
         |)
         |        queryAlias LIMIT 200
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

    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    val expected =
      s"""
         |SELECT CAST(mang_campaign_name as VARCHAR) AS mang_campaign_name, CAST(mang_impression_share as VARCHAR) AS mang_impression_share, CAST(mang_spend as VARCHAR) AS mang_spend
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, impression_share_rounded AS mang_impression_share, spend AS mang_spend
         |FROM(
         |SELECT getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)) mang_campaign_name, SUM(spend) AS spend, SUM(impressions) AS impressions, SUM(s_impressions) AS s_impressions, COALESCE(show_flag, 0) show_flag, (ROUND((decodeUDF(MAX(show_flag), 1, ROUND(CASE WHEN SUM(s_impressions) = 0 THEN 0.0 ELSE CAST(SUM(impressions) AS DOUBLE) / (SUM(s_impressions)) END, 4), NULL)), 5)) AS impression_share_rounded
         |FROM(SELECT campaign_id, SUM(spend) spend, SUM(impressions) impressions, SUM(s_impressions) s_impressions, show_flag
         |FROM ad_fact1
         |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY campaign_id, show_flag
         |
 |       )
         |af0
         |LEFT OUTER JOIN (
         |SELECT campaign_name AS mang_campaign_name, id c1_id
         |FROM campaign_presto_underlying
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c1
         |ON
         |af0.campaign_id = c1.c1_id
         |
         |GROUP BY getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)), COALESCE(show_flag, 0)
         |) OgbQueryAlias
         |)
         |        queryAlias LIMIT 200
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

    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    val expected =
      s"""
         |SELECT CAST(mang_ad_status as VARCHAR) AS mang_ad_status, CAST(mang_campaign_name as VARCHAR) AS mang_campaign_name, CAST(campaign_id as VARCHAR) AS campaign_id, CAST(mang_spend as VARCHAR) AS mang_spend, CAST(mang_engagement_rate as VARCHAR) AS mang_engagement_rate
         |FROM(
         |SELECT mang_ad_status AS mang_ad_status, mang_campaign_name AS mang_campaign_name, campaign_id AS campaign_id, spend AS mang_spend, 100 * mathUDF(engagement_count, impressions) AS mang_engagement_rate
         |FROM(
         |SELECT COALESCE(CAST(a2.mang_ad_status as VARCHAR), 'NA') mang_ad_status, getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)) mang_campaign_name, COALESCE(a2.campaign_id, 0) campaign_id, SUM(spend) AS spend, SUM(engagement_count) AS engagement_count, SUM(impressions) AS impressions
         |FROM(SELECT ad_id, campaign_id, SUM(spend) spend, SUM(engagement_count) engagement_count, SUM(impressions) impressions
         |FROM ad_fact1
         |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY ad_id, campaign_id
         |
 |       )
         |af0
         |LEFT OUTER JOIN (
         |SELECT campaign_name AS mang_campaign_name, id c1_id
         |FROM campaign_presto_underlying
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c1
         |ON
         |af0.campaign_id = c1.c1_id
         |       LEFT OUTER JOIN (
         |SELECT campaign_id AS campaign_id, decodeUDF(status, 'ON', 'ON', 'OFF') AS mang_ad_status, id a2_id
         |FROM ad_dim_presto
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |a2
         |ON
         |af0.ad_id = a2.a2_id
         |
         |GROUP BY COALESCE(CAST(a2.mang_ad_status as VARCHAR), 'NA'), getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)), COALESCE(a2.campaign_id, 0)
         |) OgbQueryAlias
         |)
         |        queryAlias LIMIT 200
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

    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    val expected =
      s"""
         |SELECT CAST(mang_ad_status as VARCHAR) AS mang_ad_status, CAST(mang_campaign_name as VARCHAR) AS mang_campaign_name, CAST(campaign_id as VARCHAR) AS campaign_id, CAST(mang_spend as VARCHAR) AS mang_spend, CAST(mang_engagement_rate as VARCHAR) AS mang_engagement_rate, CAST(mang_paid_engagement_rate as VARCHAR) AS mang_paid_engagement_rate
         |FROM(
         |SELECT mang_ad_status AS mang_ad_status, mang_campaign_name AS mang_campaign_name, campaign_id AS campaign_id, spend AS mang_spend, 100 * mathUDF(engagement_count, impressions) AS mang_engagement_rate, 100 * mathUDAF(engagement_count, 0, 0, clicks, impressions) AS mang_paid_engagement_rate
         |FROM(
         |SELECT COALESCE(CAST(a2.mang_ad_status as VARCHAR), 'NA') mang_ad_status, getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)) mang_campaign_name, COALESCE(a2.campaign_id, 0) campaign_id, SUM(spend) AS spend, SUM(clicks) AS clicks, SUM(engagement_count) AS engagement_count, SUM(impressions) AS impressions
         |FROM(SELECT ad_id, campaign_id, SUM(spend) spend, SUM(clicks) clicks, SUM(engagement_count) engagement_count, SUM(impressions) impressions
         |FROM ad_fact1
         |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY ad_id, campaign_id
         |
 |       )
         |af0
         |LEFT OUTER JOIN (
         |SELECT campaign_name AS mang_campaign_name, id c1_id
         |FROM campaign_presto_underlying
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c1
         |ON
         |af0.campaign_id = c1.c1_id
         |       LEFT OUTER JOIN (
         |SELECT campaign_id AS campaign_id, decodeUDF(status, 'ON', 'ON', 'OFF') AS mang_ad_status, id a2_id
         |FROM ad_dim_presto
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |a2
         |ON
         |af0.ad_id = a2.a2_id
         |
         |GROUP BY COALESCE(CAST(a2.mang_ad_status as VARCHAR), 'NA'), getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)), COALESCE(a2.campaign_id, 0)
         |) OgbQueryAlias
         |)
         |        queryAlias LIMIT 200
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

    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    val expected =
      s"""
         |SELECT CAST(mang_ad_status as VARCHAR) AS mang_ad_status, CAST(ad_group_id as VARCHAR) AS ad_group_id, CAST(advertiser_id as VARCHAR) AS advertiser_id, CAST(mang_campaign_name as VARCHAR) AS mang_campaign_name, CAST(campaign_id as VARCHAR) AS campaign_id, CAST(mang_spend as VARCHAR) AS mang_spend, CAST(mang_engagement_rate as VARCHAR) AS mang_engagement_rate, CAST(mang_paid_engagement_rate as VARCHAR) AS mang_paid_engagement_rate
         |FROM(
         |SELECT mang_ad_status AS mang_ad_status, ad_group_id AS ad_group_id, advertiser_id AS advertiser_id, mang_campaign_name AS mang_campaign_name, campaign_id AS campaign_id, spend AS mang_spend, 100 * mathUDF(engagement_count, impressions) AS mang_engagement_rate, 100 * mathUDAF(engagement_count, 0, 0, clicks, impressions) AS mang_paid_engagement_rate
         |FROM(
         |SELECT COALESCE(CAST(a2.mang_ad_status as VARCHAR), 'NA') mang_ad_status, COALESCE(ad_group_id, 0) ad_group_id, COALESCE(advertiser_id, 0) advertiser_id, getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)) mang_campaign_name, COALESCE(a2.campaign_id, 0) campaign_id, SUM(spend) AS spend, SUM(clicks) AS clicks, SUM(engagement_count) AS engagement_count, SUM(impressions) AS impressions
         |FROM(SELECT advertiser_id, ad_id, campaign_id, ad_group_id, SUM(spend) spend, SUM(clicks) clicks, SUM(engagement_count) engagement_count, SUM(impressions) impressions
         |FROM ad_fact1
         |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY advertiser_id, ad_id, campaign_id, ad_group_id
         |
 |       )
         |af0
         |LEFT OUTER JOIN (
         |SELECT campaign_name AS mang_campaign_name, id c1_id
         |FROM campaign_presto_underlying
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c1
         |ON
         |af0.campaign_id = c1.c1_id
         |       LEFT OUTER JOIN (
         |SELECT campaign_id AS campaign_id, decodeUDF(status, 'ON', 'ON', 'OFF') AS mang_ad_status, id a2_id
         |FROM ad_dim_presto
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |a2
         |ON
         |af0.ad_id = a2.a2_id
         |
         |GROUP BY COALESCE(CAST(a2.mang_ad_status as VARCHAR), 'NA'), COALESCE(ad_group_id, 0), COALESCE(advertiser_id, 0), getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)), COALESCE(a2.campaign_id, 0)
         |) OgbQueryAlias
         |)
         |        queryAlias LIMIT 200
         """.stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("generating presto outer group by query with greater than filter") {
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

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    val expected =
      s"""
         |SELECT CAST(mang_advertiser_name as VARCHAR) AS mang_advertiser_name, CAST(mang_impressions as VARCHAR) AS mang_impressions
         |FROM(
         |SELECT mang_advertiser_name AS mang_advertiser_name, impressions AS mang_impressions
         |FROM(
         |SELECT COALESCE(CAST(a1.mang_advertiser_name as VARCHAR), 'NA') mang_advertiser_name, SUM(impressions) AS impressions
         |FROM(SELECT account_id, SUM(impressions) impressions
         |FROM s_stats_fact_underlying
         |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY account_id
         |HAVING (SUM(impressions) > 1608)
         |       )
         |ssfu0
         |LEFT OUTER JOIN (
         |SELECT name AS mang_advertiser_name, id a1_id
         |FROM advertiser_presto
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
         |)
         |a1
         |ON
         |ssfu0.account_id = a1.a1_id
         |
         |GROUP BY COALESCE(CAST(a1.mang_advertiser_name as VARCHAR), 'NA')
         |ORDER BY mang_advertiser_name DESC, impressions DESC) OgbQueryAlias
         |)
         |        queryAlias LIMIT 200
         """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("generating presto query with outer group containing derived columns of level 2") {
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
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    val expected =
      s"""
         |SELECT CAST(mang_campaign_name as VARCHAR) AS mang_campaign_name, CAST(mang_average_cpc as VARCHAR) AS mang_average_cpc, CAST(mang_average_position as VARCHAR) AS mang_average_position, CAST(mang_impressions as VARCHAR) AS mang_impressions
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, CASE WHEN clicks = 0 THEN 0.0 ELSE CAST(spend AS DOUBLE) / clicks END AS mang_average_cpc, avg_pos AS mang_average_position, impressions AS mang_impressions
         |FROM(
         |SELECT getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)) mang_campaign_name, (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE CAST(SUM(weighted_position * impressions) AS DOUBLE) / (SUM(impressions)) END) AS avg_pos, SUM(impressions) AS impressions, SUM(clicks) AS clicks, SUM(spend) AS spend, SUM(weighted_position) AS weighted_position
         |FROM(SELECT campaign_id, SUM(impressions) impressions, SUM(clicks) clicks, SUM(spend) spend, SUM(weighted_position) weighted_position
         |FROM s_stats_fact_underlying
         |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY campaign_id
         |
 |       )
         |ssfu0
         |LEFT OUTER JOIN (
         |SELECT campaign_name AS mang_campaign_name, id c1_id
         |FROM campaign_presto_underlying
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c1
         |ON
         |CAST(ssfu0.campaign_id AS VARCHAR) = CAST(c1.c1_id AS VARCHAR)
         |
         |GROUP BY getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR))
         |) OgbQueryAlias
         |)
         |        queryAlias LIMIT 200
         """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("generating presto query with outer group by, 2 dim joins and sorting") {
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
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    val expected =
      s"""
         |SELECT CAST(advertiser_id as VARCHAR) AS advertiser_id, CAST(mang_campaign_name as VARCHAR) AS mang_campaign_name, CAST(mang_advertiser_name as VARCHAR) AS mang_advertiser_name, CAST(mang_average_cpc as VARCHAR) AS mang_average_cpc, CAST(mang_average_position as VARCHAR) AS mang_average_position, CAST(mang_impressions as VARCHAR) AS mang_impressions
         |FROM(
         |SELECT advertiser_id AS advertiser_id, mang_campaign_name AS mang_campaign_name, mang_advertiser_name AS mang_advertiser_name, CASE WHEN clicks = 0 THEN 0.0 ELSE CAST(spend AS DOUBLE) / clicks END AS mang_average_cpc, avg_pos AS mang_average_position, impressions AS mang_impressions
         |FROM(
         |SELECT COALESCE(c2.advertiser_id, 0) advertiser_id, getCsvEscapedString(CAST(COALESCE(c2.mang_campaign_name, '') AS VARCHAR)) mang_campaign_name, COALESCE(CAST(a1.mang_advertiser_name as VARCHAR), 'NA') mang_advertiser_name, (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE CAST(SUM(weighted_position * impressions) AS DOUBLE) / (SUM(impressions)) END) AS avg_pos, SUM(impressions) AS impressions, SUM(clicks) AS clicks, SUM(spend) AS spend, SUM(weighted_position) AS weighted_position
         |FROM(SELECT account_id, campaign_id, SUM(impressions) impressions, SUM(clicks) clicks, SUM(spend) spend, SUM(weighted_position) weighted_position
         |FROM s_stats_fact_underlying
         |WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY account_id, campaign_id
         |
 |       )
         |ssfu0
         |LEFT OUTER JOIN (
         |SELECT name AS mang_advertiser_name, id a1_id
         |FROM advertiser_presto
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
         |)
         |a1
         |ON
         |ssfu0.account_id = a1.a1_id
         |       LEFT OUTER JOIN (
         |SELECT advertiser_id AS advertiser_id, campaign_name AS mang_campaign_name, id c2_id
         |FROM campaign_presto_underlying
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c2
         |ON
         |CAST(ssfu0.campaign_id AS VARCHAR) = CAST(c2.c2_id AS VARCHAR)
         |
         |GROUP BY COALESCE(c2.advertiser_id, 0), getCsvEscapedString(CAST(COALESCE(c2.mang_campaign_name, '') AS VARCHAR)), COALESCE(CAST(a1.mang_advertiser_name as VARCHAR), 'NA')
         |ORDER BY impressions DESC, mang_advertiser_name DESC) OgbQueryAlias
         |)
         |        queryAlias LIMIT 200
         """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("generating presto query with outer group by,  Noop rollup cols") {
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
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    val expected =
      s"""
         |SELECT CAST(mang_campaign_name as VARCHAR) AS mang_campaign_name, CAST(mang_clicks as VARCHAR) AS mang_clicks, CAST(mang_impressions as VARCHAR) AS mang_impressions, CAST(mang_click_rate as VARCHAR) AS mang_click_rate
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, clicks AS mang_clicks, impressions AS mang_impressions, mang_click_rate AS mang_click_rate
         |FROM(
         |SELECT getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)) mang_campaign_name, SUM(clicks) AS clicks, SUM(impressions) AS impressions, (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE CAST(SUM(clicks) AS DOUBLE) / (SUM(impressions)) END) AS mang_click_rate
         |FROM(SELECT campaign_id, SUM(clicks) clicks, SUM(impressions) impressions
         |FROM ad_fact1
         |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY campaign_id
         |
 |       )
         |af0
         |LEFT OUTER JOIN (
         |SELECT campaign_name AS mang_campaign_name, id c1_id
         |FROM campaign_presto_underlying
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c1
         |ON
         |af0.campaign_id = c1.c1_id
         |
         |GROUP BY getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR))
         |) OgbQueryAlias
         |)
         |        queryAlias LIMIT 200
         """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("generating presto query with outer group by, derived cols") {
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
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    val expected =
      s"""
         |SELECT CAST(mang_campaign_name as VARCHAR) AS mang_campaign_name, CAST(mang_n_clicks as VARCHAR) AS mang_n_clicks, CAST(mang_pricing_type as VARCHAR) AS mang_pricing_type, CAST(mang_static_mapping_string as VARCHAR) AS mang_static_mapping_string, CAST(mang_impressions as VARCHAR) AS mang_impressions, CAST(mang_n_spend as VARCHAR) AS mang_n_spend, CAST(mang_month as VARCHAR) AS mang_month, CAST(mang_clicks as VARCHAR) AS mang_clicks
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, decodeUDF(stats_source, 1, clicks, 0.0) AS mang_n_clicks, mang_pricing_type AS mang_pricing_type, mang_static_mapping_string AS mang_static_mapping_string, impressions AS mang_impressions, decodeUDF(stats_source, 1, spend, 0.0) AS mang_n_spend, mang_month, clicks AS mang_clicks
         |FROM(
         |SELECT getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)) mang_campaign_name, CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END mang_pricing_type, CASE WHEN (sm_string IN ('Y')) THEN 'Yes' WHEN (sm_string IN ('N')) THEN 'No' ELSE 'NA' END mang_static_mapping_string, SUM(impressions) AS impressions, getFormattedDate(mang_month) mang_month, SUM(clicks) AS clicks, COALESCE(stats_source, 0) stats_source, SUM(spend) AS spend
         |FROM(SELECT sm_string, price_type, campaign_id, dateUDF(stats_date, 'M') mang_month, SUM(clicks) clicks, SUM(impressions) impressions, stats_source, SUM(spend) spend
         |FROM ad_fact1
         |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY sm_string, price_type, campaign_id, dateUDF(stats_date, 'M'), stats_source
         |
 |       )
         |af0
         |LEFT OUTER JOIN (
         |SELECT campaign_name AS mang_campaign_name, id c1_id
         |FROM campaign_presto_underlying
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c1
         |ON
         |af0.campaign_id = c1.c1_id
         |
         |GROUP BY getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)), CASE WHEN (price_type IN (1)) THEN 'CPC' WHEN (price_type IN (6)) THEN 'CPV' WHEN (price_type IN (2)) THEN 'CPA' WHEN (price_type IN (-10)) THEN 'CPE' WHEN (price_type IN (-20)) THEN 'CPF' WHEN (price_type IN (7)) THEN 'CPCV' WHEN (price_type IN (3)) THEN 'CPM' ELSE 'NONE' END, CASE WHEN (sm_string IN ('Y')) THEN 'Yes' WHEN (sm_string IN ('N')) THEN 'No' ELSE 'NA' END, getFormattedDate(mang_month), COALESCE(stats_source, 0)
         |ORDER BY mang_campaign_name DESC, impressions DESC) OgbQueryAlias
         |)
         |        queryAlias LIMIT 200       """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
  }

  test("generating presto query with outer group by, having and partitioning scheme, test part col renderer") {
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
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    val expected =
      s"""
         |SELECT CAST(mang_campaign_name as VARCHAR) AS mang_campaign_name, CAST(mang_bid_strategy as VARCHAR) AS mang_bid_strategy, CAST(mang_advertiser_name as VARCHAR) AS mang_advertiser_name, CAST(advertiser_id as VARCHAR) AS advertiser_id, CAST(mang_impressions as VARCHAR) AS mang_impressions, CAST(mang_test_constant_col as VARCHAR) AS mang_test_constant_col, CAST(mang_load_time as VARCHAR) AS mang_load_time
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, mang_bid_strategy AS mang_bid_strategy, mang_advertiser_name AS mang_advertiser_name, advertiser_id AS advertiser_id, actual_impressions AS mang_impressions, mang_test_constant_col AS mang_test_constant_col, mang_load_time
         |FROM(
         |SELECT getCsvEscapedString(CAST(COALESCE(c2.mang_campaign_name, '') AS VARCHAR)) mang_campaign_name, CASE WHEN (bid_strategy IN (1)) THEN 'Max Click' WHEN (bid_strategy IN (2)) THEN 'Inflection Point' ELSE 'NONE' END mang_bid_strategy, COALESCE(CAST(a1.mang_advertiser_name as VARCHAR), 'NA') mang_advertiser_name, COALESCE(c2.advertiser_id, 0) advertiser_id, SUM(actual_impressions) AS actual_impressions, ROUND(COALESCE(test_constant_col, 0), 10) mang_test_constant_col, COALESCE(CAST(mang_load_time as VARCHAR), 'NA') mang_load_time
         |FROM(SELECT bid_strategy, account_id, load_time, 'test_constant_col_value' AS test_constant_col, campaign_id, SUM(actual_impressions) actual_impressions
         |FROM bidreco_complete
         |WHERE (account_id = 12345) AND (status = 'Valid') AND (factPartCol = 123)
         |GROUP BY bid_strategy, account_id, load_time, test_constant_col, campaign_id
         |HAVING (SUM(actual_impressions) > 1608)
         |       )
         |bc0
         |LEFT OUTER JOIN (
         |SELECT name AS mang_advertiser_name, id a1_id
         |FROM advertiser_presto
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
         |)
         |a1
         |ON
         |bc0.account_id = a1.a1_id
         |       LEFT OUTER JOIN (
         |SELECT advertiser_id AS advertiser_id, campaign_name AS mang_campaign_name, id c2_id
         |FROM campaign_presto_underlying
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c2
         |ON
         |bc0.campaign_id = c2.c2_id
         |
 |GROUP BY getCsvEscapedString(CAST(COALESCE(c2.mang_campaign_name, '') AS VARCHAR)), CASE WHEN (bid_strategy IN (1)) THEN 'Max Click' WHEN (bid_strategy IN (2)) THEN 'Inflection Point' ELSE 'NONE' END, COALESCE(CAST(a1.mang_advertiser_name as VARCHAR), 'NA'), COALESCE(c2.advertiser_id, 0), ROUND(COALESCE(test_constant_col, 0), 10), COALESCE(CAST(mang_load_time as VARCHAR), 'NA')
         |ORDER BY mang_campaign_name DESC, actual_impressions DESC) OgbQueryAlias
         |)
         |        queryAlias LIMIT 200
       """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
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
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

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
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    val expected =
      s"""
         |SELECT CAST(mang_campaign_name as VARCHAR) AS mang_campaign_name, CAST(mang_advertiser_name as VARCHAR) AS mang_advertiser_name, CAST(advertiser_id as VARCHAR) AS advertiser_id, CAST(mang_bid_strategy as VARCHAR) AS mang_bid_strategy, CAST(mang_impressions as VARCHAR) AS mang_impressions, CAST(mang_clicks as VARCHAR) AS mang_clicks, CAST(mang_bid_modifier_fact as VARCHAR) AS mang_bid_modifier_fact, CAST(mang_modified_bid_fact as VARCHAR) AS mang_modified_bid_fact
         |FROM(
         |SELECT mang_campaign_name AS mang_campaign_name, mang_advertiser_name AS mang_advertiser_name, advertiser_id AS advertiser_id, mang_bid_strategy AS mang_bid_strategy, actual_impressions AS mang_impressions, actual_clicks AS mang_clicks, mang_bid_modifier_fact AS mang_bid_modifier_fact, mang_modified_bid_fact AS mang_modified_bid_fact
         |FROM(
         |SELECT getCsvEscapedString(CAST(COALESCE(c2.mang_campaign_name, '') AS VARCHAR)) mang_campaign_name, COALESCE(CAST(a1.mang_advertiser_name as VARCHAR), 'NA') mang_advertiser_name, COALESCE(c2.advertiser_id, 0) advertiser_id, CASE WHEN (bid_strategy IN (1)) THEN 'Max Click' WHEN (bid_strategy IN (2)) THEN 'Inflection Point' ELSE 'NONE' END mang_bid_strategy, SUM(actual_impressions) AS actual_impressions, SUM(actual_clicks) AS actual_clicks, SUM(recommended_bid) AS recommended_bid, (coalesce(CASE WHEN (getAbyB(recommended_bid, actual_clicks)) = 0 THEN 1 WHEN IS_NAN((getAbyB(recommended_bid, actual_clicks))) THEN 1 ELSE (getAbyB(recommended_bid, actual_clicks)) END, 1)) AS mang_bid_modifier_fact, SUM(coalesce(CASE WHEN coalesce(CASE WHEN (getAbyB(recommended_bid, actual_clicks)) = 0 THEN 1 WHEN IS_NAN((getAbyB(recommended_bid, actual_clicks))) THEN 1 ELSE (getAbyB(recommended_bid, actual_clicks)) END, 1) = 0 THEN 1 WHEN IS_NAN(coalesce(CASE WHEN (getAbyB(recommended_bid, actual_clicks)) = 0 THEN 1 WHEN IS_NAN((getAbyB(recommended_bid, actual_clicks))) THEN 1 ELSE (getAbyB(recommended_bid, actual_clicks)) END, 1)) THEN 1 ELSE coalesce(CASE WHEN (getAbyB(recommended_bid, actual_clicks)) = 0 THEN 1 WHEN IS_NAN((getAbyB(recommended_bid, actual_clicks))) THEN 1 ELSE (getAbyB(recommended_bid, actual_clicks)) END, 1) END, 1) * (getAbyB(recommended_bid, actual_impressions))) AS mang_modified_bid_fact
         |FROM(SELECT bid_strategy, account_id, campaign_id, SUM(actual_clicks) actual_clicks, SUM(actual_impressions) actual_impressions, SUM(recommended_bid) recommended_bid
         |FROM bidreco_complete
         |WHERE (account_id = 12345) AND (status = 'Valid') AND (factPartCol = 123)
         |GROUP BY bid_strategy, account_id, campaign_id
         |HAVING (SUM(actual_impressions) > 1608)
         |       )
         |bc0
         |LEFT OUTER JOIN (
         |SELECT name AS mang_advertiser_name, id a1_id
         |FROM advertiser_presto
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (id = 12345)
         |)
         |a1
         |ON
         |bc0.account_id = a1.a1_id
         |       LEFT OUTER JOIN (
         |SELECT advertiser_id AS advertiser_id, campaign_name AS mang_campaign_name, id c2_id
         |FROM campaign_presto_underlying
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
         |)
         |c2
         |ON
         |bc0.campaign_id = c2.c2_id
         |
 |GROUP BY getCsvEscapedString(CAST(COALESCE(c2.mang_campaign_name, '') AS VARCHAR)), COALESCE(CAST(a1.mang_advertiser_name as VARCHAR), 'NA'), COALESCE(c2.advertiser_id, 0), CASE WHEN (bid_strategy IN (1)) THEN 'Max Click' WHEN (bid_strategy IN (2)) THEN 'Inflection Point' ELSE 'NONE' END
         |ORDER BY mang_campaign_name DESC, actual_impressions DESC) OgbQueryAlias
         |)
         |        queryAlias LIMIT 200
       """.stripMargin

    result should equal (expected) (after being whiteSpaceNormalised)
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

    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    val expected =
      s"""
         |SELECT CAST(mang_spend as VARCHAR) AS mang_spend
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
         |FROM campaign_presto_underlying
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345) AND (lower(campaign_name) = lower('cmpgn_1'))
         |)
         |c1
         |ON
         |af0.campaign_id = c1.c1_id
         |
         |
 |) OgbQueryAlias
         |)
         |        queryAlias LIMIT 200
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

    val request: ReportingRequest = getReportingRequestAsync(jsonString)

    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipelineForQgenVersion(registry, requestModel.toOption.get, Version.v1)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    println(result)

    val expected =
      s"""
         |SELECT CAST(mang_source as VARCHAR) AS mang_source, CAST(mang_n_spend as VARCHAR) AS mang_n_spend
         |FROM(
         |SELECT mang_source AS mang_source, decodeUDF(stats_source, 1, spend, 0.0) AS mang_n_spend
         |FROM(
         |SELECT COALESCE(stats_source, 0) mang_source, SUM(spend) AS spend, stats_source AS stats_source
         |FROM(SELECT campaign_id, stats_source, SUM(spend) spend
         |FROM ad_fact1
         |WHERE (advertiser_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
         |GROUP BY campaign_id, stats_source
         |
 |       )
         |af0
         |JOIN (
         |SELECT id c1_id
         |FROM campaign_presto_underlying
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345) AND (lower(campaign_name) = lower('cmpgn_1'))
         |)
         |c1
         |ON
         |af0.campaign_id = c1.c1_id
         |
         |GROUP BY COALESCE(stats_source, 0), stats_source
         |) OgbQueryAlias
         |)
         |        queryAlias LIMIT 200
       """.stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }
}
