// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.presto

import com.yahoo.maha.core._
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.request.ReportingRequest


class PrestoQueryGeneratorTest extends BasePrestoQueryGeneratorTest {

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

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))


    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString

    assert(result != null && result.length > 0)

    println(result)

    val expected = s"""SELECT CAST(mang_day as VARCHAR) AS mang_day, CAST(advertiser_id as VARCHAR) AS advertiser_id, CAST(campaign_id as VARCHAR) AS campaign_id, CAST(mang_campaign_name as VARCHAR) AS mang_campaign_name, CAST(ad_group_id as VARCHAR) AS ad_group_id, CAST(keyword_id as VARCHAR) AS keyword_id, CAST(mang_keyword as VARCHAR) AS mang_keyword, CAST(mang_search_term as VARCHAR) AS mang_search_term, CAST(mang_delivered_match_type as VARCHAR) AS mang_delivered_match_type, CAST(mang_impressions as VARCHAR) AS mang_impressions, CAST(mang_ad_group_start_date_full as VARCHAR) AS mang_ad_group_start_date_full, CAST(mang_clicks as VARCHAR) AS mang_clicks, CAST(mang_average_cpc as VARCHAR) AS mang_average_cpc
                      |FROM(
                      |SELECT getFormattedDate(stats_date) mang_day, COALESCE(account_id, 0) advertiser_id, COALESCE(CAST(ssfu0.campaign_id as VARCHAR), 'NA') campaign_id, getCsvEscapedString(CAST(COALESCE(c1.mang_campaign_name, '') AS VARCHAR)) mang_campaign_name, COALESCE(ad_group_id, 0) ad_group_id, COALESCE(keyword_id, 0) keyword_id, getCsvEscapedString(CAST(COALESCE(keyword, '') AS VARCHAR)) mang_keyword, COALESCE(CAST(search_term as VARCHAR), 'None') mang_search_term, COALESCE(CAST(delivered_match_type as varchar), 'NA') mang_delivered_match_type, COALESCE(impressions, 0) mang_impressions, COALESCE(CAST(mang_ad_group_start_date_full as VARCHAR), 'NA') mang_ad_group_start_date_full, COALESCE(mang_clicks, 0) mang_clicks, ROUND(COALESCE((CASE WHEN clicks = 0 THEN 0.0 ELSE CAST(spend AS DOUBLE) / clicks END), 0), 10) mang_average_cpc
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

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))


    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString

    assert(result != null && result.length > 0)

    val expected = s"""SELECT CAST(advertiser_id as VARCHAR) AS advertiser_id, CAST(mang_impressions as VARCHAR) AS mang_impressions
    FROM(
      SELECT COALESCE(account_id, 0) advertiser_id, COALESCE(impressions, 0) mang_impressions
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

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))


    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString

    assert(result != null && result.length > 0)

    val expected = s"""SELECT CAST(advertiser_id as VARCHAR) AS advertiser_id, CAST(mang_impressions as VARCHAR) AS mang_impressions
    FROM(
      SELECT COALESCE(account_id, 0) advertiser_id, COALESCE(impressions, 0) mang_impressions
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
                              {"field": "Impressions"}
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

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))


    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString

    assert(result != null && result.length > 0)

    val expected = s"""SELECT CAST(advertiser_id as VARCHAR) AS advertiser_id, CAST(mang_impressions as VARCHAR) AS mang_impressions
                      |FROM(
                      |SELECT COALESCE(account_id, 0) advertiser_id, COALESCE(impressions, 0) mang_impressions
                      |FROM(SELECT account_id, SUM(impressions) impressions
                      |FROM s_stats_fact_underlying
          WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
                      |GROUP BY account_id
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

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))


    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    
    assert(result != null && result.length > 0)

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

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString
    
    assert(result != null && result.length > 0 && result.contains("campaign_presto_underlying"))
  }

  test("Duplicate registration of the generator") {
    val failRegistry = new QueryGeneratorRegistry
    val dummyPrestoQueryGenerator = new QueryGenerator[WithPrestoEngine] {
      override def generate(queryContext: QueryContext): Query = { null }
      override def engine: Engine = OracleEngine
    }
    val dummyFalseQueryGenerator = new QueryGenerator[WithDruidEngine] {
      override def generate(queryContext: QueryContext): Query = { null }
      override def engine: Engine = DruidEngine
    }
    failRegistry.register(PrestoEngine, dummyPrestoQueryGenerator)
    failRegistry.register(DruidEngine, dummyFalseQueryGenerator)

    PrestoQueryGenerator.register(failRegistry,DefaultPartitionColumnRenderer, TestPrestoUDFRegistrationFactory())
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

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))


    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString

    println(result)
    assert(result != null && result.length > 0)

    val expected =
      s"""
         |SELECT CAST(advertiser_id as VARCHAR) AS advertiser_id, CAST(mang_impressions as VARCHAR) AS mang_impressions
         |FROM(
         |SELECT COALESCE(account_id, 0) advertiser_id, COALESCE(impressions, 0) mang_impressions
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

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))


    val result =  queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[PrestoQuery].asString

    println(result)
    assert(result != null && result.length > 0)

    val expected =
      s"""
         |SELECT CAST(advertiser_id as VARCHAR) AS advertiser_id, CAST(mang_impressions as VARCHAR) AS mang_impressions
         |FROM(
         |SELECT COALESCE(account_id, 0) advertiser_id, COALESCE(impressions, 0) mang_impressions
         |FROM(SELECT account_id, SUM(impressions) impressions
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

}
