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
    println(result)
    assert(result != null && result.length > 0)
    val expected = s"""SELECT mang_day, advertiser_id, campaign_id, ad_group_id, keyword_id, mang_keyword, mang_search_term, mang_delivered_match_type, mang_impressions
FROM(
SELECT getFormattedDate(stats_date) mang_day, CAST(COALESCE(account_id, 0) as VARCHAR) advertiser_id, CAST(COALESCE(campaign_id, 0) as VARCHAR) campaign_id, CAST(COALESCE(ad_group_id, 0) as VARCHAR) ad_group_id, CAST(COALESCE(keyword_id, 0) as VARCHAR) keyword_id, getCsvEscapedString(CAST(COALESCE(keyword, '') AS VARCHAR)) mang_keyword, COALESCE(CAST(search_term as VARCHAR), 'None') mang_search_term, COALESCE(CAST(delivered_match_type as varchar), 'NA') mang_delivered_match_type, CAST(COALESCE(impressions, 0) as VARCHAR) mang_impressions
FROM(SELECT CASE WHEN (delivered_match_type IN (1)) THEN 'Exact' WHEN (delivered_match_type IN (2)) THEN 'Broad' WHEN (delivered_match_type IN (3)) THEN 'Phrase' ELSE 'UNKNOWN' END delivered_match_type, stats_date, keyword, ad_group_id, search_term, account_id, campaign_id, keyword_id, SUM(impressions) impressions
FROM s_stats_fact
WHERE (account_id = 12345) AND (stats_date >= '$fromDate' AND stats_date <= '$toDate')
GROUP BY CASE WHEN (delivered_match_type IN (1)) THEN 'Exact' WHEN (delivered_match_type IN (2)) THEN 'Broad' WHEN (delivered_match_type IN (3)) THEN 'Phrase' ELSE 'UNKNOWN' END, stats_date, keyword, ad_group_id, search_term, account_id, campaign_id, keyword_id

       )
ssf0
)""".stripMargin
    result should equal (expected) (after being whiteSpaceNormalised)
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
    println(result)
    assert(result != null && result.length > 0 && result.contains("campaign_presto_underlying"))
  }

}
