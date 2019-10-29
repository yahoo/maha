// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core.query.oracle.BaseOracleQueryGeneratorTest
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.core.{OracleEngine, RequestModel}
import org.scalatest.{FunSuite, Matchers}

/**
 * Created by pranavbhole on 06/12/16.
 */
class QueryTest extends FunSuite with Matchers with BaseOracleQueryGeneratorTest {
  def query: Query = {
    val jsonString =
              s"""{
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
                                    {"field": "Advertiser ID", "operator": "=", "value": "213"},
                                    {"field": "Campaign Name", "operator": "=", "value": "MegaCampaign"}
                                  ],
                                  "sortBy": [
                                    {"field": "Campaign Name", "order": "Asc"}
          ],
          "paginationStartIndex":-1,
          "rowsPerPage":100
        }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery]
  }

  test("QueryTest") {
    assume(query.asString != null)
    assume(query.isInstanceOf[OracleQuery])
    assume(query.engine == OracleEngine)
    val p = query
    assert(!query.aliasColumnMapJava.isEmpty)
  }

  test("NOOP QueryTest") {
    val nq= NoopQuery
    intercept[UnsupportedOperationException] {
    nq.asString
    }
    intercept[UnsupportedOperationException] {
      nq.queryContext
    }
    intercept[UnsupportedOperationException] {
      nq.additionalColumns
    }
    intercept[UnsupportedOperationException] {
      nq.engine
    }
    intercept[UnsupportedOperationException] {
      nq.aliasColumnMap
    }
  }
}
