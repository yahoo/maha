// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core.RequestModel
import com.yahoo.maha.core.query.oracle.BaseOracleQueryGeneratorTest
import com.yahoo.maha.core.request.ReportingRequest
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Created by hiral on 3/15/16.
 */
class NoopRowListTest extends AnyFunSuite with Matchers with BaseOracleQueryGeneratorTest {
  def query : Query = {
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
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery]
  }
  
  test("test addRow") {
    intercept[UnsupportedOperationException] {

      NoopRowList(query).addRow(null)
    }
  }
  test("test isEmpty") {
    intercept[UnsupportedOperationException] {
      NoopRowList(query).isEmpty
    }
  }
  test("test foreach") {
    intercept[UnsupportedOperationException] {
      NoopRowList(query).foreach(r => println(r))
    }
  }
  test("test map") {
    intercept[UnsupportedOperationException] {
      NoopRowList(query).map(r => r.cols)
    }
  }
  test("RowList LifeCycle Tests") {
    intercept[UnsupportedOperationException] {
      val nrl : Unit = NoopRowList(query).foreach(r => r)
      val rl = nrl.asInstanceOf[RowList]
      rl.withLifeCycle {
        rl.nextStage()
      }
    }
  }
}
