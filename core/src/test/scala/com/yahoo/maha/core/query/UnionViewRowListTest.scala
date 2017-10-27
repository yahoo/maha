// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core.{DecType, RequestModel}
import com.yahoo.maha.core.query.druid.{DruidQueryGenerator, DruidQuery}
import com.yahoo.maha.core.query.oracle.BaseOracleQueryGeneratorTest
import com.yahoo.maha.core.request.ReportingRequest

/**
 * Created by pranavbhole on 18/11/16.
 */
class UnionViewRowListTest extends BaseOracleQueryGeneratorTest with BaseRowListTest {

  def query : Query = {
    val jsonString =
      s"""{ "cube": "a_stats",
         |   "selectFields": [
         |      {
         |         "field": "Advertiser ID"
         |      },
         |      {
         |         "field": "Day"
         |      },
         |      {
         |         "field": "Impressions"
         |      },
         |      {
         |         "field": "Spend"
         |      }
         |   ],
         |   "filterExpressions": [
         |      {
         |         "field": "Advertiser ID",
         |         "operator": "=",
         |         "value": "12345"
         |      },
         |      {
         |         "field": "Day",
         |         "operator": "Between",
         |         "from": "$fromDate",
         |         "to": "$toDate"
         |      }
         |   ]
         |}
      """.stripMargin

    val request: ReportingRequest = getReportingRequestSync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    DruidQueryGenerator.register(queryGeneratorRegistry)

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery]
  }

  test("successfully construct partial row list") {
    val rowList : UnionViewRowList = new UnionViewRowList(Set("Advertiser ID", "Day"), query, Map("Impressions" -> DecType(), "Spend" -> DecType()), List(Map.empty, Map.empty))
    assert(rowList.columnNames === IndexedSeq("Advertiser ID", "Day", "Impressions", "Spend"))
    assert(rowList.isEmpty)

    val row = rowList.newRow

    row.addValue("Advertiser ID",java.lang.Integer.valueOf(1))
    row.addValue("Day","2016-10-10")
    row.addValue("Impressions", 1.0)
    row.addValue("Spend", 3.0)

    rowList.addRow(row)

    assert(rowList.isEmpty == false)
    assert(rowList.size == 1)


    rowList.foreach(r => assert(r === row))
    rowList.map(r => assert(r === row))

    val lookupExisting =  rowList.getRowByIndexSet(Set("1", "2016-10-10"))
    assert(lookupExisting.contains(row))

    val row2 = rowList.newRow
    row2.addValue("Advertiser ID", java.lang.Integer.valueOf(1))
    row2.addValue("Day", "2016-10-10")
    row2.addValue("Impressions",1.2)
    row2.addValue("Spend", -1.3)

    rowList.addRow(row2)

    val groupedByRow =  rowList.getRowByIndexSet(Set("1", "2016-10-10")).head
    assert(groupedByRow.getValue("Impressions") == 2.2)
    assert(groupedByRow.getValue("Spend") == 1.7)
  }
}
