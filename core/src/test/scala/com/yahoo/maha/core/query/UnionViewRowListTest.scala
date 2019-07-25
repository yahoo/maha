// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core.{DecType, RequestModel}
import com.yahoo.maha.core.query.druid.{DruidQuery, DruidQueryGenerator}
import com.yahoo.maha.core.query.oracle.BaseOracleQueryGeneratorTest
import com.yahoo.maha.core.request.ReportingRequest

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

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
         |         "field": "Impressions",
         |         "operator": "In",
         |         "values": [
         |            "1"
         |         ]
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

    DruidQueryGenerator.register(queryGeneratorRegistry, useCustomRoundingSumAggregator = true)

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery]
  }

  test("successfully construct partial row list") {
    val rowList : UnionViewRowList = new
        UnionViewRowList(
          Set("Advertiser ID", "Day"),
          query,
          Map("Impressions" -> DecType(), "Spend" -> DecType()),
          constAliasToValueMapList = List(Map("Advertiser ID" -> "12345"), Map("Advertiser ID" -> "12345", "Impressions" -> "1.0")))

    assert(rowList.forall(row=>row.isInstanceOf[Row]))

    //Base rowList tests
    assert(rowList.columnNames === IndexedSeq("Advertiser ID", "Day", "Impressions", "Spend"), "Expected columns and produced columns do not match")
    assert(rowList.isEmpty, "No rows have yet been added")

    val row = rowList.newRow

    row.addValue("Advertiser ID",java.lang.Integer.valueOf(1))
    row.addValue("Day","2016-10-10")
    row.addValue("Impressions", 1.0)
    row.addValue("Spend", 3.0)

    rowList.addRow(row)

    //Adding a row
    assert(!rowList.isEmpty, "row should be added to rowList")
    assert(rowList.size == 1, "Exactly one row should have been added")


    rowList.foreach(r => assert(r === row))
    rowList.map(r => assert(r === row))

    //reference added row
    val lookupExisting =  rowList.getRowByIndexSet(Set("12345", "2016-10-10"))
    assert(lookupExisting.contains(row))

    val row2 = rowList.newRow
    row2.addValue("Advertiser ID", java.lang.Integer.valueOf(1))
    row2.addValue("Day", "2016-10-10")
    row2.addValue("Impressions",1.2)
    row2.addValue("Spend", -1.3)

    rowList.addRow(row2)

    //check summation on rows works, and row reference must be correct
    val groupedByRow =  rowList.getRowByIndexSet(Set("12345", "2016-10-10")).head
    assert(groupedByRow.getValue("Impressions") == 2.2)
    assert(groupedByRow.getValue("Spend") == 1.7)

    val invalidGroupedByRow = rowList.getRowByIndexSet(Set("12345"))
    assert(invalidGroupedByRow.isEmpty, "Invalid row keys should return nothing")

    //Verify public variables are accessible and basic functions work
    assert(Try{rowList.nextStage()}.isSuccess, "Next stage should not throw an error")
    assert(rowList.subQuery.isEmpty, "No valid subqueries to return")
    assert(Try{rowList.end()}.isSuccess, "The end of a union view row list should be reachable")

    //Attempt to index public members and add subQueries
    rowList.addSubQuery(rowList.query)
    assert(!rowList.subQuery.isEmpty, "Copy of current query should be added as rowList's subQuery")
    assert(rowList.keys.head == Set("12345", "2016-10-10"), "Head of rowList keys should be the Day value")
    assert(rowList.updatedSize == 1, "rowSet's updated size should be 1")
    assert(!rowList.isUpdatedRowListEmpty, "The updated list should not be empty")

    val temp = QueryPipeline.unionViewPartialRowList(List(query, query, query))
  }
}
