// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core.RequestModel
import com.yahoo.maha.core.query.oracle.BaseOracleQueryGeneratorTest
import com.yahoo.maha.core.request.ReportingRequest

/**
 * Created by hiral on 1/25/16.
 */
class PartialRowListTest extends BaseOracleQueryGeneratorTest with BaseRowListTest {

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


  test("successfully construct partial row list") {
    val rowList : DimDrivenPartialRowList = new DimDrivenPartialRowList(RowGrouping("Campaign ID", List.empty), query)
    assert(rowList.columnNames === IndexedSeq("Campaign ID", "Impressions", "Campaign Name", "Campaign Status", "CTR", "TOTALROWS"))
    assert(rowList.isEmpty)

    val row = rowList.newRow
    
    row.addValue("Campaign ID",java.lang.Integer.valueOf(1))
    row.addValue("Campaign Name","name")
    row.addValue("Campaign Status","on")
    row.addValue("CTR", java.lang.Double.valueOf(1.11D))
    row.addValue("TOTALROWS", java.lang.Integer.valueOf(1))

    assert(row.aliasMap.size === 6)
    assert(row.getValue(0) === 1)
    assert(row.getValue(1) === null)
    assert(row.getValue(2) === "name")
    assert(row.getValue(3) === "on")
    assert(row.getValue(4) === 1.11D)
    assert(row.getValue(5) === 1)
    assert(row.getValue("Campaign ID") === 1)
    assert(row.getValue("Impressions") === null)
    assert(row.getValue("Campaign Name") === "name")
    assert(row.getValue("Campaign Status") === "on")
    assert(row.getValue("CTR") === 1.11D)
    assert(row.getValue("TOTALROWS") === 1)

    rowList.addRow(row)

    assert(rowList.isEmpty == false)
    assert(rowList.size == 1)

    
    rowList.foreach(r => assert(r === row))
    rowList.map(r => assert(r === row))
    
    val lookupExisting =  rowList.getRowByIndex(RowGrouping(java.lang.Integer.valueOf(1).toString, List.empty))
    assert(lookupExisting.contains(row))
    
    val row2 = rowList.newRow
    row2.addValue("Campaign ID", java.lang.Integer.valueOf(1))
    row2.addValue("Impressions",java.lang.Integer.valueOf(2))

    rowList.addRow(row2)

    assert(row.getValue("Campaign ID") === 1)
    assert(row.getValue("Impressions") === 2)
    assert(row.getValue("Campaign Name") === "name")
    assert(row.getValue("Campaign Status") === "on")
    assert(row.getValue("CTR") === 1.11D)
    assert(row.getValue("TOTALROWS") === 1)
  }

  def factDrivenQuery : Query = {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Day"},
                            {"field": "Campaign ID"},
                            {"field": "Impressions"},
                            {"field": "Campaign Name"},
                            {"field": "Campaign Status"},
                            {"field": "CTR"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "213"}
                          ]
                        }"""

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery]
  }

  test("successfully construct Fact Driven partial row list and test One to Many Merge") {
    val rowList : FactDrivenPartialRowList = new FactDrivenPartialRowList(RowGrouping("Campaign ID", List.empty), factDrivenQuery)
    assert(rowList.columnNames === IndexedSeq("Day", "Campaign ID", "Impressions", "Campaign Name", "Campaign Status", "CTR"))
    assert(rowList.isEmpty)

    val row = rowList.newRow

    row.addValue("Campaign ID",java.lang.Integer.valueOf(1))
    row.addValue("Day", "2016-10-10")
    row.addValue("Campaign Name", null)
    row.addValue("Campaign Status", null)
    row.addValue("Impressions", 100)
    row.addValue("CTR", java.lang.Double.valueOf(1.11D))
    assert(row.aliasMap.size === 6)

    rowList.addRow(row)

    val row1 = rowList.newRow

    row1.addValue("Campaign ID",java.lang.Integer.valueOf(1))
    row1.addValue("Day", "2016-10-11")
    row1.addValue("Campaign Name", null)
    row1.addValue("Campaign Status", null)
    row1.addValue("Impressions", 100)
    row1.addValue("CTR", java.lang.Double.valueOf(1.11D))
    assert(row1.aliasMap.size === 6)

    rowList.addRow(row1)

    //fail to add row with null index alias value
    {
      val badrow = rowList.newRow

      badrow.addValue("Campaign ID", null)
      badrow.addValue("Day", "2016-10-11")
      badrow.addValue("Campaign Name", null)
      badrow.addValue("Campaign Status", null)
      badrow.addValue("Impressions", 100)
      badrow.addValue("CTR", java.lang.Double.valueOf(1.11D))
      assert(badrow.aliasMap.size === 6)

      rowList.addRow(badrow)
    }
    assert(rowList.size === 2)

    val lookupExisting =  rowList.getRowByIndex(RowGrouping(java.lang.Integer.valueOf(1).toString, List.empty))
    assert(lookupExisting.size == 2)
    assert(lookupExisting.contains(row))
    assert(lookupExisting.contains(row1))

    val row2 = rowList.newRow
    row2.addValue("Campaign ID", java.lang.Integer.valueOf(1))
    row2.addValue("Campaign Name","cname")
    row2.addValue("Campaign Status","cname")

    rowList.updateRow(row2)
    //fail to update row with null index alias value

    {
      val badrow = rowList.newRow

      row2.addValue("Campaign ID", null)
      row2.addValue("Campaign Name","cname")
      row2.addValue("Campaign Status","cname")
      rowList.updateRow(badrow)
    }
    assert(rowList.size === 2)

    rowList.foreach {
      row=>
        assert(row.getValue("Campaign ID") == 1)
        assert(row.getValue("Campaign Name") === "cname")
        assert(row.getValue("Campaign Status") === "cname")
        assert(row.getValue("Impressions") === 100)
    }
  }
}
