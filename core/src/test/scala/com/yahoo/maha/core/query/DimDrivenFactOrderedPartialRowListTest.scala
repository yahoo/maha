// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core.RequestModel
import com.yahoo.maha.core.query.oracle.BaseOracleQueryGeneratorTest
import com.yahoo.maha.core.request.ReportingRequest

/**
 * Created by pavanab on 3/29/16.
 */
class DimDrivenFactOrderedPartialRowListTest extends BaseOracleQueryGeneratorTest with BaseRowListTest {

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
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery]
  }

  test("successfully construct partial row list") {
    val rowList : DimDrivenPartialRowList = new DimDrivenPartialRowList(RowGrouping("Campaign ID", List("Campaign ID")), query)
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

    assert(!rowList.isEmpty)
    
    rowList.foreach(r => assert(r === row))
    rowList.map(r => assert(r === row))
    
    val lookupExisting =  rowList.getRowByIndex(RowGrouping(java.lang.Integer.valueOf(1).toString, List("1")))
    assert(lookupExisting.contains(row))
    
    val row2 = rowList.newRow
    row2.addValue("Campaign ID", java.lang.Integer.valueOf(1))
    row2.addValue("Impressions",java.lang.Integer.valueOf(2))

    rowList.addRow(row2)
    rowList.updateRow(row2)

    val row3 = rowList.newRow
    row3.addValue("Campaign ID", java.lang.Integer.valueOf(10))
    row3.addValue("Impressions",java.lang.Integer.valueOf(2))
    rowList.updateRow(row3)

    //Should have two unique rows
    val lookupExistingSecondRow =  rowList.getRowByIndex(RowGrouping(java.lang.Integer.valueOf(10).toString, List("10")))
    assert(lookupExistingSecondRow.contains(row3))

    assert(row.getValue("Campaign ID") === 1)
    assert(row.getValue("Impressions") === 2)
    assert(row.getValue("Campaign Name") === "name")
    assert(row.getValue("Campaign Status") === "on")
    assert(row.getValue("CTR") === 1.11D)
    assert(row.getValue("TOTALROWS") === 1)
    assert(rowList.updatedSize == 2, "Should have two rows")
  }

  test("successfully construct DimDrivenFactOrderedPartialRowListTest partial row list") {
    val rowList : DimDrivenFactOrderedPartialRowList = new DimDrivenFactOrderedPartialRowList(RowGrouping("Campaign ID", List("Campaign ID")), query)
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

    assert(rowList.size == 1)

    rowList.foreach(r => assert(r === row))
    rowList.map(r => assert(r === row))

    val lookupExisting =  rowList.getRowByIndex(RowGrouping(java.lang.Integer.valueOf(1).toString, List("1")))
    assert(lookupExisting.contains(row))

    val row2 = rowList.newRow
    row2.addValue("Campaign ID", java.lang.Integer.valueOf(1))
    row2.addValue("Impressions",java.lang.Integer.valueOf(2))

    rowList.addRow(row2)

    rowList.foreach(r => assert(r === row))
    rowList.map(r => assert(r === row))

    assert(row.getValue("Campaign ID") === 1)
    assert(row.getValue("Impressions") === 2)
    assert(row.getValue("Campaign Name") === "name")
    assert(row.getValue("Campaign Status") === "on")
    assert(row.getValue("CTR") === 1.11D)
    assert(row.getValue("TOTALROWS") === 1)
  }
}
