package com.yahoo.maha.core.query

import com.yahoo.maha.core.RequestModel
import com.yahoo.maha.core.query.oracle.BaseOracleQueryGeneratorTest
import com.yahoo.maha.core.request.ReportingRequest

class FactDrivenPartialRowListTest extends BaseOracleQueryGeneratorTest with BaseRowListTest {
  def queryWithMultipleFactDimCols : Query = {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Campaign ID"},
                            {"field": "Impressions"},
                            {"field": "Campaign Name"},
                            {"field": "Campaign Status"},
                            {"field": "Pricing Type"},
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

  test("Create and merge dim-fact rowList Fact Driven") {
    //Rows should flatten & retain on unique Campaign ID and Pricing Type.
    val rowList: FactDrivenPartialRowList = FactDrivenPartialRowList(RowGrouping("Campaign ID", List("Pricing Type")), queryWithMultipleFactDimCols)

    //Rows returned by the queried Fact table.
    val factRow1 = rowList.newRow
    factRow1.addValues(List(("Campaign ID", 124), ("Impressions", 150), ("CTR", 2.25), ("Pricing Type", "CPC")))
    val factRow2 = rowList.newRow
    factRow2.addValues(List(("Campaign ID", 124), ("Impressions", 150), ("CTR", 2.20), ("Pricing Type", "CPA")))

    //Rows returned by the queried Dimension table.  Expect Campaign 123 to be removed since it isn't in the fact.
    val dimRow1 = rowList.newRow
    dimRow1.addValues(List(("Campaign ID", 123), ("Campaign Name", "MegaCampaign"), ("Campaign Status", "ON")))
    val dimRow2 = rowList.newRow
    dimRow2.addValues(List(("Campaign ID", 124), ("Campaign Name", "MegaCampaign"), ("Campaign Status", "OFF")))

    //Add & merge all rows.
    rowList.addRow(factRow1)
    rowList.addRow(factRow2)
    rowList.updateRow(dimRow1)
    rowList.updateRow(dimRow2)

    assert(rowList.updatedSize == 2)

    //Expect the Fact query to only update the relevant Dimension row.
    val expectedMergedRows: List[String] = List(
      s"""Row(Map(Campaign ID -> 0, Pricing Type -> 4, Campaign Status -> 3, Impressions -> 1, Campaign Name -> 2, CTR -> 5, TOTALROWS -> 6),ArrayBuffer(124, 150, MegaCampaign, OFF, CPC, 2.25, null))"""
      , s"""Row(Map(Campaign ID -> 0, Pricing Type -> 4, Campaign Status -> 3, Impressions -> 1, Campaign Name -> 2, CTR -> 5, TOTALROWS -> 6),ArrayBuffer(124, 150, MegaCampaign, OFF, CPA, 2.2, null))""")

    assert(rowList.length == 2)
    rowList.foreach(row => assert(expectedMergedRows.contains(row.toString)))
  }
}
