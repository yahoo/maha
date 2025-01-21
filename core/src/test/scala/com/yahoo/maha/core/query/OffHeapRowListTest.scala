package com.yahoo.maha.core.query

import java.util.concurrent.atomic.AtomicInteger

import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.parrequest2.future.ParFunction

/*
    Created by pranavbhole on 2/4/20
*/
class OffHeapRowListTest extends BaseRowListTest  {

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

  def queryWithAlias : Query = {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Campaign ID", "alias":"CampaignID"},
                            {"field": "Impressions", "alias":"Impressions"},
                            {"field": "Campaign Name", "alias":"CampaignName"},
                            {"field": "Campaign Status", "alias":"CampaignStatus"},
                            {"field": "CTR", "alias":"CTR"}
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

    val request: ReportingRequest = getReportingRequestAsync(jsonString)
    val registry = getDefaultRegistry()
    val requestModel = getRequestModel(request, registry)

    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery]
  }


  def createRows(rowList: OffHeapRowList): Unit = {
    Range.apply(100, 200).foreach {
      index =>
        val row = rowList.newRow
        row.addValue("Campaign ID", java.lang.Integer.valueOf(index))
        row.addValue("Impressions", java.lang.Integer.valueOf(2))
        row.addValue("Campaign Name", "\"name\"")
        row.addValue("Campaign Status", "o,n\n")
        row.addValue("CTR", java.lang.Double.valueOf(1.11D))
        row.addValue("TOTALROWS", java.lang.Integer.valueOf(100))

        rowList.addRow(row)
    }
  }

  def verifyRowList(verifyFn: (AtomicInteger) => Unit): Unit = {
    val rowCount = new AtomicInteger(0)
    verifyFn(rowCount)
    assert(rowCount.get() == 100)
  }

  def verifyRow(row:Row, rowCount: AtomicInteger): Boolean = {
    val value = row.getValue("Campaign ID")
    assert(value == rowCount.get()+100, "Order of the rows in the row list is not correct")
    assert(row.getValue("Impressions") == 2, "Inconsistent value for Impressions in row")
    rowCount.incrementAndGet()
    row.getValue("TOTALROWS") == 100
  }

  def genericRowListFunctionalVerification(rowList: OffHeapRowList): Unit = {
    rowList.withLifeCycle {
      createRows(rowList)
    }

    assert(rowList.size == 100, "Inconsistent size in rowList")

    assert(rowList.getTotalRowCount == 100, "Inconsistent total row count in rowList")

    verifyRowList {
      rowCount =>
        rowList.foreach(row => verifyRow(row, rowCount))
    }

    verifyRowList {
      rowCount=>
        rowList.forall(row=> {
          verifyRow(row, rowCount)
        })
    }

    verifyRowList {
      rowCount=>
        rowList.map(row=> {
          verifyRow(row, rowCount)
        })
    }

    verifyRowList {
      rowCount=>
        rowList.javaForeach(ParFunction.from(row=> {
          verifyRow(row, rowCount)
        }))
    }

    verifyRowList {
      rowCount=>
        rowList.javaMap(ParFunction.from(row=> {
          verifyRow(row, rowCount)
        }))
    }

  }

  test("Off Heap RowList test, cold start") {
    val rowList = OffHeapRowList(query, OffHeapRowListConfig(Some("target/"), inMemRowCountThreshold = 200))
    genericRowListFunctionalVerification(rowList)
    assert(rowList.isPersistentStoreInitialized() == false)
    assert(rowList.sizeOfInMemStore() == 100)
    assert(rowList.sizeOfRocksDB() == 0 )

    rowList.close()
  }


  test("Off Heap RowList test, warm start, init rocksdb") {
    val rowList = OffHeapRowList(query, OffHeapRowListConfig(Some("target/"), inMemRowCountThreshold = 10))
    genericRowListFunctionalVerification(rowList)
    assert(rowList.isPersistentStoreInitialized() == true)
    assert(rowList.sizeOfInMemStore() == 10)
    assert(rowList.sizeOfRocksDB() == 90)
    rowList.close()
  }

  test("Off Heap RowList test, warm start, init rocksdb, store all in rocksdb") {
    val rowList = OffHeapRowList(query, OffHeapRowListConfig(Some("target/"), inMemRowCountThreshold = 0))
    genericRowListFunctionalVerification(rowList)
    assert(rowList.isPersistentStoreInitialized() == true)
    assert(rowList.sizeOfInMemStore() == 0)
    assert(rowList.sizeOfRocksDB() == 100)
    rowList.close()
  }

}
