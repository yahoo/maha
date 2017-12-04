package com.yahoo.maha.utils

import com.yahoo.maha.core.query.druid.DruidQueryGenerator
import com.yahoo.maha.core.query.hive.HiveQueryGenerator
import com.yahoo.maha.core.query.oracle.OracleQueryGenerator
import com.yahoo.maha.core.{DefaultPartitionColumnRenderer, RequestModel, TestUDFRegistrationFactory}
import com.yahoo.maha.core.query.{BaseQueryContextTest, BaseQueryGeneratorTest, SharedDimSchema}
import com.yahoo.maha.core.request.ReportingRequest
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class GetTotalRowsRequestTest extends FunSuite with Matchers with BeforeAndAfterAll with BaseQueryGeneratorTest with SharedDimSchema with BaseQueryContextTest {

  override protected def beforeAll(): Unit = {
    OracleQueryGenerator.register(queryGeneratorRegistry, DefaultPartitionColumnRenderer)
    DruidQueryGenerator.register(queryGeneratorRegistry)
    HiveQueryGenerator.register(queryGeneratorRegistry, DefaultPartitionColumnRenderer, TestUDFRegistrationFactory())
  }

  val inputValidJSON =
    s"""{
       |                          "cube": "k_stats",
       |                          "selectFields": [
       |                              {"field": "Advertiser ID"},
       |                              {"field": "Advertiser Status"},
       |                              {"field": "Impressions"},
       |                              {"field": "Clicks"}
       |                          ],
       |                          "filterExpressions": [
       |                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
       |                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
       |                          ],
       |                          "sortBy": [
       |                              {"field": "Impressions", "order": "ASC"}
       |                          ],
       |                          "includeRowCount" : true,
       |                          "forceDimensionDriven": true,
       |                          "paginationStartIndex":0,
       |                          "rowsPerPage":100
       |                          }
       """.stripMargin

  val overMaxRowsJSON =
    s"""{
       |                          "cube": "k_stats",
       |                          "selectFields": [
       |                              {"field": "Advertiser ID"},
       |                              {"field": "Advertiser Status"},
       |                              {"field": "Impressions"},
       |                              {"field": "Clicks"}
       |                          ],
       |                          "filterExpressions": [
       |                              {"field": "Advertiser ID", "operator": "=", "value": "213"},
       |                              {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"}
       |                          ],
       |                          "sortBy": [
       |                              {"field": "Impressions", "order": "ASC"}
       |                          ],
       |                          "includeRowCount" : true,
       |                          "forceDimensionDriven": true,
       |                          "paginationStartIndex":0,
       |                          "rowsPerPage":100000
       |                          }
       """.stripMargin

  test("GetTotalRows: Well-formed row request should not fail") {
    val request: ReportingRequest = ReportingRequest.forceOracle(getReportingRequestSync(inputValidJSON))
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    val totalRowRequest = GetTotalRowsRequest.getTotalRows(request, pipeline, registry)
    assert(totalRowRequest.isSuccess, "Total Row Request Failed!")
  }

  test("GetTotalRows: Async Well-formed row request should not fail") {
    val request: ReportingRequest = ReportingRequest.forceOracle(getReportingRequestAsync(inputValidJSON))
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    val totalRowRequest = GetTotalRowsRequest.getTotalRows(request, pipeline, registry)
    assert(totalRowRequest.isSuccess, "Total Row Request Failed!")
  }

  test("Forcing query to Druid engine should cause failure"){
    val request: ReportingRequest = ReportingRequest.forceDruid(getReportingRequestSync(inputValidJSON))
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    val totalRowRequest = GetTotalRowsRequest.getTotalRows(request, pipeline, registry)
    assert(!totalRowRequest.isSuccess, "Requests outside of Oracle should fail to generate TOTALROWS column")
  }

  test("Forcing query to Hive engine should cause failure"){
    val request: ReportingRequest = ReportingRequest.forceHive(getReportingRequestAsync(inputValidJSON))
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    val totalRowRequest = GetTotalRowsRequest.getTotalRows(request, pipeline, registry)
    assert(!totalRowRequest.isSuccess, "Requests outside of Oracle should fail to generate TOTALROWS column")
  }

  test("Attempting too many rows in request should fail"){
    val request: ReportingRequest = ReportingRequest.forceOracle(getReportingRequestSync(overMaxRowsJSON))
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    val totalRowRequest = GetTotalRowsRequest.getTotalRows(request, pipeline, registry)
    assert(!totalRowRequest.isSuccess, "requested total tows should be less than the default max.")
  }
}
