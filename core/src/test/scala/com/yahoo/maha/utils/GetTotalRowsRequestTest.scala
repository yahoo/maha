package com.yahoo.maha.utils

import com.yahoo.maha.core.query.druid.DruidQueryGenerator
import com.yahoo.maha.core.query.hive.HiveQueryGenerator
import com.yahoo.maha.core.query.oracle.OracleQueryGenerator
import com.yahoo.maha.core.{DefaultPartitionColumnRenderer, DruidEngine, Engine, HiveEngine, OracleEngine, RequestModel, TestUDFRegistrationFactory}
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.request.ReportingRequest
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class GetTotalRowsRequestTest extends FunSuite with Matchers with BeforeAndAfterAll with BaseQueryGeneratorTest with SharedDimSchema with BaseQueryContextTest {

  def updateRowList(rowList: RowList) : Unit = {
    val row = rowList.newRow
    rowList.columnNames.foreach {
      case "TOTALROWS" => row.addValue("TOTALROWS", 42)
      case otherwise => row.addValue(otherwise, s"$otherwise-value")
    }
    rowList.addRow(row)
  }

  def getQueryExecutorContext: QueryExecutorContext = {
    val qeOracle = new QueryExecutor {
      override def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes) : QueryResult[T] = {
        updateRowList(rowList)
        QueryResult(rowList, queryAttributes, QueryResultStatus.SUCCESS)
      }
      override def engine: Engine = OracleEngine
    }

    val qeHive = new QueryExecutor {
      override def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes) : QueryResult[T] = {
        updateRowList(rowList)
        QueryResult(rowList, queryAttributes, QueryResultStatus.SUCCESS)      }
      override def engine: Engine = HiveEngine
    }
    val qeDruid = new QueryExecutor {
      override def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes) : QueryResult[T] = {
        updateRowList(rowList)
        QueryResult(rowList, queryAttributes, QueryResultStatus.SUCCESS)      }
      override def engine: Engine = DruidEngine
    }

    val qec = new QueryExecutorContext
    qec.register(qeOracle)
    qec.register(qeHive)
    qec.register(qeDruid)
    qec
  }

  override protected def beforeAll(): Unit = {
    OracleQueryGenerator.register(queryGeneratorRegistry, DefaultPartitionColumnRenderer)
    DruidQueryGenerator.register(queryGeneratorRegistry)
    HiveQueryGenerator.register(queryGeneratorRegistry, DefaultPartitionColumnRenderer, TestUDFRegistrationFactory())
  }

  val inputValidJSON : String =
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

  test("GetTotalRows: Well-formed row request should not fail") {
    val request: ReportingRequest = ReportingRequest.forceOracle(getReportingRequestSync(inputValidJSON))
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    val queryContext: QueryExecutorContext = getQueryExecutorContext

    val totalRowRequest = GetTotalRowsRequest.getTotalRows(pipeline, registry, queryContext)
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

    val queryContext: QueryExecutorContext = getQueryExecutorContext

    val totalRowRequest = GetTotalRowsRequest.getTotalRows(pipeline, registry, queryContext)
    assert(totalRowRequest.isSuccess, "Total Row Request Failed!")
  }

  test("Forcing query to Druid engine should succeed if Oracle is available"){
    val request: ReportingRequest = ReportingRequest.forceDruid(getReportingRequestSync(inputValidJSON))
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    val queryContext: QueryExecutorContext = getQueryExecutorContext

    val totalRowRequest = GetTotalRowsRequest.getTotalRows(pipeline, registry, queryContext)
    assert(totalRowRequest.isSuccess, "Requests outside of Oracle should trace back into Oracle, if applicable")
  }

  test("Forcing query to Hive engine should succeed if Oracle is available"){
    val request: ReportingRequest = ReportingRequest.forceHive(getReportingRequestAsync(inputValidJSON))
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))

    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    val pipeline = queryPipelineTry.toOption.get

    val queryContext: QueryExecutorContext = getQueryExecutorContext

    val totalRowRequest = GetTotalRowsRequest.getTotalRows(pipeline, registry, queryContext)
    assert(totalRowRequest.isSuccess, "Requests outside of Oracle should trace back into Oracle, if applicable")
  }
}
