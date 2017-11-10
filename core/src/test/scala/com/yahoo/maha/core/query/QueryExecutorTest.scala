// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import com.yahoo.maha.core.query.oracle.BaseOracleQueryGeneratorTest
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.core.{Column, Engine, OracleEngine, RequestModel}
import com.yahoo.maha.report.RowCSVWriter
import org.scalatest.{FunSuite, Matchers}
import org.apache.commons.io.FileUtils
/**
 * Created by hiral on 3/15/16.
 */
class QueryExecutorTest extends FunSuite with Matchers with BaseOracleQueryGeneratorTest {

  test("successfully execute dummy query on query executor") {
    val qe = new QueryExecutor {

      val lifecycleListener : ExecutionLifecycleListener = new NoopExecutionLifecycleListener

      override def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes): QueryResult = {
        val acquiredQueryAttributes = lifecycleListener.acquired(query, queryAttributes)
        val startedQueryAttributes = lifecycleListener.started(query, acquiredQueryAttributes)
        QueryResult(rowList, lifecycleListener.completed(query, queryAttributes), QueryResultStatus.SUCCESS)
      }

      override def engine: Engine = OracleEngine
    }

    assert(qe.acceptEngine(OracleEngine))

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

    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
    
    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery
    qe.execute(query, new NoopRowList(query), QueryAttributes.empty)
  }

  test("successfully execute query on query executor with csvrowlist") {
    val qe = new QueryExecutor {

      val lifecycleListener : ExecutionLifecycleListener = new NoopExecutionLifecycleListener

      override def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes): QueryResult = {
        val acquiredQueryAttributes = lifecycleListener.acquired(query, queryAttributes)
        val startedQueryAttributes = lifecycleListener.started(query, acquiredQueryAttributes)
        val row = rowList.newRow
        row.addValue("Campaign ID", "test-camp-id")
        row.addValue("Impressions", "10")
        row.addValue("Campaign Name", "Test Campaign")
        row.addValue("Campaign Status", "active")
        row.addValue("CTR", "0.5")
        row.addValue("TOTALROWS", "1")
        rowList.start()
        rowList.addRow(row)
        rowList.end()
        QueryResult(rowList, lifecycleListener.completed(query, queryAttributes), QueryResultStatus.SUCCESS)
      }

      override def engine: Engine = OracleEngine
    }

    assert(qe.acceptEngine(OracleEngine))

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

    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val query = queryPipelineTry.toOption.get.queryChain.drivingQuery
    val tmpPath = new File("target").toPath
    val tmpFile = Files.createTempFile(tmpPath, "pre", ".csv").toFile
    tmpFile.deleteOnExit()
    val bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tmpFile), "UTF-8"))
    val csvWriter = new RowCSVWriter(bufferedWriter, ',', RowCSVWriter.DEFAULT_QUOTE_CHARACTER)
    val rowListWithHeaders : CSVRowList = new CSVRowList(query, csvWriter, true)
    qe.execute(query, rowListWithHeaders, QueryAttributes.empty)
    csvWriter.flush()
    val fileContents = FileUtils.readFileToString(tmpFile, StandardCharsets.UTF_8).trim
    val expectedContents = "Campaign ID,Impressions,Campaign Name,Campaign Status,CTR,TOTALROWS\ntest-camp-id,10,Test Campaign,active,0.5,1"
    assert(expectedContents.equals(fileContents), s"Expected: $expectedContents Actual: $fileContents")
  }

}
