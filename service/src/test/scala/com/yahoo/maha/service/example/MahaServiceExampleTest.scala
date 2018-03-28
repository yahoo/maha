// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.example

import com.google.protobuf.ByteString
import com.yahoo.maha.core.RequestModel
import com.yahoo.maha.core.bucketing.{BucketParams, UserInfo}
import com.yahoo.maha.core.query.QueryRowList
import com.yahoo.maha.core.request._
import com.yahoo.maha.parrequest2.GeneralError
import com.yahoo.maha.parrequest2.future.ParFunction
import com.yahoo.maha.proto.MahaRequestLog.MahaRequestProto
import com.yahoo.maha.service._
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import grizzled.slf4j.Logging

/**
 * Created by pranavbhole on 09/06/17.
 */
class MahaServiceExampleTest extends BaseMahaServiceTest with Logging {

  test("Test MahaService with Example Schema") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Class ID"},
                            {"field": "Section ID"},
                            {"field": "Total Marks"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"}
                          ]
                        }"""
    val reportingRequestResult = ReportingRequest.deserializeSyncWithFactBias(jsonRequest.getBytes, schema = StudentSchema)
    require(reportingRequestResult.isSuccess)
    val reportingRequest = reportingRequestResult.toOption.get

    val bucketParams = BucketParams(UserInfo("uid", true))

    val mahaRequestLogHelper = MahaRequestLogHelper("er", mahaService)
    mahaRequestLogHelper.init(reportingRequest, None, MahaRequestProto.RequestType.SYNC, ByteString.copyFrom(jsonRequest.getBytes))

    val requestModelResultTry  = mahaService.generateRequestModel("er", reportingRequest, bucketParams, mahaRequestLogHelper)
    assert(requestModelResultTry.isSuccess)

    // Test General Error in Execute Model Test
    val resultFailure = mahaService.executeRequestModelResult("er", requestModelResultTry.get, mahaRequestLogHelper).prodRun.get(10000)
    assert(resultFailure.isLeft)
    val p = resultFailure.left.get
    assert(p.message.contains("""Failed to execute the query pipeline"""))

    // Test General Error in execute request
    val parRequestResultWithError = mahaService.executeRequest("er", reportingRequest, bucketParams, mahaRequestLogHelper)
    val q = parRequestResultWithError.prodRun.resultMap(
      ParFunction.from((t: RequestResult)
      => t)
    )
    assert(q.left.get.message.contains("""Failed to execute the query pipeline"""))

    // Test General Error in process Model
    val resultFailureToProcessModel = mahaService.processRequest("er", reportingRequest, bucketParams, mahaRequestLogHelper)
    assert(resultFailureToProcessModel.isFailure)

    //Test General Error in process Request Model
    val processRequestModelWithFailure = mahaService.processRequestModel("er", requestModelResultTry.get.model, mahaRequestLogHelper)
    assert(processRequestModelWithFailure.isFailure)

    //Create tables
    createTables()

    // Execute Model Test
    val result = mahaService.executeRequestModelResult("er", requestModelResultTry.get, mahaRequestLogHelper).prodRun.get(10000)
    assert(result.isRight)
    assert(result.right.get.rowList.asInstanceOf[QueryRowList].columnNames.contains("Student ID"))

    // Process Model Test
    val processRequestModelResult  = mahaService.processRequestModel("er", requestModelResultTry.get.model, mahaRequestLogHelper)
    assert(processRequestModelResult.isSuccess)
    assert(processRequestModelResult.get.rowList.asInstanceOf[QueryRowList].columnNames.contains("Class ID"))

    // Process Request Test
    val processRequestResult = mahaService.processRequest("er", reportingRequest, bucketParams, mahaRequestLogHelper)
    assert(processRequestResult.isSuccess)
    assert(processRequestResult.get.rowList.asInstanceOf[QueryRowList].columnNames.contains("Class ID"))

    //ExecuteRequest Test
    val executeRequestParRequestResult = mahaService.executeRequest("er", reportingRequest, bucketParams, mahaRequestLogHelper)
    assert(executeRequestParRequestResult.prodRun.get(10000).isRight)
    val requestResultOption  = Option(executeRequestParRequestResult.prodRun.get(10000))
    assert(requestResultOption.get.right.get.rowList.asInstanceOf[QueryRowList].columnNames.contains("Total Marks"))

    // Domain Tests
    val domainJsonOption = mahaService.getDomain("er")
    assert(domainJsonOption.isDefined)
    assert(domainJsonOption.get.contains("""{"dimensions":[{"name":"student","fields":["Student ID","Student Name","Student Status"]}],"schemas":{"student":["student_performance"]},"cubes":[{"name":"student_performance","mainEntityIds":{"student":"Student ID"},"maxDaysLookBack":[{"requestType":"SyncRequest","grain":"DailyGrain","days":30},{"requestType":"AsyncRequest","grain":"DailyGrain","days":30}],"maxDaysWindow":[{"requestType":"SyncRequest","grain":"DailyGrain","days":20},{"requestType":"AsyncRequest","grain":"DailyGrain","days":20},{"requestType":"SyncRequest","grain":"HourlyGrain","days":20},{"requestType":"AsyncRequest","grain":"HourlyGrain","days":20}],"fields":[{"field":"Class ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null},{"field":"Day","type":"Dimension","dataType":{"type":"Date","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null},{"field":"Remarks","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","=","LIKE"],"required":false,"filteringRequired":false,"incompatibleColumns":null},{"field":"Section ID","type":"Dimension","dataType":{"type":"Number","constraint":"3"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null},{"field":"Student ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"student","filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null},{"field":"Year","type":"Dimension","dataType":{"type":"Enum","constraint":"Freshman|Junior|Sophomore|Senior"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null},{"field":"Marks Obtained","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null},{"field":"Performance Factor","type":"Fact","dataType":{"type":"Number","constraint":"10"},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null},{"field":"Total Marks","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null}]}]}"""))
    val flattenDomainJsonOption = mahaService.getDomain("er")
    assert(flattenDomainJsonOption.isDefined)
    val cubeDomain = mahaService.getDomainForCube("er", "student_performance")
    assert(cubeDomain.isDefined)
    val flatDomain = mahaService.getFlattenDomain("er")
    assert(flatDomain.isDefined)
    val flatDomainWithoutRev = mahaService.getFlattenDomainForCube("er", "student_performance")
    assert(flatDomainWithoutRev.isDefined)
    val flatDomainWithRev = mahaService.getFlattenDomainForCube("er", "student_performance", Option(0))
    assert(flatDomainWithRev.isDefined)
    assert(!mahaService.getDomain("temp").isDefined)
    assert(!mahaService.getFlattenDomain("temp").isDefined)
    assert(!mahaService.getDomainForCube("temp", "inexistent").isDefined)
    assert(!mahaService.getFlattenDomainForCube("temp", "inexistent").isDefined)

    // test MahaRequestProcessor
    val mahaRequestProcessor : MahaRequestProcessor = MahaRequestProcessor("er", mahaService)

    def fn = {
      (requestModel: RequestModel, requestResult: RequestResult) => {
        assert(requestResult.rowList.columns.size  ==  4)
        assert(requestResult.rowList.asInstanceOf[QueryRowList].columnNames.contains("Total Marks"))
        println("Inside onSuccess function")
      }
    }

    mahaRequestProcessor.onSuccess(fn)
    mahaRequestProcessor.onFailure((error: GeneralError) => println(error.message))
    mahaRequestProcessor.withRequestModelValidator(
      (requestModelResult) => {
        // Defining the sample/custom post requestModelResultTry execution steps to be executed
        val model = requestModelResultTry.get.model
        if (model.hasNonDrivingDimNonFKNonPKFilter && model.hasLowCardinalityDimFilters && model.isSyncRequest) {
          warn("Costly Outer group by request with high SLA, should not be the SYNC request"+model)
        }
      }
    )

    mahaRequestProcessor.withRequestResultValidator(
      (requestResult) => {
        // Defining the sample/custom post requestResultTry execution steps to be executed
        val model = requestResult.rowList.asInstanceOf[QueryRowList].query.queryContext.requestModel
        val isFactOnlyOperation = model.dimensionsCandidates.isEmpty
        if(isFactOnlyOperation && model.includeRowCount) {
          requestResult.copy(totalRowsOption = Some(5000))
        }
      }
    )

    mahaRequestProcessor.process(bucketParams, reportingRequest, jsonRequest.getBytes)
    val thrown = intercept[IllegalArgumentException] {
      val failedProcessor = MahaRequestProcessor("er", mahaService)
      failedProcessor.process(bucketParams, reportingRequest, jsonRequest.getBytes)
    }
  }

  test("Test MahaService with Example Schema generating valid Dim Candidates") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Student Name"},
                            {"field": "Admitted Year"},
                            {"field": "Student Status"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"}
                          ],
                         "sortBy": [
                            {"field": "Admitted Year", "order": "Asc"},
                            {"field": "Student ID", "order": "Desc"}
                          ]
                        }"""

    val reportingRequestResult = ReportingRequest.deserializeSyncWithFactBias(jsonRequest.getBytes, schema = StudentSchema)
    require(reportingRequestResult.isSuccess)
    val reportingRequest = reportingRequestResult.toOption.get

    val bucketParams = BucketParams(UserInfo("uid", true))

    val mahaRequestLogHelper = MahaRequestLogHelper("er", mahaService)

    val requestModelResultTry  = mahaService.generateRequestModel("er", reportingRequest, bucketParams, mahaRequestLogHelper)
    assert(requestModelResultTry.isSuccess)

    val processRequestResult = mahaService.processRequest("er", reportingRequest, bucketParams, mahaRequestLogHelper)
    assert(processRequestResult.isFailure, "Request should fail with invalid SQL syntax.")
    }
}
