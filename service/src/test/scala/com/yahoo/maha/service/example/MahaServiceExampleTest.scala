// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.example

import com.yahoo.maha.core.bucketing._
import com.yahoo.maha.core.query.{QueryRowList, Version}
import com.yahoo.maha.core.request._
import com.yahoo.maha.log.MultiColoMahaRequestLogWriter
import com.yahoo.maha.parrequest2.GeneralError
import com.yahoo.maha.parrequest2.future.ParFunction
import com.yahoo.maha.service._
import com.yahoo.maha.service.error.MahaServiceBadRequestException
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import grizzled.slf4j.Logging
import org.scalatest.BeforeAndAfterAll

/**
 * Created by pranavbhole on 09/06/17.
 */
class MahaServiceExampleTest extends BaseMahaServiceTest with Logging with BeforeAndAfterAll {

  override protected def afterAll(): Unit =  {
    super.afterAll()
    server.shutdownNow()
  }

  val jsonRequestHive = s"""{
                          "cube": "student_performance",
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Marks Obtained"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"}
                          ],
                         "sortBy": [
                            {"field": "Marks Obtained", "order": "Asc"}
                          ]
                        }"""

  val reportingRequestHive  =  {
    val reportingRequestHiveResult = ReportingRequest.deserializeAsync(jsonRequestHive.getBytes, schema = StudentSchema)
    require(reportingRequestHiveResult.isSuccess)
    ReportingRequest.forceHive(reportingRequestHiveResult.toOption.get)
  }

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

    val mahaRequestContext = MahaRequestContext(REGISTRY,
      bucketParams,
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")

    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter)

    val requestModelResultTry  = mahaService.generateRequestModel("er", reportingRequest, bucketParams)
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
    assert(resultFailureToProcessModel.isLeft)

    //Test General Error in process Request Model
    val processRequestModelWithFailure = mahaService.processRequestModel("er", requestModelResultTry.get.model, mahaRequestLogHelper)
    assert(processRequestModelWithFailure.isLeft)

    //Create tables
    createTables()

    // Execute Model Test
    val result = mahaService.executeRequestModelResult("er", requestModelResultTry.get, mahaRequestLogHelper).prodRun.get(10000)
    assert(result.isRight)
    assert(result.right.get.queryPipelineResult.rowList.asInstanceOf[QueryRowList].columnNames.contains("Student ID"))

    // Process Model Test
    val processRequestModelResult  = mahaService.processRequestModel("er", requestModelResultTry.get.model, mahaRequestLogHelper)
    assert(processRequestModelResult.isRight)
    assert(processRequestModelResult.right.get.queryPipelineResult.rowList.asInstanceOf[QueryRowList].columnNames.contains("Class ID"))

    // Process Request Test
    val processRequestResult = mahaService.processRequest("er", reportingRequest, bucketParams, mahaRequestLogHelper)
    assert(processRequestResult.isRight)
    assert(processRequestResult.right.get.queryPipelineResult.rowList.asInstanceOf[QueryRowList].columnNames.contains("Class ID"))

    //ExecuteRequest Test
    val executeRequestParRequestResult = mahaService.executeRequest("er", reportingRequest, bucketParams, mahaRequestLogHelper)
    assert(executeRequestParRequestResult.prodRun.get(10000).isRight)
    val requestResultOption  = Option(executeRequestParRequestResult.prodRun.get(10000))
    assert(requestResultOption.get.right.get.queryPipelineResult.rowList.asInstanceOf[QueryRowList].columnNames.contains("Total Marks"))

    // Domain Tests
    val domainJsonOption = mahaService.getDomain("er")
    assert(domainJsonOption.isDefined)
    assert(domainJsonOption.get.contains("""{"dimensions":[{"name":"section","fields":["Section ID","Student ID","Section Status","Class ID","Section Name"],"fieldsWithSchemas":[{"name":"Section Status","allowedSchemas":[]},{"name":"Class ID","allowedSchemas":[]},{"name":"Section ID","allowedSchemas":[]},{"name":"Section Name","allowedSchemas":[]},{"name":"Student ID","allowedSchemas":[]}]},{"name":"class","fields":["Class Name","Class Status","Professor Name","Class ID"],"fieldsWithSchemas":[{"name":"Class Name","allowedSchemas":[]},{"name":"Class Status","allowedSchemas":[]},{"name":"Professor Name","allowedSchemas":[]},{"name":"Class ID","allowedSchemas":[]}]},{"name":"student","fields":["Profile URL","Student Name","Student ID","Student Status"],"fieldsWithSchemas":[{"name":"Profile URL","allowedSchemas":[]},{"name":"Student Name","allowedSchemas":[]},{"name":"Student ID","allowedSchemas":[]},{"name":"Student Status","allowedSchemas":[]}]}],"schemas":{"student":["student_performance","student_performance2"]},"cubes":[{"name":"student_performance","mainEntityIds":{"student":"Student ID"},"maxDaysLookBack":[{"requestType":"SyncRequest","grain":"DailyGrain","days":30},{"requestType":"AsyncRequest","grain":"DailyGrain","days":30}],"maxDaysWindow":[{"requestType":"SyncRequest","grain":"DailyGrain","days":20},{"requestType":"AsyncRequest","grain":"DailyGrain","days":20},{"requestType":"SyncRequest","grain":"HourlyGrain","days":20},{"requestType":"AsyncRequest","grain":"HourlyGrain","days":20}],"fields":[{"field":"Class ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"class","filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Day","type":"Dimension","dataType":{"type":"Date","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Month","type":"Dimension","dataType":{"type":"Date","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Remarks","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","=","LIKE"],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Section ID","type":"Dimension","dataType":{"type":"Number","constraint":"3"},"dimensionName":null,"filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Student ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"student","filterable":true,"filterOperations":["IN","LIKE","=","BETWEEN","=="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Top Student ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["=="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Year","type":"Dimension","dataType":{"type":"Enum","constraint":"Freshman|Junior|Sophomore|Senior"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Marks Obtained","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","LIKE","=","BETWEEN","=="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null,"allowedSchemas":null},{"field":"Performance Factor","type":"Fact","dataType":{"type":"Number","constraint":"10"},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null,"allowedSchemas":null},{"field":"Total Marks","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","LIKE","=","BETWEEN","=="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null,"allowedSchemas":null}]},{"name":"student_performance2","mainEntityIds":{"student":"Student ID"},"maxDaysLookBack":[{"requestType":"SyncRequest","grain":"DailyGrain","days":30},{"requestType":"AsyncRequest","grain":"DailyGrain","days":30}],"maxDaysWindow":[{"requestType":"SyncRequest","grain":"DailyGrain","days":20},{"requestType":"AsyncRequest","grain":"DailyGrain","days":20},{"requestType":"SyncRequest","grain":"HourlyGrain","days":20},{"requestType":"AsyncRequest","grain":"HourlyGrain","days":20}],"fields":[{"field":"Class ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"class","filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Day","type":"Dimension","dataType":{"type":"Date","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Month","type":"Dimension","dataType":{"type":"Date","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Section ID","type":"Dimension","dataType":{"type":"Number","constraint":"3"},"dimensionName":"section","filterable":true,"filterOperations":["IN","NOT IN","="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Student ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"student","filterable":true,"filterOperations":["IN","=","=="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Top Student ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["=="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Year","type":"Dimension","dataType":{"type":"Enum","constraint":"Freshman|Junior|Sophomore|Senior"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Marks Obtained","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null,"allowedSchemas":null},{"field":"Total Marks","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null,"allowedSchemas":null}]}]}"""))
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

    val mahaRequestProcessor = new MahaSyncRequestProcessor(mahaRequestContext,
      DefaultRequestCoordinator(mahaService),
      mahaServiceConfig.mahaRequestLogWriter
    )

    def fn = {
      (requestCoordinatorResult: RequestCoordinatorResult) => {
        val requestResult = requestCoordinatorResult.successResults.head._2
        assert(requestResult.queryPipelineResult.rowList.columns.size  ==  4)
        assert(requestResult.queryPipelineResult.rowList.asInstanceOf[QueryRowList].columnNames.contains("Total Marks"))
        logger.info("Inside onSuccess function")
      }
    }

    mahaRequestProcessor.onSuccess(fn)
    mahaRequestProcessor.onFailure((error: GeneralError) => logger.error(error.message))

    mahaRequestProcessor.process()
    val thrown = intercept[IllegalArgumentException] {
      val failedProcessor = MahaSyncRequestProcessor(mahaRequestContext, DefaultRequestCoordinator(mahaService), mahaServiceConfig.mahaRequestLogWriter)
      failedProcessor.process()
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

    val mahaRequestContext = MahaRequestContext(REGISTRY,
      bucketParams,
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")
    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter)

    val requestModelResultTry  = mahaService.generateRequestModel("er", reportingRequest, bucketParams)
    assert(requestModelResultTry.isSuccess)

    val processRequestResult = mahaService.processRequest("er", reportingRequest, bucketParams, mahaRequestLogHelper)
    assert(processRequestResult.isRight)

    val parRequestResult = mahaService.executeRequest(REGISTRY, ReportingRequest.forceHive(reportingRequest),bucketParams, mahaRequestLogHelper)
    val result = parRequestResult.prodRun.get(1000)
    assert(result.isRight)

    assert(mahaServiceConfig.mahaRequestLogWriter.isInstanceOf[MultiColoMahaRequestLogWriter])
  }

  test("Test RequestModel Failure using mahaService") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Student Name"},
                            {"field": "Admitted Year"},
                            {"field": "Student Status Unknown Column"}
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

    val mahaRequestContext = MahaRequestContext(REGISTRY,
      bucketParams,
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")
    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter)

    val requestModelResultTry  = mahaService.generateRequestModel("er", reportingRequest, bucketParams)
    assert(requestModelResultTry.isFailure)

    val exception = intercept[MahaServiceBadRequestException] {
      mahaService.processRequest("er", reportingRequest, bucketParams, mahaRequestLogHelper)
    }
    assert(exception.source.get.getMessage.contains("ERROR_CODE:10005 Failed to find primary key alias for Student Status Unknown Column"))

    val executionException = intercept[MahaServiceBadRequestException] {
          val parRequestResult = mahaService.executeRequest(REGISTRY, ReportingRequest.forceHive(reportingRequest),bucketParams, mahaRequestLogHelper)
       assert(parRequestResult.prodRun.get(800).isLeft)
    }
    val thrown = intercept[IllegalArgumentException] {
       mahaService.executeRequest("unknown", ReportingRequest.forceHive(reportingRequest),bucketParams, mahaRequestLogHelper)
    }

    assert(executionException.source.get.getMessage.contains("ERROR_CODE:10005 Failed to find primary key alias for Student Status Unknown Column"))
  }

  test("Test MahaService with Example Schema generating valid query pipeline") {

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

    val mahaRequestContext = MahaRequestContext(REGISTRY,
      bucketParams,
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")

    val requestModelResultTry  = mahaService.generateRequestModel("er", reportingRequest, bucketParams)
    assert(requestModelResultTry.isSuccess)

    val queryPipelines = mahaService.generateQueryPipelines("er", requestModelResultTry.get.model, BucketParams())
    assert(queryPipelines._1.isSuccess)

    val queryPipelinesWithForceVersion = mahaService.generateQueryPipelines("er", requestModelResultTry.get.model, BucketParams(forceQueryGenVersion = Some(Version.v1)))
    assert(queryPipelinesWithForceVersion._1.isFailure)
  }

  test("Test Query Gen bucketing") {
    val bucketParams = BucketParams(userInfo = UserInfo("maha", true), forceRevision = Some(0))
    val requestModelHiveResultTry  = mahaService.generateRequestModel("er", reportingRequestHive, bucketParams)
    assert(requestModelHiveResultTry.isSuccess)

    val queryPipelinesWithQueryGenBuckets = mahaService.generateQueryPipelines("er", requestModelHiveResultTry.get.model, bucketParams)
    assert(queryPipelinesWithQueryGenBuckets._1.isSuccess)
    assert(queryPipelinesWithQueryGenBuckets._1.get.queryChain.drivingQuery.queryGenVersion == Some(Version.v0))
  }

}
