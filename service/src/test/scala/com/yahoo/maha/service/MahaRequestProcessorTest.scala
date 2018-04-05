package com.yahoo.maha.service

import com.yahoo.maha.core.bucketing.{BucketParams, UserInfo}
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import org.scalatest.BeforeAndAfterAll

/**
 * Created by pranavbhole on 21/03/18.
 */
class MahaRequestProcessorTest extends BaseMahaServiceTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    createTables()
  }

  test("Test MahaRequestProcessor instantiation") {
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
    var assertCount = 0

    val mahaRequestProcessor = new MahaRequestProcessor(REGISTRY,
      DefaultRequestCoordinator(mahaService),
      mahaServiceConfig.mahaRequestLogWriter
    )
    mahaRequestProcessor.onSuccess((requestModel, requestResult) => {
      assert(requestResult.rowList.columns.nonEmpty)
      assertCount+=1
    })
    mahaRequestProcessor.onFailure((ge) => {
      assertCount-=1
    })
    mahaRequestProcessor.process(BucketParams(UserInfo("uid", true)), reportingRequest, jsonRequest.getBytes)

    Thread.sleep(1000)
    assert(assertCount == 1)
  }

  test("Test MahaRequestProcessor RequestModel Validation Failure") {
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
    var assertCount = 0

    val mahaRequestProcessor = new MahaRequestProcessor(REGISTRY,
      DefaultRequestCoordinator(mahaService),
      mahaServiceConfig.mahaRequestLogWriter
    )

    mahaRequestProcessor.onSuccess((requestModel, requestResult) => {
      assertCount+=1
    })

    mahaRequestProcessor.onFailure((ge) => {
      assertCount-=1
    })
    mahaRequestProcessor.process(BucketParams(UserInfo("uid", true)), reportingRequest, jsonRequest.getBytes)

    Thread.sleep(900)
    assert(assertCount == 1)
  }

  test("Test MahaRequestProcessor RequestResult Validation Failure") {
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
    var assertCount = 0;

    val mahaRequestLogHelper = MahaRequestLogHelper(REGISTRY, mahaServiceConfig.mahaRequestLogWriter)

    val mahaRequestProcessor = new MahaRequestProcessor(REGISTRY,
      DefaultRequestCoordinator(mahaService),
      mahaServiceConfig.mahaRequestLogWriter,
      mahaRequestLogHelperOption = Some(mahaRequestLogHelper)
    )

    mahaRequestProcessor.onSuccess((requestModel, requestResult) => {
      throw new IllegalArgumentException("failed in success function")
      assertCount-=1
    })

    mahaRequestProcessor.onFailure((ge) => {
      assertCount+=1
    })
    mahaRequestProcessor.process(BucketParams(UserInfo("uid", true)), reportingRequest, jsonRequest.getBytes)

    Thread.sleep(900)
    assert(assertCount == 0)
  }

}
