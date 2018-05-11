package com.yahoo.maha.service

import com.yahoo.maha.core.bucketing.{BucketParams, UserInfo}
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import org.scalatest.BeforeAndAfterAll

/**
 * Created by pranavbhole on 21/03/18.
 */
class MahaSyncRequestProcessorTest extends BaseMahaServiceTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    createTables()
  }

  override protected def afterAll(): Unit =  {
    super.afterAll()
    server.shutdownNow()
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

    val mahaRequestContext = MahaRequestContext(REGISTRY,
      BucketParams(UserInfo("uid", true)),
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")

    val mahaRequestProcessor = new MahaSyncRequestProcessor(mahaRequestContext,
      DefaultRequestCoordinator(mahaService),
      mahaServiceConfig.mahaRequestLogWriter
    )
    mahaRequestProcessor.onSuccess((requestCoordinatorResult: RequestCoordinatorResult) => {
      assert(requestCoordinatorResult.successResults.head._2.queryPipelineResult.rowList.columns.nonEmpty)
      assertCount+=1
    })
    mahaRequestProcessor.onFailure((ge) => {
      assertCount-=1
    })
    mahaRequestProcessor.process()

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

    val mahaRequestContext = MahaRequestContext(REGISTRY,
      BucketParams(UserInfo("uid", true)),
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")

    val processorFactory = MahaSyncRequestProcessorFactory(DefaultRequestCoordinator(mahaService),
      mahaService,
      mahaServiceConfig.mahaRequestLogWriter)

    val mahaRequestProcessor = processorFactory.create(mahaRequestContext
      , "test", MahaRequestLogHelper(mahaRequestContext, mahaService.mahaRequestLogWriter))

    mahaRequestProcessor.onSuccess((requestCoordinatorResult: RequestCoordinatorResult) => {
      assertCount+=1
    })

    mahaRequestProcessor.onFailure((ge) => {
      assertCount-=1
    })
    mahaRequestProcessor.process()

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
    var assertCount = 0

    val mahaRequestContext = MahaRequestContext(REGISTRY,
      BucketParams(UserInfo("uid", true)),
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")

    val processorFactory = MahaSyncRequestProcessorFactory(DefaultRequestCoordinator(mahaService),
      mahaService,
      mahaServiceConfig.mahaRequestLogWriter)

    val mahaRequestProcessor = processorFactory.create(mahaRequestContext, "test")

    mahaRequestProcessor.onSuccess((requestCoordinatorResult: RequestCoordinatorResult) => {
      throw new IllegalArgumentException("failed in success function")
      assertCount-=1
    })

    mahaRequestProcessor.onFailure((ge) => {
      assertCount+=1
    })
    mahaRequestProcessor.process()

    Thread.sleep(900)
    assert(assertCount == 1)
  }

  test("Test MahaRequestProcessor request model failure") {
    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "selectFields": [
                            {"field": "Student Blah"},
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

    val mahaRequestContext = MahaRequestContext(REGISTRY,
      BucketParams(UserInfo("uid", true)),
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")

    val processorFactory = MahaSyncRequestProcessorFactory(DefaultRequestCoordinator(mahaService),
      mahaService,
      mahaServiceConfig.mahaRequestLogWriter)

    val mahaRequestProcessor = processorFactory.create(mahaRequestContext, "test")

    mahaRequestProcessor.onSuccess((requestCoordinatorResult: RequestCoordinatorResult) => {
      throw new IllegalArgumentException("failed in success function")
      assertCount-=1
    })

    mahaRequestProcessor.onFailure((ge) => {
      logger.info(ge.message)
      ge.throwableOption.foreach(_.printStackTrace())
      assertCount+=1
    })
    mahaRequestProcessor.process()

    Thread.sleep(900)
    assert(assertCount == 1)
  }

}
