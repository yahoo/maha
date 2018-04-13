package com.yahoo.maha.service.curators

import com.yahoo.maha.core.bucketing.{BucketParams, UserInfo}
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.parrequest2.future.{ParFunction, ParRequest}
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.utils.{CuratorMahaRequestLogHelper, MahaRequestLogHelper}
import com.yahoo.maha.service.{BaseMahaServiceTest, MahaRequestContext}

import scala.util.Try

/**
 * Created by pranavbhole on 12/04/18.
 */
class DefaultCuratorTest extends BaseMahaServiceTest {
  createTables()

  val jsonRequest = s"""{
                          "cube": "student_performance",
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Class ID"},
                            {"field": "Section ID"},
                            {"field": "Total Marks"}
                          ],
                          "sortBy": [
                            {"field": "Total Marks", "order": "Desc"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"}
                          ]
                        }"""
  val reportingRequestResult = ReportingRequest.deserializeSyncWithFactBias(jsonRequest.getBytes, schema = StudentSchema)
  require(reportingRequestResult.isSuccess)
  val reportingRequest = reportingRequestResult.toOption.get
  val bucketParams = BucketParams(UserInfo("uid", isInternal = true))

  val mahaRequestContext = MahaRequestContext(REGISTRY,
    bucketParams,
    reportingRequest,
    jsonRequest.getBytes,
    Map.empty, "rid", "uid")

  val curatorMahaRequestLogHelper = CuratorMahaRequestLogHelper(MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter))

  test("Default Curator test") {

    class CuratorCustomPostProcessor extends CuratorResultPostProcessor {
      override def process(mahaRequestContext: MahaRequestContext, curatorResult: CuratorResult): CuratorResult =  {
        val requestResult = curatorResult.requestResultTry.get
        curatorResult.copy(requestResultTry = Try(requestResult.copy(rowCountOption = Some(1))))
      }
    }

    val defaultCurator = DefaultCurator(curatorResultPostProcessor = new CuratorCustomPostProcessor)

    val defaultParRequest: ParRequest[CuratorResult] = defaultCurator.process(Map.empty, mahaRequestContext, mahaService, curatorMahaRequestLogHelper, NoConfig)

    defaultParRequest.resultMap(ParFunction.fromScala(
      (curatorResult: CuratorResult) => {
        assert(curatorResult.requestResultTry.isSuccess)
        assert(curatorResult.requestResultTry.get.rowCountOption.isDefined)
        assert(curatorResult.requestResultTry.get.rowCountOption.get == 1)
        curatorResult
      }
    ))

  }


  test("Default Curator test with failing curatorResultPostProcessor") {

    class CuratorCustomPostProcessor extends CuratorResultPostProcessor {
      override def process(mahaRequestContext: MahaRequestContext, curatorResult: CuratorResult): CuratorResult =  {
        throw new IllegalArgumentException("CuratorResultPostProcessor failed")
      }
    }

    val defaultCurator = DefaultCurator(curatorResultPostProcessor = new CuratorCustomPostProcessor())

    val defaultParRequest: ParRequest[CuratorResult] = defaultCurator.process(Map.empty, mahaRequestContext, mahaService, curatorMahaRequestLogHelper, NoConfig)

    defaultParRequest.resultMap[CuratorResult](
        ParFunction.fromScala(
     (curatorResult: CuratorResult) => {
       assert(curatorResult.requestResultTry.isSuccess)
       curatorResult
     }
    )
    )
  }

}
