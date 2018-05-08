package com.yahoo.maha.service.curators

import com.yahoo.maha.core.ColumnInfo
import com.yahoo.maha.core.bucketing.{BucketParams, UserInfo}
import com.yahoo.maha.core.query.Row
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.parrequest2.GeneralError
import com.yahoo.maha.parrequest2.future.{ParFunction, ParRequest}
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.utils.{CuratorMahaRequestLogHelper, MahaRequestLogHelper}
import com.yahoo.maha.service.{BaseMahaServiceTest, CuratorInjector, MahaRequestContext, RequestResult}
import org.scalatest.BeforeAndAfterAll

import scala.collection.mutable.ArrayBuffer

/**
 * Created by pranavbhole on 12/04/18.
 */
class DefaultCuratorTest extends BaseMahaServiceTest with BeforeAndAfterAll{

  override protected def afterAll(): Unit =  {
    super.afterAll()
    server.shutdownNow()
  }

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

  val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter)
  val curatorMahaRequestLogHelper = CuratorMahaRequestLogHelper(mahaRequestLogHelper)

  test("Default Curator test") {

    class CuratorCustomPostProcessor extends CuratorResultPostProcessor {
      override def process(mahaRequestContext: MahaRequestContext, requestResult: RequestResult) : Either[GeneralError, RequestResult] = {
        val columns: IndexedSeq[ColumnInfo] = requestResult.queryPipelineResult.queryChain.drivingQuery.queryContext.requestModel.requestCols
        val aliasMap : Map[String, Int] = columns.map(_.alias).zipWithIndex.toMap
        val data = new ArrayBuffer[Any](initialSize = aliasMap.size)
        data+="sid"
        data+="cid"
        data+="sid"
        data+=999
        val row = Row(aliasMap, data)
        requestResult.queryPipelineResult.rowList.addRow(row)
        new Right(requestResult)
      }
    }

    val defaultCurator = DefaultCurator(curatorResultPostProcessor = new CuratorCustomPostProcessor)

    val curatorInjector = new CuratorInjector(2, mahaService, mahaRequestLogHelper, Set.empty)
    val defaultParRequest: Either[CuratorError, ParRequest[CuratorResult]] = defaultCurator
      .process(Map.empty, mahaRequestContext, mahaService, curatorMahaRequestLogHelper, NoConfig, curatorInjector)

    assert(defaultParRequest.isRight)

  }


  test("Default Curator test with failing curatorResultPostProcessor") {

    class CuratorCustomPostProcessor extends CuratorResultPostProcessor {
      override def process(mahaRequestContext: MahaRequestContext, requestResult: RequestResult) : Either[GeneralError, RequestResult] = {
        throw new IllegalArgumentException("CuratorResultPostProcessor failed")
      }
    }

    val defaultCurator = DefaultCurator(curatorResultPostProcessor = new CuratorCustomPostProcessor())
    val curatorInjector = new CuratorInjector(2, mahaService, mahaRequestLogHelper, Set.empty)

    val defaultParRequest: Either[GeneralError, ParRequest[CuratorResult]] = defaultCurator
      .process(Map.empty, mahaRequestContext, mahaService, curatorMahaRequestLogHelper, NoConfig, curatorInjector)

    defaultParRequest.right.get.resultMap[CuratorResult](
        ParFunction.fromScala(
     (curatorResult: CuratorResult) => {
       assert(curatorResult.parRequestResultOption.get.prodRun.get().isRight)
       curatorResult
     }
    )
    )
  }

  test("Default Curator test with BadTestRequestModelValidator") {

    val defaultCurator = DefaultCurator(new BadTestRequestModelValidator)
    val curatorInjector = new CuratorInjector(2, mahaService, mahaRequestLogHelper, Set.empty)

    val defaultParRequest: Either[GeneralError, ParRequest[CuratorResult]] = defaultCurator
      .process(Map.empty, mahaRequestContext, mahaService, curatorMahaRequestLogHelper, NoConfig, curatorInjector)

    assert(defaultParRequest.isLeft)

  }


}
