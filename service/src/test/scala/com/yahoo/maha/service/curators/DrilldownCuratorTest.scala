// Copyright 2018, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.curators

import com.yahoo.maha.core.bucketing.{BucketParams, UserInfo}
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.jdbc._
import com.yahoo.maha.parrequest2.GeneralError
import com.yahoo.maha.parrequest2.future.ParRequest
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.utils.{CuratorMahaRequestLogHelper, MahaRequestLogHelper}
import com.yahoo.maha.service.{BaseMahaServiceTest, CuratorInjector, MahaRequestContext, ParRequestResult, RequestResult}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.scalatest.BeforeAndAfterAll


/**
 * Created by pranavbhole on 10/04/18.
 */
class DrilldownCuratorTest extends BaseMahaServiceTest with BeforeAndAfterAll {
  val today: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now())
  val yesterday: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(1))

  override protected def beforeAll(): Unit =  {
    super.beforeAll()
    createTables()
    stageData()
  }

  override protected def afterAll(): Unit =  {
    super.afterAll()
    server.shutdownNow()
  }


  def stageData(): Unit = {
    val insertSql = """INSERT INTO student_grade_sheet (year, section_id, student_id, class_id, total_marks, date, comment, month, top_student_id)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    val rows: List[Seq[Any]] = List(
      Seq(1, 100, 213, 200, 135, yesterday.toString, "some comment 1", yesterday.toString, 213),
      Seq(1, 100, 213, 198, 120, yesterday.toString, "some comment 2", yesterday.toString, 213),
      Seq(1, 500, 213, 197, 190, yesterday.toString, "some comment 3", yesterday.toString, 213)
    )

    rows.foreach {
      row =>
        val result = jdbcConnection.get.executeUpdate(insertSql, row)
        assert(result.isSuccess)
    }
    var count = 0
    jdbcConnection.get.queryForObject("select * from student_grade_sheet") {
      rs =>
        while (rs.next()) {
          count += 1
        }
    }
    assert(rows.size == count)

  }

  test("Test failure of DrilldownCurator with no default curator") {
    val jsonRequest = s"""{
                          "cube": "unknown",
                          "curators" : {
                            "totalmetrics" : {
                              "config" : {
                              }
                            }
                          },
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
    val curatorMahaRequestLogHelper =  CuratorMahaRequestLogHelper(mahaRequestLogHelper)


    val drilldownCurator = new DrilldownCurator()
    val curatorInjector = new CuratorInjector(2, mahaService, mahaRequestLogHelper, Set.empty)

    val drilldownCuratorResult: Either[CuratorError, ParRequest[CuratorResult]] = drilldownCurator
      .process(Map.empty, mahaRequestContext, mahaService, curatorMahaRequestLogHelper, NoConfig, curatorInjector)

    assert(drilldownCuratorResult.isLeft)
    assert(drilldownCuratorResult.left.get.message === "default curator required!")
  }

  test("Test failure of DrilldownCurator with default curator failed") {
    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "totalmetrics" : {
                              "config" : {
                              }
                            }
                          },
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Class ID"},
                            {"field": "Section ID"},
                            {"field": "Unknown"}
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
    val curatorMahaRequestLogHelper =  CuratorMahaRequestLogHelper(mahaRequestLogHelper)


    val defaultCurator = new DefaultCurator()
    val drilldownCurator = new DrilldownCurator()
    val curatorInjector = new CuratorInjector(2, mahaService, mahaRequestLogHelper, Set.empty)
    val resultMap: Map[String, Either[CuratorError, ParRequest[CuratorResult]]] = Map(
      DefaultCurator.name -> new Left(CuratorError(defaultCurator, NoConfig, GeneralError.from("stage","error")))
    )

    val drilldownCuratorResult: Either[CuratorError, ParRequest[CuratorResult]] = drilldownCurator
      .process(resultMap, mahaRequestContext, mahaService, curatorMahaRequestLogHelper, NoConfig, curatorInjector)

    assert(drilldownCuratorResult.isLeft)
    assert(drilldownCuratorResult.left.get.error.message === "error")
  }

  test("Test failure of DrilldownCurator with no default curator request result") {
    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "totalmetrics" : {
                              "config" : {
                              }
                            }
                          },
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
    val curatorMahaRequestLogHelper =  CuratorMahaRequestLogHelper(mahaRequestLogHelper)


    val pse = mahaService.getParallelServiceExecutor(mahaRequestContext)
    val requestModelResult = mahaService
      .generateRequestModel(REGISTRY, reportingRequest, bucketParams).toOption.get
    val qp = mahaService.generateQueryPipelines(REGISTRY, requestModelResult.model, bucketParams)._1
    val curatorInjector = new CuratorInjector(2, mahaService, mahaRequestLogHelper, Set.empty)
    val defaultCurator = new DefaultCurator()
    //val requestResultParRequest: ParRequest[RequestResult] = pse.immediateResult("requestResult", new Left[GeneralError, RequestResult](GeneralError.from("stage","fail")))
    //val parRequestResult = ParRequestResult(qp, requestResultParRequest, None)
    val defaultParRequest: Either[CuratorError, ParRequest[CuratorResult]] = new Right(pse.immediateResult("curatorResult",
      new Right(CuratorResult(defaultCurator, NoConfig, None, requestModelResult))
    ))
    val drilldownCurator = new DrilldownCurator()
    val resultMap: Map[String, Either[CuratorError, ParRequest[CuratorResult]]] = Map(
      DefaultCurator.name -> defaultParRequest
    )

    val drilldownCuratorResult: Either[CuratorError, ParRequest[CuratorResult]] = drilldownCurator
      .process(resultMap, mahaRequestContext, mahaService, curatorMahaRequestLogHelper, NoConfig, curatorInjector)

    assert(drilldownCuratorResult.isRight)

    val result = drilldownCuratorResult.right.get.get()
    assert(result.isLeft)
    assert(result.left.get.message === "no result from default curator, cannot continue")
  }

  test("Test failure of DrilldownCurator with failing default curator request result") {
    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "totalmetrics" : {
                              "config" : {
                              }
                            }
                          },
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
    val curatorMahaRequestLogHelper =  CuratorMahaRequestLogHelper(mahaRequestLogHelper)


    val pse = mahaService.getParallelServiceExecutor(mahaRequestContext)
    val requestModelResult = mahaService
      .generateRequestModel(REGISTRY, reportingRequest, bucketParams).toOption.get
    val qp = mahaService.generateQueryPipelines(REGISTRY, requestModelResult.model, bucketParams)._1
    val curatorInjector = new CuratorInjector(2, mahaService, mahaRequestLogHelper, Set.empty)
    val defaultCurator = new DefaultCurator()
    val requestResultParRequest: ParRequest[RequestResult] = pse.immediateResult("requestResult"
      , new Left[GeneralError, RequestResult](GeneralError.from("stage","fail")))
    val parRequestResult = ParRequestResult(qp, requestResultParRequest, None)
    val defaultParRequest: Either[CuratorError, ParRequest[CuratorResult]] = new Right(pse.immediateResult("curatorResult",
      new Right(CuratorResult(defaultCurator, NoConfig, Option(parRequestResult), requestModelResult))
    ))
    val drilldownCurator = new DrilldownCurator()
    val resultMap: Map[String, Either[CuratorError, ParRequest[CuratorResult]]] = Map(
      DefaultCurator.name -> defaultParRequest
    )

    val drilldownCuratorResult: Either[CuratorError, ParRequest[CuratorResult]] = drilldownCurator
      .process(resultMap, mahaRequestContext, mahaService, curatorMahaRequestLogHelper, NoConfig, curatorInjector)

    assert(drilldownCuratorResult.isRight)

    val result = drilldownCuratorResult.right.get.get()
    assert(result.isLeft)
    assert(result.left.get.message === "fail")
  }
}
