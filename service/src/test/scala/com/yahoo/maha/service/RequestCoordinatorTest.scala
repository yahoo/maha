// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service

import java.util.concurrent.atomic.AtomicInteger

import com.yahoo.maha.core.bucketing.{BucketParams, UserInfo}
import com.yahoo.maha.core.request._
import com.yahoo.maha.jdbc.{Seq, _}
import com.yahoo.maha.parrequest2.GeneralError
import com.yahoo.maha.parrequest2.future.{ParFunction, ParRequest}
import com.yahoo.maha.service.curators._
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.scalatest.BeforeAndAfterAll
import scala.collection.JavaConverters._

class RequestCoordinatorTest extends BaseMahaServiceTest with BeforeAndAfterAll {

  val today: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now())
  val yesterday: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(1))

  override def beforeAll(): Unit = {
    createTables()
    val insertSql = """INSERT INTO student_grade_sheet (year, section_id, student_id, class_id, total_marks, date, comment, month)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""

    val rows: List[Seq[Any]] = List(
      Seq(1, 100, 213, 200, 135, DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(9)), "some comment 1", today.toString),
      Seq(1, 100, 213, 198, 120, DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(10)), "some comment 2", today.toString),
      Seq(1, 500, 213, 197, 190, DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(10)), "some comment 3", today.toString),
      Seq(1, 100, 213, 200, 125, today.toString, "some comment 1", today.toString),
      Seq(1, 100, 213, 198, 180, yesterday.toString, "some comment 2", today.toString),
      Seq(1, 200, 213, 199, 175, today.toString, "some comment 3", today.toString)
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

  test("Test successful processing of Default curator") {

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

    val requestCoordinator: RequestCoordinator = DefaultRequestCoordinator(mahaService)

    val requestCoordinatorResult: Either[GeneralError, RequestCoordinatorResult] = requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper)
    val defaultCuratorResult: ParRequest[CuratorResult] = requestCoordinatorResult.right.get.resultMap(DefaultCurator.name)

    val defaultCuratorResultEither = defaultCuratorResult.resultMap((t: CuratorResult) => t)
    defaultCuratorResultEither.fold((t: GeneralError) => {
      fail(t.message)
    },(curatorResult: CuratorResult) => {
      assert(curatorResult.requestResultTry.isSuccess)
      val defaultExpectedSet = Set(
        "Row(Map(Student ID -> 0, Class ID -> 1, Section ID -> 2, Total Marks -> 3),ArrayBuffer(213, 200, 100, 125))",
        "Row(Map(Student ID -> 0, Class ID -> 1, Section ID -> 2, Total Marks -> 3),ArrayBuffer(213, 198, 100, 180))",
        "Row(Map(Student ID -> 0, Class ID -> 1, Section ID -> 2, Total Marks -> 3),ArrayBuffer(213, 199, 200, 175))"
      )

      var defaultCount = 0
      curatorResult.requestResultTry.get.queryPipelineResult.rowList.foreach( row => {
        println(row.toString)
        assert(defaultExpectedSet.contains(row.toString))
        defaultCount+=1
      })

      assert(defaultExpectedSet.size == defaultCount)
    })

  }

  test("Test successful processing of Timeshift curator") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "test-blah-curator": { "config": {}},
                            "test-blah_curator2": { "config": {}},
                            "timeshift" : {
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
    val reportingRequest = ReportingRequest.enableDebug(reportingRequestResult.toOption.get)

    assert(reportingRequest.curatorJsonConfigMap.contains(TimeShiftCurator.name))
    val bucketParams = BucketParams(UserInfo("uid", isInternal = true))

    val requestCoordinator: RequestCoordinator = DefaultRequestCoordinator(mahaService)


    val mahaRequestContext = MahaRequestContext(REGISTRY,
      bucketParams,
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")
    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter)

    val requestCoordinatorResult: Either[GeneralError, RequestCoordinatorResult] = requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper)
    val timeShiftCuratorResult: ParRequest[CuratorResult] = requestCoordinatorResult.right.get.resultMap(TimeShiftCurator.name)

    val timeShiftCuratorResultEither = timeShiftCuratorResult.resultMap((t: CuratorResult) => t)
    timeShiftCuratorResultEither.fold((t: GeneralError) => {
      fail(t.message)
    },(curatorResult: CuratorResult) => {
      assert(curatorResult.requestResultTry.isSuccess)
      val expectedSet = Set(
        "Row(Map(Total Marks Prev -> 4, Section ID -> 2, Total Marks Pct Change -> 5, Student ID -> 0, Total Marks -> 3, Class ID -> 1),ArrayBuffer(213, 200, 100, 125, 135, -7.41))",
        "Row(Map(Total Marks Prev -> 4, Section ID -> 2, Total Marks Pct Change -> 5, Student ID -> 0, Total Marks -> 3, Class ID -> 1),ArrayBuffer(213, 198, 100, 180, 120, 50.0))",
        "Row(Map(Total Marks Prev -> 4, Section ID -> 2, Total Marks Pct Change -> 5, Student ID -> 0, Total Marks -> 3, Class ID -> 1),ArrayBuffer(213, 199, 200, 175, 0, 100.0))"
      )

      var cnt = 0
      curatorResult.requestResultTry.get.queryPipelineResult.rowList.foreach( row => {
        println(row.toString)
        assert(expectedSet.contains(row.toString))
        cnt+=1
      })

      assert(expectedSet.size == cnt)
    })

  }

  test("Test failure when processing of multiple curator when one is a singleton") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "drilldown" : {
                              "config" : {
                                "dimension": "Section ID"
                              }
                            },
                            "timeshift" : {
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
    val reportingRequest = ReportingRequest.enableDebug(reportingRequestResult.toOption.get)

    val bucketParams = BucketParams(UserInfo("uid", isInternal = true))

    val requestCoordinator: RequestCoordinator = DefaultRequestCoordinator(mahaService)


    val mahaRequestContext = MahaRequestContext(REGISTRY,
      bucketParams,
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")
    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter)

    val requestCoordinatorResult: Either[GeneralError, RequestCoordinatorResult] = requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper)
    assert(requestCoordinatorResult.isLeft)
    assert(requestCoordinatorResult.left.get.message === "Singleton curators can only be requested by themselves but found TreeSet((drilldown,true), (timeshift,true))")

  }

  test("successfully return result when curator process returns error") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "fail" : {
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
    val reportingRequest = ReportingRequest.enableDebug(reportingRequestResult.toOption.get)

    val bucketParams = BucketParams(UserInfo("uid", isInternal = true))

    val requestCoordinator: RequestCoordinator = DefaultRequestCoordinator(mahaService)


    val mahaRequestContext = MahaRequestContext(REGISTRY,
      bucketParams,
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")
    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter)

    val requestCoordinatorResult: Either[GeneralError, RequestCoordinatorResult] = requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper)
    assert(requestCoordinatorResult.isRight)

    val resultList = requestCoordinatorResult.right.get.combinedResultList
    val errCall = new AtomicInteger(0)
    val sucCall = new AtomicInteger(0)
    val foldResult = resultList.fold[Unit](ParFunction.fromScala({
      err =>
        errCall.incrementAndGet()
        println(err.message)
        assert(false, "Should not have been called, even though non default curator failed")
    }), ParFunction.fromScala({
      seq: java.util.List[Either[GeneralError, CuratorResult]] =>
        val resultList = seq.asScala
        assert(resultList.size == 2)
        sucCall.incrementAndGet()
        assert(resultList(0).isRight)
        assert(resultList(0).right.get.curator.name === DefaultCurator.name)
        assert(resultList(1).isLeft)
        assert(resultList(1).left.get.message === "failed")
    }))
    val result = foldResult.get
    assert(errCall.get() === 0)
    assert(sucCall.get() === 1)
    assert(result.isRight)
  }

  test("successfully return result when default curator process returns error") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "fail" : {
                              "config" : {
                              }
                            }
                          },
                          "selectFields": [
                            {"field": "Student Blah"},
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
    val reportingRequest = ReportingRequest.enableDebug(reportingRequestResult.toOption.get)

    val bucketParams = BucketParams(UserInfo("uid", isInternal = true))

    val requestCoordinator: RequestCoordinator = DefaultRequestCoordinator(mahaService)


    val mahaRequestContext = MahaRequestContext(REGISTRY,
      bucketParams,
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")
    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter)

    val requestCoordinatorResult: Either[GeneralError, RequestCoordinatorResult] = requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper)
    assert(requestCoordinatorResult.isRight)

    val resultList = requestCoordinatorResult.right.get.combinedResultList
    val errCall = new AtomicInteger(0)
    val sucCall = new AtomicInteger(0)
    val foldResult = resultList.fold[Unit](ParFunction.fromScala({
      err =>
        errCall.incrementAndGet()
        println(err.message)
        println(err.throwableOption.map(_.getMessage))
        assert(false, "should not have been called!")
    }), ParFunction.fromScala({
      seq: java.util.List[Either[GeneralError, CuratorResult]] =>
        val resultList = seq.asScala
        assert(resultList.size == 2)
        sucCall.incrementAndGet()
        println(resultList(0))
        println(resultList(1))
        assert(resultList(0).isLeft)
        assert(resultList(0).left.get.message === "requirement failed: ERROR_CODE:10005 Failed to find primary key alias for Student Blah")
        assert(resultList(1).isLeft)
        assert(resultList(1).left.get.message === "failed")
    }))
    val result = foldResult.get
    assert(errCall.get() === 0)
    assert(sucCall.get() === 1)
    assert(result.isRight)
  }

  test("Curator compare test") {
    val defaultCurator = new DefaultCurator()
    val timeShiftCurator = new TimeShiftCurator()
    assert(defaultCurator.compare(timeShiftCurator) == -1)
    assert(defaultCurator.compare(defaultCurator) == 0)
  }

  test("Test successful processing of Drilldown curator") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "drilldown" : {
                              "config" : {
                                "dimension": "Section ID"
                              }
                            }
                          },
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

    val requestCoordinator: RequestCoordinator = DefaultRequestCoordinator(mahaService)

    val mahaRequestContext = MahaRequestContext(REGISTRY,
      bucketParams,
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")
    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter)

    val requestCoordinatorResult: Either[GeneralError, RequestCoordinatorResult] = requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper)
    val drillDownCuratorResult: ParRequest[CuratorResult] = requestCoordinatorResult.right.get.resultMap(DrilldownCurator.name)

    val timeShiftCuratorResultEither = drillDownCuratorResult.resultMap((t: CuratorResult) => t)
    timeShiftCuratorResultEither.fold((t: GeneralError) => {
      fail(t.message)
    },(curatorResult: CuratorResult) => {
      assert(curatorResult.requestResultTry.isSuccess)
      val expectedSet = Set(
        "Row(Map(Section ID -> 0, Total Marks -> 1),ArrayBuffer(100, 305))",
        "Row(Map(Section ID -> 0, Total Marks -> 1),ArrayBuffer(200, 175))"
      )

      var cnt = 0
      curatorResult.requestResultTry.get.queryPipelineResult.rowList.foreach( row => {
        println(row.toString)
        assert(expectedSet.contains(row.toString))
        cnt+=1
      })

      assert(expectedSet.size == cnt)
    })

  }
}

