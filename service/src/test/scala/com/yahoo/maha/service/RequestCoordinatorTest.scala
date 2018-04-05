// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service

import com.yahoo.maha.core.bucketing.{BucketParams, UserInfo}
import com.yahoo.maha.core.request._
import com.yahoo.maha.jdbc.{Seq, _}
import com.yahoo.maha.parrequest2.GeneralError
import com.yahoo.maha.parrequest2.future.ParRequest
import com.yahoo.maha.service.curators.CuratorResult
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.scalatest.BeforeAndAfterAll

class RequestCoordinatorTest extends BaseMahaServiceTest with BeforeAndAfterAll {

  val today: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now())
  val yesterday: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(1))

  override def beforeAll(): Unit = {
    createTables()
    val insertSql = """INSERT INTO student_grade_sheet (year, section_id, student_id, class_id, total_marks, date, comment)
     VALUES (?, ?, ?, ?, ?, ?, ?)"""

    val rows: List[Seq[Any]] = List(
      Seq(1, 100, 213, 200, 135, DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(9)), "some comment 1"),
      Seq(1, 100, 213, 198, 120, DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(10)), "some comment 2"),
      Seq(1, 500, 213, 197, 190, DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(10)), "some comment 3"),
      Seq(1, 100, 213, 200, 125, today.toString, "some comment 1"),
      Seq(1, 100, 213, 198, 180, yesterday.toString, "some comment 2"),
      Seq(1, 200, 213, 199, 175, today.toString, "some comment 3")
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

    val bucketParams = BucketParams(UserInfo("uid", true))

    val mahaRequestLogHelper = MahaRequestLogHelper(REGISTRY, mahaServiceConfig.mahaRequestLogWriter)

    val mahaRequestProcessor = new MahaRequestProcessor(REGISTRY,
      DefaultRequestCoordinator(mahaService),
      mahaServiceConfig.mahaRequestLogWriter,
      mahaRequestLogHelperOption= Some(mahaRequestLogHelper)
    )

    val requestCoordinator: RequestCoordinator = new DefaultRequestCoordinator(mahaService)

    val defaultCuratorResult: ParRequest[CuratorResult] = requestCoordinator.execute("er", bucketParams, reportingRequest, mahaRequestLogHelper)

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
      curatorResult.requestResultTry.get.rowList.foreach( row => {
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
    val reportingRequest = reportingRequestResult.toOption.get

    val bucketParams = BucketParams(UserInfo("uid", true))

    val mahaRequestLogHelper = MahaRequestLogHelper("er", mahaServiceConfig.mahaRequestLogWriter)

    val requestCoordinator: RequestCoordinator = new DefaultRequestCoordinator(mahaService)

    val timeShiftCuratorResult: ParRequest[CuratorResult] = requestCoordinator.execute("er", bucketParams, reportingRequest, mahaRequestLogHelper)

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
      curatorResult.requestResultTry.get.rowList.foreach( row => {
        println(row.toString)
        assert(expectedSet.contains(row.toString))
        cnt+=1
      })

      assert(expectedSet.size == cnt)
    })

  }

}
