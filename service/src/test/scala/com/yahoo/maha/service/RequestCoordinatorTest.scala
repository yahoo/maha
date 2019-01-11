// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service

import com.yahoo.maha.core.bucketing.{BucketParams, UserInfo}
import com.yahoo.maha.core.request.Parameter.Debug
import com.yahoo.maha.core.request._
import com.yahoo.maha.jdbc.{Seq, _}
import com.yahoo.maha.parrequest2.future.ParRequest
import com.yahoo.maha.service.curators._
import com.yahoo.maha.service.error.MahaServiceExecutionException
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.output.{JsonOutputFormat, StringStream}
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.scalatest.BeforeAndAfterAll

class RequestCoordinatorTest extends BaseMahaServiceTest with BeforeAndAfterAll {

  val today: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now())
  val yesterday: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(1))

  protected def getRequestCoordinatorResult(result: Either[RequestCoordinatorError, ParRequest[RequestCoordinatorResult]]) : RequestCoordinatorResult = {
    require(result.isRight, result.left.get.toString)
    val parResult = result.right.get.get()
    require(parResult.isRight, parResult.left.get.toString)
    parResult.right.get
  }
  override def beforeAll(): Unit = {
    createTables()
    val insertSql = """INSERT INTO student_grade_sheet (year, section_id, student_id, class_id, total_marks, date, comment, month)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""

    val studentInsertSql =
      """INSERT INTO student (id, name, admitted_year, status, department_id)
        VALUES (?, ?, ?, ?, ?)"""

    val classInsertSql =
      """INSERT INTO class (id, name, start_year, status, department_id)
        VALUES (?, ?, ?, ?, ?)"""

    val sectionInsertSql =
      """INSERT INTO section (id, name, student_id, class_id, start_year, status)
        VALUES (?, ?, ?, ?, ?, ?)"""

    val rows: List[Seq[Any]] = List(
      Seq(1, 100, 213, 200, 135, DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(9)), "some comment 1", today.toString),
      Seq(1, 100, 213, 198, 120, DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(10)), "some comment 2", today.toString),
      Seq(1, 500, 213, 197, 190, DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(10)), "some comment 3", today.toString),
      Seq(1, 100, 213, 200, 125, today.toString, "some comment 1", today.toString),
      Seq(1, 100, 213, 198, 180, yesterday.toString, "some comment 2", today.toString),
      Seq(1, 200, 213, 199, 175, today.toString, "some comment 3", today.toString)
    )

    val studentRows: List[Seq[Any]] = List(
      Seq(213, "Bryant", 2017, "ACTIVE", 54321)
    )

    val classRows: List[Seq[Any]] = List(
      Seq(200, "Class A", 2017, "ACTIVE", 54321)
    )

    val sectionRows: List[Seq[Any]] = List(
      Seq(310, "Section A", 213, 200, 2017, "ACTIVE")
    )

    rows.foreach {
      row =>
        val result = jdbcConnection.get.executeUpdate(insertSql, row)
        assert(result.isSuccess)
    }

    rows.foreach {
      row =>
        val result = jdbcConnection.get.executeUpdate(insertSql.replaceAllLiterally("student_grade_sheet", "student_grade_sheet_again"), row)
        assert(result.isSuccess)
    }

    studentRows.foreach{
      row =>
        val result = jdbcConnection.get.executeUpdate(studentInsertSql, row)
        assert(result.isSuccess)
    }

    classRows.foreach{
      row =>
        val result = jdbcConnection.get.executeUpdate(classInsertSql, row)
        assert(result.isSuccess)
    }

    sectionRows.foreach {
      row =>
        val result = jdbcConnection.get.executeUpdate(sectionInsertSql, row)
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

  override protected def afterAll(): Unit =  {
    super.afterAll()
    server.shutdownNow()
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

    val requestCoordinatorResult: RequestCoordinatorResult = getRequestCoordinatorResult(requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper))
    val defaultCuratorRequestResult: RequestResult = requestCoordinatorResult.successResults(DefaultCurator.name)

    val defaultExpectedSet = Set(
      "Row(Map(Student ID -> 0, Class ID -> 1, Section ID -> 2, Total Marks -> 3),ArrayBuffer(213, 200, 100, 125))",
      "Row(Map(Student ID -> 0, Class ID -> 1, Section ID -> 2, Total Marks -> 3),ArrayBuffer(213, 198, 100, 180))",
      "Row(Map(Student ID -> 0, Class ID -> 1, Section ID -> 2, Total Marks -> 3),ArrayBuffer(213, 199, 200, 175))"
    )

    var defaultCount = 0
    defaultCuratorRequestResult.queryPipelineResult.rowList.foreach( row => {
      
      assert(defaultExpectedSet.contains(row.toString))
      defaultCount+=1
    })

    assert(defaultExpectedSet.size == defaultCount)
  }

  test("Test successful processing of Default curator with dimension driven") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Section ID"},
                            {"field": "Total Marks"}
                          ],
                          "sortBy": [
                            {"field": "Total Marks", "order": "Desc"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"}
                          ],
                          "forceDimensionDriven": true
                        }"""
    val reportingRequestResult = ReportingRequest.deserializeSyncWithFactBias(jsonRequest.getBytes, schema = StudentSchema)
    require(reportingRequestResult.isSuccess, reportingRequestResult.toString)
    val reportingRequest = reportingRequestResult.toOption.get

    val bucketParams = BucketParams(UserInfo("uid", isInternal = true))

    val mahaRequestContext = MahaRequestContext(REGISTRY,
      bucketParams,
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")
    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter)

    val requestCoordinator: RequestCoordinator = DefaultRequestCoordinator(mahaService)

    val requestCoordinatorResult: RequestCoordinatorResult = getRequestCoordinatorResult(requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper))
    val defaultCuratorRequestResult: RequestResult = requestCoordinatorResult.successResults(DefaultCurator.name)

    val defaultExpectedSet = Set(
      "Row(Map(Student ID -> 0, Section ID -> 1, Total Marks -> 2),ArrayBuffer(213, 100, 305))",
      "Row(Map(Student ID -> 0, Section ID -> 1, Total Marks -> 2),ArrayBuffer(213, 200, 175))"
    )

    var defaultCount = 0
    defaultCuratorRequestResult.queryPipelineResult.rowList.foreach( row => {
      
      assert(defaultExpectedSet.contains(row.toString))
      defaultCount+=1
    })

    assert(defaultExpectedSet.size == defaultCount)
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

    val requestCoordinatorResult: RequestCoordinatorResult = getRequestCoordinatorResult(requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper))
    val timeShiftCuratorRequestResult: RequestResult = requestCoordinatorResult.successResults(TimeShiftCurator.name)

    val expectedSeq = IndexedSeq(
      "Row(Map(Total Marks Prev -> 4, Section ID -> 2, Total Marks Pct Change -> 5, Student ID -> 0, Total Marks -> 3, Class ID -> 1),ArrayBuffer(213, 199, 200, 175, 0, 100.0))",
      "Row(Map(Total Marks Prev -> 4, Section ID -> 2, Total Marks Pct Change -> 5, Student ID -> 0, Total Marks -> 3, Class ID -> 1),ArrayBuffer(213, 198, 100, 180, 120, 50.0))",
      "Row(Map(Total Marks Prev -> 4, Section ID -> 2, Total Marks Pct Change -> 5, Student ID -> 0, Total Marks -> 3, Class ID -> 1),ArrayBuffer(213, 200, 100, 125, 135, -7.41))"
    )

    var cnt = 0
    timeShiftCuratorRequestResult.queryPipelineResult.rowList.foreach( row => {
      
      assert(expectedSeq(cnt) === row.toString)
      cnt+=1
    })

    assert(expectedSeq.size === cnt)
  }

  test("Test successful processing of Timeshift curator with desc order") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "test-blah-curator": { "config": {}},
                            "test-blah_curator2": { "config": {}},
                            "timeshift" : {
                              "config" : {
                                "sortBy" : {
                                  "field" : "Total Marks",
                                  "order" : "Desc"
                                }
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

    val requestCoordinatorResult: RequestCoordinatorResult = getRequestCoordinatorResult(requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper))
    val timeShiftCuratorRequestResult: RequestResult = requestCoordinatorResult.successResults(TimeShiftCurator.name)

    val expectedSeq = IndexedSeq(
      "Row(Map(Total Marks Prev -> 4, Section ID -> 2, Total Marks Pct Change -> 5, Student ID -> 0, Total Marks -> 3, Class ID -> 1),ArrayBuffer(213, 199, 200, 175, 0, 100.0))",
      "Row(Map(Total Marks Prev -> 4, Section ID -> 2, Total Marks Pct Change -> 5, Student ID -> 0, Total Marks -> 3, Class ID -> 1),ArrayBuffer(213, 198, 100, 180, 120, 50.0))",
      "Row(Map(Total Marks Prev -> 4, Section ID -> 2, Total Marks Pct Change -> 5, Student ID -> 0, Total Marks -> 3, Class ID -> 1),ArrayBuffer(213, 200, 100, 125, 135, -7.41))"
    )

    var cnt = 0
    timeShiftCuratorRequestResult.queryPipelineResult.rowList.foreach( row => {

      assert(expectedSeq(cnt) === row.toString)
      cnt+=1
    })

    assert(expectedSeq.size === cnt)
  }

  test("Test successful processing of Timeshift curator with asc order") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "test-blah-curator": { "config": {}},
                            "test-blah_curator2": { "config": {}},
                            "timeshift" : {
                              "config" : {
                                "sortBy" : {
                                  "field" : "Total Marks",
                                  "order" : "Asc"
                                }
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

    val requestCoordinatorResult: RequestCoordinatorResult = getRequestCoordinatorResult(requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper))
    val timeShiftCuratorRequestResult: RequestResult = requestCoordinatorResult.successResults(TimeShiftCurator.name)

    val expectedSeq = IndexedSeq(
      "Row(Map(Total Marks Prev -> 4, Section ID -> 2, Total Marks Pct Change -> 5, Student ID -> 0, Total Marks -> 3, Class ID -> 1),ArrayBuffer(213, 200, 100, 125, 135, -7.41))",
      "Row(Map(Total Marks Prev -> 4, Section ID -> 2, Total Marks Pct Change -> 5, Student ID -> 0, Total Marks -> 3, Class ID -> 1),ArrayBuffer(213, 198, 100, 180, 120, 50.0))",
      "Row(Map(Total Marks Prev -> 4, Section ID -> 2, Total Marks Pct Change -> 5, Student ID -> 0, Total Marks -> 3, Class ID -> 1),ArrayBuffer(213, 199, 200, 175, 0, 100.0))"
    )

    var cnt = 0
    timeShiftCuratorRequestResult.queryPipelineResult.rowList.foreach( row => {

      assert(expectedSeq(cnt) === row.toString)
      cnt+=1
    })

    assert(expectedSeq.size === cnt)
  }

  test("Test successful processing of Timeshift curator with desc order for unknown field") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "test-blah-curator": { "config": {}},
                            "test-blah_curator2": { "config": {}},
                            "timeshift" : {
                              "config" : {
                                "sortBy" : {
                                  "field" : "unknown field!",
                                  "order" : "Desc"
                                }
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

    val requestCoordinatorResult: RequestCoordinatorResult = getRequestCoordinatorResult(requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper))
    val timeShiftCuratorRequestResult: RequestResult = requestCoordinatorResult.successResults(TimeShiftCurator.name)

    val expectedSeq = IndexedSeq(
      "Row(Map(Total Marks Prev -> 4, Section ID -> 2, Total Marks Pct Change -> 5, Student ID -> 0, Total Marks -> 3, Class ID -> 1),ArrayBuffer(213, 199, 200, 175, 0, 100.0))",
      "Row(Map(Total Marks Prev -> 4, Section ID -> 2, Total Marks Pct Change -> 5, Student ID -> 0, Total Marks -> 3, Class ID -> 1),ArrayBuffer(213, 198, 100, 180, 120, 50.0))",
      "Row(Map(Total Marks Prev -> 4, Section ID -> 2, Total Marks Pct Change -> 5, Student ID -> 0, Total Marks -> 3, Class ID -> 1),ArrayBuffer(213, 200, 100, 125, 135, -7.41))"
    )

    var cnt = 0
    timeShiftCuratorRequestResult.queryPipelineResult.rowList.foreach( row => {

      assert(expectedSeq(cnt) === row.toString)
      cnt+=1
    })

    assert(expectedSeq.size === cnt)
  }

  test("Test successful processing of Timeshift curator with bad config") {

    val jsonRequest =
      s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "test-blah-curator": { "config": {}},
                            "test-blah_curator2": { "config": {}},
                            "timeshift" : {
                              "config" : {
                                "sortBy" : {
                                  "blah" : "Total Marks",
                                  "order" : "Desc"
                                }
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

    val requestCoordinatorErrorOrResult: Either[RequestCoordinatorError, ParRequest[RequestCoordinatorResult]] = requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper)
    assert(requestCoordinatorErrorOrResult.isLeft)
    assert(requestCoordinatorErrorOrResult.left.get.generalError.toString.contains("blah"))
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

    val requestCoordinatorResult: Either[RequestCoordinatorError, ParRequest[RequestCoordinatorResult]] = requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper)
    assert(requestCoordinatorResult.isLeft)
    assert(requestCoordinatorResult.left.get.message === "Singleton curators can only be requested by themselves but found TreeSet((drilldown,false), (timeshift,true))")

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

    val requestCoordinatorResult: RequestCoordinatorResult =
      getRequestCoordinatorResult(requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper))

    assert(requestCoordinatorResult.successResults.size === 1)
    assert(requestCoordinatorResult.successResults.contains(DefaultCurator.name))
    assert(requestCoordinatorResult.failureResults.size === 1)
    assert(requestCoordinatorResult.failureResults.contains(FailingCurator.name))
    assert(requestCoordinatorResult.failureResults(FailingCurator.name).error.message === "failed")
  }

  test("successfully return result when par request for curator result returns error") {

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
      Map("faillevel" -> "curatorresult"), "rid", "uid")
    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter)

    val requestCoordinatorResult: RequestCoordinatorResult =
      getRequestCoordinatorResult(requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper))

    assert(requestCoordinatorResult.successResults.size === 1)
    assert(requestCoordinatorResult.successResults.contains(DefaultCurator.name))
    assert(requestCoordinatorResult.failureResults.size === 1)
    assert(requestCoordinatorResult.failureResults.contains(FailingCurator.name))
    assert(requestCoordinatorResult.failureResults(FailingCurator.name).error.message === "failed")
  }

  test("successfully return result when par request for request result returns error") {

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
      Map("faillevel" -> "requestresult"), "rid", "uid")
    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter)

    val requestCoordinatorResult: RequestCoordinatorResult =
      getRequestCoordinatorResult(requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper))

    assert(requestCoordinatorResult.successResults.size === 1)
    assert(requestCoordinatorResult.successResults.contains(DefaultCurator.name))
    assert(requestCoordinatorResult.failureResults.size === 1)
    assert(requestCoordinatorResult.failureResults.contains(FailingCurator.name))
    assert(requestCoordinatorResult.failureResults(FailingCurator.name).error.message === "failed")
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

    val requestCoordinatorResult: Either[RequestCoordinatorError, ParRequest[RequestCoordinatorResult]] = requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper)
    assert(requestCoordinatorResult.isLeft)
    assert(requestCoordinatorResult.left.get.generalError.message === "requirement failed: ERROR_CODE:10005 Failed to find primary key alias for Student Blah")
  }

  test("Curator compare test") {
    val defaultCurator = new DefaultCurator()
    val timeShiftCurator = new TimeShiftCurator()
    assert(defaultCurator.compare(timeShiftCurator) == -1)
    assert(defaultCurator.compare(defaultCurator) == 0)
  }

  test("Test successful processing of Drilldown curator with no dim candidates") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "drilldown" : {
                              "config" : {
                                "dimension": "Remarks"
                              }
                            }
                          },
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Class ID"},
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

    val requestCoordinatorResult: RequestCoordinatorResult = getRequestCoordinatorResult(requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper))
    assert(requestCoordinatorResult.successResults.contains(DefaultCurator.name))
    val drillDownCuratorResult: RequestResult = requestCoordinatorResult.successResults(DrilldownCurator.name)
    val expectedSet = Set(
      "Row(Map(Remarks -> 0, Student ID -> 1, Total Marks -> 2),ArrayBuffer(some comment 1, 213, 125))",
      "Row(Map(Remarks -> 0, Student ID -> 1, Total Marks -> 2),ArrayBuffer(some comment 2, 213, 180))",
      "Row(Map(Remarks -> 0, Student ID -> 1, Total Marks -> 2),ArrayBuffer(some comment 3, 213, 175))"
    )

    var cnt = 0
    drillDownCuratorResult.queryPipelineResult.rowList.foreach( row => {
      
      assert(expectedSet.contains(row.toString))
      cnt+=1
    })

    assert(expectedSet.size == cnt)
  }

  test("Test successful processing of Drilldown curator with dim candidates") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "drilldown" : {
                              "config" : {
                                "dimension": "Remarks",
                                "enforceFilters": true
                              }
                            }
                          },
                          "selectFields": [
                            {"field": "Class Name"},
                            {"field": "Student Name"},
                            {"field": "Student ID"},
                            {"field": "Total Marks"}
                          ],
                          "sortBy": [
                            {"field": "Total Marks", "order": "Desc"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"},
                            {"field": "Class ID", "operator": "=", "value": "200"}
                          ],
                          "includeRowCount": false
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

    val requestCoordinatorResult: RequestCoordinatorResult = getRequestCoordinatorResult(requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper))
    assert(requestCoordinatorResult.successResults.contains(DefaultCurator.name))
    val drillDownCuratorResult: RequestResult = requestCoordinatorResult.successResults(DrilldownCurator.name)
    val expectedSet = Set(
      "Row(Map(Remarks -> 0, Student ID -> 1, Total Marks -> 2),ArrayBuffer(some comment 1, 213, 125))"
    )

    var cnt = 0
    drillDownCuratorResult.queryPipelineResult.rowList.foreach( row => {
      
      assert(expectedSet.contains(row.toString))
      cnt+=1
    })

    assert(expectedSet.size == cnt)
  }

  test("Test successful processing of Drilldown curator with dim candidates without enforcing filters") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "drilldown" : {
                              "config" : {
                                "dimension": "Remarks",
                                "enforceFilters": false
                              }
                            }
                          },
                          "selectFields": [
                            {"field": "Class Name"},
                            {"field": "Student Name"},
                            {"field": "Student ID"},
                            {"field": "Total Marks"}
                          ],
                          "sortBy": [
                            {"field": "Total Marks", "order": "Desc"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"},
                            {"field": "Class ID", "operator": "=", "value": "200"}
                          ],
                          "includeRowCount": false
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

    val requestCoordinatorResult: RequestCoordinatorResult = getRequestCoordinatorResult(requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper))
    assert(requestCoordinatorResult.successResults.contains(DefaultCurator.name))
    val drillDownCuratorResult: RequestResult = requestCoordinatorResult.successResults(DrilldownCurator.name)
    val expectedSet = Set(
      "Row(Map(Remarks -> 0, Student ID -> 1, Total Marks -> 2),ArrayBuffer(some comment 1, 213, 125))",
      "Row(Map(Remarks -> 0, Student ID -> 1, Total Marks -> 2),ArrayBuffer(some comment 2, 213, 180))",
      "Row(Map(Remarks -> 0, Student ID -> 1, Total Marks -> 2),ArrayBuffer(some comment 3, 213, 175))"
    )

    var cnt = 0
    drillDownCuratorResult.queryPipelineResult.rowList.foreach( row => {
      
      assert(expectedSet.contains(row.toString))
      cnt+=1
    })

    assert(expectedSet.size == cnt)
  }

  test("successful erroring of Drilldown curator when join key not present in request") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "drilldown" : {
                              "config" : {
                                "dimension": "Remarks"
                              }
                            }
                          },
                          "selectFields": [
                            {"field": "Class Name"},
                            {"field": "Student Name"},
                            {"field": "Total Marks"}
                          ],
                          "sortBy": [
                            {"field": "Total Marks", "order": "Desc"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"},
                            {"field": "Class ID", "operator": "=", "value": "200"}
                          ],
                          "includeRowCount": false
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

    val requestCoordinatorResult: RequestCoordinatorResult = getRequestCoordinatorResult(requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper))
    val drillDownError: CuratorError = requestCoordinatorResult.failureResults(DrilldownCurator.name)
    assert(drillDownError.error.message === """requirement failed: Primary key of most granular dim MUST be present in requested cols to join against : Student ID""")
  }

  test("Test failed processing of Drilldown curator on non-existent DrillDown Dim") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "drilldown" : {
                              "config" : {
                                "dimension": "Gender"
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

    val requestCoordinatorResult: RequestCoordinatorResult = getRequestCoordinatorResult(requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper))
    assert(requestCoordinatorResult.successResults.contains(DefaultCurator.name))
    val drillDownCuratorError: CuratorError = requestCoordinatorResult.failureResults(DrilldownCurator.name)
    assert(drillDownCuratorError.error.message.contains("Gender"))
  }

  test("Test successful processing of TotalMetrics Curator") {

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

    val requestCoordinatorResult: RequestCoordinatorResult = getRequestCoordinatorResult(requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper))
    assert(requestCoordinatorResult.successResults.contains(DefaultCurator.name))
    val totalMetricsCuratorResult: RequestResult = requestCoordinatorResult.successResults(TotalMetricsCurator.name)
    val expectedSet = Set("Row(Map(Total Marks -> 0),ArrayBuffer(480))")

    var cnt = 0
    totalMetricsCuratorResult.queryPipelineResult.rowList.foreach( row => {
      
      assert(expectedSet.contains(row.toString))
      cnt+=1
    })

    assert(expectedSet.size == cnt)
  }

  test("Test successful processing of TotalMetrics Curator with bad forceRevision") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "totalmetrics" : {
                              "config" : {
                                "forceRevision": 10
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

    val requestCoordinatorResult: RequestCoordinatorResult = getRequestCoordinatorResult(requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper))
    assert(requestCoordinatorResult.successResults.contains(DefaultCurator.name))
    val totalMetricsCuratorResult: RequestResult = requestCoordinatorResult.successResults(TotalMetricsCurator.name)
    val expectedSet = Set("Row(Map(Total Marks -> 0),ArrayBuffer(480))")

    var cnt = 0
    totalMetricsCuratorResult.queryPipelineResult.rowList.foreach( row => {

      assert(expectedSet.contains(row.toString))
      cnt+=1
    })

    assert(expectedSet.size == cnt)
  }

  test("Test successful processing of Default curator and RowCountCurator MultiEngine (druid+oracle) case") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Class ID"},
                            {"field": "Section ID"},
                            {"field": "Total Marks"},
                            {"field": "Student Name"}
                          ],
                          "sortBy": [
                            {"field": "Total Marks", "order": "Desc"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"}
                          ],
                         "includeRowCount" : true,
                         "forceDimensionDriven" : true
                        }"""
    val reportingRequestResult = ReportingRequest.deserializeSyncWithFactBias(jsonRequest.getBytes, schema = StudentSchema)
    require(reportingRequestResult.isSuccess)
    val reportingRequest = reportingRequestResult.toOption.get

    // Revision 1 is druid + oracle case
    val bucketParams = BucketParams(UserInfo("uid", isInternal = true), forceRevision = Some(1))

    val mahaRequestContext = MahaRequestContext(REGISTRY,
      bucketParams,
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")
    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter)

    val requestCoordinator: RequestCoordinator = DefaultRequestCoordinator(mahaService)

    val requestCoordinatorResult: RequestCoordinatorResult = requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper).right.get.get().right.get
    val defaultCuratorRequestResult: RequestResult = requestCoordinatorResult.successResults(DefaultCurator.name)
    val rowcountCuratorRequestResult: CuratorError = requestCoordinatorResult.failureResults(RowCountCurator.name)

    val defaultExpectedSet = Set(
      "Row(Map(Section ID -> 2, Student Name -> 4, Student ID -> 0, Total Marks -> 3, Class ID -> 1),ArrayBuffer(213, 200, 100, 99, Bryant))"
    )

    var defaultCount = 0
    defaultCuratorRequestResult.queryPipelineResult.rowList.foreach(
      row => {
        //
        assert(defaultExpectedSet.contains(row.toString))
        defaultCount+=1
      })

    assert(defaultExpectedSet.size == defaultCount)


    // H2 can not execute the Oracle Specific syntax of COUNT(*) OVER([*]) TOTALROWS, h2 has plan to fix it in next release 1.5
    val rowCountCuratorError = rowcountCuratorRequestResult.error.throwableOption.get
    assert(rowCountCuratorError.isInstanceOf[MahaServiceExecutionException])
    val mahaServiceExecutionException = rowCountCuratorError.asInstanceOf[MahaServiceExecutionException]
    assert(mahaServiceExecutionException.source.get.getMessage.contains("Syntax error in SQL statement"))

    // Setting the rowCount as  rowCountCurator fails
    mahaRequestContext.mutableState.put(RowCountCurator.name, 1)

    val jsonStreamingOutput = JsonOutputFormat(requestCoordinatorResult)

    val stringStream =  new StringStream()

    jsonStreamingOutput.writeStream(stringStream)
    val result = stringStream.toString()

    val expectedJson = s"""{"header":{"cube":"student_performance","fields":[{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Class ID","fieldType":"DIM"},{"fieldName":"Section ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"},{"fieldName":"Student Name","fieldType":"DIM"},{"fieldName":"ROW_COUNT","fieldType":"CONSTANT"}],"maxRows":200},"rows":[[213,200,100,99,"Bryant",1]],"curators":{}}"""
    

    assert(result.contains(expectedJson))

    //assert on misc variables
    val tempCurator: RowCountCurator = new RowCountCurator()
    assert(!tempCurator.isSingleton)
    assert(!tempCurator.requiresDefaultCurator)
    assert(tempCurator.level == 1)
    assert(tempCurator.priority == 1)

  }

  test("Test should generate default total row query on Oracle-Only, Dim-only request in OracleQueryGenerator.") {

    val jsonRequest =
      s"""{
                          "cube": "student_performance",
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Admitted Year"},
                            {"field": "Student Name"}
                          ],
                          "sortBy": [
                            {"field": "Student ID", "order": "Desc"},
                            {"field": "Admitted Year", "order": "Asc"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"}
                          ],
                         "includeRowCount" : true,
                         "forceDimensionDriven" : true,
                         "additionalParameters": {"debug": true}
                        }"""
    val reportingRequestResult = ReportingRequest.deserializeSyncWithFactBias(jsonRequest.getBytes, schema = StudentSchema)
    require(reportingRequestResult.isSuccess)
    val reportingRequest = ReportingRequest.enableDebug(reportingRequestResult.toOption.get)

    // Revision 1 is druid + oracle case
    val bucketParams = BucketParams(UserInfo("uid", isInternal = true), forceRevision = Some(1))

    val mahaRequestContext = MahaRequestContext(REGISTRY,
      bucketParams,
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")
    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter)

    val requestCoordinator: RequestCoordinator = DefaultRequestCoordinator(mahaService)

    val requestCoordinatorResult1 = requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper)
    val requestCoordinatorResult2 = requestCoordinatorResult1.right.get.get()

    /**
      * Currently generates :
      * SELECT  *
      * FROM (SELECT s0.id "Student ID", s0.admitted_year "Admitted Year", s0.name "Student Name", Count(*) OVER() TOTALROWS, ROWNUM as ROW_NUMBER
      * FROM
      * (SELECT  id, admitted_year, name
      * FROM student
      * WHERE (id = 213)
      * ORDER BY 1 DESC , 2 ASC NULLS LAST ) s0
      *
      *
      * )
      * WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
      *
      * This is a row count query with preserved ordering.    The goal in this test is to figure out where we can remove ordering in the default query,
      * as UI-generated dim only queries with 1 row requested do not require ordering & default to oracle-only single engine.
      */
  }

  test("successful remove of DrillDown curator cross-cube fields when second cube lacks facts from initial request") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "drilldown" : {
                              "config" : {
                                "dimension": "Class ID",
                                "cube": "student_performance2"
                              }
                            }
                          },
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Total Marks"},
                            {"field": "Remarks"},
                            {"field": "Class ID"},
                            {"field": "Performance Factor"}
                          ],
                          "sortBy": [
                            {"field": "Total Marks", "order": "Desc"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"},
                            {"field": "Class ID", "operator": "=", "value": "200"}
                          ],
                          "includeRowCount": false
                        }"""
    val reportingRequestResult = ReportingRequest.deserializeSyncWithFactBias(jsonRequest.getBytes, schema = StudentSchema)
    require(reportingRequestResult.isSuccess)
    val reportingRequest = reportingRequestResult.toOption.get.copy(additionalParameters = Map(Parameter.Debug -> DebugValue(value = true)))

    // Revision 1 is druid + oracle case
    val bucketParams = BucketParams(UserInfo("uid", isInternal = true), forceRevision = Some(1))

    val mahaRequestContext = MahaRequestContext(REGISTRY,
      bucketParams,
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")
    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter)

    val requestCoordinator: RequestCoordinator = DefaultRequestCoordinator(mahaService)

    val requestCoordinatorResult: RequestCoordinatorResult = getRequestCoordinatorResult(requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper))
    assert(requestCoordinatorResult.successResults.contains(DefaultCurator.name))
    assert(requestCoordinatorResult.successResults.contains(DrilldownCurator.name))
    val jsonStreamingOutput = JsonOutputFormat(requestCoordinatorResult)

    val stringStream =  new StringStream()

    jsonStreamingOutput.writeStream(stringStream)
    val result = stringStream.toString()

    val expectedJson = s"""{"drilldown":{"result":{"header":{"cube":"student_performance2","fields":[{"fieldName":"Class ID","fieldType":"DIM"},{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"}],"maxRows":1000,"debug":{}}"""
    

    assert(result.contains(expectedJson))
  }

  test("Force fail DrillDown using wrong drill down cube.") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "drilldown" : {
                              "config" : {
                                "dimension": "Class ID",
                                "cube": "student_performance222"
                              }
                            }
                          },
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Total Marks"},
                            {"field": "Remarks"},
                            {"field": "Class ID"},
                            {"field": "Performance Factor"}
                          ],
                          "sortBy": [
                            {"field": "Total Marks", "order": "Desc"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"},
                            {"field": "Class ID", "operator": "=", "value": "200"}
                          ],
                          "includeRowCount": false
                        }"""
    val reportingRequestResult = ReportingRequest.deserializeSyncWithFactBias(jsonRequest.getBytes, schema = StudentSchema)
    require(reportingRequestResult.isSuccess)
    val reportingRequest = reportingRequestResult.toOption.get.copy(additionalParameters = Map(Parameter.Debug -> DebugValue(value = true)))

    // Revision 1 is druid + oracle case
    val bucketParams = BucketParams(UserInfo("uid", isInternal = true), forceRevision = Some(10))

    val mahaRequestContext = MahaRequestContext(REGISTRY,
      bucketParams,
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")
    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter)

    val requestCoordinator: RequestCoordinator = DefaultRequestCoordinator(mahaService)

    val requestCoordinatorResult: RequestCoordinatorResult = getRequestCoordinatorResult(requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper))
    assert(requestCoordinatorResult.successResults.contains(DefaultCurator.name))
    assert(requestCoordinatorResult.failureResults.contains(DrilldownCurator.name))
    val jsonStreamingOutput = JsonOutputFormat(requestCoordinatorResult)

    val stringStream =  new StringStream()

    jsonStreamingOutput.writeStream(stringStream)
    val result = stringStream.toString()

    val expectedJson = s"""{"message":"MahaServiceBadRequestException: requirement failed: Default revision not found for cube student_performance222 in the registry"}"""
    

    assert(result.contains(expectedJson))
  }

  test("DrillDown Curator should discard non selected orderBy fields") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "drilldown" : {
                              "config" : {
                                "dimension": "Class ID",
                                "cube": "student_performance2"
                              }
                            }
                          },
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Total Marks"},
                            {"field": "Student Name"},
                            {"field": "Class ID"},
                            {"field": "Performance Factor"}
                          ],
                          "sortBy": [
                            {"field": "Student Name", "order": "Desc"},
                            {"field": "Total Marks", "order": "Desc"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"},
                            {"field": "Class ID", "operator": "=", "value": "200"}
                          ],
                          "includeRowCount": false
                        }"""
    val reportingRequestResult = ReportingRequest.deserializeSyncWithFactBias(jsonRequest.getBytes, schema = StudentSchema)
    require(reportingRequestResult.isSuccess)
    val reportingRequest = reportingRequestResult.toOption.get.copy(additionalParameters = Map(Parameter.Debug -> DebugValue(value = true)))

    // Revision 1 is druid + oracle case
    val bucketParams = BucketParams(UserInfo("uid", isInternal = true), forceRevision = Some(10))

    val mahaRequestContext = MahaRequestContext(REGISTRY,
      bucketParams,
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")
    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter)

    val requestCoordinator: RequestCoordinator = DefaultRequestCoordinator(mahaService)

    val requestCoordinatorResult: RequestCoordinatorResult = getRequestCoordinatorResult(requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper))

    assert(requestCoordinatorResult.successResults.contains(DefaultCurator.name))
    assert(requestCoordinatorResult.successResults.contains(DrilldownCurator.name))

    val drillDownCuratorResult = requestCoordinatorResult.curatorResult(DrilldownCurator.name)

    val drillDownReportingRequest = drillDownCuratorResult.requestModelReference.model.reportingRequest

    assert(drillDownReportingRequest.sortBy.size == 1)
    assert(drillDownReportingRequest.sortBy.map(_.field).contains("Total Marks"))

    val jsonStreamingOutput = JsonOutputFormat(requestCoordinatorResult)

    val stringStream =  new StringStream()

    jsonStreamingOutput.writeStream(stringStream)
    val result = stringStream.toString()

    println(result)

    val expectedJson = s"""{"header":{"cube":"student_performance","fields":[{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"},{"fieldName":"Student Name","fieldType":"DIM"},{"fieldName":"Class ID","fieldType":"DIM"},{"fieldName":"Performance Factor","fieldType":"FACT"}],"maxRows":200,"debug":{}},"rows":[[213,125,"Bryant",200,null]],"curators":{"drilldown":{"result":{"header":{"cube":"student_performance2","fields":[{"fieldName":"Class ID","fieldType":"DIM"},{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"}],"maxRows":1000,"debug":{}},"rows":[[198,213,180],[199,213,175],[200,213,125]]}}}}"""

    assert(result === expectedJson)
  }

  test("DrillDown Curator should work correctly with level 3 dimension with the id injections(Here Section ID is being injected in the drilldown request)") {

    val jsonRequest = s"""{
                          "cube": "student_performance2",
                          "curators" : {
                            "drilldown" : {
                              "config" : {
                                "enforceFilters": true,
                                "dimension": "Section Status",
                                "cube": "student_performance2"
                              }
                            }
                          },
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Total Marks"},
                            {"field": "Section ID"}
                          ],
                          "sortBy": [
                            {"field": "Total Marks", "order": "Desc"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"}
                          ],
                          "includeRowCount": false
                        }"""
    val reportingRequestResult = ReportingRequest.deserializeSyncWithFactBias(jsonRequest.getBytes, schema = StudentSchema)
    require(reportingRequestResult.isSuccess)
    val reportingRequest = reportingRequestResult.toOption.get.copy(additionalParameters = Map(Parameter.Debug -> DebugValue(value = true)))

    // Revision 1 is druid + oracle case
    val bucketParams = BucketParams(UserInfo("uid", isInternal = true), forceRevision = Some(10))

    val mahaRequestContext = MahaRequestContext(REGISTRY,
      bucketParams,
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")
    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter)

    val requestCoordinator: RequestCoordinator = DefaultRequestCoordinator(mahaService)

    val requestCoordinatorResult: RequestCoordinatorResult = getRequestCoordinatorResult(requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper))

    assert(requestCoordinatorResult.successResults.contains(DefaultCurator.name))
    assert(requestCoordinatorResult.successResults.contains(DrilldownCurator.name))

    val drillDownCuratorResult = requestCoordinatorResult.curatorResult(DrilldownCurator.name)

    val drillDownReportingRequest = drillDownCuratorResult.requestModelReference.model.reportingRequest

    assert(drillDownReportingRequest.sortBy.size == 1)
    assert(drillDownReportingRequest.sortBy.map(_.field).contains("Total Marks"))

    val jsonStreamingOutput = JsonOutputFormat(requestCoordinatorResult)

    val stringStream =  new StringStream()

    jsonStreamingOutput.writeStream(stringStream)
    val result = stringStream.toString()

    println(result)

    val expectedJson = s"""{"header":{"cube":"student_performance2","fields":[{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"},{"fieldName":"Section ID","fieldType":"DIM"}],"maxRows":200,"debug":{}},"rows":[[213,305,100],[213,175,200]],"curators":{"drilldown":{"result":{"header":{"cube":"student_performance2","fields":[{"fieldName":"Section Status","fieldType":"DIM"},{"fieldName":"Section ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"}],"maxRows":1000,"debug":{}},"rows":[[null,0,305],[null,0,175]]}}}}"""

    assert(result === expectedJson)
  }

  test("DrillDown Curator should work correctly with level 3 dimension with the id injections with NO Data from Default Curator(Here Section ID is being injected in the drilldown request)") {

    val jsonRequest = s"""{
                          "cube": "student_performance2",
                          "curators" : {
                            "drilldown" : {
                              "config" : {
                                "enforceFilters": true,
                                "dimension": "Section Status",
                                "cube": "student_performance2"
                              }
                            }
                          },
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Total Marks"},
                            {"field": "Section ID"}
                          ],
                          "sortBy": [
                            {"field": "Total Marks", "order": "Desc"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "12345"}
                          ],
                          "includeRowCount": false
                        }"""
    val reportingRequestResult = ReportingRequest.deserializeSyncWithFactBias(jsonRequest.getBytes, schema = StudentSchema)
    require(reportingRequestResult.isSuccess)
    val reportingRequest = reportingRequestResult.toOption.get.copy(additionalParameters = Map(Parameter.Debug -> DebugValue(value = true)))

    // Revision 1 is druid + oracle case
    val bucketParams = BucketParams(UserInfo("uid", isInternal = true), forceRevision = Some(10))

    val mahaRequestContext = MahaRequestContext(REGISTRY,
      bucketParams,
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")
    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter)

    val requestCoordinator: RequestCoordinator = DefaultRequestCoordinator(mahaService)

    val requestCoordinatorResult: RequestCoordinatorResult = getRequestCoordinatorResult(requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper))

    assert(requestCoordinatorResult.successResults.contains(DefaultCurator.name))
    assert(requestCoordinatorResult.successResults.contains(DrilldownCurator.name))

    val drillDownCuratorResult = requestCoordinatorResult.curatorResult(DrilldownCurator.name)

    val drillDownReportingRequest = drillDownCuratorResult.requestModelReference.model.reportingRequest

    assert(drillDownReportingRequest.sortBy.size == 1)
    assert(drillDownReportingRequest.sortBy.map(_.field).contains("Total Marks"))

    val jsonStreamingOutput = JsonOutputFormat(requestCoordinatorResult)

    val stringStream =  new StringStream()

    jsonStreamingOutput.writeStream(stringStream)
    val result = stringStream.toString()

    println(result)

    val expectedJson = s"""{"header":{"cube":"student_performance2","fields":[{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"},{"fieldName":"Section ID","fieldType":"DIM"}],"maxRows":200,"debug":{}},"rows":[],"curators":{"drilldown":{"result":{"header":{"cube":"student_performance2","fields":[{"fieldName":"Section Status","fieldType":"DIM"},{"fieldName":"Section ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"}],"maxRows":1000,"debug":{}},"rows":[]}}}}"""

    assert(result === expectedJson)
  }

  test("DrillDown Curator should override the paginationStartIndex given in the original request") {

    val jsonRequest = s"""{
                          "cube": "student_performance2",
                          "curators" : {
                            "drilldown" : {
                              "config" : {
                                "enforceFilters": true,
                                "dimension": "Section Status",
                                "cube": "student_performance2"
                              }
                            }
                          },
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Total Marks"},
                            {"field": "Section ID"}
                          ],
                          "sortBy": [
                            {"field": "Total Marks", "order": "Desc"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"}
                          ],
                          "includeRowCount": false,
                          "paginationStartIndex" : 1
                        }"""
    val reportingRequestResult = ReportingRequest.deserializeSyncWithFactBias(jsonRequest.getBytes, schema = StudentSchema)
    require(reportingRequestResult.isSuccess)
    val reportingRequest = reportingRequestResult.toOption.get.copy(additionalParameters = Map(Parameter.Debug -> DebugValue(value = true)))

    // Revision 1 is druid + oracle case
    val bucketParams = BucketParams(UserInfo("uid", isInternal = true), forceRevision = Some(10))

    val mahaRequestContext = MahaRequestContext(REGISTRY,
      bucketParams,
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")
    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter)

    val requestCoordinator: RequestCoordinator = DefaultRequestCoordinator(mahaService)

    val requestCoordinatorResult: RequestCoordinatorResult = getRequestCoordinatorResult(requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper))

    assert(requestCoordinatorResult.successResults.contains(DefaultCurator.name))
    assert(requestCoordinatorResult.successResults.contains(DrilldownCurator.name))

    val drillDownCuratorResult = requestCoordinatorResult.curatorResult(DrilldownCurator.name)

    val drillDownReportingRequest = drillDownCuratorResult.requestModelReference.model.reportingRequest

    assert(drillDownReportingRequest.sortBy.size == 1)
    assert(drillDownReportingRequest.sortBy.map(_.field).contains("Total Marks"))

    val jsonStreamingOutput = JsonOutputFormat(requestCoordinatorResult)

    val stringStream =  new StringStream()

    jsonStreamingOutput.writeStream(stringStream)
    val result = stringStream.toString()

    println(result)

    val expectedJson = s"""{"header":{"cube":"student_performance2","fields":[{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"},{"fieldName":"Section ID","fieldType":"DIM"}],"maxRows":200,"debug":{}},"rows":[[213,175,200]],"curators":{"drilldown":{"result":{"header":{"cube":"student_performance2","fields":[{"fieldName":"Section Status","fieldType":"DIM"},{"fieldName":"Section ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"}],"maxRows":1000,"debug":{}},"rows":[[null,0,175]]}}}}"""

    assert(result === expectedJson)
  }

}

