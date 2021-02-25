// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service

import com.yahoo.maha.core.bucketing.{BucketParams, UserInfo}
import com.yahoo.maha.core.query.OracleQuery
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
import org.scalatest.matchers.should.Matchers._

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
    val insertSql = """INSERT INTO student_grade_sheet (year, section_id, student_id, class_id, total_marks, obtained_marks, date, comment, month, top_student_id, researcher_id, lab_id, class_volunteer_id, science_lab_volunteer_id, tutor_id)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    val studentInsertSql =
      """INSERT INTO student (profile_url, id, name, admitted_year, status, department_id, researcher_id, class_volunteer_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""

    val classInsertSql =
      """INSERT INTO class (id, name, start_year, status, department_id, professor)
        VALUES (?, ?, ?, ?, ?, ?)"""

    val sectionInsertSql =
      """INSERT INTO section (id, name, student_id, class_id, start_year, status, lab_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)"""

    val rows: List[Seq[Any]] = List(
      Seq(1, 100, 213, 200, 135, 135, DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(9)), "some comment 1", today.toString, 213, 122, 2, 101, 201, 301),
      Seq(1, 100, 213, 198, 120, 120, DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(10)), "some comment 2", today.toString, 213, 122, 2, 101, 201, 301),
      Seq(1, 500, 213, 197, 190, 190, DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(10)), "some comment 3", today.toString, 213, 122, 2, 101, 201, 301),
      Seq(1, 100, 213, 200, 125, 125, today.toString, "some comment 1", today.toString, 213, 122, 2, 101, 201, 301),
      Seq(1, 100, 213, 198, 180, 180, yesterday.toString, "some comment 2", today.toString, 213, 122, 2, 101, 201, 301),
      Seq(1, 200, 213, 199, 175, 175, today.toString, "some comment 3", today.toString, 213, 122, 2, 101, 201, 301),
      Seq(1, 311, 214, 201, 100, 90, DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(4)), "some comment 1", today.toString, 213, 122, 2, 102, 202, 302),
      Seq(1, 311, 214, 201, 125, 100, today.toString, "some comment 1", today.toString, 213, 122, 2, 102, 202, 302),
      Seq(1, 311, 214, 198, 180, 150, yesterday.toString, "some comment 2", today.toString, 213, 122, 2, 102, 202, 302),
      Seq(1, 311, 214, 199, 175, 145, today.toString, "some comment 3", today.toString, 213, 122, 2, 102, 202, 302),
    )

    val studentRows: List[Seq[Any]] = List(
      Seq("www.google.com",213, "ACTIVE", 2017, "ACTIVE", 54321, 122, 101),
      Seq("www.google2.com",214, "ACTIVE", 2017, "ACTIVE", 54321, 122, 102)
    )

    val classRows: List[Seq[Any]] = List(
      Seq(200, "Class A", 2017, "ACTIVE", 54321, "M. Byson"),
      Seq(201, "Class B", 2017, "ACTIVE", 54321, "L. Johnson")
    )

    val sectionRows: List[Seq[Any]] = List(
      Seq(310, "Section A", 213, 200, 2017, "ACTIVE", 2),
      Seq(311, "Section B", 214, 201, 2017, "ACTIVE", 2)
    )

    rows.foreach {
      row =>
        val result = jdbcConnection.get.executeUpdate(insertSql, row)
        assert(result.isSuccess, s"Insertion failed: ${result.failed}")
    }

    rows.foreach {
      row =>
        val result = jdbcConnection.get.executeUpdate(insertSql.replaceAllLiterally("student_grade_sheet", "student_grade_sheet_again"), row)
        assert(result.isSuccess, s"Insertion failed: ${result.failed}")
    }

    studentRows.foreach{
      row =>
        val result = jdbcConnection.get.executeUpdate(studentInsertSql, row)
        assert(result.isSuccess, s"Insertion failed: ${result.failed}")
    }

    classRows.foreach{
      row =>
        val result = jdbcConnection.get.executeUpdate(classInsertSql, row)
        assert(result.isSuccess, s"Insertion failed: ${result.failed}")
    }

    sectionRows.foreach {
      row =>
        val result = jdbcConnection.get.executeUpdate(sectionInsertSql, row)
        assert(result.isSuccess, s"Insertion failed: ${result.failed}")
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

  test("Test successful student award query - top student in class above a threshold grade.") {
    val jsonRequest =
      s"""
         |{
         |  "cube": "student_performance",
         |  "selectFields": [
         |    {"field": "Student ID"},
         |    {"field": "Student Name"},
         |    {"field": "Marks Obtained"},
         |    {"field": "Top Student ID"},
         |    {"field": "Professor Name"}
         |  ],
         |  "sortBy": [
         |    {"field": "Top Student ID", "order": "Desc"}
         |  ],
         |  "filterExpressions": [
         |    {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
         |    {"field": "Student Name", "operator": "==", "compareTo": "Student Status"},
         |    {"field": "Student ID", "operator": "between", "from": "0", "to": "1000"}
         |  ]
         |}
       """.stripMargin

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
    val defaultCuratorRequestResult: RequestResult = requestCoordinatorResult.successResults(DefaultCurator.name).head.requestResult

    val requestResultString = defaultCuratorRequestResult.queryPipelineResult.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    var defaultCount = 0
    defaultCuratorRequestResult.queryPipelineResult.rowList.foreach( row => {

      //println(row.toString)
      defaultCount+=1
    })

    assert(defaultCount == 2)
    val expectedStringList = List(
      "WHERE (student_id >= 0 AND student_id <= 1000)", "WHERE (id >= 0 AND id <= 1000) AND (name = status)")
    for(item <- expectedStringList) assert(requestResultString.contains(item))
  }

  test("Test successful student award query - query via metrics") {
    val jsonRequest =
      s"""
         |{
         |  "cube": "student_performance",
         |  "selectFields": [
         |    {"field": "Student ID"},
         |    {"field": "Student Name"},
         |    {"field": "Marks Obtained"},
         |    {"field": "Total Marks"},
         |    {"field": "Top Student ID"},
         |    {"field": "Professor Name"}
         |  ],
         |  "sortBy": [
         |    {"field": "Top Student ID", "order": "Desc"}
         |  ],
         |  "filterExpressions": [
         |    {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
         |    {"field": "Marks Obtained", "operator": "==", "compareTo": "Total Marks"},
         |    {"field": "Student ID", "operator": "between", "from": "0", "to": "1000"}
         |  ]
         |}
       """.stripMargin

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
    val defaultCuratorRequestResult: RequestResult = requestCoordinatorResult.successResults(DefaultCurator.name).head.requestResult

    val requestResultString = defaultCuratorRequestResult.queryPipelineResult.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString

    var defaultCount = 0
    defaultCuratorRequestResult.queryPipelineResult.rowList.foreach( row => {

      assert(row.toString.contains("355, 355") || row.toString.contains("125, 125"))
      defaultCount+=1
    })

    assert(defaultCount == 2)
    val expectedStringList = List(
      "HAVING (SUM(obtained_marks) = SUM(total_marks))", s"""ORDER BY "Top Student ID" DESC""")
    for(item <- expectedStringList) assert(requestResultString.contains(item))
  }

  test("Test failed student award query - can't compare metric to dimension.") {
    val jsonRequest =
      s"""
         |{
         |  "cube": "student_performance",
         |  "selectFields": [
         |    {"field": "Student ID"},
         |    {"field": "Student Name"},
         |    {"field": "Marks Obtained"},
         |    {"field": "Total Marks"},
         |    {"field": "Top Student ID"},
         |    {"field": "Professor Name"}
         |  ],
         |  "sortBy": [
         |    {"field": "Top Student ID", "order": "Desc"}
         |  ],
         |  "filterExpressions": [
         |    {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
         |    {"field": "Marks Obtained", "operator": "==", "compareTo": "Student ID"},
         |    {"field": "Student ID", "operator": "between", "from": "0", "to": "1000"}
         |  ]
         |}
       """.stripMargin

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

    val thrown = intercept[IllegalArgumentException] {
      getRequestCoordinatorResult(requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper))
    }

    assert(thrown.getMessage.contains("Metric-Dim Comparison Failed: Can only compare dim-dim or metric-metric"))

  }

  test("Test failed student award query - can't compare dimension to metric.") {
    val jsonRequest =
      s"""
         |{
         |  "cube": "student_performance",
         |  "selectFields": [
         |    {"field": "Student ID"},
         |    {"field": "Student Name"},
         |    {"field": "Marks Obtained"},
         |    {"field": "Total Marks"},
         |    {"field": "Top Student ID"},
         |    {"field": "Professor Name"}
         |  ],
         |  "sortBy": [
         |    {"field": "Top Student ID", "order": "Desc"}
         |  ],
         |  "filterExpressions": [
         |    {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
         |    {"field": "Top Student ID", "operator": "==", "compareTo": "Marks Obtained"},
         |    {"field": "Student ID", "operator": "between", "from": "0", "to": "1000"}
         |  ]
         |}
       """.stripMargin

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

    val thrown = intercept[IllegalArgumentException] {
      getRequestCoordinatorResult(requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper))
    }

    assert(thrown.getMessage.contains("Dim-Metric Comparison Failed: Can only compare dim-dim or metric-metric"))

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
    val defaultCuratorRequestResult: RequestResult = requestCoordinatorResult.successResults(DefaultCurator.name).head.requestResult

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
    val defaultCuratorRequestResult: RequestResult = requestCoordinatorResult.successResults(DefaultCurator.name).head.requestResult

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
    val timeShiftCuratorRequestResult: RequestResult = requestCoordinatorResult.successResults(TimeShiftCurator.name).head.requestResult

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

  test("Test successful processing of Timeshift curator with days offset") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "test-blah-curator": { "config": {}},
                            "test-blah_curator2": { "config": {}},
                            "timeshift" : {
                              "config" : {
                                "daysOffset": 1
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
                            {"field": "Total Marks", "order": "Desc"},
                            {"field": "Class ID", "order": "Desc"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$yesterday", "to": "$today"},
                            {"field": "Student ID", "operator": "=", "value": "214"}
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
    val timeShiftCuratorRequestResult: RequestResult = requestCoordinatorResult.successResults(TimeShiftCurator.name).head.requestResult

    val expectedSeq = IndexedSeq(
      "Row(Map(Total Marks Prev -> 4, Section ID -> 2, Total Marks Pct Change -> 5, Student ID -> 0, Total Marks -> 3, Class ID -> 1),ArrayBuffer(214, 198, 311, 180, 0, 100.0))",
      "Row(Map(Total Marks Prev -> 4, Section ID -> 2, Total Marks Pct Change -> 5, Student ID -> 0, Total Marks -> 3, Class ID -> 1),ArrayBuffer(214, 199, 311, 175, 0, 100.0))",
      "Row(Map(Total Marks Prev -> 4, Section ID -> 2, Total Marks Pct Change -> 5, Student ID -> 0, Total Marks -> 3, Class ID -> 1),ArrayBuffer(214, 201, 311, 125, 100, 25.0))"
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
    val timeShiftCuratorRequestResult: RequestResult = requestCoordinatorResult.successResults(TimeShiftCurator.name).head.requestResult

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
    val timeShiftCuratorRequestResult: RequestResult = requestCoordinatorResult.successResults(TimeShiftCurator.name).head.requestResult

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
    val timeShiftCuratorRequestResult: RequestResult = requestCoordinatorResult.successResults(TimeShiftCurator.name).head.requestResult

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
    assert(requestCoordinatorResult.failureResults(FailingCurator.name).head.error.message === "failed")
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
    assert(requestCoordinatorResult.failureResults(FailingCurator.name).head.error.message === "failed")
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
    assert(requestCoordinatorResult.failureResults(FailingCurator.name).head.error.message === "failed")
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
                                "dimensions": ["Year", "Remarks"]
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
    val drillDownCuratorResult: RequestResult = requestCoordinatorResult.successResults(DrilldownCurator.name).head.requestResult
    val expectedSet = Set(
      "Row(Map(Year -> 0, Remarks -> 1, Student ID -> 2, Total Marks -> 3),ArrayBuffer(Freshman, some comment 1, 213, 125))",
      "Row(Map(Year -> 0, Remarks -> 1, Student ID -> 2, Total Marks -> 3),ArrayBuffer(Freshman, some comment 2, 213, 180))",
      "Row(Map(Year -> 0, Remarks -> 1, Student ID -> 2, Total Marks -> 3),ArrayBuffer(Freshman, some comment 3, 213, 175))",
    )

    var cnt = 0
    drillDownCuratorResult.queryPipelineResult.rowList.foreach( row => {
      
      assert(expectedSet.contains(row.toString), row.toString)
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
    val drillDownCuratorResult: RequestResult = requestCoordinatorResult.successResults(DrilldownCurator.name).head.requestResult
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

  test("Test successful processing of Drilldown curator with primary key and no dims") {

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
                            {"field": "Section ID"},
                            {"field": "Top Student ID"},
                            {"field": "Total Marks"}
                          ],
                          "sortBy": [
                            {"field": "Total Marks", "order": "Desc"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "214"}
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
    assert(requestCoordinatorResult.successResults.contains(DrilldownCurator.name), requestCoordinatorResult.failureResults)
    val drillDownCuratorResult: RequestResult = requestCoordinatorResult.successResults(DrilldownCurator.name).head.requestResult
    val expectedSet = Set(
      "Row(Map(Remarks -> 0, Section ID -> 1, Total Marks -> 2),ArrayBuffer(some comment 1, 311, 225))"
      , "Row(Map(Remarks -> 0, Section ID -> 1, Total Marks -> 2),ArrayBuffer(some comment 2, 311, 180))"
      , "Row(Map(Remarks -> 0, Section ID -> 1, Total Marks -> 2),ArrayBuffer(some comment 3, 311, 175))"
    )

    var cnt = 0
    drillDownCuratorResult.queryPipelineResult.rowList.foreach( row => {

      assert(expectedSet.contains(row.toString))
      cnt+=1
    })

    assert(expectedSet.size == cnt)
  }

  test("Test successful processing of Drilldown curator with filters") {

    //the Student ID filter in drill down should be ignored in favor of the same filter in outer query
    val jsonRequest =
      s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "drilldown" : {
                              "config" : {
                                "dimension": "Remarks",
                                "enforceFilters": true,
                                "filters": [{
                                    "field": "Remarks",
                                    "operator": "IN",
                                    "values": ["some comment 1", "some comment 2"]
                                }, {"field": "Student ID", "operator": "=", "value": "100"}]
                              }
                            }
                          },
                          "selectFields": [
                            {"field": "Section ID"},
                            {"field": "Top Student ID"},
                            {"field": "Total Marks"}
                          ],
                          "sortBy": [
                            {"field": "Total Marks", "order": "Desc"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "214"}
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
    assert(requestCoordinatorResult.successResults.contains(DrilldownCurator.name), requestCoordinatorResult.failureResults)
    val drillDownCuratorResult: RequestResult = requestCoordinatorResult.successResults(DrilldownCurator.name).head.requestResult
    val expectedSet = Set(
      "Row(Map(Remarks -> 0, Section ID -> 1, Total Marks -> 2),ArrayBuffer(some comment 1, 311, 225))"
      , "Row(Map(Remarks -> 0, Section ID -> 1, Total Marks -> 2),ArrayBuffer(some comment 2, 311, 180))"
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
    val drillDownCuratorResult: RequestResult = requestCoordinatorResult.successResults(DrilldownCurator.name).head.requestResult
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
    val drillDownError: CuratorError = requestCoordinatorResult.failureResults(DrilldownCurator.name).head
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
    val drillDownCuratorError: CuratorError = requestCoordinatorResult.failureResults(DrilldownCurator.name).head
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
    val totalMetricsCuratorResult: RequestResult = requestCoordinatorResult.successResults(TotalMetricsCurator.name).head.requestResult
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
    val totalMetricsCuratorResult: RequestResult = requestCoordinatorResult.successResults(TotalMetricsCurator.name).head.requestResult
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
    val defaultCuratorRequestResult: RequestResult = requestCoordinatorResult.successResults(DefaultCurator.name).head.requestResult
    val rowcountCuratorRequestResult: CuratorError = requestCoordinatorResult.failureResults(RowCountCurator.name).head

    val defaultExpectedSet = Set(
      "Row(Map(Section ID -> 2, Student Name -> 4, Student ID -> 0, Total Marks -> 3, Class ID -> 1),ArrayBuffer(213, 200, 100, 99, ACTIVE))"
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
//
    val expectedJson = """\{"header":\{"cube":"student_performance","fields":\[\{"fieldName":"Student ID","fieldType":"DIM"\},\{"fieldName":"Class ID","fieldType":"DIM"\},\{"fieldName":"Section ID","fieldType":"DIM"\},\{"fieldName":"Total Marks","fieldType":"FACT"\},\{"fieldName":"Student Name","fieldType":"DIM"\},\{"fieldName":"ROW_COUNT","fieldType":"CONSTANT"\}\],"maxRows":200\},"rows":\[\[213,200,100,99,"ACTIVE",1\]\],"curators":\{.*\}\}"""
    result should fullyMatch regex expectedJson

    //assert on misc variables
    val tempCurator: RowCountCurator = new RowCountCurator()
    assert(!tempCurator.isSingleton)
    assert(tempCurator.requiresDefaultCurator)
    assert(tempCurator.level == 1)
    assert(tempCurator.priority == 1)
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
                            {"field": "Remarks2"},
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

    val expectedJson = s"""{"drilldown":{"result":{"header":{"cube":"student_performance2","fields":[{"fieldName":"Class ID","fieldType":"DIM"},{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"}],"maxRows":1000,"debug":{"engineStats":[{"engine":"Oracle","tableName":"student_grade_sheet_again","queryTime":"""
    

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
                                "cube": "student_performance2",
                                "enforceFilters": false
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
    val reportingRequest = reportingRequestResult.toOption.get.copy(additionalParameters = Map(Parameter.Debug -> DebugValue(value = false)))

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

    val drillDownCuratorAndRequestResult = requestCoordinatorResult.successResults(DrilldownCurator.name).head
    val drillDownCuratorResult = drillDownCuratorAndRequestResult.curatorResult

    val drillDownReportingRequest = drillDownCuratorResult.requestModelReference.model.reportingRequest

    assert(drillDownReportingRequest.sortBy.size == 1)
    assert(drillDownReportingRequest.sortBy.map(_.field).contains("Total Marks"))

    val jsonStreamingOutput = JsonOutputFormat(requestCoordinatorResult)

    val stringStream =  new StringStream()

    jsonStreamingOutput.writeStream(stringStream)
    val result = stringStream.toString()



    val expectedJson = s"""{"header":{"cube":"student_performance","fields":[{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"},{"fieldName":"Student Name","fieldType":"DIM"},{"fieldName":"Class ID","fieldType":"DIM"},{"fieldName":"Performance Factor","fieldType":"FACT"}],"maxRows":200},"rows":[[213,125,"ACTIVE",200,1.0]],"curators":{"drilldown":{"result":{"header":{"cube":"student_performance2","fields":[{"fieldName":"Class ID","fieldType":"DIM"},{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"}],"maxRows":1000},"rows":[[198,213,180],[199,213,175],[200,213,125]]}}}}"""

    assert(result === expectedJson)
  }

  test("DrillDown Curator should fail with error on empty filters") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "drilldown" : {
                              "config" : {
                                "dimension": "Class ID",
                                "cube": "student_performance2",
                                "enforceFilters": false
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
                            {"field": "Student ID", "operator": "=", "value": "444"},
                            {"field": "Class ID", "operator": "=", "value": "555"}
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

    val drillDownCuratorResult = requestCoordinatorResult.failureResults(DrilldownCurator.name).head
    assert(drillDownCuratorResult.error.message.contains("requirement failed: Request must apply filters or enforce ReportingRequest input filters!  Check enforceFilters parameter value."))
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
    val reportingRequest = reportingRequestResult.toOption.get.copy(additionalParameters = Map(Parameter.Debug -> DebugValue(value = false)))

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

    val drillDownCuratorAndRequestResult = requestCoordinatorResult.successResults(DrilldownCurator.name).head
    val drillDownCuratorResult = drillDownCuratorAndRequestResult.curatorResult

    val drillDownReportingRequest = drillDownCuratorResult.requestModelReference.model.reportingRequest

    assert(drillDownReportingRequest.sortBy.size == 1)
    assert(drillDownReportingRequest.sortBy.map(_.field).contains("Total Marks"))

    val jsonStreamingOutput = JsonOutputFormat(requestCoordinatorResult)

    val stringStream =  new StringStream()

    jsonStreamingOutput.writeStream(stringStream)
    val result = stringStream.toString()



    val expectedJson = s"""{"header":{"cube":"student_performance2","fields":[{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"},{"fieldName":"Section ID","fieldType":"DIM"}],"maxRows":200},"rows":[[213,305,100],[213,175,200]],"curators":{"drilldown":{"result":{"header":{"cube":"student_performance2","fields":[{"fieldName":"Section Status","fieldType":"DIM"},{"fieldName":"Section ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"}],"maxRows":1000},"rows":[[null,0,305],[null,0,175]]}}}}"""

    assert(result === expectedJson)
  }

  test("DrillDown Curator should work correctly with multiple drilldown requests)") {

    val jsonRequest = s"""{
                          "cube": "student_performance2",
                          "curators" : {
                            "drilldown" : {
                              "config" : [{
                                "enforceFilters": true,
                                "dimension": "Section Status",
                                "cube": "student_performance2",
                                "facts": [{"field": "Marks Obtained"}],
                                "additiveFacts": true
                              },
                              {
                                "enforceFilters": true,
                                "dimension": "Section Start Year",
                                "cube": "student_performance2",
                                "facts": [{"field": "Marks Obtained"}],
                                "additiveFacts": false
                              }]
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
    val reportingRequest = reportingRequestResult.toOption.get.copy(additionalParameters = Map(Parameter.Debug -> DebugValue(value = false)))

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

    //this checks first request
    val drillDownCuratorAndRequestResult = requestCoordinatorResult.successResults(DrilldownCurator.name).head
    val drillDownCuratorResult = drillDownCuratorAndRequestResult.curatorResult

    val drillDownReportingRequest = drillDownCuratorResult.requestModelReference.model.reportingRequest

    assert(drillDownReportingRequest.sortBy.size == 1)
    assert(drillDownReportingRequest.sortBy.map(_.field).contains("Total Marks"))

    //this checks second request
    val drillDownCuratorAndRequestResult2 = requestCoordinatorResult.successResults(DrilldownCurator.name).last
    val drillDownCuratorResult2 = drillDownCuratorAndRequestResult2.curatorResult

    val drillDownReportingRequest2 = drillDownCuratorResult2.requestModelReference.model.reportingRequest

    assert(drillDownReportingRequest2.sortBy.isEmpty)

    val jsonStreamingOutput = JsonOutputFormat(requestCoordinatorResult)

    val stringStream =  new StringStream()

    jsonStreamingOutput.writeStream(stringStream)
    val result = stringStream.toString()
    val expectedJson = """{"header":{"cube":"student_performance2","fields":[{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"},{"fieldName":"Section ID","fieldType":"DIM"}],"maxRows":200},"rows":[[213,305,100],[213,175,200]],"curators":{"drilldown":{"results":[{"index":0,"header":{"cube":"student_performance2","fields":[{"fieldName":"Section Status","fieldType":"DIM"},{"fieldName":"Section ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"},{"fieldName":"Marks Obtained","fieldType":"FACT"}],"maxRows":1000},"rows":[[null,0,305,305],[null,0,175,175]]},{"index":1,"header":{"cube":"student_performance2","fields":[{"fieldName":"Section Start Year","fieldType":"DIM"},{"fieldName":"Section ID","fieldType":"DIM"},{"fieldName":"Marks Obtained","fieldType":"FACT"}],"maxRows":1000},"rows":[[0,0,305],[0,0,175]]}]}}}"""
    assert(result === expectedJson)
  }

  test("DrillDown Curator should work correctly with multiple drilldown requests with some failed)") {

    val jsonRequest = s"""{
                          "cube": "student_performance2",
                          "curators" : {
                            "drilldown" : {
                              "config" : [{
                                "enforceFilters": true,
                                "dimension": "Section Status",
                                "cube": "student_performance2"
                              },
                              {
                                "enforceFilters": true,
                                "dimension": "Section Start Year",
                                "cube": "student_performance222"
                              }]
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
    val reportingRequest = reportingRequestResult.toOption.get.copy(additionalParameters = Map(Parameter.Debug -> DebugValue(value = false)))

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

    val drillDownCuratorAndRequestResult = requestCoordinatorResult.successResults(DrilldownCurator.name).head
    val drillDownCuratorResult = drillDownCuratorAndRequestResult.curatorResult

    val drillDownReportingRequest = drillDownCuratorResult.requestModelReference.model.reportingRequest

    assert(drillDownReportingRequest.sortBy.size == 1)
    assert(drillDownReportingRequest.sortBy.map(_.field).contains("Total Marks"))

    val jsonStreamingOutput = JsonOutputFormat(requestCoordinatorResult)

    val stringStream =  new StringStream()

    jsonStreamingOutput.writeStream(stringStream)
    val result = stringStream.toString()



    val expectedJson = """{"header":{"cube":"student_performance2","fields":[{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"},{"fieldName":"Section ID","fieldType":"DIM"}],"maxRows":200},"rows":[[213,305,100],[213,175,200]],"curators":{"drilldown":{"results":[{"index":0,"header":{"cube":"student_performance2","fields":[{"fieldName":"Section Status","fieldType":"DIM"},{"fieldName":"Section ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"}],"maxRows":1000},"rows":[[null,0,305],[null,0,175]]}],"errors":[{"index":1,"message":"MahaServiceBadRequestException: requirement failed: Default revision not found for cube student_performance222 in the registry"}]}}}""".stripMargin
    assert(result === expectedJson)
  }

  test("DrillDown Curator should fail with no most granular key") {

    val jsonRequest = s"""{
                          "cube": "student_performance2",
                          "curators" : {
                            "drilldown" : {
                              "config" : [{
                                "enforceFilters": true,
                                "dimension": "Section Status",
                                "cube": "student_performance2"
                              }]
                            }
                          },
                          "selectFields": [
                            {"field": "Total Marks"}
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
    val reportingRequest = reportingRequestResult.toOption.get.copy(additionalParameters = Map(Parameter.Debug -> DebugValue(value = false)))

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

    val drillDownError = requestCoordinatorResult.failureResults(DrilldownCurator.name).head

    assert(drillDownError.error.message === "no primary key alias found in request", drillDownError.error)

    val jsonStreamingOutput = JsonOutputFormat(requestCoordinatorResult)

    val stringStream =  new StringStream()

    jsonStreamingOutput.writeStream(stringStream)
    val result = stringStream.toString()

    val expectedJson = """{"header":{"cube":"student_performance2","fields":[{"fieldName":"Total Marks","fieldType":"FACT"}],"maxRows":200},"rows":[[480]],"curators":{"drilldown":{"error":{"message":"MahaServiceBadRequestException: No primary key alias found in request"}}}}""".stripMargin
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
    val reportingRequest = reportingRequestResult.toOption.get.copy(additionalParameters = Map(Parameter.Debug -> DebugValue(value = false)))

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

    val drillDownCuratorAndRequestResult = requestCoordinatorResult.successResults(DrilldownCurator.name).head
    val drillDownCuratorResult = drillDownCuratorAndRequestResult.curatorResult

    val drillDownReportingRequest = drillDownCuratorResult.requestModelReference.model.reportingRequest

    assert(drillDownReportingRequest.sortBy.size == 1)
    assert(drillDownReportingRequest.sortBy.map(_.field).contains("Total Marks"))

    val jsonStreamingOutput = JsonOutputFormat(requestCoordinatorResult)

    val stringStream =  new StringStream()

    jsonStreamingOutput.writeStream(stringStream)
    val result = stringStream.toString()



    val expectedJson = s"""{"header":{"cube":"student_performance2","fields":[{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"},{"fieldName":"Section ID","fieldType":"DIM"}],"maxRows":200},"rows":[],"curators":{"drilldown":{"result":{"header":{"cube":"student_performance2","fields":[{"fieldName":"Section Status","fieldType":"DIM"},{"fieldName":"Section ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"}],"maxRows":1000},"rows":[]}}}}"""

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
    val reportingRequest = reportingRequestResult.toOption.get.copy(additionalParameters = Map(Parameter.Debug -> DebugValue(value = false)))

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

    val drillDownCuratorAndRequestResult = requestCoordinatorResult.successResults(DrilldownCurator.name).head
    val drillDownCuratorResult = drillDownCuratorAndRequestResult.curatorResult

    val drillDownReportingRequest = drillDownCuratorResult.requestModelReference.model.reportingRequest

    assert(drillDownReportingRequest.sortBy.size == 1)
    assert(drillDownReportingRequest.sortBy.map(_.field).contains("Total Marks"))

    val jsonStreamingOutput = JsonOutputFormat(requestCoordinatorResult)

    val stringStream =  new StringStream()

    jsonStreamingOutput.writeStream(stringStream)
    val result = stringStream.toString()



    val expectedJson = s"""{"header":{"cube":"student_performance2","fields":[{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"},{"fieldName":"Section ID","fieldType":"DIM"}],"maxRows":200},"rows":[[213,175,200]],"curators":{"drilldown":{"result":{"header":{"cube":"student_performance2","fields":[{"fieldName":"Section Status","fieldType":"DIM"},{"fieldName":"Section ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"}],"maxRows":1000},"rows":[[null,0,175]]}}}}"""

    assert(result === expectedJson)
  }

  test("Test RowCountCurator for Druid fact-driven query") {

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
                          ],
                          "curators" : {
                            "rowcount" : {
                              "config" : {
                                "isFactDriven": true
                              }
                            }
                          }
                        }"""
    val reportingRequestResult = ReportingRequest.deserializeSync(jsonRequest.getBytes, schema = StudentSchema)
    require(reportingRequestResult.isSuccess)
    val reportingRequest = reportingRequestResult.toOption.get

    val bucketParams = BucketParams(UserInfo("uid", isInternal = true), forceRevision = Some(1))

    val mahaRequestContext = MahaRequestContext(REGISTRY,
      bucketParams,
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")
    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter)

    val requestCoordinator: RequestCoordinator = DefaultRequestCoordinator(mahaService)

    val executedResult = requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper)
    val requestCoordinatorResult = getRequestCoordinatorResult(executedResult)
    assert(requestCoordinatorResult.successResults.contains(DefaultCurator.name))
    assert(requestCoordinatorResult.successResults.contains(RowCountCurator.name))

    val jsonStreamingOutput = JsonOutputFormat(requestCoordinatorResult)
    val stringStream =  new StringStream()
    jsonStreamingOutput.writeStream(stringStream)
    val result = stringStream.toString()
//

    val expectedJson = """\{"header":\{"cube":"student_performance","fields":\[\{"fieldName":"Student ID","fieldType":"DIM"\},\{"fieldName":"Class ID","fieldType":"DIM"\},\{"fieldName":"Section ID","fieldType":"DIM"\},\{"fieldName":"Total Marks","fieldType":"FACT"\}\],"maxRows":200\},"rows":\[\[213,200,100,99\]\],"curators":\{"rowcount":\{"result":\{"header":\{"cube":"student_performance","fields":\[\{"fieldName":"TOTALROWS","fieldType":"FACT"\}\],"maxRows":1\},"rows":\[\[.*\]\]\}\}\}\}"""

    result should fullyMatch regex expectedJson

  }
}

