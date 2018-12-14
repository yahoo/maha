// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.curators

import com.yahoo.maha.core.RequestModel
import com.yahoo.maha.core.bucketing.{BucketParams, UserInfo}
import com.yahoo.maha.core.query.OracleQuery
import com.yahoo.maha.core.request.{DebugValue, Parameter, ReportingRequest}
import com.yahoo.maha.jdbc._
import com.yahoo.maha.parrequest2.future.ParRequest
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.utils.{CuratorMahaRequestLogHelper, MahaRequestLogHelper}
import com.yahoo.maha.service.{BaseMahaServiceTest, CuratorInjector, MahaRequestContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.scalatest.{BeforeAndAfterAll, Matchers}
import com.yahoo.maha.core._

/**
 * Created by pranavbhole on 24/04/18.
 */
class RowCountCuratorTest  extends BaseMahaServiceTest with BeforeAndAfterAll with Matchers {
  val today: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now())
  val yesterday: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(1))

  override protected def afterAll(): Unit =  {
    super.afterAll()
    server.shutdownNow()
  }

  override protected def beforeAll(): Unit =  {
    super.beforeAll()
    createTables()
    stageData()
  }


  def stageData(): Unit = {
    val insertSql = """INSERT INTO student_grade_sheet (year, section_id, student_id, class_id, total_marks, date, comment, month)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""

    val rows: List[Seq[Any]] = List(
      Seq(1, 100, 213, 200, 135, yesterday.toString, "some comment 1", yesterday.toString),
      Seq(1, 100, 213, 198, 120, yesterday.toString, "some comment 2", yesterday.toString),
      Seq(1, 500, 213, 197, 190, yesterday.toString, "some comment 3", yesterday.toString)
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

  test("Test processing of RowCountCurator with fact only operations") {


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
                          ],
                          "includeRowCount" : true
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


    val rowCountCurator = RowCountCurator()
    val curatorInjector = new CuratorInjector(2, mahaService, mahaRequestLogHelper, Set.empty)

    val rowCountCuratorResult: Either[CuratorError, ParRequest[CuratorResult]] = rowCountCurator
      .process(Map.empty, mahaRequestContext, mahaService, curatorMahaRequestLogHelper, NoConfig, curatorInjector)

    assert(rowCountCuratorResult.isRight)

    val parRequestCuratorResult = rowCountCuratorResult.right.get.get(1000)
    assert(parRequestCuratorResult.isRight)
  }

  test("Test processing of RowCountCurator with fact only operations with dim cost > 5000") {


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
                          ],
                          "includeRowCount" : true
                        }"""
    val reportingRequestResult = ReportingRequest.deserializeSyncWithFactBias(jsonRequest.getBytes, schema = StudentSchema)
    require(reportingRequestResult.isSuccess)
    val reportingRequest = ReportingRequest.enableDebug(reportingRequestResult.toOption.get)

    val bucketParams = BucketParams(UserInfo("uid", true))


    val mahaRequestContext = MahaRequestContext(REGISTRY,
      bucketParams,
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")

    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter)
    val curatorMahaRequestLogHelper =  CuratorMahaRequestLogHelper(mahaRequestLogHelper)


    val rowCountCurator = RowCountCurator()
    val curatorInjector = new CuratorInjector(2, mahaService, mahaRequestLogHelper, Set.empty)

    val rowCountCuratorResult: Either[CuratorError, ParRequest[CuratorResult]] = rowCountCurator
      .process(Map.empty, mahaRequestContext, mahaService, curatorMahaRequestLogHelper, NoConfig, curatorInjector)

    assert(rowCountCuratorResult.isRight)

    val parRequestCuratorResult = rowCountCuratorResult.right.get.get(1000)
    assert(parRequestCuratorResult.isRight)
  }

  test("Test processing of RowCountCurator with failure of RowCountCurator with dim driven request") {

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
                            {"field": "Section ID"},
                            {"field": "Total Marks"}
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
    val reportingRequest = reportingRequestResult.toOption.get.copy(additionalParameters = Map(Parameter.Debug -> DebugValue(value = true)))

    val bucketParams = BucketParams(UserInfo("uid", true))

    val mahaRequestContext = MahaRequestContext(REGISTRY,
      bucketParams,
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")

    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter)
    val curatorMahaRequestLogHelper =  CuratorMahaRequestLogHelper(mahaRequestLogHelper)

    val rowCountCurator = RowCountCurator()
    val curatorInjector = new CuratorInjector(2, mahaService, mahaRequestLogHelper, Set.empty)

    val rowCountCuratorResult: Either[CuratorError, ParRequest[CuratorResult]] = rowCountCurator
      .process(Map.empty, mahaRequestContext, mahaService, curatorMahaRequestLogHelper, NoConfig, curatorInjector)

    assert(rowCountCuratorResult.isRight)
    val parReq = rowCountCuratorResult.right.get

    val result = parReq.get(1000)
    val parReqOption = result.right.get.parRequestResultOption
    assert(parReqOption.isDefined)

    assert(parReqOption.get.queryPipeline.isSuccess)
    val totalRowQueryPipelineTry = parReqOption.get.queryPipeline
    assert(totalRowQueryPipelineTry.isSuccess)
    val totalRowQueryPipeline = totalRowQueryPipelineTry.get

    assert(totalRowQueryPipeline.factBestCandidate.isEmpty)
    assert(totalRowQueryPipeline.bestDimCandidates.nonEmpty)

    // H2 Can not execute the oracle total row request, thus cant assert the actual row count of the dimension

  }

  test("Test curator Injection") {

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
                          ],
                          "includeRowCount" : true,
                          "forceDimensionDriven" : true
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

    val curatorInjector = new CuratorInjector(2, mahaService, mahaRequestLogHelper, Set.empty)

    curatorInjector.injectCurator(TotalMetricsCurator.name, Map.empty, mahaRequestContext, NoConfig)

    assert(curatorInjector.curatorList.nonEmpty)
    assert(curatorInjector.orderedResultList.nonEmpty)
  }

  test("Test totalrows with non-PK dim filter") {

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
                            {"field": "Section ID"},
                            {"field": "Total Marks"},
                            {"field": "Remarks"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"},
                            {"field": "Remarks", "operator": "=", "value": "Acceptable."}
                          ],
                          "includeRowCount" : true,
                          "forceDimensionDriven" : true
                        }"""
    val reportingRequestResult = ReportingRequest.deserializeSyncWithFactBias(jsonRequest.getBytes, schema = StudentSchema)
    require(reportingRequestResult.isSuccess)
    val reportingRequest = reportingRequestResult.toOption.get.copy(additionalParameters = Map(Parameter.Debug -> DebugValue(value = true)))

    val bucketParams = BucketParams(UserInfo("uid", true))

    val mahaRequestContext = MahaRequestContext(REGISTRY,
      bucketParams,
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")

    val mahaRequestLogHelper = MahaRequestLogHelper(mahaRequestContext, mahaServiceConfig.mahaRequestLogWriter)
    val curatorMahaRequestLogHelper =  CuratorMahaRequestLogHelper(mahaRequestLogHelper)

    val rowCountCurator = RowCountCurator()
    val curatorInjector = new CuratorInjector(2, mahaService, mahaRequestLogHelper, Set.empty)

    val rowCountCuratorResult: Either[CuratorError, ParRequest[CuratorResult]] = rowCountCurator
      .process(Map.empty, mahaRequestContext, mahaService, curatorMahaRequestLogHelper, NoConfig, curatorInjector)

    assert(rowCountCuratorResult.isRight)
    val parReq = rowCountCuratorResult.right.get

    val result = parReq.get(1000)
    val parReqOption = result.right.get.parRequestResultOption
    assert(parReqOption.isDefined)

    assert(parReqOption.get.queryPipeline.isSuccess)
    val totalRowQueryPipelineTry = parReqOption.get.queryPipeline
    assert(totalRowQueryPipelineTry.isSuccess)
    val totalRowQueryPipeline = totalRowQueryPipelineTry.get

    assert(totalRowQueryPipeline.factBestCandidate.isEmpty)
    assert(totalRowQueryPipeline.bestDimCandidates.nonEmpty)

    val totalRowsQuery = totalRowQueryPipeline.queryChain.drivingQuery.asInstanceOf[OracleQuery].asString
    val expectedTotalRowQuery_correct =
      s"""
         |SELECT /*+ FIRST_ROWS */ *
         |      FROM (SELECT s0.id "Student ID", Count(*) OVER() TOTALROWS, ROWNUM as ROW_NUMBER
         |            FROM
         |                (SELECT  id
         |            FROM student
         |            WHERE (id = 213)
         |             ) s0
         |
         |
 |           )
         |             WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 1
       """.stripMargin
    totalRowsQuery should equal(expectedTotalRowQuery_correct)(after being whiteSpaceNormalised)
  }

  test("Test curator Injection Failure") {

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
                          ],
                          "includeRowCount" : true,
                          "forceDimensionDriven" : true
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

    val curatorInjector = new CuratorInjector(2, mahaService, mahaRequestLogHelper, Set(TotalMetricsCurator.name))
    try {
      curatorInjector.injectCurator(TotalMetricsCurator.name, Map.empty, mahaRequestContext, NoConfig)
      assert(false)
    } catch {
      case e:Exception=>
       }
  }

  test("Test failure of RowCountCurator") {

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
                          ],
                          "includeRowCount" : true
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


    val rowCountCurator = RowCountCurator()
    val curatorInjector = new CuratorInjector(2, mahaService, mahaRequestLogHelper, Set.empty)

    val rowCountCuratorResult: Either[CuratorError, ParRequest[CuratorResult]] = rowCountCurator
      .process(Map.empty, mahaRequestContext, mahaService, curatorMahaRequestLogHelper, NoConfig, curatorInjector)

    assert(rowCountCuratorResult.isLeft)
    assert(rowCountCuratorResult.left.get.message.contains("cube does not exist : unknown"))
  }

  test("Test failure of RowCountCurator unknown col") {

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
                            {"field": "Unkown Total Marks"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"}
                          ],
                          "includeRowCount" : true
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

    val rowCountCurator = RowCountCurator()
    val curatorInjector = new CuratorInjector(2, mahaService, mahaRequestLogHelper, Set.empty)

    val rowCountCuratorResult: Either[CuratorError, ParRequest[CuratorResult]] = rowCountCurator
      .process(Map.empty, mahaRequestContext, mahaService, curatorMahaRequestLogHelper, NoConfig, curatorInjector)

    assert(rowCountCuratorResult.isLeft)
    assert(rowCountCuratorResult.left.get.message.contains("Failed to find primary key alias for Unkown Total Marks"))
  }

  test("Test failure of RowCountCurator with BadTestRequestModelValidator") {

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
                            {"field": "Unkown Total Marks"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"}
                          ],
                          "includeRowCount" : true
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

    val rowCountCurator = RowCountCurator(new BadTestRequestModelValidator)
    val curatorInjector = new CuratorInjector(2, mahaService, mahaRequestLogHelper, Set.empty)

    val rowCountCuratorResult: Either[CuratorError, ParRequest[CuratorResult]] = rowCountCurator
      .process(Map.empty, mahaRequestContext, mahaService, curatorMahaRequestLogHelper, NoConfig, curatorInjector)

    assert(rowCountCuratorResult.isLeft)
  }


}
