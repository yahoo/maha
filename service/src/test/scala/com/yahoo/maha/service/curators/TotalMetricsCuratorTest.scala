package com.yahoo.maha.service.curators

import com.yahoo.maha.core.bucketing.{BucketParams, UserInfo}
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.jdbc._
import com.yahoo.maha.parrequest2.future.{ParFunction, ParRequest}
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.utils.{CuratorMahaRequestLogHelper, MahaRequestLogHelper}
import com.yahoo.maha.service.{BaseMahaServiceTest, CuratorInjector, MahaRequestContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.scalatest.BeforeAndAfterAll


/**
 * Created by pranavbhole on 10/04/18.
 */
class TotalMetricsCuratorTest extends BaseMahaServiceTest with BeforeAndAfterAll {
  val today: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now())
  val yesterday: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(1))

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
        println(result)
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

  test("Test successful processing of TotalMetricsCurator") {


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


    val totalMetricsCurator = TotalMetricsCurator()
    val curatorInjector = new CuratorInjector(2, mahaService, mahaRequestLogHelper, Set.empty)

    val totalMetricsCuratorResult: Either[CuratorError, ParRequest[CuratorResult]] = totalMetricsCurator
      .process(Map.empty, mahaRequestContext, mahaService, curatorMahaRequestLogHelper, NoConfig, curatorInjector)

    val queryPipelineResult = totalMetricsCuratorResult
      .right.get.get().right.get.parRequestResultOption.get.prodRun.get().right.get.queryPipelineResult
    var rowCount = 0
    queryPipelineResult.rowList.foreach {
      row=>
        rowCount+=1
        println(row.toString)
        assert(row.getValue("Total Marks") == 445)
    }
    assert(rowCount == 1)
  }

}
