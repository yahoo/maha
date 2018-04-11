package com.yahoo.maha.service.curators

import com.yahoo.maha.core.bucketing.{BucketParams, UserInfo}
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.jdbc._
import com.yahoo.maha.parrequest2.future.{ParFunction, ParRequest}
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import com.yahoo.maha.service.{BaseMahaServiceTest, MahaRequestContext}
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
    val insertSql = """INSERT INTO student_grade_sheet (year, section_id, student_id, class_id, total_marks, date, comment)
     VALUES (?, ?, ?, ?, ?, ?, ?)"""

    val rows: List[Seq[Any]] = List(
      Seq(1, 100, 213, 200, 135, yesterday.toString, "some comment 1"),
      Seq(1, 100, 213, 198, 120, yesterday.toString, "some comment 2"),
      Seq(1, 500, 213, 197, 190, yesterday.toString, "some comment 3")
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

  test("Test successful processing of TotalMetricsCurator") {


    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "totalMetrics" : {
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
                            {"field": "Student ID", "operator": "=", "value": "123"}
                          ]
                        }"""
    val reportingRequestResult = ReportingRequest.deserializeSyncWithFactBias(jsonRequest.getBytes, schema = StudentSchema)
    require(reportingRequestResult.isSuccess)
    val reportingRequest = reportingRequestResult.toOption.get

    val bucketParams = BucketParams(UserInfo("uid", true))

    val mahaRequestLogHelper = MahaRequestLogHelper(REGISTRY, mahaServiceConfig.mahaRequestLogWriter)

    val mahaRequestContext = MahaRequestContext(REGISTRY,
      bucketParams,
      reportingRequest,
      jsonRequest.getBytes,
      Map.empty, "rid", "uid")

    val totalMetricsCurator = TotalMetricsCurator()

    val totalMetricsCuratorResult: ParRequest[CuratorResult] = totalMetricsCurator.process(mahaRequestContext, mahaService, mahaRequestLogHelper)


    val successFunction : ParFunction[CuratorResult, CuratorResult]  = ParFunction.fromScala(
      (curatorResult) => {
        curatorResult
      }
    )

    val resultEither = totalMetricsCuratorResult.resultMap[CuratorResult](successFunction)
    assert(resultEither.isRight)

    resultEither.fold[CuratorResult](
    (error) => {
        throw new IllegalArgumentException()
     },
    (curatorResult) => {
    assert(curatorResult.isInstanceOf[TotalMetricsCuratorResult])
    val totalMetricCuratorResult = curatorResult.asInstanceOf[TotalMetricsCuratorResult]
    assert(totalMetricCuratorResult.totalMetricsResultTry.isSuccess)
    val rowList = totalMetricCuratorResult.totalMetricsResultTry.get.queryPipelineResult.rowList
      rowList.foreach {
        row=>
           println(row)
      }
    totalMetricCuratorResult.requestResultTry.get.queryPipelineResult.rowList.foreach {
    row=> println(row)
    }

      curatorResult
    }
   )

  }

}
