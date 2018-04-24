// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.output

import java.io.OutputStream
import java.util.Date

import com.yahoo.maha.core.{Engine, OracleEngine, RequestModelResult}
import com.yahoo.maha.core.bucketing.{BucketParams, UserInfo}
import com.yahoo.maha.core.query.{CompleteRowList, QueryAttributes, QueryPipelineResult}
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.service.{BaseMahaServiceTest, MahaRequestContext, ParRequestResult, RequestCoordinatorResult, RequestResult}
import com.yahoo.maha.service.curators.{CuratorResult, DefaultCurator, DrilldownCurator, NoConfig}
import com.yahoo.maha.service.datasource.IngestionTimeUpdater
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.utils.MahaRequestLogHelper

import scala.util.Try

/**
  * Created by hiral on 4/11/18.
  */
class JsonOutputFormatTest extends BaseMahaServiceTest {

  createTables()

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
                          "includeRowCount" : true
                        }"""

  val reportingRequest = ReportingRequest.deserializeSync(jsonRequest.getBytes, StudentSchema).toOption.get

  val registry = mahaServiceConfig.registry(REGISTRY)

  val bucketParams = BucketParams(UserInfo("uid", isInternal = true))

  val mahaRequestContext = MahaRequestContext(REGISTRY,
    bucketParams,
    reportingRequest,
    jsonRequest.getBytes,
    Map.empty, "rid", "uid")
  val (pse, queryPipeline, query, queryChain)  = {

    val requestModel = mahaService.generateRequestModel(REGISTRY
      , reportingRequest
      , BucketParams(UserInfo("test", false))
      , MahaRequestLogHelper(mahaRequestContext, mahaService.mahaRequestLogWriter)).toOption.get
    val factory = registry.queryPipelineFactory.from(requestModel.model, QueryAttributes.empty)
    val queryChain = factory.get.queryChain
    val pse = mahaService.getParallelServiceExecutor(mahaRequestContext)
    (pse, factory.get, queryChain.drivingQuery, queryChain)
  }

  val timeStampString = new Date().toString

  class StringStream extends OutputStream {
    val stringBuilder = new StringBuilder()
    override def write(b: Int): Unit = {
      stringBuilder.append(b.toChar)
    }
    override def toString() : String = stringBuilder.toString()
  }

  case class TestOracleIngestionTimeUpdater(engine: Engine, source: String) extends IngestionTimeUpdater {
    override def getIngestionTime(dataSource: String): Option[String] = {
      Some(timeStampString)
    }
  }

  class TestCurator extends DrilldownCurator {
    override val name = "TestCurator"
    override val isSingleton = false
  }

  test("Test JsonOutputFormat with DefaultCurator, totalRow Option, empty curator result") {

    val rowList = CompleteRowList(query)

    val row = rowList.newRow
    row.addValue("Student ID", 123)
    row.addValue("Class ID", 234)
    row.addValue("Section ID", 345)
    row.addValue("Total Marks", 99)
    // Oracle Engine should return the Row Count along with the result, can not simulate it here, thus skipping.

    rowList.addRow(row)

    val queryPipelineResult = QueryPipelineResult(queryPipeline, queryChain, rowList, QueryAttributes.empty)
    val requestResult = pse.immediateResult("label", new Right(RequestResult(queryPipelineResult)))
    val parRequestResult = ParRequestResult(Try(queryPipeline), requestResult, None)
    val requestModelResult = RequestModelResult(query.queryContext.requestModel, None)
    val defaultCurator = DefaultCurator()
    val curatorResult = CuratorResult(defaultCurator, NoConfig, Option(parRequestResult), requestModelResult)

    val requestCoordinatorResult = RequestCoordinatorResult(IndexedSeq(defaultCurator)
      , Map(DefaultCurator.name -> curatorResult)
      , Map.empty, Map(DefaultCurator.name -> curatorResult.parRequestResultOption.get.prodRun.get().right.get)
      , mahaRequestContext)

    val jsonStreamingOutput = JsonOutputFormat(requestCoordinatorResult
      , Map(OracleEngine-> TestOracleIngestionTimeUpdater(OracleEngine, "testSource")))

    val stringStream =  new StringStream()

    jsonStreamingOutput.writeStream(stringStream)
    val result = stringStream.toString()
    println(result)
    stringStream.close()
    val expected  = s"""{"header":{"lastIngestTime":"$timeStampString","source":"student_grade_sheet","cube":"student_performance","fields":[{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Class ID","fieldType":"DIM"},{"fieldName":"Section ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"},{"fieldName":"ROW_COUNT","fieldType":"CONSTANT"}],"maxRows":200},"rows":[[123,234,345,99]],"curators":{}}"""
    println(expected)
    assert(result.equals(expected))
  }

  test("Test JsonOutputFormat with DefaultCurator and valid other curator result") {

    val rowList = CompleteRowList(query)

    val row = rowList.newRow
    row.addValue("Student ID", 123)
    row.addValue("Class ID", 234)
    row.addValue("Section ID", 345)
    row.addValue("Total Marks", 99)
    rowList.addRow(row)

    val queryPipelineResult = QueryPipelineResult(queryPipeline, queryChain, rowList, QueryAttributes.empty)
    val requestResult = pse.immediateResult("label", new Right(RequestResult(queryPipelineResult)))
    val parRequestResult = ParRequestResult(Try(queryPipeline), requestResult, None)
    val requestModelResult = RequestModelResult(query.queryContext.requestModel, None)
    val defaultCurator = DefaultCurator()
    val curatorResult1 = CuratorResult(defaultCurator, NoConfig, Option(parRequestResult), requestModelResult)

    val testCurator = new TestCurator()
    val curatorResult2 = CuratorResult(testCurator, NoConfig, Option(parRequestResult), requestModelResult)


    val curatorResults= IndexedSeq(curatorResult1, curatorResult2)

    val requestCoordinatorResult = RequestCoordinatorResult(IndexedSeq(defaultCurator)
      , Map(DefaultCurator.name -> curatorResult1, curatorResult2.curator.name -> curatorResult2)
      , Map.empty
      , Map(DefaultCurator.name -> curatorResult1.parRequestResultOption.get.prodRun.get().right.get
        , "TestCurator" -> curatorResult2.parRequestResultOption.get.prodRun.get().right.get
      )
      , mahaRequestContext)
    val jsonStreamingOutput = JsonOutputFormat(requestCoordinatorResult)

    val stringStream =  new StringStream()

    jsonStreamingOutput.writeStream(stringStream)
    val result = stringStream.toString()
    println(result)
    stringStream.close()
    assert(result.equals(s"""{"header":{"cube":"student_performance","fields":[{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Class ID","fieldType":"DIM"},{"fieldName":"Section ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"},{"fieldName":"ROW_COUNT","fieldType":"CONSTANT"}],"maxRows":200},"rows":[[123,234,345,99]],"curators":{"TestCurator":{"result":{"header":{"cube":"student_performance","fields":[{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Class ID","fieldType":"DIM"},{"fieldName":"Section ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"},{"fieldName":"ROW_COUNT","fieldType":"CONSTANT"}],"maxRows":200},"rows":[[123,234,345,99]]}}}}""".stripMargin))
  }

}
