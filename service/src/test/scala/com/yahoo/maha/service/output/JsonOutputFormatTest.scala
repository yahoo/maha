// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.output

import java.util.Date

import com.fasterxml.jackson.core.JsonGenerator
import com.yahoo.maha.core.bucketing.{BucketParams, UserInfo}
import com.yahoo.maha.core.query.{CompleteRowList, QueryAttributes, QueryPipelineResult, QueryRowList}
import com.yahoo.maha.core.request.{ReportingRequest, RowCountQuery}
import com.yahoo.maha.core.{DruidEngine, Engine, OracleEngine, RequestModelResult}
import com.yahoo.maha.jdbc.List
import com.yahoo.maha.service.curators._
import com.yahoo.maha.service.datasource.IngestionTimeUpdater
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import com.yahoo.maha.service.{BaseMahaServiceTest, CuratorAndRequestResult, CuratorInjector, MahaRequestContext, ParRequestResult, RequestCoordinatorResult, RequestResult}
import org.json4s.JsonAST.{JInt, JObject}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should

import scala.util.Try

/**
  * Created by hiral on 4/11/18.
  */
class JsonOutputFormatTest extends BaseMahaServiceTest with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    createTables()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    server.shutdownNow()
  }

  val jsonRequest =
    s"""{
                          "cube": "student_performance",
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Class ID"},
                            {"field": "Section ID"},
                            {"field": "Total Marks"},
                            {"field": "Sample Constant Field", "value" :"Test Result"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"}
                          ],
                          "includeRowCount" : true,
                          "additionalParameters": {
                              "debug": true
                          }
                        }"""
  val registry = mahaServiceConfig.registry(REGISTRY)
  val reportingRequest = ReportingRequest.forceOracle(ReportingRequest.deserializeSync(jsonRequest.getBytes, StudentSchema).toOption.get)


  val bucketParams = BucketParams(UserInfo("uid", isInternal = true), forceRevision = Some(0))

  val mahaRequestContext = MahaRequestContext(REGISTRY,
    bucketParams,
    reportingRequest,
    jsonRequest.getBytes,
    Map.empty, "rid", "uid")

  val mahaRequestLogBuilder = MahaRequestLogHelper(mahaRequestContext, mahaService.mahaRequestLogWriter)
  val (pse, queryPipeline, query, queryChain) = {
    val bucketParams = BucketParams(UserInfo("test", false), forceRevision = Some(0))

    val requestModel = mahaService.generateRequestModel(REGISTRY
      , reportingRequest
      , bucketParams).toOption.get
    val factory = registry.queryPipelineFactory.from(requestModel.model, QueryAttributes.empty, bucketParams)
    val queryChain = factory.get.queryChain
    val pse = mahaService.getParallelServiceExecutor(mahaRequestContext)
    (pse, factory.get, queryChain.drivingQuery, queryChain)
  }

  val (fd_pse, fd_queryPipeline, fd_query, fd_queryChain) = {

    val requestModel = mahaService.generateRequestModel(REGISTRY
      , ReportingRequest
        .enableDebug(
          ReportingRequest.withLabels(
            ReportingRequest.withTestName(
              ReportingRequest.forceDruid(
                reportingRequest.copy(forceDimensionDriven = false, forceFactDriven = true)
              ), "test1"
            ), List("lb1", "lb2", "lb3")
          )
        )
      , BucketParams(UserInfo("test", false), forceRevision = Option(1))).toOption.get
    val factory = registry.queryPipelineFactory.from(requestModel.model, QueryAttributes.empty, bucketParams)
    val queryChain = factory.get.queryChain
    val pse = mahaService.getParallelServiceExecutor(mahaRequestContext)
    (pse, factory.get, queryChain.drivingQuery, queryChain)
  }

  val timeStampString = new Date().toString

  case class TestOracleIngestionTimeUpdater(engine: Engine, source: String) extends IngestionTimeUpdater {
    override def getIngestionTime(dataSource: String): Option[String] = {
      Some(timeStampString)
    }

    override def getIngestionTimeLong(dataSource: String): Option[Long] = Some(new Date().getTime)
  }

  class TestCurator extends DrilldownCurator {
    override val name = "TestCurator"
    override val isSingleton = false
  }

  class TestDebugRenderer extends DebugRenderer {
    override def render(mahaRequestContext: MahaRequestContext, jsonGenerator: JsonGenerator): Unit = {
      jsonGenerator.writeFieldName("debug")
      jsonGenerator.writeBoolean(true)
    }
  }

  val debugRenderer = Option(new TestDebugRenderer)

  test("Test JsonOutputFormat with DefaultCurator, totalRow Option, empty curator result") {

    val rowList = CompleteRowList(query)

    val row = rowList.newRow
    row.addValue("Student ID", 123)
    row.addValue("Class ID", 234)
    row.addValue("Section ID", 345)
    row.addValue("Total Marks", 99)
    // Oracle Engine should return the Row Count along with the result, can not simulate it here, thus skipping.

    rowList.addRow(row)

    val queryPipelineResult = QueryPipelineResult(queryPipeline, queryChain, rowList, QueryAttributes.empty, Map.empty)
    val requestResult = pse.immediateResult("label", new Right(RequestResult(queryPipelineResult)))
    val parRequestResult = ParRequestResult(Try(queryPipeline), requestResult, None)
    val requestModelResult = RequestModelResult(query.queryContext.requestModel, None)
    val defaultCurator = DefaultCurator()
    val curatorResult = CuratorResult(defaultCurator, NoConfig, Option(parRequestResult), requestModelResult)

    val requestCoordinatorResult = RequestCoordinatorResult(IndexedSeq(defaultCurator)
      , Map.empty
      , Map(DefaultCurator.name -> IndexedSeq(CuratorAndRequestResult(curatorResult
        , curatorResult.parRequestResultOption.get.prodRun.get().right.get)))
      , mahaRequestContext)

    val jsonStreamingOutput = JsonOutputFormat(requestCoordinatorResult
      , Map(OracleEngine -> TestOracleIngestionTimeUpdater(OracleEngine, "testSource"))
      , debugRenderer
    )

    val stringStream = new StringStream()

    jsonStreamingOutput.writeStream(stringStream)
    val result = stringStream.toString()
    stringStream.close()
    val expected = s""""cube":"student_performance","fields":[{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Class ID","fieldType":"DIM"},{"fieldName":"Section ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"},{"fieldName":"Sample Constant Field","fieldType":"CONSTANT"},{"fieldName":"ROW_COUNT","fieldType":"CONSTANT"}],"maxRows":200},"rows":[[123,234,345,99,"Test Result",null]],"curators":{}}"""
    if (!result.contains(expected)) {
      //println(s"mahaRequestContext.mutableState : ${mahaRequestContext.mutableState}")
      //println(s"dimCardinalityEstimate : ${query.queryContext.requestModel.dimCardinalityEstimate}")
      jdbcConnection.get.queryForObject("select id, name from section") {
        rs =>
          while (rs.next()) {
            println("found unexpected data in section id : " + rs.getArray(0))
            println("found unexpected data in section data : " + rs.getArray(1))
          }
      }
    }
    assert(result.contains(expected))
    assert(jsonStreamingOutput.ingestionTimeUpdaterMap.get(OracleEngine).get.getIngestionTimeLongAsJava("abc").isDefined)
  }

  test("Test JsonOutputFormat with DefaultCurator and valid other curator result") {

    val rowList = CompleteRowList(query)

    val row = rowList.newRow
    row.addValue("Student ID", 123)
    row.addValue("Class ID", 234)
    row.addValue("Section ID", 345)
    row.addValue("Total Marks", 99)
    rowList.addRow(row)

    val queryPipelineResult = QueryPipelineResult(queryPipeline, queryChain, rowList, QueryAttributes.empty, Map.empty)
    val requestResult = pse.immediateResult("label", new Right(RequestResult(queryPipelineResult)))
    val parRequestResult = ParRequestResult(Try(queryPipeline), requestResult, None)
    val requestModelResult = RequestModelResult(query.queryContext.requestModel, None)
    val defaultCurator = DefaultCurator()
    val curatorResult1 = CuratorResult(defaultCurator, NoConfig, Option(parRequestResult), requestModelResult)

    val testCurator = new TestCurator()
    val curatorResult2 = CuratorResult(testCurator, NoConfig, Option(parRequestResult), requestModelResult)


    val curatorResults = IndexedSeq(curatorResult1, curatorResult2)

    val requestCoordinatorResult = RequestCoordinatorResult(IndexedSeq(defaultCurator, testCurator)
      , Map.empty
      , Map(DefaultCurator.name -> IndexedSeq(CuratorAndRequestResult(curatorResult1, curatorResult1.parRequestResultOption.get.prodRun.get().right.get))
        , "TestCurator" -> IndexedSeq(CuratorAndRequestResult(curatorResult2, curatorResult2.parRequestResultOption.get.prodRun.get().right.get))
      )
      , mahaRequestContext)
    val jsonStreamingOutput = JsonOutputFormat(requestCoordinatorResult, debugRenderer = Option(new NoopDebugRenderer))

    val stringStream = new StringStream()

    jsonStreamingOutput.writeStream(stringStream)
    val result = stringStream.toString()
    stringStream.close()
    assert(result.contains(s"""{"header":{"cube":"student_performance","fields":[{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Class ID","fieldType":"DIM"},{"fieldName":"Section ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"},{"fieldName":"Sample Constant Field","fieldType":"CONSTANT"},{"fieldName":"ROW_COUNT","fieldType":"CONSTANT"}],"maxRows":200},"rows":[[123,234,345,99,"Test Result",null]],"curators":{"TestCurator":{"result":{"header":{"cube":"student_performance","fields":[{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Class ID","fieldType":"DIM"},{"fieldName":"Section ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"},{"fieldName":"Sample Constant Field","fieldType":"CONSTANT"},{"fieldName":"ROW_COUNT","fieldType":"CONSTANT"}],"maxRows":200},"rows":[[123,234,345,99,"Test Result",null]]}}}}""".stripMargin))
  }

  test("Test JsonOutputFormat with DefaultCurator and failed other curator result") {

    val rowList = CompleteRowList(query)

    val row = rowList.newRow
    row.addValue("Student ID", 123)
    row.addValue("Class ID", 234)
    row.addValue("Section ID", 345)
    row.addValue("Total Marks", 99)
    rowList.addRow(row)

    val queryPipelineResult = QueryPipelineResult(queryPipeline, queryChain, rowList, QueryAttributes.empty, Map.empty)
    val requestResult = pse.immediateResult("label", new Right(RequestResult(queryPipelineResult)))
    val parRequestResult = ParRequestResult(Try(queryPipeline), requestResult, None)
    val requestModelResult = RequestModelResult(query.queryContext.requestModel, None)
    val defaultCurator = DefaultCurator()
    val curatorResult1 = CuratorResult(defaultCurator, NoConfig, Option(parRequestResult), requestModelResult)

    val failingCurator = new FailingCurator()
    val curatorInjector = new CuratorInjector(2, mahaService, mahaRequestLogBuilder, Set(FailingCurator.name))
    val curatorResult2 = failingCurator.process(Map.empty
      , mahaRequestContext, mahaService, mahaRequestLogBuilder.curatorLogBuilder(failingCurator), NoConfig, curatorInjector)

    val curatorResults = IndexedSeq(curatorResult1, curatorResult2)

    val requestCoordinatorResult = RequestCoordinatorResult(IndexedSeq(defaultCurator, failingCurator)
      , Map(failingCurator.name -> IndexedSeq(curatorResult2.left.get))
      , Map(DefaultCurator.name -> IndexedSeq(CuratorAndRequestResult(curatorResult1, curatorResult1.parRequestResultOption.get.prodRun.get().right.get)))
      , mahaRequestContext)
    val jsonStreamingOutput = JsonOutputFormat(requestCoordinatorResult)

    val stringStream = new StringStream()

    jsonStreamingOutput.writeStream(stringStream)
    val result = stringStream.toString()
    stringStream.close()
    assert(result === """{"header":{"cube":"student_performance","fields":[{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Class ID","fieldType":"DIM"},{"fieldName":"Section ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"},{"fieldName":"Sample Constant Field","fieldType":"CONSTANT"},{"fieldName":"ROW_COUNT","fieldType":"CONSTANT"}],"maxRows":200},"rows":[[123,234,345,99,"Test Result",null]],"curators":{"fail":{"error":{"message":"failed"}}}}""")
  }

  test("Test JsonOutputFormat with DefaultCurator row count in row list") {

    val rowList = CompleteRowList(query)

    val row = rowList.newRow
    row.addValue("Student ID", 123)
    row.addValue("Class ID", 234)
    row.addValue("Section ID", 345)
    row.addValue("Total Marks", 99)
    row.addValue(QueryRowList.ROW_COUNT_ALIAS, 101)
    rowList.addRow(row)

    val queryPipelineResult = QueryPipelineResult(queryPipeline, queryChain, rowList, QueryAttributes.empty, Map.empty)
    val requestResult = pse.immediateResult("label", new Right(RequestResult(queryPipelineResult)))
    val parRequestResult = ParRequestResult(Try(queryPipeline), requestResult, None)
    val requestModelResult = RequestModelResult(query.queryContext.requestModel, None)
    val defaultCurator = DefaultCurator()
    val curatorResult1 = CuratorResult(defaultCurator, NoConfig, Option(parRequestResult), requestModelResult)

    val curatorInjector = new CuratorInjector(2, mahaService, mahaRequestLogBuilder, Set(FailingCurator.name))

    val curatorResults = IndexedSeq(curatorResult1)

    val requestCoordinatorResult = RequestCoordinatorResult(IndexedSeq(defaultCurator)
      , Map.empty
      , Map(DefaultCurator.name -> IndexedSeq(CuratorAndRequestResult(curatorResult1, curatorResult1.parRequestResultOption.get.prodRun.get().right.get)))
      , mahaRequestContext)
    val jsonStreamingOutput = JsonOutputFormat(requestCoordinatorResult)

    val stringStream = new StringStream()

    jsonStreamingOutput.writeStream(stringStream)
    val result = stringStream.toString()
    stringStream.close()
    assert(result === """{"header":{"cube":"student_performance","fields":[{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Class ID","fieldType":"DIM"},{"fieldName":"Section ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"},{"fieldName":"Sample Constant Field","fieldType":"CONSTANT"},{"fieldName":"ROW_COUNT","fieldType":"CONSTANT"}],"maxRows":200},"rows":[[123,234,345,99,"Test Result",101]],"curators":{}}""")
  }

  test("Test JsonOutputFormat with DefaultCurator row count in row list for fact driven query") {

    val rowList = CompleteRowList(fd_query)

    val row = rowList.newRow
    row.addValue("Student ID", 123)
    row.addValue("Class ID", 234)
    row.addValue("Section ID", 345)
    row.addValue("Total Marks", 99)
    rowList.addRow(row)

    val queryPipelineResult = QueryPipelineResult(fd_queryPipeline, fd_queryChain, rowList, QueryAttributes.empty, Map(DruidEngine -> JObject("pagingIdentifiers" -> JObject("wikipedia_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9" -> JInt(4)))))
    val requestResult = pse.immediateResult("label", new Right(RequestResult(queryPipelineResult)))
    val parRequestResult = ParRequestResult(Try(fd_queryPipeline), requestResult, None)
    val requestModelResult = RequestModelResult(fd_query.queryContext.requestModel, None)
    val defaultCurator = DefaultCurator()
    val curatorResult1 = CuratorResult(defaultCurator, NoConfig, Option(parRequestResult), requestModelResult)

    val curatorInjector = new CuratorInjector(2, mahaService, mahaRequestLogBuilder, Set(FailingCurator.name))

    val curatorResults = IndexedSeq(curatorResult1)

    val requestCoordinatorResult = RequestCoordinatorResult(IndexedSeq(defaultCurator)
      , Map.empty
      , Map(DefaultCurator.name -> IndexedSeq(CuratorAndRequestResult(curatorResult1, curatorResult1.parRequestResultOption.get.prodRun.get().right.get)))
      , mahaRequestContext)
    val jsonStreamingOutput = JsonOutputFormat(requestCoordinatorResult, debugRenderer = debugRenderer)

    val stringStream = new StringStream()

    jsonStreamingOutput.writeStream(stringStream)
    val result = stringStream.toString()
    stringStream.close()
    assert(result === """{"header":{"cube":"student_performance","fields":[{"fieldName":"Student ID","fieldType":"DIM"},{"fieldName":"Class ID","fieldType":"DIM"},{"fieldName":"Section ID","fieldType":"DIM"},{"fieldName":"Total Marks","fieldType":"FACT"},{"fieldName":"Sample Constant Field","fieldType":"CONSTANT"},{"fieldName":"ROW_COUNT","fieldType":"CONSTANT"}],"maxRows":200,"debug":{"testName":"test1","labels":["lb1","lb2","lb3"],"debug":true},"pagination":{"Druid":{"pagingIdentifiers":{"wikipedia_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9":4}}}},"rows":[[123,234,345,99,"Test Result",1]],"curators":{}}""")
  }

  test("Test JsonOutputFormat with RowCountCurator while rowList is empty") {

    val rowCountRequest =
      s"""{
                          "cube": "student_performance",
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Class ID"},
                            {"field": "Section ID"},
                            {"field": "Total Marks"},
                            {"field": "Sample Constant Field", "value" :"Test Result"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"}
                          ],
                          "additionalParameters": {
                              "debug": true
                          },
                          "curators" : {
                            "rowcount" : {
                              "config" : {
                                "isFactDriven": true
                              }
                            }
                          }
                        }"""


    val reportingRequest = ReportingRequest.forceDruid(ReportingRequest.deserializeSync(rowCountRequest.getBytes, StudentSchema).toOption.get)

    val (pse, queryPipeline, drivingQuery, queryChain) = {
      val requestModel = mahaService.generateRequestModel(REGISTRY
        , reportingRequest.copy(queryType = RowCountQuery)
        , BucketParams(UserInfo("test", false), forceRevision = Option(1))).toOption.get
      val factory = registry.queryPipelineFactory.from(requestModel.model, QueryAttributes.empty, bucketParams)
      val queryChain = factory.get.queryChain
      val pse = mahaService.getParallelServiceExecutor(mahaRequestContext)
      (pse, factory.get, queryChain.drivingQuery, queryChain)
    }

    val rowList = CompleteRowList(drivingQuery)

    val queryPipelineResult = QueryPipelineResult(queryPipeline, queryChain, rowList, QueryAttributes.empty, Map.empty)
    val requestResult = pse.immediateResult("label", new Right(RequestResult(queryPipelineResult)))
    val parRequestResult = ParRequestResult(Try(queryPipeline), requestResult, None)
    val requestModelResult = RequestModelResult(drivingQuery.queryContext.requestModel, None)

    val defaultCurator = DefaultCurator()
    val rowCountCurator = RowCountCurator()
    val curatorResult1 = CuratorResult(defaultCurator, NoConfig, Option(parRequestResult), requestModelResult)
    val curatorResult2 = CuratorResult(rowCountCurator, NoConfig, Option(parRequestResult), requestModelResult)

    val requestCoordinatorResult = RequestCoordinatorResult(IndexedSeq(defaultCurator, rowCountCurator)
      , Map.empty
      , Map(DefaultCurator.name -> IndexedSeq(CuratorAndRequestResult(curatorResult1, curatorResult1.parRequestResultOption.get.prodRun.get().right.get)),
            RowCountCurator.name -> IndexedSeq(CuratorAndRequestResult(curatorResult2, curatorResult2.parRequestResultOption.get.prodRun.get().right.get))
      )
      , mahaRequestContext)
    val jsonStreamingOutput = JsonOutputFormat(requestCoordinatorResult)

    val stringStream = new StringStream()

    jsonStreamingOutput.writeStream(stringStream)
    val result = stringStream.toString()
    stringStream.close()

    val expectedJson = """\{.*,"curators":\{"rowcount":\{"result":\{"header":\{"cube":"student_performance","fields":\[\{"fieldName":"TOTALROWS","fieldType":"FACT"\}\],"maxRows":200\},"rows":\[\[0\]\]\}\}\}\}"""
    assert(result.matches(expectedJson))

  }
}
