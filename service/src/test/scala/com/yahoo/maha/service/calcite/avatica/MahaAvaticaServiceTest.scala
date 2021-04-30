package com.yahoo.maha.service.calcite.avatica

import com.google.common.collect.Maps
import com.yahoo.maha.core.bucketing.{BucketParams, UserInfo}
import com.yahoo.maha.core.query.{CompleteRowList, QueryAttributes, QueryRowList}
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.service.calcite.DefaultMahaCalciteSqlParser
import com.yahoo.maha.service.example.ExampleSchema
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import com.yahoo.maha.service.{BaseMahaServiceTest, MahaRequestContext, MahaService}
import org.apache.calcite.avatica.remote.Service.{FetchRequest, OpenConnectionRequest, PrepareAndExecuteRequest}

class MahaAvaticaServiceTest extends BaseMahaServiceTest {

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

  // Mock execution function
  val executeFunction:(MahaRequestContext,MahaService) => QueryRowList = (mahaRequestContext, mahaService)=> {
    val rowList = CompleteRowList(query)
    val row = rowList.newRow
    row.addValue("Student ID", 123)
    row.addValue("Class ID", 234)
    row.addValue("Section ID", 345)
    row.addValue("Total Marks", 99)
    rowList.addRow(row)
    rowList
  }
  val connectionID = "123-con"

  test("Test Avatica Service") {
    val mahaAvaticaService = new DefaultMahaAvaticaService(executeFunction,
      DefaultMahaCalciteSqlParser(mahaServiceConfig),
      mahaService,
      new DefaultAvaticaRowListTransformer(),
      (schma)=> ExampleSchema.namesToValuesMap(schma),
      REGISTRY,
      StudentSchema,
      ReportingRequest,
      new DefaultConnectionUserInfoProvider
    )

    mahaAvaticaService(new OpenConnectionRequest(connectionID, Maps.newHashMap()))
    val result =  mahaAvaticaService(new PrepareAndExecuteRequest(connectionID, 1, "select * from student_performance", 10))
    assert(result.results.size() == 1)
    val frame = result.results.get(0).firstFrame
    assert(frame!=null)
    var count = 0;
    val rowsIt = frame.rows.iterator();
    rowsIt.forEachRemaining(s=> {
      count+=1
    })
    assert(count == 1)
  }
  test("Test Avatica Service with Describe table request") {
    val mahaAvaticaService = new DefaultMahaAvaticaService(executeFunction,
      DefaultMahaCalciteSqlParser(mahaServiceConfig),
      mahaService,
      new DefaultAvaticaRowListTransformer(),
      (schma)=> ExampleSchema.namesToValuesMap(schma),
      REGISTRY,
      StudentSchema,
      ReportingRequest,
      new DefaultConnectionUserInfoProvider
    )

    mahaAvaticaService(new OpenConnectionRequest(connectionID, Maps.newHashMap()))
    val result =  mahaAvaticaService(new PrepareAndExecuteRequest(connectionID, 1, "describe student_performance", 10))
    assert(result.results.size() == 1)
    val frame = result.results.get(0).firstFrame
    assert(frame!=null)
    var count = 0;
    val rowsIt = frame.rows.iterator();
    rowsIt.forEachRemaining(s=> {
      count+=1
    })
    assert(count > 10)

  }

  test("Noop avatica service") {
    val noopMahaAvaticaService = new NoopMahaAvaticaService()
    noopMahaAvaticaService.apply(new PrepareAndExecuteRequest("", 1, "", 10))
    noopMahaAvaticaService.apply(new OpenConnectionRequest())
    noopMahaAvaticaService.apply(new FetchRequest("", 1, 0, 10))
  }


}