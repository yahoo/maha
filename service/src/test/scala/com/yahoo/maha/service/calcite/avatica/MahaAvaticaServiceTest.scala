package com.yahoo.maha.service.calcite.avatica

import com.google.common.collect.Maps
import com.yahoo.maha.core.bucketing.{BucketParams, UserInfo}
import com.yahoo.maha.core.helper.jdbc.ColumnMetadata
import com.yahoo.maha.core.query.{CompleteRowList, QueryAttributes, QueryRowList}
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.service.calcite.DefaultMahaCalciteSqlParser
import com.yahoo.maha.service.example.ExampleSchema
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import com.yahoo.maha.service.{BaseMahaServiceTest, MahaRequestContext, MahaService, RegistryConfig}
import org.apache.calcite.avatica.ColumnMetaData
import org.apache.calcite.avatica.proto.Requests.TablesRequest
import org.apache.calcite.avatica.remote.Service
import org.apache.calcite.avatica.remote.Service.{FetchRequest, OpenConnectionRequest, PrepareAndExecuteRequest}

import scala.collection.mutable.ArrayBuffer

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
    assert(count > 19)

  }

  test("Test that Describe table request returns distinct rows") {
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
    val rowsIt = frame.rows.iterator();
    val distinctCols = scala.collection.mutable.Set[String]()
    rowsIt.forEachRemaining(s=> {
      assert(distinctCols.add(s.asInstanceOf[Array[String]](0))==true)
    })
  }

  test("Noop avatica service") {
    val noopMahaAvaticaService = new NoopMahaAvaticaService()
    noopMahaAvaticaService.apply(new PrepareAndExecuteRequest("", 1, "", 10))
    noopMahaAvaticaService.apply(new OpenConnectionRequest())
    noopMahaAvaticaService.apply(new FetchRequest("", 1, 0, 10))
  }

  test("Test get table requests") {
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

    val result =  mahaAvaticaService(new Service.TablesRequest(connectionID, "", "", "", null))
    //println(result)
    assert(result != null && result.signature != null && result.firstFrame != null)
    val columns = result.signature.columns
    assert(columns != null && columns.size() == 10)
    val rows = result.firstFrame.rows
    var count = 0
    rows.iterator().forEachRemaining(s=> {
      assert(s.asInstanceOf[Array[String]](2).contains("student_performance") || s.asInstanceOf[Array[String]](2).contains("druid_performance"))
      count+=1
    })
    val factMaps = mahaServiceConfig.registry.values.map{
      case registryConfig: RegistryConfig => registryConfig.registry.factMap
    }

    val expected_count = factMaps.flatten.map(_._1).map(_._1).asInstanceOf[List[String]].distinct.size
    assert(count == expected_count)
  }

  test("Test get column requests") {
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

    val result =  mahaAvaticaService(new Service.ColumnsRequest(connectionID, "", "", "student_performance", null))
    //println(result)
    assert(result != null && result.signature != null && result.firstFrame != null)
    val columnsMetaDataList = result.signature.columns

    assert(columnsMetaDataList.get(4).`type`.name.equals("INTEGER")) //DATA_TYPE
    assert(columnsMetaDataList.get(6).`type`.name.equals("INTEGER")) //COLUMN_SIZE
    assert(columnsMetaDataList.get(8).`type`.name.equals("INTEGER")) //DECIMAL_DIGITS
    assert(columnsMetaDataList.get(10).`type`.name.equals("INTEGER")) //NULLABLE

    val rows = result.firstFrame.rows
    var count = 0
    rows.iterator().forEachRemaining(s=> {
      assert(s.asInstanceOf[Array[Object]].size == 24)
      count+=1
    })
    assert(count > 19)
  }
}