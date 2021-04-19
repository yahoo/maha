// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.example

import com.yahoo.maha.core.bucketing._
import com.yahoo.maha.core.query.druid.{DruidQueryGenerator, SyncDruidQueryOptimizer}
import com.yahoo.maha.core.query.hive.HiveQueryGenerator
import com.yahoo.maha.core.query.oracle.{BaseOracleQueryGeneratorTest, OracleQueryGenerator}
import com.yahoo.maha.core.query.presto.PrestoQueryGenerator
import com.yahoo.maha.core.query.{QueryRowList, Version}
import com.yahoo.maha.core.registry.{Registry, RegistryBuilder}
import com.yahoo.maha.core.request._
import com.yahoo.maha.log.MultiColoMahaRequestLogWriter
import com.yahoo.maha.parrequest2.GeneralError
import com.yahoo.maha.parrequest2.future.ParFunction
import com.yahoo.maha.service._
import com.yahoo.maha.service.error.MahaServiceBadRequestException
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import grizzled.slf4j.Logging
import org.scalatest.BeforeAndAfterAll
import com.yahoo.maha.core.{DefaultPartitionColumnRenderer, TestPrestoUDFRegistrationFactory, TestUDFRegistrationFactory, whiteSpaceNormalised}

/**
 * Created by pranavbhole on 09/06/17.
 */
class MahaServiceExampleTest extends BaseMahaServiceTest with Logging with BeforeAndAfterAll {

  override protected def afterAll(): Unit =  {
    super.afterAll()
    server.shutdownNow()
  }

  val jsonRequestHive = s"""{
                          "cube": "student_performance",
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Marks Obtained"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"}
                          ],
                         "sortBy": [
                            {"field": "Marks Obtained", "order": "Asc"}
                          ]
                        }"""

  val reportingRequestHive  =  {
    val reportingRequestHiveResult = ReportingRequest.deserializeAsync(jsonRequestHive.getBytes, schema = StudentSchema)
    require(reportingRequestHiveResult.isSuccess)
    ReportingRequest.forceHive(reportingRequestHiveResult.toOption.get)
  }

  test("Test MahaService with Example Schema") {

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

    val requestModelResultTry  = mahaService.generateRequestModel("er", reportingRequest, bucketParams)
    assert(requestModelResultTry.isSuccess)

    // Test General Error in Execute Model Test
    val resultFailure = mahaService.executeRequestModelResult("er", requestModelResultTry.get, mahaRequestLogHelper).prodRun.get(10000)
    assert(resultFailure.isLeft)
    val p = resultFailure.left.get
    assert(p.message.contains("""Failed to execute the query pipeline"""))

    // Test General Error in execute request
    val parRequestResultWithError = mahaService.executeRequest("er", reportingRequest, bucketParams, mahaRequestLogHelper)
    val q = parRequestResultWithError.prodRun.resultMap(
      ParFunction.from((t: RequestResult)
      => t)
    )
    assert(q.left.get.message.contains("""Failed to execute the query pipeline"""))

    // Test General Error in process Model
    val resultFailureToProcessModel = mahaService.processRequest("er", reportingRequest, bucketParams, mahaRequestLogHelper)
    assert(resultFailureToProcessModel.isLeft)

    //Test General Error in process Request Model
    val processRequestModelWithFailure = mahaService.processRequestModel("er", requestModelResultTry.get.model, mahaRequestLogHelper)
    assert(processRequestModelWithFailure.isLeft)

    //Create tables
    createTables()

    // Execute Model Test
    val result = mahaService.executeRequestModelResult("er", requestModelResultTry.get, mahaRequestLogHelper).prodRun.get(10000)
    assert(result.isRight)
    assert(result.right.get.queryPipelineResult.rowList.asInstanceOf[QueryRowList].columnNames.contains("Student ID"))

    // Process Model Test
    val processRequestModelResult  = mahaService.processRequestModel("er", requestModelResultTry.get.model, mahaRequestLogHelper)
    assert(processRequestModelResult.isRight)
    assert(processRequestModelResult.right.get.queryPipelineResult.rowList.asInstanceOf[QueryRowList].columnNames.contains("Class ID"))

    // Process Request Test
    val processRequestResult = mahaService.processRequest("er", reportingRequest, bucketParams, mahaRequestLogHelper)
    assert(processRequestResult.isRight)
    assert(processRequestResult.right.get.queryPipelineResult.rowList.asInstanceOf[QueryRowList].columnNames.contains("Class ID"))

    //ExecuteRequest Test
    val executeRequestParRequestResult = mahaService.executeRequest("er", reportingRequest, bucketParams, mahaRequestLogHelper)
    assert(executeRequestParRequestResult.prodRun.get(10000).isRight)
    val requestResultOption  = Option(executeRequestParRequestResult.prodRun.get(10000))
    assert(requestResultOption.get.right.get.queryPipelineResult.rowList.asInstanceOf[QueryRowList].columnNames.contains("Total Marks"))

    // Domain Tests
    val cubesJsonOption = mahaService.getCubes("er")
    assert(cubesJsonOption.isDefined)
    assert(cubesJsonOption.get === """["student_performance","student_performance2"]""")
    val domainJsonOption = mahaService.getDomain("er")
    assert(domainJsonOption.isDefined)
    assert(domainJsonOption.get.contains("""{"dimensions":[{"name":"remarks","fields":["Remarks","Remark URL","Remark Name","Remark Status"],"fieldsWithSchemas":[{"name":"Remarks","allowedSchemas":[]},{"name":"Remark URL","allowedSchemas":[]},{"name":"Remark Name","allowedSchemas":[]},{"name":"Remark Status","allowedSchemas":[]}]},{"name":"researcher","fields":["Researcher Profile URL","Science Lab Volunteer ID","Researcher Name","Tutor ID","Researcher ID","Researcher Status"],"fieldsWithSchemas":[{"name":"Science Lab Volunteer ID","allowedSchemas":[]},{"name":"Researcher Status","allowedSchemas":[]},{"name":"Researcher ID","allowedSchemas":[]},{"name":"Researcher Name","allowedSchemas":[]},{"name":"Researcher Profile URL","allowedSchemas":[]},{"name":"Tutor ID","allowedSchemas":[]}]},{"name":"tutors","fields":["Tutor ID","Tutor Name","Tutor Status"],"fieldsWithSchemas":[{"name":"Tutor ID","allowedSchemas":[]},{"name":"Tutor Name","allowedSchemas":[]},{"name":"Tutor Status","allowedSchemas":[]}]},{"name":"science_lab_volunteers","fields":["Science Lab Volunteer ID","Science Lab Volunteer Name","Science Lab Volunteer Status"],"fieldsWithSchemas":[{"name":"Science Lab Volunteer ID","allowedSchemas":[]},{"name":"Science Lab Volunteer Name","allowedSchemas":[]},{"name":"Science Lab Volunteer Status","allowedSchemas":[]}]},{"name":"class_volunteers","fields":["Class Volunteer ID","Class Volunteer Name","Class Volunteer Status"],"fieldsWithSchemas":[{"name":"Class Volunteer ID","allowedSchemas":[]},{"name":"Class Volunteer Name","allowedSchemas":[]},{"name":"Class Volunteer Status","allowedSchemas":[]}]},{"name":"class","fields":["Class Name","Class Status","Professor Name","Class ID"],"fieldsWithSchemas":[{"name":"Class Name","allowedSchemas":[]},{"name":"Class Status","allowedSchemas":[]},{"name":"Professor Name","allowedSchemas":[]},{"name":"Class ID","allowedSchemas":[]}]},{"name":"section","fields":["Section ID","Lab ID","Student ID","Section Status","Class ID","Section Name"],"fieldsWithSchemas":[{"name":"Section Status","allowedSchemas":[]},{"name":"Class ID","allowedSchemas":[]},{"name":"Section ID","allowedSchemas":[]},{"name":"Section Name","allowedSchemas":[]},{"name":"Student ID","allowedSchemas":[]},{"name":"Lab ID","allowedSchemas":[]}]},{"name":"labs","fields":["Lab Name","Lab ID","Lab Status","Researcher ID"],"fieldsWithSchemas":[{"name":"Lab Name","allowedSchemas":[]},{"name":"Lab ID","allowedSchemas":[]},{"name":"Lab Status","allowedSchemas":[]},{"name":"Researcher ID","allowedSchemas":[]}]},{"name":"student","fields":["Profile URL","Student Name","Student ID","Student Status"],"fieldsWithSchemas":[{"name":"Profile URL","allowedSchemas":[]},{"name":"Student Name","allowedSchemas":[]},{"name":"Student ID","allowedSchemas":[]},{"name":"Student Status","allowedSchemas":[]}]}],"schemas":{"student":["student_performance","student_performance2"]},"cubes":[{"name":"student_performance","mainEntityIds":{"student":"Student ID"},"maxDaysLookBack":[{"requestType":"SyncRequest","grain":"DailyGrain","days":30},{"requestType":"AsyncRequest","grain":"DailyGrain","days":30}],"maxDaysWindow":[{"requestType":"SyncRequest","grain":"DailyGrain","days":20},{"requestType":"AsyncRequest","grain":"DailyGrain","days":20},{"requestType":"SyncRequest","grain":"HourlyGrain","days":20},{"requestType":"AsyncRequest","grain":"HourlyGrain","days":20}],"fields":[{"field":"Class ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"class","filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Day","type":"Dimension","dataType":{"type":"Date","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Month","type":"Dimension","dataType":{"type":"Date","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Remarks","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","=","LIKE"],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Section ID","type":"Dimension","dataType":{"type":"Number","constraint":"3"},"dimensionName":null,"filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Student ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"student","filterable":true,"filterOperations":["IN","LIKE","=","BETWEEN","=="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Top Student ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["=="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Year","type":"Dimension","dataType":{"type":"Enum","constraint":"Freshman|Junior|Sophomore|Senior"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Marks Obtained","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","LIKE","=","BETWEEN","=="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null,"allowedSchemas":null},{"field":"Performance Factor","type":"Fact","dataType":{"type":"Number","constraint":"10"},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null,"allowedSchemas":null},{"field":"Total Marks","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","LIKE","=","BETWEEN","=="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null,"allowedSchemas":null}]},{"name":"student_performance2","mainEntityIds":{"student":"Student ID"},"maxDaysLookBack":[{"requestType":"SyncRequest","grain":"DailyGrain","days":30},{"requestType":"AsyncRequest","grain":"DailyGrain","days":30}],"maxDaysWindow":[{"requestType":"SyncRequest","grain":"DailyGrain","days":20},{"requestType":"AsyncRequest","grain":"DailyGrain","days":20},{"requestType":"SyncRequest","grain":"HourlyGrain","days":20},{"requestType":"AsyncRequest","grain":"HourlyGrain","days":20}],"fields":[{"field":"Class ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"class","filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Day","type":"Dimension","dataType":{"type":"Date","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Month","type":"Dimension","dataType":{"type":"Date","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Section ID","type":"Dimension","dataType":{"type":"Number","constraint":"3"},"dimensionName":"section","filterable":true,"filterOperations":["IN","NOT IN","="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Student ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"student","filterable":true,"filterOperations":["IN","=","=="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Top Student ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["=="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Year","type":"Dimension","dataType":{"type":"Enum","constraint":"Freshman|Junior|Sophomore|Senior"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null,"isImageColumn":false,"allowedSchemas":null},{"field":"Marks Obtained","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null,"allowedSchemas":null},{"field":"Total Marks","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null,"allowedSchemas":null}]}]}""".stripMargin))
    val flattenDomainJsonOption = mahaService.getDomain("er")
    assert(flattenDomainJsonOption.isDefined)
    val cubeDomain = mahaService.getDomainForCube("er", "student_performance")
    assert(cubeDomain.isDefined)
    val flatDomain = mahaService.getFlattenDomain("er")
    assert(flatDomain.isDefined)
    val flatDomainWithoutRev = mahaService.getFlattenDomainForCube("er", "student_performance")
    assert(flatDomainWithoutRev.isDefined)
    val flatDomainWithRev = mahaService.getFlattenDomainForCube("er", "student_performance", Option(0))
    assert(flatDomainWithRev.isDefined)
    assert(!mahaService.getDomain("temp").isDefined)
    assert(!mahaService.getFlattenDomain("temp").isDefined)
    assert(!mahaService.getDomainForCube("temp", "inexistent").isDefined)
    assert(!mahaService.getFlattenDomainForCube("temp", "inexistent").isDefined)

    val mahaRequestProcessor = new MahaSyncRequestProcessor(mahaRequestContext,
      DefaultRequestCoordinator(mahaService),
      mahaServiceConfig.mahaRequestLogWriter
    )

    def fn = {
      (requestCoordinatorResult: RequestCoordinatorResult) => {
        val requestResult = requestCoordinatorResult.successResults.head._2.head.requestResult
        assert(requestResult.queryPipelineResult.rowList.columns.size  ==  4)
        assert(requestResult.queryPipelineResult.rowList.asInstanceOf[QueryRowList].columnNames.contains("Total Marks"))
        logger.info("Inside onSuccess function")
      }
    }

    mahaRequestProcessor.onSuccess(fn)
    mahaRequestProcessor.onFailure((error: GeneralError) => logger.error(error.message))

    mahaRequestProcessor.process()
    val thrown = intercept[IllegalArgumentException] {
      val failedProcessor = MahaSyncRequestProcessor(mahaRequestContext, DefaultRequestCoordinator(mahaService), mahaServiceConfig.mahaRequestLogWriter)
      failedProcessor.process()
    }
  }

  test("Test MahaService with Example Schema generating valid Dim Candidates") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Student Name"},
                            {"field": "Admitted Year"},
                            {"field": "Student Status"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"}
                          ],
                         "sortBy": [
                            {"field": "Admitted Year", "order": "Asc"},
                            {"field": "Student ID", "order": "Desc"}
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

    val requestModelResultTry  = mahaService.generateRequestModel("er", reportingRequest, bucketParams)
    assert(requestModelResultTry.isSuccess)

    val processRequestResult = mahaService.processRequest("er", reportingRequest, bucketParams, mahaRequestLogHelper)
    assert(processRequestResult.isRight)

    val parRequestResult = mahaService.executeRequest(REGISTRY, ReportingRequest.forceHive(reportingRequest),bucketParams, mahaRequestLogHelper)
    val result = parRequestResult.prodRun.get(1000)
    assert(result.isRight)

    assert(mahaServiceConfig.mahaRequestLogWriter.isInstanceOf[MultiColoMahaRequestLogWriter])
  }

  test("Test RequestModel Failure using mahaService") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Student Name"},
                            {"field": "Admitted Year"},
                            {"field": "Student Status Unknown Column"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"}
                          ],
                         "sortBy": [
                            {"field": "Admitted Year", "order": "Asc"},
                            {"field": "Student ID", "order": "Desc"}
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

    val requestModelResultTry  = mahaService.generateRequestModel("er", reportingRequest, bucketParams)
    assert(requestModelResultTry.isFailure)

    val exception = intercept[MahaServiceBadRequestException] {
      mahaService.processRequest("er", reportingRequest, bucketParams, mahaRequestLogHelper)
    }
    assert(exception.source.get.getMessage.contains("ERROR_CODE:10005 Failed to find primary key alias for Student Status Unknown Column"))

    val executionException = intercept[MahaServiceBadRequestException] {
          val parRequestResult = mahaService.executeRequest(REGISTRY, ReportingRequest.forceHive(reportingRequest),bucketParams, mahaRequestLogHelper)
       assert(parRequestResult.prodRun.get(800).isLeft)
    }
    val thrown = intercept[IllegalArgumentException] {
       mahaService.executeRequest("unknown", ReportingRequest.forceHive(reportingRequest),bucketParams, mahaRequestLogHelper)
    }

    assert(executionException.source.get.getMessage.contains("ERROR_CODE:10005 Failed to find primary key alias for Student Status Unknown Column"))
  }

  test("Test MahaService with Example Schema generating valid query pipeline") {

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Student Name"},
                            {"field": "Admitted Year"},
                            {"field": "Student Status"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"}
                          ],
                         "sortBy": [
                            {"field": "Admitted Year", "order": "Asc"},
                            {"field": "Student ID", "order": "Desc"}
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

    val requestModelResultTry  = mahaService.generateRequestModel("er", reportingRequest, bucketParams)
    assert(requestModelResultTry.isSuccess)

    val queryPipelines = mahaService.generateQueryPipelines("er", requestModelResultTry.get.model, BucketParams())
    assert(queryPipelines._1.isSuccess)

    val queryPipelinesWithForceVersion = mahaService.generateQueryPipelines("er", requestModelResultTry.get.model, BucketParams(forceQueryGenVersion = Some(Version.v1)))
    assert(queryPipelinesWithForceVersion._1.isFailure)
  }

  test("Test Query Gen bucketing") {
    val bucketParams = BucketParams(userInfo = UserInfo("maha", true), forceRevision = Some(0))
    val requestModelHiveResultTry  = mahaService.generateRequestModel("er", reportingRequestHive, bucketParams)
    assert(requestModelHiveResultTry.isSuccess)

    val queryPipelinesWithQueryGenBuckets = mahaService.generateQueryPipelines("er", requestModelHiveResultTry.get.model, bucketParams)
    assert(queryPipelinesWithQueryGenBuckets._1.isSuccess)
    assert(queryPipelinesWithQueryGenBuckets._1.get.queryChain.drivingQuery.queryGenVersion == Some(Version.v0))
  }

}

class RequestModelSameDimLevelJoinTest extends BaseOracleQueryGeneratorTest {

  override protected def beforeAll(): Unit = {
    OracleQueryGenerator.register(queryGeneratorRegistry, DefaultPartitionColumnRenderer)
    HiveQueryGenerator.register(queryGeneratorRegistry, DefaultPartitionColumnRenderer, TestUDFRegistrationFactory())
    DruidQueryGenerator.register(queryGeneratorRegistry, queryOptimizer = new SyncDruidQueryOptimizer(timeout = 5000))
    PrestoQueryGenerator.register(queryGeneratorRegistry, DefaultPartitionColumnRenderer, TestPrestoUDFRegistrationFactory())
  }

  def getExampleRegistry(): Registry = {
    val registryBuilder = new RegistryBuilder
    new SampleDimensionSchemaRegistrationFactory().register(registryBuilder)
    new SampleFactSchemaRegistrationFactory().register(registryBuilder)
    val registry = registryBuilder.build()
    registry
  }

  lazy val exampleRegistry: Registry = getExampleRegistry()

  test("Test: query only FK in a dim table should succeed") {
    val jsonString =
      s"""
         |{
         |    "cube": "student_performance",
         |    "forceDimensionDriven": true,
         |    "selectFields": [
         |        {
         |            "field": "Student ID"
         |        },
         |        {
         |            "field": "Researcher ID"
         |        },
         |        {
         |            "field": "Class Volunteer ID"
         |        }
         |    ],
         |    "filterExpressions": [
         |        {
         |            "field": "Day",
         |            "operator": "between",
         |            "from": "$fromDate",
         |            "to": "$toDate"
         |        },
         |        {
         |            "field": "Student ID",
         |            "operator": "=",
         |            "value": "213"
         |        }
         |    ]
         |}
         |""".stripMargin

    val request: ReportingRequest = getReportingRequestSync(jsonString, StudentSchema)
    val registry = exampleRegistry
    val res = getRequestModel(request, registry, revision = Some(3))
    assert(res.isSuccess, s"Building request model failed.")

    val queryPipelineTry = generatePipeline(res.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val result = queryPipeline.queryChain.drivingQuery.asString
    println(result)

    val expected =
      s"""
         |SELECT  *
         |      FROM (
         |          SELECT "Student ID", "Researcher ID", "Class Volunteer ID", ROWNUM AS ROW_NUMBER
         |              FROM(SELECT sv0.id "Student ID", sv0.researcher_id "Researcher ID", sv0.class_volunteer_id "Class Volunteer ID"
         |                  FROM
         |                (SELECT  class_volunteer_id, researcher_id, id
         |            FROM student_v1
         |            WHERE (id = 213)
         |             ) sv0
         |
         |
         |                  ))
         |                   WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
         |""".stripMargin

    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Test: 2 same dim level tables join should succeed") {
    val jsonString = s"""{
                        "cube": "student_performance",
                        "isDimDriven": true,
                        "selectFields": [
                            {
                                "field": "Student Name"
                            },
                            {
                                "field": "Researcher Name"
                            }
                        ],
                        "filterExpressions": [
                            {
                                "field": "Day",
                                "operator": "between",
                                "from": "$fromDate",
                                "to": "$toDate"
                            },
                            {
                                "field": "Student ID",
                                "operator": "=",
                                "value": "213"
                            }
                        ]
                    }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString, StudentSchema)
    val registry = exampleRegistry
    val res = getRequestModel(request, registry, revision = Some(3))
    assert(res.isSuccess, s"Building request model failed.")

    val queryPipelineTry = generatePipeline(res.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val result = queryPipeline.queryChain.drivingQuery.asString
    //println(result)
    val expected = s"""SELECT  *
                      |      FROM (
                      |          SELECT "Student Name", "Researcher Name", ROWNUM AS ROW_NUMBER
                      |              FROM(SELECT sv1.name "Student Name", r0.name "Researcher Name"
                      |                  FROM
                      |               ( (SELECT  researcher_id, name, id
                      |            FROM student_v1
                      |            WHERE (id = 213)
                      |             ) sv1
                      |          INNER JOIN
                      |            (SELECT  name, id
                      |            FROM researcher
                      |
                      |             ) r0
                      |              ON( sv1.researcher_id = r0.id )
                      |               )
                      |
                      |                  ))
                      |                   WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
                      |""".stripMargin

    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Test: 2 same dim level tables join, with Student Name as filter, should succeed") {
    val jsonString =
      s"""
         |{
         |    "cube": "student_performance",
         |    "isDimDriven": true,
         |    "selectFields": [
         |        {
         |            "field": "Student Name"
         |        },
         |        {
         |            "field": "Researcher Name"
         |        }
         |    ],
         |    "filterExpressions": [
         |        {
         |            "field": "Day",
         |            "operator": "between",
         |            "from": "$fromDate",
         |            "to": "$toDate"
         |        },
         |        {
         |            "field": "Student ID",
         |            "operator": "=",
         |            "value": "213"
         |        },
         |        {
         |            "field": "Student Name",
         |            "operator": "=",
         |            "value": "testName1"
         |        }
         |    ]
         |}
         |""".stripMargin

    val request: ReportingRequest = getReportingRequestSync(jsonString, StudentSchema)
    val registry = exampleRegistry
    val res = getRequestModel(request, registry, revision = Some(3))
    assert(res.isSuccess, s"Building request model failed.")

    val queryPipelineTry = generatePipeline(res.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val result = queryPipeline.queryChain.drivingQuery.asString
    //println(result)

    val expected =
      s"""
         |SELECT  *
         |      FROM (
         |          SELECT "Student Name", "Researcher Name", ROWNUM AS ROW_NUMBER
         |              FROM(SELECT sv1.name "Student Name", r0.name "Researcher Name"
         |                  FROM
         |               ( (SELECT  researcher_id, name, id
         |            FROM student_v1
         |            WHERE (id = 213) AND (name = 'testName1')
         |             ) sv1
         |          INNER JOIN
         |            (SELECT  name, id
         |            FROM researcher
         |
         |             ) r0
         |              ON( sv1.researcher_id = r0.id )
         |               )
         |
         |                  ))
         |                   WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
         |""".stripMargin

    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Test: 2 same dim level tables join, with Researcher Name as filter, should succeed") {
    val jsonString =
      s"""
         |{
         |    "cube": "student_performance",
         |    "isDimDriven": true,
         |    "selectFields": [
         |        {
         |            "field": "Student Name"
         |        },
         |        {
         |            "field": "Researcher Name"
         |        }
         |    ],
         |    "filterExpressions": [
         |        {
         |            "field": "Day",
         |            "operator": "between",
         |            "from": "$fromDate",
         |            "to": "$toDate"
         |        },
         |        {
         |            "field": "Student ID",
         |            "operator": "=",
         |            "value": "213"
         |        },
         |        {
         |            "field": "Researcher Name",
         |            "operator": "=",
         |            "value": "testName1"
         |        }
         |    ]
         |}
         |""".stripMargin

    val request: ReportingRequest = getReportingRequestSync(jsonString, StudentSchema)
    val registry = exampleRegistry
    val res = getRequestModel(request, registry, revision = Some(3))
    assert(res.isSuccess, s"Building request model failed.")

    val queryPipelineTry = generatePipeline(res.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val result = queryPipeline.queryChain.drivingQuery.asString
    //println(result)

    val expected =
      s"""
         |SELECT  *
         |      FROM (
         |          SELECT "Student Name", "Researcher Name", ROWNUM AS ROW_NUMBER
         |              FROM(SELECT sv1.name "Student Name", r0.name "Researcher Name"
         |                  FROM
         |               ( (SELECT  researcher_id, name, id
         |            FROM student_v1
         |            WHERE (id = 213)
         |             ) sv1
         |          INNER JOIN
         |            (SELECT  name, id
         |            FROM researcher
         |            WHERE (name = 'testName1')
         |             ) r0
         |              ON( sv1.researcher_id = r0.id )
         |               )
         |
         |                  ))
         |                   WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
         |""".stripMargin

    result should equal(expected)(after being whiteSpaceNormalised)
  }

  /*
   * This test case will be failed with the following error: (need to fix)
   * queryPipelineTry.isSuccess was false Fail to get the query pipeline - requirement failed: level of primary dimension must be greater than subquery dimension, dim=student, subquery dim=researcher
   */
  test("Test: 2 same dim level tables join, with Researcher Name as filter but not in requested field, should succeed") {
    val jsonString =
      s"""
         |{
         |    "cube": "student_performance",
         |    "isDimDriven": true,
         |    "selectFields": [
         |        {
         |            "field": "Student Name"
         |        }
         |    ],
         |    "filterExpressions": [
         |        {
         |            "field": "Day",
         |            "operator": "between",
         |            "from": "$fromDate",
         |            "to": "$toDate"
         |        },
         |        {
         |            "field": "Student ID",
         |            "operator": "=",
         |            "value": "213"
         |        },
         |        {
         |            "field": "Researcher Name",
         |            "operator": "=",
         |            "value": "testName1"
         |        }
         |    ]
         |}
         |""".stripMargin

    val request: ReportingRequest = getReportingRequestSync(jsonString, StudentSchema)
    val registry = exampleRegistry
    val res = getRequestModel(request, registry, revision = Some(3))
    assert(res.isSuccess, s"Building request model failed.")

    val queryPipelineTry = generatePipeline(res.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val result = queryPipeline.queryChain.drivingQuery.asString
    //println(result)

    val expected =
      s"""
         |SELECT  *
         |      FROM (
         |          SELECT "Student Name", ROWNUM AS ROW_NUMBER
         |              FROM(SELECT sv0.name "Student Name"
         |                  FROM
         |               ( (SELECT  researcher_id, name, id
         |            FROM student_v1
         |            WHERE (researcher_id IN (SELECT id FROM researcher WHERE (name = 'testName1'))) AND (id = 213)
         |             ) sv0
         |          )
         |
         |                  ))
         |                   WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
         |""".stripMargin

    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Test: 2 same dim level tables join, with Student Name as filter but not in requested field, should succeed") {
    val jsonString =
      s"""
         |{
         |    "cube": "student_performance",
         |    "isDimDriven": true,
         |    "selectFields": [
         |        {
         |            "field": "Researcher Name"
         |        }
         |    ],
         |    "filterExpressions": [
         |        {
         |            "field": "Day",
         |            "operator": "between",
         |            "from": "$fromDate",
         |            "to": "$toDate"
         |        },
         |        {
         |            "field": "Student ID",
         |            "operator": "=",
         |            "value": "213"
         |        },
         |        {
         |            "field": "Student Name",
         |            "operator": "=",
         |            "value": "testName1"
         |        }
         |    ]
         |}
         |""".stripMargin

    val request: ReportingRequest = getReportingRequestSync(jsonString, StudentSchema)
    val registry = exampleRegistry
    val res = getRequestModel(request, registry, revision = Some(3))
    assert(res.isSuccess, s"Building request model failed.")

    val queryPipelineTry = generatePipeline(res.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val result = queryPipeline.queryChain.drivingQuery.asString
    //println(result)

    val expected =
      s"""
         |SELECT  *
         |      FROM (
         |          SELECT "Researcher Name", ROWNUM AS ROW_NUMBER
         |              FROM(SELECT r0.name "Researcher Name"
         |                  FROM
         |               ( (SELECT  researcher_id, id
         |            FROM student_v1
         |            WHERE (id = 213) AND (name = 'testName1')
         |             ) sv1
         |          INNER JOIN
         |            (SELECT  name, id
         |            FROM researcher
         |
         |             ) r0
         |              ON( sv1.researcher_id = r0.id )
         |               )
         |
         |                  ))
         |                   WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
         |""".stripMargin

    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Test: 2 same dim level tables join, with Student Status as filter, should succeed") {
    val jsonString =
      s"""
         |{
         |    "cube": "student_performance",
         |    "isDimDriven": true,
         |    "selectFields": [
         |        {
         |            "field": "Student Name"
         |        },
         |        {
         |            "field": "Researcher Name"
         |        }
         |    ],
         |    "filterExpressions": [
         |        {
         |            "field": "Day",
         |            "operator": "between",
         |            "from": "$fromDate",
         |            "to": "$toDate"
         |        },
         |        {
         |            "field": "Student ID",
         |            "operator": "=",
         |            "value": "213"
         |        },
         |        {
         |            "field": "Student Status",
         |            "operator": "=",
         |            "value": "admitted"
         |        }
         |    ]
         |}
         |""".stripMargin

    val request: ReportingRequest = getReportingRequestSync(jsonString, StudentSchema)
    val registry = exampleRegistry
    val res = getRequestModel(request, registry, revision = Some(3))
    assert(res.isSuccess, s"Building request model failed.")

    val queryPipelineTry = generatePipeline(res.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val result = queryPipeline.queryChain.drivingQuery.asString
    //println(result)

    val expected =
      s"""
         |SELECT  *
         |      FROM (
         |          SELECT "Student Name", "Researcher Name", ROWNUM AS ROW_NUMBER
         |              FROM(SELECT sv1.name "Student Name", r0.name "Researcher Name"
         |                  FROM
         |               ( (SELECT  researcher_id, name, id
         |            FROM student_v1
         |            WHERE (id = 213) AND (status = 'admitted')
         |             ) sv1
         |          INNER JOIN
         |            (SELECT  name, id
         |            FROM researcher
         |
         |             ) r0
         |              ON( sv1.researcher_id = r0.id )
         |               )
         |
         |                  ))
         |                   WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
         |""".stripMargin

    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Test: 2 same dim level tables join, with Researcher Status as filter, should succeed") {
    val jsonString =
      s"""
         |{
         |    "cube": "student_performance",
         |    "isDimDriven": true,
         |    "selectFields": [
         |        {
         |            "field": "Student Name"
         |        },
         |        {
         |            "field": "Researcher Name"
         |        }
         |    ],
         |    "filterExpressions": [
         |        {
         |            "field": "Day",
         |            "operator": "between",
         |            "from": "$fromDate",
         |            "to": "$toDate"
         |        },
         |        {
         |            "field": "Student ID",
         |            "operator": "=",
         |            "value": "213"
         |        },
         |        {
         |            "field": "Researcher Status",
         |            "operator": "=",
         |            "value": "admitted"
         |        }
         |    ]
         |}
         |""".stripMargin

    val request: ReportingRequest = getReportingRequestSync(jsonString, StudentSchema)
    val registry = exampleRegistry
    val res = getRequestModel(request, registry, revision = Some(3))
    assert(res.isSuccess, s"Building request model failed.")

    val queryPipelineTry = generatePipeline(res.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val result = queryPipeline.queryChain.drivingQuery.asString
    //println(result)

    val expected =
      s"""
         |SELECT  *
         |      FROM (
         |          SELECT "Student Name", "Researcher Name", ROWNUM AS ROW_NUMBER
         |              FROM(SELECT sv1.name "Student Name", r0.name "Researcher Name"
         |                  FROM
         |               ( (SELECT  researcher_id, name, id
         |            FROM student_v1
         |            WHERE (id = 213)
         |             ) sv1
         |          INNER JOIN
         |            (SELECT  name, id
         |            FROM researcher
         |            WHERE (status = 'admitted')
         |             ) r0
         |              ON( sv1.researcher_id = r0.id )
         |               )
         |
         |                  ))
         |                   WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
         |""".stripMargin

    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Test: 2 same dim level tables join, order by Student Name, should succeed") {
    val jsonString =
      s"""
         |{
         |    "cube": "student_performance",
         |    "isDimDriven": true,
         |    "selectFields": [
         |        {
         |            "field": "Student Name"
         |        },
         |        {
         |            "field": "Researcher Name"
         |        }
         |    ],
         |    "filterExpressions": [
         |        {
         |            "field": "Day",
         |            "operator": "between",
         |            "from": "$fromDate",
         |            "to": "$toDate"
         |        },
         |        {
         |            "field": "Student ID",
         |            "operator": "=",
         |            "value": "213"
         |        }
         |    ],
         |    "sortBy": [
         |        {
         |            "field": "Student Name",
         |            "order": "Asc"
         |        }
         |    ]
         |}
         |""".stripMargin

    val request: ReportingRequest = getReportingRequestSync(jsonString, StudentSchema)
    val registry = exampleRegistry
    val res = getRequestModel(request, registry, revision = Some(3))
    assert(res.isSuccess, s"Building request model failed.")

    val queryPipelineTry = generatePipeline(res.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val result = queryPipeline.queryChain.drivingQuery.asString
    //println(result)

    val expected =
      s"""
         |SELECT  *
         |      FROM (
         |          SELECT "Student Name", "Researcher Name", ROWNUM AS ROW_NUMBER
         |              FROM(SELECT sv1.name "Student Name", r0.name "Researcher Name"
         |                  FROM
         |               ( (SELECT  name, researcher_id, id
         |            FROM student_v1
         |            WHERE (id = 213)
         |            ORDER BY 1 ASC NULLS LAST ) sv1
         |          INNER JOIN
         |            (SELECT  name, id
         |            FROM researcher
         |
         |             ) r0
         |              ON( sv1.researcher_id = r0.id )
         |               )
         |
         |                  ))
         |                   WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
         |""".stripMargin

    result should equal(expected)(after being whiteSpaceNormalised)
  }

  // generated query does not contain order by Researcher Name, but similar issues exist in maha-cdw too.
  ignore("Test: 2 same dim level tables join, order by Researcher Name, should succeed") {
    val jsonString =
      s"""
         |{
         |    "cube": "student_performance",
         |    "isDimDriven": true,
         |    "selectFields": [
         |        {
         |            "field": "Student Name"
         |        },
         |        {
         |            "field": "Researcher Name"
         |        }
         |    ],
         |    "filterExpressions": [
         |        {
         |            "field": "Day",
         |            "operator": "between",
         |            "from": "$fromDate",
         |            "to": "$toDate"
         |        },
         |        {
         |            "field": "Student ID",
         |            "operator": "=",
         |            "value": "213"
         |        }
         |    ],
         |    "sortBy": [
         |        {
         |            "field": "Researcher Name",
         |            "order": "Asc"
         |        }
         |    ]
         |}
         |""".stripMargin

    val request: ReportingRequest = getReportingRequestSync(jsonString, StudentSchema)
    val registry = exampleRegistry
    val res = getRequestModel(request, registry, revision = Some(3))
    assert(res.isSuccess, s"Building request model failed.")

    val queryPipelineTry = generatePipeline(res.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val result = queryPipeline.queryChain.drivingQuery.asString
    //println(result)

    val expected =
      s"""
         |SELECT  *
         |      FROM (
         |          SELECT "Student Name", "Researcher Name", ROWNUM AS ROW_NUMBER
         |              FROM(SELECT sv1.name "Student Name", r0.name "Researcher Name"
         |                  FROM
         |               ( (SELECT  researcher_id, name, id
         |            FROM student_v1
         |            WHERE (id = 213)
         |             ) sv1
         |          INNER JOIN
         |            (SELECT  name, id
         |            FROM researcher
         |
         |             ) r0
         |              ON( sv1.researcher_id = r0.id )
         |               )
         |
         |                  ))
         |                   WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
         |""".stripMargin

    result should equal(expected)(after being whiteSpaceNormalised)
  }

  // generated query does not contain order by Researcher Name, need to fix
  ignore("Test: 2 same dim level tables join, order by Student Name and Researcher Name, should succeed") {
    val jsonString =
      s"""
         |{
         |    "cube": "student_performance",
         |    "isDimDriven": true,
         |    "selectFields": [
         |        {
         |            "field": "Student Name"
         |        },
         |        {
         |            "field": "Researcher Name"
         |        }
         |    ],
         |    "filterExpressions": [
         |        {
         |            "field": "Day",
         |            "operator": "between",
         |            "from": "$fromDate",
         |            "to": "$toDate"
         |        },
         |        {
         |            "field": "Student ID",
         |            "operator": "=",
         |            "value": "213"
         |        }
         |    ],
         |    "sortBy": [
         |        {
         |            "field": "Student Name",
         |            "order": "Asc"
         |        },
         |        {
         |            "field": "Researcher Name",
         |            "order": "Asc"
         |        }
         |    ]
         |}
         |""".stripMargin

    val request: ReportingRequest = getReportingRequestSync(jsonString, StudentSchema)
    val registry = exampleRegistry
    val res = getRequestModel(request, registry, revision = Some(3))
    assert(res.isSuccess, s"Building request model failed.")

    val queryPipelineTry = generatePipeline(res.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val result = queryPipeline.queryChain.drivingQuery.asString
    //println(result)

    val expected =
      s"""
         |SELECT  *
         |      FROM (
         |          SELECT "Student Name", "Researcher Name", ROWNUM AS ROW_NUMBER
         |              FROM(SELECT sv1.name "Student Name", r0.name "Researcher Name"
         |                  FROM
         |               ( (SELECT  name, researcher_id, id
         |            FROM student_v1
         |            WHERE (id = 213)
         |            ORDER BY 1 ASC NULLS LAST ) sv1
         |          INNER JOIN
         |            (SELECT  name, id
         |            FROM researcher
         |
         |             ) r0
         |              ON( sv1.researcher_id = r0.id )
         |               )
         |
         |                  ))
         |                   WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
         |""".stripMargin

    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Testing same level join with four dims") {
    val jsonString = s"""{
                        "cube": "student_performance",
                        "isDimDriven": true,
                        "selectFields": [
                            {
                                "field": "Student Name"
                            },
                            {
                                "field": "Lab Name"
                            },
                            {
                                "field": "Researcher Name"
                            },
                            {
                                "field": "Section Name"
                            }
                        ],
                        "filterExpressions": [
                            {
                                "field": "Day",
                                "operator": "between",
                                "from": "$fromDate",
                                "to": "$toDate"
                            },
                            {
                                "field": "Student ID",
                                "operator": "=",
                                "value": "213"
                            }
                        ]
                    }"""

    val request: ReportingRequest = getReportingRequestSync(jsonString, StudentSchema)
    val registry = exampleRegistry
    val res = getRequestModel(request, registry, revision = Some(3))
    assert(res.isSuccess, s"Building request model failed.")

    val queryPipelineTry = generatePipeline(res.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val result = queryPipeline.queryChain.drivingQuery.asString
    //println(result)

    val expected =
      s"""
         |SELECT  *
         |      FROM (
         |          SELECT "Student Name", "Lab Name", "Researcher Name", "Section Name", ROWNUM AS ROW_NUMBER
         |              FROM(SELECT sv1.name "Student Name", l3.name "Lab Name", r0.name "Researcher Name", s2.name "Section Name"
         |                  FROM
         |               ( (SELECT  lab_id, student_id, name, id
         |            FROM section
         |            WHERE (student_id = 213)
         |             ) s2
         |          INNER JOIN
         |            (SELECT  researcher_id, name, id
         |            FROM lab
         |
         |             ) l3
         |              ON( s2.lab_id = l3.id )
         |               INNER JOIN
         |            (SELECT  researcher_id, name, id
         |            FROM student_v1
         |
         |             ) sv1
         |              ON( s2.student_id = sv1.id )
         |               INNER JOIN
         |            (SELECT  name, id
         |            FROM researcher
         |
         |             ) r0
         |              ON( sv1.researcher_id = r0.id )
         |               )
         |
         |                  ))
         |                   WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
         |""".stripMargin

    result should equal(expected)(after being whiteSpaceNormalised)
  }


  test("Test: 3 same level dim tables join should be succeed (student, researchers, class_volunteers)") {
    val jsonString : String =
      s"""
         |{
         |    "cube": "student_performance",
         |    "isDimDriven": true,
         |    "selectFields": [
         |        {
         |            "field": "Student Name"
         |        },
         |        {
         |            "field": "Researcher Name"
         |        },
         |        {
         |            "field": "Class Volunteer Name"
         |        }
         |    ],
         |    "filterExpressions": [
         |        {
         |            "field": "Day",
         |            "operator": "between",
         |            "from": "$fromDate",
         |            "to": "$toDate"
         |        },
         |        {
         |            "field": "Student ID",
         |            "operator": "=",
         |            "value": "213"
         |        }
         |    ]
         |}
         |""".stripMargin

    val request: ReportingRequest = getReportingRequestSync(jsonString, StudentSchema)
    val registry = exampleRegistry
    val res = getRequestModel(request, registry, revision = Some(3))
    assert(res.isSuccess, s"Building request model failed.")

    val queryPipelineTry = generatePipeline(res.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val result = queryPipeline.queryChain.drivingQuery.asString
    //println(result)

    val expected =
      s"""
         |SELECT  *
         |      FROM (
         |          SELECT "Student Name", "Researcher Name", "Class Volunteer Name", ROWNUM AS ROW_NUMBER
         |              FROM(SELECT sv1.name "Student Name", r2.name "Researcher Name", cv0.name "Class Volunteer Name"
         |                  FROM
         |               ( (SELECT  class_volunteer_id, researcher_id, name, id
         |            FROM student_v1
         |            WHERE (id = 213)
         |             ) sv1
         |          INNER JOIN
         |            (SELECT  name, id
         |            FROM researcher
         |
         |             ) r2
         |              ON( sv1.researcher_id = r2.id )
         |               INNER JOIN
         |            (SELECT  name, id
         |            FROM class_volunteer
         |
         |             ) cv0
         |              ON( sv1.class_volunteer_id = cv0.id )
         |               )
         |
         |                  ))
         |                   WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
         |""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Test: 3 same level dim tables join should be succeed (student, researchers, science_lab_volunteers)") {
    val jsonString : String =
      s"""
         |{
         |    "cube": "student_performance",
         |    "isDimDriven": true,
         |    "selectFields": [
         |        {
         |            "field": "Student Name"
         |        },
         |        {
         |            "field": "Researcher Name"
         |        },
         |        {
         |            "field": "Science Lab Volunteer Name"
         |        }
         |    ],
         |    "filterExpressions": [
         |        {
         |            "field": "Day",
         |            "operator": "between",
         |            "from": "$fromDate",
         |            "to": "$toDate"
         |        },
         |        {
         |            "field": "Student ID",
         |            "operator": "=",
         |            "value": "213"
         |        }
         |    ]
         |}
         |""".stripMargin

    val request: ReportingRequest = getReportingRequestSync(jsonString, StudentSchema)
    val registry = exampleRegistry
    val res = getRequestModel(request, registry, revision = Some(3))
    assert(res.isSuccess, s"Building request model failed.")

    val queryPipelineTry = generatePipeline(res.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val result = queryPipeline.queryChain.drivingQuery.asString
    //println(result)

    val expected =
      s"""
         |SELECT  *
         |      FROM (
         |          SELECT "Student Name", "Researcher Name", "Science Lab Volunteer Name", ROWNUM AS ROW_NUMBER
         |              FROM(SELECT sv2.name "Student Name", r1.name "Researcher Name", slv0.name "Science Lab Volunteer Name"
         |                  FROM
         |               ( (SELECT  researcher_id, name, id
         |            FROM student_v1
         |            WHERE (id = 213)
         |             ) sv2
         |          INNER JOIN
         |            (SELECT  science_lab_volunteer_id, name, id
         |            FROM researcher
         |
         |             ) r1
         |              ON( sv2.researcher_id = r1.id )
         |               INNER JOIN
         |            (SELECT  name, id
         |            FROM science_lab_volunteer
         |
         |             ) slv0
         |              ON( r1.science_lab_volunteer_id = slv0.id )
         |               )
         |
         |                  ))
         |                   WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
         |""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Test: 3 same level dim tables join should be succeed (researchers, science_lab_volunteers, tutor)") {
    val jsonString : String =
      s"""
         |{
         |    "cube": "student_performance",
         |    "isDimDriven": true,
         |    "selectFields": [
         |        {
         |            "field": "Researcher Name"
         |        },
         |        {
         |            "field": "Science Lab Volunteer Name"
         |        },
         |        {
         |            "field": "Tutor Name"
         |        }
         |    ],
         |    "filterExpressions": [
         |        {
         |            "field": "Day",
         |            "operator": "between",
         |            "from": "$fromDate",
         |            "to": "$toDate"
         |        },
         |        {
         |            "field": "Student ID",
         |            "operator": "=",
         |            "value": "213"
         |        }
         |    ]
         |}
         |""".stripMargin

    val request: ReportingRequest = getReportingRequestSync(jsonString, StudentSchema)
    val registry = exampleRegistry
    val res = getRequestModel(request, registry, revision = Some(3))
    assert(res.isSuccess, s"should not fail on same level join")

    val queryPipelineTry = generatePipeline(res.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val result = queryPipeline.queryChain.drivingQuery.asString
    //println(result)

    val expected =
      s"""
         |SELECT  *
         |      FROM (
         |          SELECT "Researcher Name", "Science Lab Volunteer Name", "Tutor Name", ROWNUM AS ROW_NUMBER
         |              FROM(SELECT r1.name "Researcher Name", slv0.name "Science Lab Volunteer Name", t2.name "Tutor Name"
         |                  FROM
         |               ( (SELECT  science_lab_volunteer_id, tutor_id, name, id
         |            FROM researcher
         |
         |             ) r1
         |          INNER JOIN
         |            (SELECT  name, id
         |            FROM tutor
         |
         |             ) t2
         |              ON( r1.tutor_id = t2.id )
         |               INNER JOIN
         |            (SELECT  name, id
         |            FROM science_lab_volunteer
         |
         |             ) slv0
         |              ON( r1.science_lab_volunteer_id = slv0.id )
         |               )
         |
         |                  ))
         |                   WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
         |""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Test: 3 same level dim tables join should be succeed, with more fields, filters") {
    val jsonString : String =
      s"""
         |{
         |    "cube": "student_performance",
         |    "isDimDriven": true,
         |    "selectFields": [
         |        {
         |            "field": "Student Name"
         |        },
         |        {
         |            "field": "Researcher Name"
         |        },
         |        {
         |            "field": "Science Lab Volunteer Name"
         |        },
         |        {
         |            "field": "Student Status"
         |        },
         |        {
         |            "field": "Science Lab Volunteer Status"
         |        }
         |    ],
         |    "filterExpressions": [
         |        {
         |            "field": "Day",
         |            "operator": "between",
         |            "from": "$fromDate",
         |            "to": "$toDate"
         |        },
         |        {
         |            "field": "Student ID",
         |            "operator": "=",
         |            "value": "213"
         |        },
         |        {
         |            "field": "Researcher Name",
         |            "operator": "=",
         |            "value": "testName"
         |        },
         |        {
         |            "field": "Science Lab Volunteer Status",
         |            "operator": "=",
         |            "value": "admitted"
         |        },
         |        {
         |            "field": "Researcher Status",
         |            "operator": "=",
         |            "value": "admitted"
         |        }
         |    ]
         |}
         |""".stripMargin

    val request: ReportingRequest = getReportingRequestSync(jsonString, StudentSchema)
    val registry = exampleRegistry
    val res = getRequestModel(request, registry, revision = Some(3))
    assert(res.isSuccess, s"Building request model failed.")

    val queryPipelineTry = generatePipeline(res.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val result = queryPipeline.queryChain.drivingQuery.asString
    //println(result)

    val expected =
      s"""
         |SELECT  *
         |      FROM (
         |          SELECT "Student Name", "Researcher Name", "Science Lab Volunteer Name", "Student Status", "Science Lab Volunteer Status", ROWNUM AS ROW_NUMBER
         |              FROM(SELECT sv2.name "Student Name", r1.name "Researcher Name", slv0.name "Science Lab Volunteer Name", sv2.status "Student Status", slv0.status "Science Lab Volunteer Status"
         |                  FROM
         |               ( (SELECT  researcher_id, name, status, id
         |            FROM student_v1
         |            WHERE (id = 213)
         |             ) sv2
         |          INNER JOIN
         |            (SELECT  science_lab_volunteer_id, name, id
         |            FROM researcher
         |            WHERE (name = 'testName') AND (status = 'admitted')
         |             ) r1
         |              ON( sv2.researcher_id = r1.id )
         |               INNER JOIN
         |            (SELECT  name, status, id
         |            FROM science_lab_volunteer
         |            WHERE (status = 'admitted')
         |             ) slv0
         |              ON( r1.science_lab_volunteer_id = slv0.id )
         |               )
         |
         |                  ))
         |                   WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
         |""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Test: 4 same level dim tables join should be succeed (student, researchers, class_volunteers, science_lab_volunteers)") {
    val jsonString : String =
      s"""
         |{
         |    "cube": "student_performance",
         |    "isDimDriven": true,
         |    "selectFields": [
         |        {
         |            "field": "Student Name"
         |        },
         |        {
         |            "field": "Class Volunteer Name"
         |        },
         |        {
         |            "field": "Researcher Name"
         |        },
         |        {
         |            "field": "Science Lab Volunteer Name"
         |        }
         |    ],
         |    "filterExpressions": [
         |        {
         |            "field": "Day",
         |            "operator": "between",
         |            "from": "$fromDate",
         |            "to": "$toDate"
         |        },
         |        {
         |            "field": "Student ID",
         |            "operator": "=",
         |            "value": "213"
         |        }
         |    ]
         |}
         |""".stripMargin

    val request: ReportingRequest = getReportingRequestSync(jsonString, StudentSchema)
    val registry = exampleRegistry
    val res = getRequestModel(request, registry, revision = Some(3))
    assert(res.isSuccess, s"Building request model failed.")

    val queryPipelineTry = generatePipeline(res.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val result = queryPipeline.queryChain.drivingQuery.asString
    //println(result)

    val expected =
      s"""
         |SELECT  *
         |      FROM (
         |          SELECT "Student Name", "Class Volunteer Name", "Researcher Name", "Science Lab Volunteer Name", ROWNUM AS ROW_NUMBER
         |              FROM(SELECT sv1.name "Student Name", cv0.name "Class Volunteer Name", r3.name "Researcher Name", slv2.name "Science Lab Volunteer Name"
         |                  FROM
         |               ( (SELECT  class_volunteer_id, researcher_id, name, id
         |            FROM student_v1
         |            WHERE (id = 213)
         |             ) sv1
         |          INNER JOIN
         |            (SELECT  science_lab_volunteer_id, name, id
         |            FROM researcher
         |
         |             ) r3
         |              ON( sv1.researcher_id = r3.id )
         |               INNER JOIN
         |            (SELECT  name, id
         |            FROM science_lab_volunteer
         |
         |             ) slv2
         |              ON( r3.science_lab_volunteer_id = slv2.id )
         |               INNER JOIN
         |            (SELECT  name, id
         |            FROM class_volunteer
         |
         |             ) cv0
         |              ON( sv1.class_volunteer_id = cv0.id )
         |               )
         |
         |                  ))
         |                   WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
         |""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  // sometimes failed, because of random order between science lab volunteer and class volunteer
  test("Testing 5 same level dim tables join") {
    val jsonString : String =
      s"""
         |{
         |    "cube": "student_performance",
         |    "isDimDriven": true,
         |    "selectFields": [
         |        {
         |            "field": "Student Name"
         |        },
         |        {
         |            "field": "Researcher Name"
         |        },
         |        {
         |            "field": "Class Volunteer Name"
         |        },
         |        {
         |            "field": "Science Lab Volunteer Name"
         |        },
         |        {
         |            "field": "Tutor Name"
         |        }
         |    ],
         |    "filterExpressions": [
         |        {
         |            "field": "Day",
         |            "operator": "between",
         |            "from": "$fromDate",
         |            "to": "$toDate"
         |        },
         |        {
         |            "field": "Student ID",
         |            "operator": "=",
         |            "value": "213"
         |        }
         |    ]
         |}
         |""".stripMargin

    val request: ReportingRequest = getReportingRequestSync(jsonString, StudentSchema)
    val registry = exampleRegistry
    val res = getRequestModel(request, registry, revision = Some(3))
    assert(res.isSuccess, s"Building request model failed.")

    val queryPipelineTry = generatePipeline(res.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val result = queryPipeline.queryChain.drivingQuery.asString
    //println(result)

    val expected =
      s"""
         |SELECT  *
         |      FROM (
         |          SELECT "Student Name", "Researcher Name", "Class Volunteer Name", "Science Lab Volunteer Name", "Tutor Name", ROWNUM AS ROW_NUMBER
         |              FROM(SELECT sv1.name "Student Name", r3.name "Researcher Name", cv0.name "Class Volunteer Name", slv2.name "Science Lab Volunteer Name", t4.name "Tutor Name"
         |                  FROM
         |               ( (SELECT  class_volunteer_id, researcher_id, name, id
         |            FROM student_v1
         |            WHERE (id = 213)
         |             ) sv1
         |          INNER JOIN
         |            (SELECT  science_lab_volunteer_id, tutor_id, name, id
         |            FROM researcher
         |
         |             ) r3
         |              ON( sv1.researcher_id = r3.id )
         |               INNER JOIN
         |            (SELECT  name, id
         |            FROM tutor
         |
         |             ) t4
         |              ON( r3.tutor_id = t4.id )
         |               INNER JOIN
         |            (SELECT  name, id
         |            FROM science_lab_volunteer
         |
         |             ) slv2
         |              ON( r3.science_lab_volunteer_id = slv2.id )
         |               INNER JOIN
         |            (SELECT  name, id
         |            FROM class_volunteer
         |
         |             ) cv0
         |              ON( sv1.class_volunteer_id = cv0.id )
         |               )
         |
         |                  ))
         |                   WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
         |""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Test: fact table join with 2 same dim level tables should succeed") {
    val jsonString =
      s"""
         |{
         |    "cube": "student_performance",
         |    "selectFields": [
         |        {
         |            "field": "Student Name"
         |        },
         |        {
         |            "field": "Researcher Name"
         |        },
         |        {
         |            "field": "Researcher Status"
         |        },
         |        {
         |            "field": "Total Marks"
         |        }
         |    ],
         |    "filterExpressions": [
         |        {
         |            "field": "Day",
         |            "operator": "between",
         |            "from": "$fromDate",
         |            "to": "$toDate"
         |        },
         |        {
         |            "field": "Student ID",
         |            "operator": "=",
         |            "value": "213"
         |        }
         |    ]
         |}
         |""".stripMargin

    val request: ReportingRequest = ReportingRequest.forceOracle(getReportingRequestSync(jsonString, StudentSchema))
    val registry = exampleRegistry
    val res = getRequestModel(request, registry, revision = Some(3))
    assert(res.isSuccess, s"Building request model failed.")

    val queryPipelineTry = generatePipeline(res.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val result = queryPipeline.queryChain.drivingQuery.asString
    //println(result)

    val expected =
      s"""
         |SELECT *
         |FROM (SELECT sv2.name "Student Name", r1.name "Researcher Name", r1.status "Researcher Status", sp0."total_marks" "Total Marks"
         |      FROM (SELECT
         |                   student_id, researcher_id, SUM(total_marks) AS "total_marks"
         |            FROM student_performance FactAlias
         |            WHERE (date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY student_id, researcher_id
         |
         |           ) sp0
         |           LEFT OUTER JOIN
         |               ( (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  researcher_id, name, id
         |            FROM student_v1
         |            WHERE (id = 213)
         |             ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200) sv2
         |          LEFT OUTER JOIN
         |            (SELECT  name, status, id
         |            FROM researcher
         |
         |             ) r1
         |              ON( sv2.researcher_id = r1.id )
         |               )  ON (sp0.student_id = sv2.id)
         |
         |)
         |""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Test: fact table join with 2 same dim level tables, with filter from the 3rd same dim level table should succeed") {
    val jsonString =
      s"""
         |{
         |    "cube": "student_performance",
         |    "selectFields": [
         |        {
         |            "field": "Student Name"
         |        },
         |        {
         |            "field": "Researcher Name"
         |        },
         |        {
         |            "field": "Researcher Status"
         |        },
         |        {
         |            "field": "Total Marks"
         |        }
         |    ],
         |    "filterExpressions": [
         |        {
         |            "field": "Day",
         |            "operator": "between",
         |            "from": "$fromDate",
         |            "to": "$toDate"
         |        },
         |        {
         |            "field": "Student ID",
         |            "operator": "=",
         |            "value": "213"
         |        },
         |        {
         |            "field": "Tutor Status",
         |            "operator": "=",
         |            "value": "admitted"
         |        }
         |    ]
         |}
         |""".stripMargin

    val request: ReportingRequest = ReportingRequest.forceOracle(getReportingRequestSync(jsonString, StudentSchema))
    val registry = exampleRegistry
    val res = getRequestModel(request, registry, revision = Some(3))
    assert(res.isSuccess, s"Building request model failed.")

    val queryPipelineTry = generatePipeline(res.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val result = queryPipeline.queryChain.drivingQuery.asString
    //println(result)

    val expected =
      s"""
         |SELECT *
         |FROM (SELECT sv3.name "Student Name", r2.name "Researcher Name", r2.status "Researcher Status", sp0."total_marks" "Total Marks"
         |      FROM (SELECT
         |                   tutor_id, student_id, researcher_id, SUM(total_marks) AS "total_marks"
         |            FROM student_performance FactAlias
         |            WHERE (date >= trunc(to_date('$fromDate', 'YYYY-MM-DD')) AND date <= trunc(to_date('$toDate', 'YYYY-MM-DD')))
         |            GROUP BY tutor_id, student_id, researcher_id
         |
         |           ) sp0
         |           INNER JOIN
         |               ( (SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (SELECT  researcher_id, name, id
         |            FROM student_v1
         |            WHERE (id = 213)
         |             ) WHERE ROWNUM <= 200) D ) WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200) sv3
         |          INNER JOIN
         |            (SELECT  tutor_id, name, status, id
         |            FROM researcher
         |
         |             ) r2
         |              ON( sv3.researcher_id = r2.id )
         |               INNER JOIN
         |            (SELECT  id
         |            FROM tutor
         |            WHERE (status = 'admitted')
         |             ) t1
         |              ON( r2.tutor_id = t1.id )
         |               )  ON (sp0.student_id = sv3.id)
         |
         |)
         |""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Test: fact table join with 2 same dim level tables in Hive should succeed") {
    val jsonString =
      s"""
         |{
         |    "cube": "student_performance",
         |    "selectFields": [
         |        {
         |            "field": "Student Name"
         |        },
         |        {
         |            "field": "Researcher Name"
         |        },
         |        {
         |            "field": "Researcher Status"
         |        },
         |        {
         |            "field": "Marks Obtained"
         |        }
         |    ],
         |    "filterExpressions": [
         |        {
         |            "field": "Day",
         |            "operator": "between",
         |            "from": "$fromDate",
         |            "to": "$toDate"
         |        },
         |        {
         |            "field": "Student ID",
         |            "operator": "=",
         |            "value": "213"
         |        },
         |        {
         |            "field": "Tutor Status",
         |            "operator": "=",
         |            "value": "admitted"
         |        }
         |    ]
         |}
         |""".stripMargin

    val request: ReportingRequest = ReportingRequest.forceHive(getReportingRequestAsync(jsonString, StudentSchema))
    val registry = exampleRegistry
    val res = getRequestModel(request, registry, revision = Some(3))
    assert(res.isSuccess, s"Building request model failed.")

    val queryPipelineTry = generatePipeline(res.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val result = queryPipeline.queryChain.drivingQuery.asString
    //println(result)

    val expected =
      s"""
         |SELECT CONCAT_WS(",",NVL(CAST(mang_student_name AS STRING), ''), NVL(CAST(mang_researcher_name AS STRING), ''), NVL(CAST(mang_researcher_status AS STRING), ''), NVL(CAST(mang_marks_obtained AS STRING), ''))
         |FROM(
         |SELECT COALESCE(s3.mang_student_name, "NA") mang_student_name, COALESCE(r2.mang_researcher_name, "NA") mang_researcher_name, COALESCE(r2.mang_researcher_status, "NA") mang_researcher_status, COALESCE(obtained_marks, 0L) mang_marks_obtained
         |FROM(SELECT tutor_id, student_id, researcher_id, SUM(obtained_marks) obtained_marks
         |FROM hive_student_performance
         |WHERE (student_id = 213) AND (date >= '$fromDateHive' AND date <= '$toDateHive')
         |GROUP BY tutor_id, student_id, researcher_id
         |
         |       )
         |hsp0
         |JOIN (
         |SELECT id t1_id
         |FROM hive_tutor
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' )) AND (status = 'admitted')
         |)
         |t1
         |ON
         |hsp0.tutor_id = t1.t1_id
         |       JOIN (
         |SELECT tutor_id AS tutor_id, name AS mang_researcher_name, status AS mang_researcher_status, id r2_id
         |FROM hive_researcher
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ))
         |)
         |r2
         |ON
         |hsp0.researcher_id = r2.r2_id
         |       JOIN (
         |SELECT researcher_id AS researcher_id, name AS mang_student_name, id s3_id
         |FROM hive_student_v1
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' )) AND (id = 213)
         |)
         |s3
         |ON
         |hsp0.student_id = s3.s3_id
         |
         |)
         |        queryAlias LIMIT 200
         |""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Test: fact table join with 2 same dim level tables in Presto should succeed") {
    val jsonString =
      s"""
         |{
         |    "cube": "student_performance",
         |    "selectFields": [
         |        {
         |            "field": "Student Name"
         |        },
         |        {
         |            "field": "Researcher Name"
         |        },
         |        {
         |            "field": "Researcher Status"
         |        },
         |        {
         |            "field": "Marks Obtained"
         |        }
         |    ],
         |    "filterExpressions": [
         |        {
         |            "field": "Day",
         |            "operator": "between",
         |            "from": "$fromDate",
         |            "to": "$toDate"
         |        },
         |        {
         |            "field": "Student ID",
         |            "operator": "=",
         |            "value": "213"
         |        },
         |        {
         |            "field": "Tutor Status",
         |            "operator": "=",
         |            "value": "admitted"
         |        }
         |    ]
         |}
         |""".stripMargin

    val request: ReportingRequest = ReportingRequest.forcePresto(getReportingRequestAsync(jsonString, StudentSchema))
    val registry = exampleRegistry
    val res = getRequestModel(request, registry, revision = Some(3))
    assert(res.isSuccess, s"Building request model failed.")

    val queryPipelineTry = generatePipeline(res.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val result = queryPipeline.queryChain.drivingQuery.asString
    //println(result)

    val expected =
      s"""
         |SELECT CAST(mang_student_name as VARCHAR) AS mang_student_name, CAST(mang_researcher_name as VARCHAR) AS mang_researcher_name, CAST(mang_researcher_status as VARCHAR) AS mang_researcher_status, CAST(mang_marks_obtained as VARCHAR) AS mang_marks_obtained
         |FROM(
         |SELECT COALESCE(CAST(s3.mang_student_name as VARCHAR), 'NA') mang_student_name, COALESCE(CAST(r2.mang_researcher_name as VARCHAR), 'NA') mang_researcher_name, COALESCE(CAST(r2.mang_researcher_status as VARCHAR), 'NA') mang_researcher_status, COALESCE(CAST(obtained_marks as bigint), 0) mang_marks_obtained
         |FROM(SELECT tutor_id, student_id, researcher_id, SUM(obtained_marks) obtained_marks
         |FROM presto_student_performance
         |WHERE (student_id = 213) AND (date >= '$fromDateHive' AND date <= '$toDateHive')
         |GROUP BY tutor_id, student_id, researcher_id
         |
         |       )
         |psp0
         |JOIN (
         |SELECT id t1_id
         |FROM presto_tutor
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' )) AND (status = 'admitted')
         |)
         |t1
         |ON
         |psp0.tutor_id = t1.t1_id
         |       JOIN (
         |SELECT tutor_id AS tutor_id, name AS mang_researcher_name, status AS mang_researcher_status, id r2_id
         |FROM presto_researcher
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ))
         |)
         |r2
         |ON
         |psp0.researcher_id = r2.r2_id
         |       JOIN (
         |SELECT researcher_id AS researcher_id, name AS mang_student_name, id s3_id
         |FROM presto_student_v1
         |WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' )) AND (id = 213)
         |)
         |s3
         |ON
         |psp0.student_id = s3.s3_id
         |
         |
         |          )
         |        queryAlias LIMIT 200
         |""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

  test("Test: fact table join with 2 same dim level tables in Druid should succeed") {
    val jsonString =
      s"""
         |{
         |    "cube": "student_performance",
         |    "selectFields": [
         |        {
         |            "field": "Student Name"
         |        },
         |        {
         |            "field": "Researcher Name"
         |        },
         |        {
         |            "field": "Researcher Status"
         |        },
         |        {
         |            "field": "Marks Obtained"
         |        }
         |    ],
         |    "filterExpressions": [
         |        {
         |            "field": "Day",
         |            "operator": "between",
         |            "from": "$fromDate",
         |            "to": "$toDate"
         |        },
         |        {
         |            "field": "Student ID",
         |            "operator": "=",
         |            "value": "213"
         |        },
         |        {
         |            "field": "Tutor Status",
         |            "operator": "=",
         |            "value": "admitted"
         |        }
         |    ]
         |}
         |""".stripMargin

    val request: ReportingRequest = ReportingRequest.forceDruid(getReportingRequestSync(jsonString, StudentSchema))
    val registry = exampleRegistry
    val res = getRequestModel(request, registry, revision = Some(3))
    assert(res.isSuccess, s"Building request model failed.")

    val queryPipelineTry = generatePipeline(res.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val result = queryPipeline.queryChain.drivingQuery.asString
    //println(result)

    val expected = """\{"queryType":"groupBy","dataSource":\{"type":"table","name":"dr_student_performance"\},"intervals":\{"type":"intervals","intervals":\[".*"\]\},"virtualColumns":\[\],"filter":\{"type":"and","fields":\[\{"type":"or","fields":\[\{"type":"selector","dimension":"date","value":".*"\},\{"type":"selector","dimension":"date","value":".*"\},\{"type":"selector","dimension":"date","value":".*"\},\{"type":"selector","dimension":"date","value":".*"\},\{"type":"selector","dimension":"date","value":".*"\},\{"type":"selector","dimension":"date","value":".*"\},\{"type":"selector","dimension":"date","value":".*"\},\{"type":"selector","dimension":"date","value":".*"\}\]\},\{"type":"selector","dimension":"student_id","value":"213"\}\]\},"granularity":\{"type":"all"\},"dimensions":\[\{"type":"default","dimension":"name","outputName":"Researcher Name","outputType":"STRING"\},\{"type":"default","dimension":"status","outputName":"Researcher Status","outputType":"STRING"\},\{"type":"default","dimension":"name","outputName":"Student Name","outputType":"STRING"\},\{"type":"default","dimension":"status","outputName":"Tutor Status","outputType":"STRING"\}\],"aggregations":\[\{"type":"longSum","name":"Marks Obtained","fieldName":"obtained_marks"\}\],"postAggregations":\[\],"limitSpec":\{"type":"default","columns":\[\],"limit":400\},"context":\{"applyLimitPushDown":"false","uncoveredIntervalsLimit":1,"groupByIsSingleThreaded":true,"timeout":5000,"queryId":".*"\},"descending":false\}""".r
    result should fullyMatch regex expected
  }

  test("Test: Columns from multiple same dimLevel dimensions") {
    val jsonString =
      s"""
         |{
         |    "cube": "student_performance",
         |    "isDimDriven": true,
         |    "selectFields": [
         |        {
         |            "field": "Student Name"
         |        },
         |        {
         |            "field": "Researcher Name"
         |        },
         |        {
         |            "field": "Class Volunteer Name"
         |        },
         |        {
         |            "field": "Science Lab Volunteer Name"
         |        },
         |        {
         |            "field": "Tutor Name"
         |        },
         |        {
         |            "field": "Section Name"
         |        },
         |        {
         |            "field": "Class Name"
         |        }
         |    ],
         |    "filterExpressions": [
         |        {
         |            "field": "Day",
         |            "operator": "between",
         |            "from": "$fromDate",
         |            "to": "$toDate"
         |        },
         |        {
         |            "field": "Student ID",
         |            "operator": "=",
         |            "value": "213"
         |        }
         |    ]
         |}
         |""".stripMargin
    val request: ReportingRequest = ReportingRequest.forceDruid(getReportingRequestSync(jsonString, StudentSchema))
    val registry = exampleRegistry
    val res = getRequestModel(request, registry, revision = Some(3))
    assert(res.isSuccess, s"Building request model failed.")

    val queryPipelineTry = generatePipeline(res.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    val queryPipeline = queryPipelineTry.toOption.get
    val result = queryPipeline.queryChain.drivingQuery.asString
    //println(result)

    val expected = s"""SELECT  *
                      |      FROM (
                      |          SELECT "Student Name", "Researcher Name", "Class Volunteer Name", "Science Lab Volunteer Name", "Tutor Name", "Section Name", "Class Name", ROWNUM AS ROW_NUMBER
                      |              FROM(SELECT sv3.name "Student Name", r5.name "Researcher Name", cv2.name "Class Volunteer Name", slv4.name "Science Lab Volunteer Name", t6.name "Tutor Name", s1.name "Section Name", c0.name "Class Name"
                      |                  FROM
                      |               ( (SELECT  class_id, student_id, name, id
                      |            FROM section
                      |            WHERE (student_id = 213)
                      |             ) s1
                      |          INNER JOIN
                      |            (SELECT  class_volunteer_id, researcher_id, name, id
                      |            FROM student_v1
                      |
                      |             ) sv3
                      |              ON( s1.student_id = sv3.id )
                      |               INNER JOIN
                      |            (SELECT  science_lab_volunteer_id, tutor_id, name, id
                      |            FROM researcher
                      |
                      |             ) r5
                      |              ON( sv3.researcher_id = r5.id )
                      |               INNER JOIN
                      |            (SELECT  name, id
                      |            FROM tutor
                      |
                      |             ) t6
                      |              ON( r5.tutor_id = t6.id )
                      |               INNER JOIN
                      |            (SELECT  name, id
                      |            FROM science_lab_volunteer
                      |
                      |             ) slv4
                      |              ON( r5.science_lab_volunteer_id = slv4.id )
                      |               INNER JOIN
                      |            (SELECT  name, id
                      |            FROM class_volunteer
                      |
                      |             ) cv2
                      |              ON( sv3.class_volunteer_id = cv2.id )
                      |               INNER JOIN
                      |            (SELECT  name, id
                      |            FROM class
                      |
                      |             ) c0
                      |              ON( s1.class_id = c0.id )
                      |               )
                      |
                      |                  ))
                      |                   WHERE ROW_NUMBER >= 1 AND ROW_NUMBER <= 200
                      |""".stripMargin
    result should equal(expected)(after being whiteSpaceNormalised)
  }

}