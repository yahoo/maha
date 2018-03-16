// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.example

import java.util.UUID

import com.yahoo.maha.core.RequestModel
import com.yahoo.maha.core.bucketing.{BucketParams, UserInfo}
import com.yahoo.maha.core.ddl.OracleDDLGenerator
import com.yahoo.maha.core.request._
import com.yahoo.maha.jdbc.JdbcConnection
import com.yahoo.maha.parrequest.GeneralError
import com.yahoo.maha.proto.MahaRequestLog.MahaRequestProto
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.factory.BaseFactoryTest
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import com.yahoo.maha.service.{DefaultMahaService, MahaRequestProcessor, MahaServiceConfig, RequestResult}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

/**
 * Created by pranavbhole on 09/06/17.
 */
class MahaServiceExampleTest extends BaseFactoryTest {

  private var dataSource: Option[HikariDataSource] = None
  private var jdbcConnection: Option[JdbcConnection] = None

  val h2dbId = UUID.randomUUID().toString.replace("-","")

  def initJdbcToH2(): Unit = {
    val config = new HikariConfig()
    config.setJdbcUrl(s"jdbc:h2:mem:$h2dbId;MODE=Oracle;DB_CLOSE_DELAY=-1")
    config.setUsername("sa")
    config.setPassword("h2.test.database.password")
    config.setMaximumPoolSize(1)
    dataSource = Option(new HikariDataSource(config))
    jdbcConnection = dataSource.map(new JdbcConnection(_))
  }

  test("Test MahaService with Example Schema") {
    val jsonString = s"""
                       |{
                       |   "registryMap": {
                       |      "er": {
                       |         "factRegistrationClass": "com.yahoo.maha.service.example.SampleFactSchemaRegistrationFactory",
                       |         "dimensionRegistrationClass": "com.yahoo.maha.service.example.SampleDimensionSchemaRegistrationFactory",
                       |         "executors": [
                       |            "oracleExec",
                       |            "druidExec"
                       |         ],
                       |         "generators": [
                       |            "oracle",
                       |            "druid"
                       |         ],
                       |         "bucketingConfigName": "erBucket",
                       |         "utcTimeProviderName": "erUTC",
                       |         "parallelServiceExecutorName": "erParallelExec",
                       |         "dimEstimatorFactoryClass": "com.yahoo.maha.service.factory.DefaultDimCostEstimatorFactory",
                       |         "dimEstimatorFactoryConfig": "",
                       |         "factEstimatorFactoryClass": "com.yahoo.maha.service.factory.DefaultFactCostEstimatorFactory",
                       |         "factEstimatorFactoryConfig": "",
                       |         "defaultPublicFactRevisionMap": {
                       |            "student_performance": 0
                       |         },
                       |         "defaultPublicDimRevisionMap": {
                       |            "student": 0
                       |         }
                       |      }
                       |   },
                       |   "executorMap": {
                       |      "oracleExec": {
                       |         "factoryClass": "com.yahoo.maha.service.factory.OracleQueryExecutoryFactory",
                       |         "config": {
                       |            "dataSourceFactoryClass": "com.yahoo.maha.service.factory.HikariDataSourceFactory",
                       |            "dataSourceFactoryConfig": {
                       |               "driverClassName": "org.h2.Driver",
                       |               "jdbcUrl": "jdbc:h2:mem:$h2dbId;MODE=Oracle;DB_CLOSE_DELAY=-1",
                       |               "username": "sa",
                       |               "passwordProviderFactoryClassName": "com.yahoo.maha.service.factory.PassThroughPasswordProviderFactory",
                       |               "passwordProviderConfig": [
                       |                  {
                       |                     "key": "value"
                       |                  }
                       |               ],
                       |               "passwordKey": "h2.test.database.password",
                       |               "poolName": "test-pool",
                       |               "maximumPoolSize": 10,
                       |               "minimumIdle": 1,
                       |               "autoCommit": true,
                       |               "connectionTestQuery": "SELECT 1 FROM DUAL",
                       |               "validationTimeout": 1000000,
                       |               "idleTimeout": 1000000,
                       |               "maxLifetime": 10000000,
                       |               "dataSourceProperties": [
                       |                  {
                       |                     "key": "propertyKey",
                       |                     "value": "propertyValue"
                       |                  }
                       |               ]
                       |            },
                       |            "jdbcConnectionFetchSize": 10,
                       |            "lifecycleListenerFactoryClass": "com.yahoo.maha.service.factory.NoopExecutionLifecycleListenerFactory",
                       |            "lifecycleListenerFactoryConfig": [
                       |               {
                       |                  "key": "value"
                       |               }
                       |            ]
                       |         }
                       |      },
                       |      "druidExec": {
                       |         "factoryClass": "com.yahoo.maha.service.factory.DruidQueryExecutoryFactory",
                       |         "config": {
                       |            "druidQueryExecutorConfigFactoryClassName": "com.yahoo.maha.service.factory.DefaultDruidQueryExecutorConfigFactory",
                       |            "druidQueryExecutorConfigJsonConfig": {
                       |               "maxConnectionsPerHost": 100,
                       |               "maxConnections": 10000,
                       |               "connectionTimeout": 140000,
                       |               "timeoutRetryInterval": 100,
                       |               "timeoutThreshold": 9000,
                       |               "degradationConfigName": "TestConfig",
                       |               "url": "http://broker.druid.test.maha.com",
                       |               "headers": {
                       |                  "key": "value"
                       |               },
                       |               "readTimeout": 10000,
                       |               "requestTimeout": 10000,
                       |               "pooledConnectionIdleTimeout": 10000,
                       |               "timeoutMaxResponseTimeInMs": 30000,
                       |               "enableRetryOn500": true,
                       |               "retryDelayMillis": 1000,
                       |               "maxRetry": 3,
                       |               "enableFallbackOnUncoveredIntervals" : true
                       |            },
                       |            "lifecycleListenerFactoryClass": "com.yahoo.maha.service.factory.NoopExecutionLifecycleListenerFactory",
                       |            "lifecycleListenerFactoryConfig": [
                       |               {
                       |                  "key": "value"
                       |               }
                       |            ],
                       |            "resultSetTransformersFactoryClassName": "com.yahoo.maha.service.factory.DefaultResultSetTransformersFactory",
                       |            "resultSetTransformersFactoryConfig": [
                       |               {
                       |                  "key": "value"
                       |               }
                       |            ]
                       |         }
                       |      }
                       |   },
                       |   "generatorMap": {
                       |      "oracle": {
                       |         "factoryClass": "com.yahoo.maha.service.factory.OracleQueryGeneratorFactory",
                       |         "config": {
                       |            "partitionColumnRendererClass": "com.yahoo.maha.service.factory.DefaultPartitionColumnRendererFactory",
                       |            "partitionColumnRendererConfig": [
                       |               {
                       |                  "key": "value"
                       |               }
                       |            ],
                       |            "literalMapperClass": "com.yahoo.maha.service.factory.DefaultOracleLiteralMapperFactory",
                       |            "literalMapperConfig": [
                       |               {
                       |                  "key": "value"
                       |               }
                       |            ]
                       |         }
                       |      },
                       |      "druid": {
                       |         "factoryClass": "com.yahoo.maha.service.factory.DruidQueryGeneratorFactory",
                       |         "config": {
                       |            "queryOptimizerClass": "com.yahoo.maha.service.factory.DefaultDruidQueryOptimizerFactory",
                       |            "queryOptimizerConfig": [
                       |               {
                       |                  "key": "value"
                       |               }
                       |            ],
                       |            "dimCardinality": 40000,
                       |            "maximumMaxRows": 5000,
                       |            "maximumTopNMaxRows": 400,
                       |            "maximumMaxRowsAsync": 100000
                       |         }
                       |      }
                       |   },
                       |   "bucketingConfigMap": {
                       |      "erBucket": {
                       |         "factoryClass": "com.yahoo.maha.service.factory.DefaultBucketingConfigFactory",
                       |         "config": [{
                       |	  "cube": "student_performance",
                       |		"internal": [{
                       |			"revision": 0,
                       |      "percent": 10
                       |		}, {
                       |      "revision": 1,
                       |      "percent": 90
                       |    }],
                       |		"external": [{
                       |			"revision": 0,
                       |      "percent": 90
                       |		}, {
                       |      "revision": 1,
                       |      "percent": 10
                       |		}],
                       |    "dryRun": [{
                       |			"revision": 0,
                       |      "percent": 10,
                       |      "engine" : "Oracle"
                       |		}, {
                       |      "revision": 1,
                       |      "percent": 10
                       |    }],
                       |    "userWhiteList": [{
                       |      "user" : "uid",
                       |      "revision": 0
                       |    }]
                       |}]
                       |      },
                       |      "irBucket": {
                       |         "factoryClass": "com.yahoo.maha.service.factory.DefaultBucketingConfigFactory",
                       |         "config": [{
                       |	  "cube": "student_performance",
                       |		"internal": [{
                       |			"revision": 0,
                       |      "percent": 10
                       |		}, {
                       |      "revision": 1,
                       |      "percent": 90
                       |    }],
                       |		"external": [{
                       |			"revision": 0,
                       |      "percent": 90
                       |		}, {
                       |      "revision": 1,
                       |      "percent": 10
                       |		}],
                       |    "dryRun": [{
                       |			"revision": 0,
                       |      "percent": 100,
                       |      "engine" : "Oracle"
                       |		}, {
                       |      "revision": 1,
                       |      "percent": 10
                       |    }],
                       |    "userWhiteList": [{
                       |      "user" : "uid",
                       |      "revision": 0
                       |    }]
                       |}]
                       |      }
                       |   },
                       |   "utcTimeProviderMap": {
                       |      "erUTC": {
                       |         "factoryClass": "com.yahoo.maha.service.factory.PassThroughUTCTimeProviderFactory",
                       |         "config": {
                       |            "k": "v"
                       |         }
                       |      },
                       |      "irUTC": {
                       |         "factoryClass": "com.yahoo.maha.service.factory.PassThroughUTCTimeProviderFactory",
                       |         "config": {
                       |            "k": "v"
                       |         }
                       |      }
                       |   },
                       |   "parallelServiceExecutorConfigMap": {
                       |      "erParallelExec": {
                       |         "factoryClass": "com.yahoo.maha.service.factory.DefaultParallelServiceExecutoryFactory",
                       |         "config": {
                       |            "rejectedExecutionHandlerClass": "com.yahoo.maha.service.factory.DefaultRejectedExecutionHandlerFactory",
                       |            "rejectedExecutionHandlerConfig": "",
                       |            "poolName": "maha-test-pool",
                       |            "defaultTimeoutMillis": 10000,
                       |            "threadPoolSize": 3,
                       |            "queueSize": 3
                       |         }
                       |      },
                       |      "irParallelExec": {
                       |         "factoryClass": "com.yahoo.maha.service.factory.DefaultParallelServiceExecutoryFactory",
                       |         "config": {
                       |            "rejectedExecutionHandlerClass": "com.yahoo.maha.service.factory.DefaultRejectedExecutionHandlerFactory",
                       |            "rejectedExecutionHandlerConfig": "",
                       |            "poolName": "maha-test-pool",
                       |            "defaultTimeoutMillis": 10000,
                       |            "threadPoolSize": 3,
                       |            "queueSize": 3
                       |         }
                       |      }
                       |   },
                       |   "mahaRequestLoggingConfig" : {
                       |    "factoryClass": "com.yahoo.maha.service.factory.KafkaMahaRequestLogWriterFactory",
                       |    "config" : {
                       |      "kafkaBrokerList" : "",
                       |      "bootstrapServers" : "",
                       |      "producerType" : "",
                       |      "serializerClass" : "" ,
                       |      "requestRequiredAcks" : "",
                       |      "kafkaBlockOnBufferFull" : "",
                       |      "batchNumMessages" : "" ,
                       |      "topicName" : "",
                       |      "bufferMemory" : "",
                       |      "maxBlockMs" : ""
                       |    },
                       |    "isLoggingEnabled" : false
                       |   }
                       |}
                       |
                       |	""".stripMargin

    initJdbcToH2()
    val mahaServiceResult = MahaServiceConfig.fromJson(jsonString.getBytes("utf-8"))
    assert(mahaServiceResult.isSuccess)
    val mahaServiceConfig = mahaServiceResult.toOption.get
    assert(mahaServiceConfig.registry.get("er").isDefined)
    val erRegistryConfig = mahaServiceConfig.registry.get("er").get
    val erRegistry= erRegistryConfig.registry
    assert(erRegistry.isCubeDefined("student_performance"))
    assert(erRegistry.getDimension("student").isDefined)
    val mahaService  = new DefaultMahaService(mahaServiceConfig)

    val ddlGenerator = new OracleDDLGenerator
    assert(jdbcConnection.isDefined)

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

    val mahaRequestLogHelper = MahaRequestLogHelper("er", mahaService)

    val requestModelResultTry  = mahaService.generateRequestModel("er", reportingRequest, bucketParams, mahaRequestLogHelper)
    assert(requestModelResultTry.isSuccess)

    // Test General Error in Execute Model Test
    val resultFailure = mahaService.executeRequestModelResult("er", requestModelResultTry.get, mahaRequestLogHelper).prodRun.get(10000)
    assert(resultFailure.isLeft)
    resultFailure.mapLeft(
      (t: GeneralError)=>
        assert(t.message.contains(s"""Table "STUDENT_GRADE_SHEET" not found; SQL statement"""))
    )

    // Test General Error in execute request
    val parRequestResultWithError = mahaService.executeRequest("er", reportingRequest, bucketParams, mahaRequestLogHelper)
    parRequestResultWithError.prodRun.resultMap(
      (t: RequestResult)
      => t
    ).mapLeft(
      (t: GeneralError)=>
        assert(t.message.contains(s"""Table "STUDENT_GRADE_SHEET" not found; SQL statement"""))
    )

    // Test General Error in process Model
    val resultFailureToProcessModel = mahaService.processRequest("er", reportingRequest, bucketParams, mahaRequestLogHelper)
    assert(resultFailureToProcessModel.isFailure)

    //Test General Error in process Request Model
    val processRequestModelWithFailure = mahaService.processRequestModel("er", requestModelResultTry.get.model, mahaRequestLogHelper)
    assert(processRequestModelWithFailure.isFailure)


    // Create Tables
    erRegistry.factMap.values.foreach {
      publicFact =>
        publicFact.factList.foreach {
          fact=>
            val ddl = ddlGenerator.toDDL(fact)
            assert(jdbcConnection.get.executeUpdate(ddl).isSuccess)
        }
    }

    // Execute Model Test
    val result = mahaService.executeRequestModelResult("er", requestModelResultTry.get, mahaRequestLogHelper).prodRun.get(10000)
    assert(result.isRight)
    assert(result.right.get.rowList.columnNames.contains("Student ID"))

    // Process Model Test
    val processRequestModelResult  = mahaService.processRequestModel("er", requestModelResultTry.get.model, mahaRequestLogHelper)
    assert(processRequestModelResult.isSuccess)
    assert(processRequestModelResult.get.rowList.columnNames.contains("Class ID"))

    // Process Request Test
    val processRequestResult = mahaService.processRequest("er", reportingRequest, bucketParams, mahaRequestLogHelper)
    assert(processRequestResult.isSuccess)
    assert(processRequestResult.get.rowList.columnNames.contains("Class ID"))

    //ExecuteRequest Test
    val executeRequestParRequestResult = mahaService.executeRequest("er", reportingRequest, bucketParams, mahaRequestLogHelper)
    assert(executeRequestParRequestResult.prodRun.get(10000).isRight)
    val requestResultOption = executeRequestParRequestResult.prodRun.get(10000).toOption
    assert(requestResultOption.get.rowList.columnNames.contains("Total Marks"))

    // Domain Tests
    val domainJsonOption = mahaService.getDomain("er")
    assert(domainJsonOption.isDefined)
    assert(domainJsonOption.get.contains("""{"dimensions":[{"name":"student","fields":["Student ID","Student Name","Student Status"]}],"schemas":{"student":["student_performance"]},"cubes":[{"name":"student_performance","mainEntityIds":{"student":"Student ID"},"maxDaysLookBack":[{"requestType":"SyncRequest","grain":"DailyGrain","days":30},{"requestType":"AsyncRequest","grain":"DailyGrain","days":30}],"maxDaysWindow":[{"requestType":"SyncRequest","grain":"DailyGrain","days":20},{"requestType":"AsyncRequest","grain":"DailyGrain","days":20},{"requestType":"SyncRequest","grain":"HourlyGrain","days":20},{"requestType":"AsyncRequest","grain":"HourlyGrain","days":20}],"fields":[{"field":"Class ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null},{"field":"Day","type":"Dimension","dataType":{"type":"Date","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null},{"field":"Remarks","type":"Dimension","dataType":{"type":"String","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","=","LIKE"],"required":false,"filteringRequired":false,"incompatibleColumns":null},{"field":"Section ID","type":"Dimension","dataType":{"type":"Number","constraint":"3"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null},{"field":"Student ID","type":"Dimension","dataType":{"type":"Number","constraint":null},"dimensionName":"student","filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null},{"field":"Year","type":"Dimension","dataType":{"type":"Enum","constraint":"Freshman|Junior|Sophomore|Senior"},"dimensionName":null,"filterable":true,"filterOperations":["="],"required":false,"filteringRequired":false,"incompatibleColumns":null},{"field":"Marks Obtained","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null},{"field":"Performance Factor","type":"Fact","dataType":{"type":"Number","constraint":"10"},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null},{"field":"Total Marks","type":"Fact","dataType":{"type":"Number","constraint":null},"dimensionName":null,"filterable":true,"filterOperations":["IN","BETWEEN","="],"required":false,"filteringRequired":false,"rollupExpression":"SumRollup","incompatibleColumns":null}]}]}"""))
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

    // test MahaRequestProcessor
    val mahaRequestProcessor : MahaRequestProcessor = MahaRequestProcessor("er", bucketParams, reportingRequest, mahaService, jsonRequest.getBytes)

    def fn = {
      (requestModel: RequestModel, requestResult: RequestResult) => {
        assert(requestResult.rowList.columnNames.contains("Total Marks"))
        println("Inside onSuccess function")
      }
    }

    mahaRequestProcessor.onSuccess(fn)
    mahaRequestProcessor.onFailure((error: GeneralError) => println(error.message))
    val protoBuilder: MahaRequestProto.Builder = mahaRequestProcessor.process()
    assert(protoBuilder.getDrivingTable == "student_grade_sheet")
    assert(protoBuilder.getStatus == 200)
    assert(protoBuilder.getRequestEndTime > System.currentTimeMillis() - 30000)

    val thrown = intercept[IllegalArgumentException] {
      val failedProcessor = MahaRequestProcessor("er", bucketParams, reportingRequest, mahaService, jsonRequest.getBytes)
      failedProcessor.process()
    }
  }

  test("Test MahaService with Example Schema generating valid Dim Candidates") {
    val jsonString = s"""
                        |{
                        |   "registryMap": {
                        |      "er": {
                        |         "factRegistrationClass": "com.yahoo.maha.service.example.SampleFactSchemaRegistrationFactory",
                        |         "dimensionRegistrationClass": "com.yahoo.maha.service.example.SampleDimensionSchemaRegistrationFactory",
                        |         "executors": [
                        |            "oracleExec",
                        |            "druidExec"
                        |         ],
                        |         "generators": [
                        |            "oracle",
                        |            "druid"
                        |         ],
                        |         "bucketingConfigName": "erBucket",
                        |         "utcTimeProviderName": "erUTC",
                        |         "parallelServiceExecutorName": "erParallelExec",
                        |         "dimEstimatorFactoryClass": "com.yahoo.maha.service.factory.DefaultDimCostEstimatorFactory",
                        |         "dimEstimatorFactoryConfig": "",
                        |         "factEstimatorFactoryClass": "com.yahoo.maha.service.factory.DefaultFactCostEstimatorFactory",
                        |         "factEstimatorFactoryConfig": "",
                        |         "defaultPublicFactRevisionMap": {
                        |            "student_performance": 0
                        |         },
                        |         "defaultPublicDimRevisionMap": {
                        |            "student": 0
                        |         }
                        |      }
                        |   },
                        |   "executorMap": {
                        |      "oracleExec": {
                        |         "factoryClass": "com.yahoo.maha.service.factory.OracleQueryExecutoryFactory",
                        |         "config": {
                        |            "dataSourceFactoryClass": "com.yahoo.maha.service.factory.HikariDataSourceFactory",
                        |            "dataSourceFactoryConfig": {
                        |               "driverClassName": "org.h2.Driver",
                        |               "jdbcUrl": "jdbc:h2:mem:$h2dbId;MODE=Oracle;DB_CLOSE_DELAY=-1",
                        |               "username": "sa",
                        |               "passwordProviderFactoryClassName": "com.yahoo.maha.service.factory.PassThroughPasswordProviderFactory",
                        |               "passwordProviderConfig": [
                        |                  {
                        |                     "key": "value"
                        |                  }
                        |               ],
                        |               "passwordKey": "h2.test.database.password",
                        |               "poolName": "test-pool",
                        |               "maximumPoolSize": 10,
                        |               "minimumIdle": 1,
                        |               "autoCommit": true,
                        |               "connectionTestQuery": "SELECT 1 FROM DUAL",
                        |               "validationTimeout": 1000000,
                        |               "idleTimeout": 1000000,
                        |               "maxLifetime": 10000000,
                        |               "dataSourceProperties": [
                        |                  {
                        |                     "key": "propertyKey",
                        |                     "value": "propertyValue"
                        |                  }
                        |               ]
                        |            },
                        |            "jdbcConnectionFetchSize": 10,
                        |            "lifecycleListenerFactoryClass": "com.yahoo.maha.service.factory.NoopExecutionLifecycleListenerFactory",
                        |            "lifecycleListenerFactoryConfig": [
                        |               {
                        |                  "key": "value"
                        |               }
                        |            ]
                        |         }
                        |      },
                        |      "druidExec": {
                        |         "factoryClass": "com.yahoo.maha.service.factory.DruidQueryExecutoryFactory",
                        |         "config": {
                        |            "druidQueryExecutorConfigFactoryClassName": "com.yahoo.maha.service.factory.DefaultDruidQueryExecutorConfigFactory",
                        |            "druidQueryExecutorConfigJsonConfig": {
                        |               "maxConnectionsPerHost": 100,
                        |               "maxConnections": 10000,
                        |               "connectionTimeout": 140000,
                        |               "timeoutRetryInterval": 100,
                        |               "timeoutThreshold": 9000,
                        |               "degradationConfigName": "TestConfig",
                        |               "url": "http://broker.druid.test.maha.com",
                        |               "headers": {
                        |                  "key": "value"
                        |               },
                        |               "readTimeout": 10000,
                        |               "requestTimeout": 10000,
                        |               "pooledConnectionIdleTimeout": 10000,
                        |               "timeoutMaxResponseTimeInMs": 30000,
                        |               "enableRetryOn500": true,
                        |               "retryDelayMillis": 1000,
                        |               "maxRetry": 3,
                        |               "enableFallbackOnUncoveredIntervals" : true
                        |            },
                        |            "lifecycleListenerFactoryClass": "com.yahoo.maha.service.factory.NoopExecutionLifecycleListenerFactory",
                        |            "lifecycleListenerFactoryConfig": [
                        |               {
                        |                  "key": "value"
                        |               }
                        |            ],
                        |            "resultSetTransformersFactoryClassName": "com.yahoo.maha.service.factory.DefaultResultSetTransformersFactory",
                        |            "resultSetTransformersFactoryConfig": [
                        |               {
                        |                  "key": "value"
                        |               }
                        |            ]
                        |         }
                        |      }
                        |   },
                        |   "generatorMap": {
                        |      "oracle": {
                        |         "factoryClass": "com.yahoo.maha.service.factory.OracleQueryGeneratorFactory",
                        |         "config": {
                        |            "partitionColumnRendererClass": "com.yahoo.maha.service.factory.DefaultPartitionColumnRendererFactory",
                        |            "partitionColumnRendererConfig": [
                        |               {
                        |                  "key": "value"
                        |               }
                        |            ],
                        |            "literalMapperClass": "com.yahoo.maha.service.factory.DefaultOracleLiteralMapperFactory",
                        |            "literalMapperConfig": [
                        |               {
                        |                  "key": "value"
                        |               }
                        |            ]
                        |         }
                        |      },
                        |      "druid": {
                        |         "factoryClass": "com.yahoo.maha.service.factory.DruidQueryGeneratorFactory",
                        |         "config": {
                        |            "queryOptimizerClass": "com.yahoo.maha.service.factory.DefaultDruidQueryOptimizerFactory",
                        |            "queryOptimizerConfig": [
                        |               {
                        |                  "key": "value"
                        |               }
                        |            ],
                        |            "dimCardinality": 40000,
                        |            "maximumMaxRows": 5000,
                        |            "maximumTopNMaxRows": 400,
                        |            "maximumMaxRowsAsync": 100000
                        |         }
                        |      }
                        |   },
                        |   "bucketingConfigMap": {
                        |      "erBucket": {
                        |         "factoryClass": "com.yahoo.maha.service.factory.DefaultBucketingConfigFactory",
                        |         "config": [{
                        |	  "cube": "student_performance",
                        |		"internal": [{
                        |			"revision": 0,
                        |      "percent": 10
                        |		}, {
                        |      "revision": 1,
                        |      "percent": 90
                        |    }],
                        |		"external": [{
                        |			"revision": 0,
                        |      "percent": 90
                        |		}, {
                        |      "revision": 1,
                        |      "percent": 10
                        |		}],
                        |    "dryRun": [{
                        |			"revision": 0,
                        |      "percent": 10,
                        |      "engine" : "Oracle"
                        |		}, {
                        |      "revision": 1,
                        |      "percent": 10
                        |    }],
                        |    "userWhiteList": [{
                        |      "user" : "uid",
                        |      "revision": 0
                        |    }]
                        |}]
                        |      },
                        |      "irBucket": {
                        |         "factoryClass": "com.yahoo.maha.service.factory.DefaultBucketingConfigFactory",
                        |         "config": [{
                        |	  "cube": "student_performance",
                        |		"internal": [{
                        |			"revision": 0,
                        |      "percent": 10
                        |		}, {
                        |      "revision": 1,
                        |      "percent": 90
                        |    }],
                        |		"external": [{
                        |			"revision": 0,
                        |      "percent": 90
                        |		}, {
                        |      "revision": 1,
                        |      "percent": 10
                        |		}],
                        |    "dryRun": [{
                        |			"revision": 0,
                        |      "percent": 100,
                        |      "engine" : "Oracle"
                        |		}, {
                        |      "revision": 1,
                        |      "percent": 10
                        |    }],
                        |    "userWhiteList": [{
                        |      "user" : "uid",
                        |      "revision": 0
                        |    }]
                        |}]
                        |      }
                        |   },
                        |   "utcTimeProviderMap": {
                        |      "erUTC": {
                        |         "factoryClass": "com.yahoo.maha.service.factory.PassThroughUTCTimeProviderFactory",
                        |         "config": {
                        |            "k": "v"
                        |         }
                        |      },
                        |      "irUTC": {
                        |         "factoryClass": "com.yahoo.maha.service.factory.PassThroughUTCTimeProviderFactory",
                        |         "config": {
                        |            "k": "v"
                        |         }
                        |      }
                        |   },
                        |   "parallelServiceExecutorConfigMap": {
                        |      "erParallelExec": {
                        |         "factoryClass": "com.yahoo.maha.service.factory.DefaultParallelServiceExecutoryFactory",
                        |         "config": {
                        |            "rejectedExecutionHandlerClass": "com.yahoo.maha.service.factory.DefaultRejectedExecutionHandlerFactory",
                        |            "rejectedExecutionHandlerConfig": "",
                        |            "poolName": "maha-test-pool",
                        |            "defaultTimeoutMillis": 10000,
                        |            "threadPoolSize": 3,
                        |            "queueSize": 3
                        |         }
                        |      },
                        |      "irParallelExec": {
                        |         "factoryClass": "com.yahoo.maha.service.factory.DefaultParallelServiceExecutoryFactory",
                        |         "config": {
                        |            "rejectedExecutionHandlerClass": "com.yahoo.maha.service.factory.DefaultRejectedExecutionHandlerFactory",
                        |            "rejectedExecutionHandlerConfig": "",
                        |            "poolName": "maha-test-pool",
                        |            "defaultTimeoutMillis": 10000,
                        |            "threadPoolSize": 3,
                        |            "queueSize": 3
                        |         }
                        |      }
                        |   },
                        |   "mahaRequestLoggingConfig" : {
                        |    "factoryClass": "com.yahoo.maha.service.factory.KafkaMahaRequestLogWriterFactory",
                        |    "config" : {
                        |      "kafkaBrokerList" : "",
                        |      "bootstrapServers" : "",
                        |      "producerType" : "",
                        |      "serializerClass" : "" ,
                        |      "requestRequiredAcks" : "",
                        |      "kafkaBlockOnBufferFull" : "",
                        |      "batchNumMessages" : "" ,
                        |      "topicName" : "",
                        |      "bufferMemory" : "",
                        |      "maxBlockMs" : ""
                        |    },
                        |    "isLoggingEnabled" : false
                        |   }
                        |}
                        |
                       |	""".stripMargin

    initJdbcToH2()
    val mahaServiceResult = MahaServiceConfig.fromJson(jsonString.getBytes("utf-8"))
    assert(mahaServiceResult.isSuccess)
    val mahaServiceConfig = mahaServiceResult.toOption.get
    assert(mahaServiceConfig.registry.get("er").isDefined)
    val erRegistryConfig = mahaServiceConfig.registry.get("er").get
    val erRegistry= erRegistryConfig.registry
    assert(erRegistry.isCubeDefined("student_performance"))
    assert(erRegistry.getDimension("student").isDefined)
    val mahaService  = new DefaultMahaService(mahaServiceConfig)

    val ddlGenerator = new OracleDDLGenerator
    assert(jdbcConnection.isDefined)

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

    val mahaRequestLogHelper = MahaRequestLogHelper("er", mahaService)

    // Start Logging with App Logger
    mahaService.mahaServiceAppLogger.start(reportingRequest)

    val requestModelResultTry  = mahaService.generateRequestModel("er", reportingRequest, bucketParams, mahaRequestLogHelper)
    val eitherResult = mahaService.rmResultPostProcessor.process(requestModelResultTry, mahaRequestLogHelper)

    assert(eitherResult.isRight)

    // Create Tables
    erRegistry.dimMap.values.foreach {
      publicFact =>
        publicFact.dimList.foreach {
          dim=>
            val ddl = ddlGenerator.toDDL(dim)
            assert(jdbcConnection.get.executeUpdate(ddl).isSuccess)
        }
    }

    val requestResultTry = mahaService.processRequest("er", reportingRequest, bucketParams, mahaRequestLogHelper)
    val eitherRequestResult = mahaService.requestResultPostProcessor.process(requestResultTry, mahaRequestLogHelper)
    assert(eitherRequestResult.isLeft, "Request should fail with invalid SQL syntax.")

    // Stop Logging with App Logger
    mahaService.mahaServiceAppLogger.stop
  }
}
