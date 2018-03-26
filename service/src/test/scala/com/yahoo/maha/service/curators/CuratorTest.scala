// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.curators

import java.util.UUID

import com.yahoo.maha.core.RequestModelResult
import com.yahoo.maha.core.bucketing.{BucketParams, UserInfo}
import com.yahoo.maha.core.ddl.OracleDDLGenerator
import com.yahoo.maha.core.request._
import com.yahoo.maha.jdbc.{JdbcConnection, Seq, _}
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.factory.BaseFactoryTest
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import com.yahoo.maha.service.{DefaultMahaService, MahaServiceConfig, RequestResult}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.Try

class CuratorTest extends BaseFactoryTest {

  private var dataSource: Option[HikariDataSource] = None
  private var jdbcConnection: Option[JdbcConnection] = None

  val h2dbId = UUID.randomUUID().toString.replace("-","")
  val today: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now())
  val yesterday: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(1))

  def initJdbcToH2(): Unit = {
    val config = new HikariConfig()
    config.setJdbcUrl(s"jdbc:h2:mem:$h2dbId;MODE=Oracle;DB_CLOSE_DELAY=-1")
    config.setUsername("sa")
    config.setPassword("h2.test.database.password")
    config.setMaximumPoolSize(1)
    dataSource = Option(new HikariDataSource(config))
    jdbcConnection = dataSource.map(new JdbcConnection(_))
  }

  test("Test Timeshift") {
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

    // Create Tables
    erRegistry.factMap.values.foreach {
      publicFact =>
        publicFact.factList.foreach {
          fact=>
            val ddl = ddlGenerator.toDDL(fact)
            assert(jdbcConnection.get.executeUpdate(ddl).isSuccess)
        }
    }

    val insertSql =
      """
        INSERT INTO student_grade_sheet (year, section_id, student_id, class_id, total_marks, date, comment)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      """

    val rows: List[Seq[Any]] = List(
      Seq(1, 100, 213, 200, 135, DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(9)), "some comment 1"),
      Seq(1, 100, 213, 198, 120, DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(10)), "some comment 2"),
      Seq(1, 500, 213, 197, 190, DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(10)), "some comment 3"),
      Seq(1, 100, 213, 200, 125, today.toString, "some comment 1"),
      Seq(1, 100, 213, 198, 180, yesterday.toString, "some comment 2"),
      Seq(1, 200, 213, 199, 175, today.toString, "some comment 3")
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

    val jsonRequest = s"""{
                          "cube": "student_performance",
                          "curators" : {
                            "timeshift" : {
                              "config" : {
                              }
                            },
                            "default" : {
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

    val bucketParams = BucketParams(UserInfo("uid", true))

    val mahaRequestLogHelper = MahaRequestLogHelper("er", mahaService)

    val defaultCuratorResult: (Try[RequestModelResult], Try[RequestResult]) = new DefaultCurator().process("er", bucketParams, reportingRequest, mahaService, mahaRequestLogHelper,Map.empty, Map.empty)
    assert(defaultCuratorResult._1.isSuccess)
    assert(defaultCuratorResult._2.isSuccess)

    val defaultExpectedSet = Set(
      "Row(Map(Student ID -> 0, Class ID -> 1, Section ID -> 2, Total Marks -> 3),ArrayBuffer(213, 200, 100, 125))",
      "Row(Map(Student ID -> 0, Class ID -> 1, Section ID -> 2, Total Marks -> 3),ArrayBuffer(213, 198, 100, 180))",
      "Row(Map(Student ID -> 0, Class ID -> 1, Section ID -> 2, Total Marks -> 3),ArrayBuffer(213, 199, 200, 175))"
    )

    var defaultCount = 0
    defaultCuratorResult._2.get.rowList.foreach( row => {
      println(row.toString)
      assert(defaultExpectedSet.contains(row.toString))
      defaultCount+=1
    })

    assert(defaultExpectedSet.size == defaultCount)

    val timeShiftProcessResult: (Try[RequestModelResult], Try[RequestResult]) = new TimeShift().process("er", bucketParams, reportingRequest, mahaService, mahaRequestLogHelper, Map(DefaultCurator.name -> defaultCuratorResult), Map.empty)
    assert(timeShiftProcessResult._1.isSuccess)
    assert(timeShiftProcessResult._2.isSuccess)

    val expectedSet = Set(
      "Row(Map(Total Marks Prev -> 4, Section ID -> 2, Total Marks Pct Change -> 5, Student ID -> 0, Total Marks -> 3, Class ID -> 1),ArrayBuffer(213, 200, 100, 125, 135, -7.41))",
      "Row(Map(Total Marks Prev -> 4, Section ID -> 2, Total Marks Pct Change -> 5, Student ID -> 0, Total Marks -> 3, Class ID -> 1),ArrayBuffer(213, 198, 100, 180, 120, 50.0))",
      "Row(Map(Total Marks Prev -> 4, Section ID -> 2, Total Marks Pct Change -> 5, Student ID -> 0, Total Marks -> 3, Class ID -> 1),ArrayBuffer(213, 199, 200, 175, 0, 100.0))"
    )

    var cnt = 0
    timeShiftProcessResult._2.get.rowList.foreach( row => {
      println(row.toString)
      assert(expectedSet.contains(row.toString))
      cnt+=1
    })

    assert(expectedSet.size == cnt)

  }

}
