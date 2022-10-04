// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import cats.effect.IO
import com.netflix.archaius.config.DefaultSettableConfig
import com.yahoo.maha.core.CoreSchema.AdvertiserSchema
import com.yahoo.maha.core.bucketing.BucketParams
import com.yahoo.maha.core.query.druid.{DruidQueryGenerator, SyncDruidQueryOptimizer}
import com.yahoo.maha.core.query.{DefaultQueryPipelineFactory, NoopExecutionLifecycleListener, QueryAttributes, QueryExecutorContext, QueryGeneratorRegistry, ResultSetTransformer}
import com.yahoo.maha.core.{DailyGrain, DruidEngine, NoopUserTimeZoneProvider, OracleEngine, PassThroughUTCTimeProvider, RequestModel, Schema, UTCTimeProvider, UserTimeZoneProvider}
import com.yahoo.maha.core.registry.Registry
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.executor.druid.{DruidQueryExecutor, DruidQueryExecutorConfig}
import com.yahoo.maha.service.config.dynamic.DynamicConfigurations
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.service.{DefaultMahaService, DynamicMahaServiceConfig, MahaServiceConfig}
import org.http4s.server.blaze.BlazeBuilder
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalaz.{Failure, Success}

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.util.Try

/**
 * Created by pranavbhole on 06/06/17.
 */
class MahaServiceQueryExecutionTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with TestWebService {

  var server: org.http4s.server.Server[IO] = null
  protected[this] val fromDate = DailyGrain.toFormattedString(DateTime.now(DateTimeZone.UTC).minusDays(7))
  protected[this] val toDate = DailyGrain.toFormattedString(DateTime.now(DateTimeZone.UTC))

  override def beforeAll(): Unit = {
    super.beforeAll()
    val builder = BlazeBuilder[IO].mountService(service, "/mock").bindSocketAddress(new InetSocketAddress("localhost", 6667))
    server = builder.start.unsafeRunSync()
    info("Started blaze server")
  }

  override def afterAll {
    info("Stopping blaze server")
    server.shutdown
  }

  def getReportingRequestSync(jsonString: String, schema: Schema = StudentSchema) = {
    val result = ReportingRequest.deserializeSync(jsonString.getBytes(StandardCharsets.UTF_8), schema)
    require(result.isSuccess, result)
    result.toOption.get
  }

  def getReportingRequestAsync(jsonString: String, schema: Schema = StudentSchema) = {
    val result = ReportingRequest.deserializeAsync(jsonString.getBytes(StandardCharsets.UTF_8), schema)
    require(result.isSuccess, result)
    result.toOption.get
  }

  def getRequestModel(request: ReportingRequest
                      , registry: Registry
                      //I've made userTimeZoneProvider required so on upgrade, users are forced to provide it
                      //instead of a silent failure with using a Noop provider
                      , userTimeZoneProvider: UserTimeZoneProvider = NoopUserTimeZoneProvider
                      , utcTimeProvider: UTCTimeProvider = PassThroughUTCTimeProvider
                      , revision: Option[Int] = None
                     ): Try[RequestModel] = {
    RequestModel.from(request, registry, userTimeZoneProvider, utcTimeProvider, revision)
  }

  private[this] def withDruidQueryExecutor(url: String,
                                           enableFallbackOnUncoveredIntervals: Boolean = false,
                                           allowPartialIfResultExceedsMaxRowLimit: Boolean = false)(fn: DruidQueryExecutor => Unit): Unit = {
    val executor = new DruidQueryExecutor(new DruidQueryExecutorConfig(50, 500, 5000, 5000, 5000, "config", url, None, 30000, 30000, 30000, 30000,
      true, 500, 3, enableFallbackOnUncoveredIntervals, allowPartialIfResultExceedsMaxRowLimit = allowPartialIfResultExceedsMaxRowLimit), new NoopExecutionLifecycleListener, ResultSetTransformer.DEFAULT_TRANSFORMS)
    try {
      fn(executor)
    } finally {
      executor.close()
    }
  }

  private[this] def getDruidQueryGenerator(maximumMaxRowsAsync: Int = 100): DruidQueryGenerator = {
    new DruidQueryGenerator(new SyncDruidQueryOptimizer(), 40000, maximumMaxRowsAsync = maximumMaxRowsAsync)
  }

  test("Generate a Druid query on the new URI by config") {
    //create & verify dynamic service
    val dynamicConfig = new DefaultSettableConfig()
    dynamicConfig.setProperty("student_performance.external.rev0.percent", 10)
    dynamicConfig.setProperty("student_performance.external.rev1.percent", 90)
    val dynamicConfigurations = new DynamicConfigurations(dynamicConfig)

    val dynamicServiceConfig = DynamicMahaServiceConfig.fromJson(dynamicConfigJson.getBytes(StandardCharsets.UTF_8), dynamicConfigurations)
    assert(dynamicServiceConfig.isSuccess, s"Failed to create dynamic config: $dynamicServiceConfig")

    //assert on alternative URI settings & bucketing
    val cubeUriSettings = dynamicServiceConfig.toOption.get.registry("er").bucketSelector.bucketingConfig.getConfigForCube("student_performance").get.uriPercentage
    assert(cubeUriSettings("http://localhost:6667/mock/alternative_endpoint") == 10)
    assert(cubeUriSettings("http://localhost:6667/mock/alternative_endpoint_2") == 90)

    val cubeDruidUriSettings = dynamicServiceConfig.toOption.get.registry("er").bucketSelector.bucketingConfig.getConfigForCube("druid_performance").get.uriPercentage
    assert(cubeDruidUriSettings("http://localhost:6667/mock/alternative_endpoint") == 25)
    assert(cubeDruidUriSettings("http://localhost:6667/mock/alternative_endpoint_2") == 75)

    val mahaService = DefaultMahaService(dynamicServiceConfig.toOption.get)

    //count actual requests by bucket
    var countFirstBucket = 0
    var countSecondBucket = 0
    val range = 1 to 100
    for(_ <- range) {
      val rrAsync = getReportingRequestAsync(simpleRequest)
      val rmAsync = mahaService.generateRequestModel("er", rrAsync, BucketParams(forceRevision = Some(0), forceEngine = Some(DruidEngine)))

      assert(rmAsync.isSuccess, "Building requestModel failed.")
      val uri = rmAsync.get.model.uri

      assert(uri.equals("http://localhost:6667/mock/alternative_endpoint") || uri.equals("http://localhost:6667/mock/alternative_endpoint_2"))

      val altQueryGeneratorRegistry = new QueryGeneratorRegistry
      altQueryGeneratorRegistry.register(DruidEngine, getDruidQueryGenerator())
      val queryPipelineFactoryLocal = new DefaultQueryPipelineFactory(druidMultiQueryEngineList = List(OracleEngine))(altQueryGeneratorRegistry)
      val queryPipelineTry = queryPipelineFactoryLocal.from(rmAsync.toOption.get.model, QueryAttributes.empty)

      assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))
      withDruidQueryExecutor("http://localhost:6667/mock/whiteGloveGroupBy") {
        druidExecutor =>

          val queryExecContext: QueryExecutorContext = new QueryExecutorContext
          queryExecContext.register(druidExecutor)

          val result = queryPipelineTry.toOption.get.execute(queryExecContext)
          assert(result.isSuccess)
          val expectedList = List(
            "Row(Map(Student ID -> 0, Marks Obtained -> 1),ArrayBuffer(213, 867))",
            "Row(Map(Student ID -> 0, Marks Obtained -> 1),ArrayBuffer(213, 5309))"
          )
          assert(result.get.rowList.length == 1)
          result.get.rowList.foreach {
            row =>
              assert(expectedList.contains(row.toString))
              if(expectedList.head == row.toString)
                countFirstBucket += 1
              else
                countSecondBucket += 1
          }

      }
    }
    println("first bucket: " + countFirstBucket + " second: " + countSecondBucket)
    assert(countFirstBucket > 1 && countSecondBucket > 1 && countSecondBucket > countFirstBucket, "first bucket: " + countFirstBucket + " second: " + countSecondBucket)
  }

  val simpleRequest: String =
    s"""
       |{
       |    "cube": "druid_performance",
       |    "selectFields": [
       |        {
       |            "field": "Student ID"
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
       |        }
       |    ]
       |}
       |""".stripMargin

  val dynamicConfigJson =
    s"""
        {
  "registryMap": {
    "er": {
      "factRegistrationClass": "com.yahoo.maha.service.example.SampleFactSchemaRegistrationFactory",
      "dimensionRegistrationClass": "com.yahoo.maha.service.example.SampleDimensionSchemaRegistrationFactory",
      "executors": [
        "druidExec"
      ],
      "generators": [
        "druid",
        "hive",
        "presto"
      ],
      "bucketingConfigName": "commonBucket",
      "userTimeZoneProviderName": "commonUserTimeZone",
      "utcTimeProviderName": "commonUTC",
      "parallelServiceExecutorName": "commonParallelExec",
      "dimEstimatorFactoryClass": "com.yahoo.maha.service.factory.DefaultDimCostEstimatorFactory",
      "dimEstimatorFactoryConfig": {},
      "factEstimatorFactoryClass": "com.yahoo.maha.service.factory.DefaultFactCostEstimatorFactory",
      "factEstimatorFactoryConfig": {},
      "defaultPublicFactRevisionMap": {

      },
      "defaultPublicDimRevisionMap": {

      },
      "defaultFactEngine": "druid",
      "druidMultiEngineQueryList": []
    }
  },
  "executorMap": {
    "oracleExec": {
      "factoryClass": "com.yahoo.maha.service.factory.OracleQueryExecutoryFactory",
      "config": {
        "dataSourceName": "oracleDataSource",
        "jdbcConnectionFetchSize": 10,
        "lifecycleListenerFactoryClass": "com.yahoo.maha.service.factory.NoopExecutionLifecycleListenerFactory",
        "lifecycleListenerFactoryConfig": [{
          "key": "value"
        }]
      }
    },
    "druidExecFactEngagement": {
      "factoryClass": "com.yahoo.maha.service.factory.DruidQueryExecutoryFactory",
      "config": {
        "druidQueryExecutorConfigFactoryClassName": "com.yahoo.maha.service.factory.DefaultDruidQueryExecutorConfigFactory",
        "druidQueryExecutorConfigJsonConfig": {
          "maxConnectionsPerHost": 100,
          "maxConnections": 10000,
          "connectionTimeout": 140000,
          "timeoutRetryInterval": 100,
          "timeoutThreshold": 9000,
          "degradationConfigName": "TestConfig",
          "url": "http://localhost:6667/mock/basicquery",
          "headers": {
            "key": "value"
          },
          "readTimeout": 10000,
          "requestTimeout": 10000,
          "pooledConnectionIdleTimeout": 10000,
          "timeoutMaxResponseTimeInMs": 30000,
          "enableRetryOn500": true,
          "retryDelayMillis": 1000,
          "maxRetry": 1,
          "enableFallbackOnUncoveredIntervals": true
        },
        "lifecycleListenerFactoryClass": "com.yahoo.maha.service.factory.NoopExecutionLifecycleListenerFactory",
        "lifecycleListenerFactoryConfig": [{
          "key": "value"
        }],
        "resultSetTransformersFactoryClassName": "com.yahoo.maha.service.factory.DefaultResultSetTransformersFactory",
        "resultSetTransformersFactoryConfig": [{
          "key": "value"
        }],
        "authHeaderProviderFactoryClassName": "com.yahoo.maha.service.factory.NoopAuthHeaderProviderFactory",
        "authHeaderProviderFactoryConfig": {
          "domain": "test",
          "service": "druid",
          "privateKeyName": "key",
          "privateKeyId": "0"
        }
      }
    },
    "druidExecCampaignPerformance": {
      "factoryClass": "com.yahoo.maha.service.factory.DruidQueryExecutoryFactory",
      "config": {
        "druidQueryExecutorConfigFactoryClassName": "com.yahoo.maha.service.factory.DefaultDruidQueryExecutorConfigFactory",
        "druidQueryExecutorConfigJsonConfig": {
          "maxConnectionsPerHost": 100,
          "maxConnections": 10000,
          "connectionTimeout": 140000,
          "timeoutRetryInterval": 100,
          "timeoutThreshold": 9000,
          "degradationConfigName": "TestConfig",
          "url": "http://localhost:6667/mock/studentperformance",
          "headers": {
            "key": "value"
          },
          "readTimeout": 10000,
          "requestTimeout": 10000,
          "pooledConnectionIdleTimeout": 10000,
          "timeoutMaxResponseTimeInMs": 30000,
          "enableRetryOn500": true,
          "retryDelayMillis": 1000,
          "maxRetry": 1,
          "enableFallbackOnUncoveredIntervals": true
        },
        "lifecycleListenerFactoryClass": "com.yahoo.maha.service.factory.NoopExecutionLifecycleListenerFactory",
        "lifecycleListenerFactoryConfig": [{
          "key": "value"
        }],
        "resultSetTransformersFactoryClassName": "com.yahoo.maha.service.factory.DefaultResultSetTransformersFactory",
        "resultSetTransformersFactoryConfig": [{
          "key": "value"
        }],
        "authHeaderProviderFactoryClassName": "com.yahoo.maha.service.factory.NoopAuthHeaderProviderFactory",
        "authHeaderProviderFactoryConfig": {
          "domain": "test",
          "service": "druid",
          "privateKeyName": "key",
          "privateKeyId": "0"
        }
      }
    },
    "druidExec": {
          "factoryClass": "com.yahoo.maha.service.factory.DruidQueryExecutoryFactory",
          "config": {
            "druidQueryExecutorConfigFactoryClassName": "com.yahoo.maha.service.factory.DefaultDruidQueryExecutorConfigFactory",
            "druidQueryExecutorConfigJsonConfig": {
              "maxConnectionsPerHost": 100,
              "maxConnections": 10000,
              "connectionTimeout": 140000,
              "timeoutRetryInterval": 100,
              "timeoutThreshold": 9000,
              "degradationConfigName": "TestConfig",
              "url": "http://localhost:8082/druid/v2/?pretty",
              "headers": {
                "Content-Type": "application/json"
              },
              "readTimeout": 10000,
              "requestTimeout": 10000,
              "pooledConnectionIdleTimeout": 10000,
              "timeoutMaxResponseTimeInMs": 30000,
              "enableRetryOn500": true,
              "retryDelayMillis": 1000,
              "maxRetry": 3,
              "enableFallbackOnUncoveredIntervals" : false
            },
            "lifecycleListenerFactoryClass": "com.yahoo.maha.service.factory.NoopExecutionLifecycleListenerFactory",
            "lifecycleListenerFactoryConfig": [
              {
                "key": "value"
              }
            ],
            "resultSetTransformersFactoryClassName": "com.yahoo.maha.service.factory.DefaultResultSetTransformersFactory",
            "resultSetTransformersFactoryConfig": [
              {
                "key": "value"
              }
            ],
            "authHeaderProviderFactoryClassName": "com.yahoo.maha.service.factory.NoopAuthHeaderProviderFactory",
            "authHeaderProviderFactoryConfig" : {
              "domain" : "none",
              "service" :"druid",
              "privateKeyName" : "none",
              "privateKeyId" : "0"
            }
          }
        }
  },
  "generatorMap": {
    "oracle": {
      "factoryClass": "com.yahoo.maha.service.factory.OracleQueryGeneratorFactory",
      "config": {
        "partitionColumnRendererClass": "com.yahoo.maha.service.factory.DefaultPartitionColumnRendererFactory",
        "partitionColumnRendererConfig": [{
          "key": "value"
        }],
        "literalMapperClass": "com.yahoo.maha.service.factory.DefaultOracleLiteralMapperFactory",
        "literalMapperConfig": [{
          "key": "value"
        }]
      }
    },
    "druid": {
      "factoryClass": "com.yahoo.maha.service.factory.DruidQueryGeneratorFactory",
      "config": {
        "queryOptimizerClass": "com.yahoo.maha.service.factory.DefaultDruidQueryOptimizerFactory",
        "queryOptimizerConfig": [{
          "key": "value"
        }],
        "dimCardinality": 40000,
        "maximumMaxRows": 5000,
        "maximumTopNMaxRows": 400,
        "maximumMaxRowsAsync": 100000,
        "shouldLimitInnerQueries": true,
        "useCustomRoundingSumAggregator": true
      }
    },
    "presto": {
      "factoryClass": "com.yahoo.maha.service.factory.PrestoQueryGeneratorFactory",
      "config": {
        "partitionColumnRendererClass": "com.yahoo.maha.service.factory.DefaultPartitionColumnRendererFactory",
        "partitionColumnRendererConfig": [],
        "udfRegistrationFactoryName": "com.yahoo.maha.service.factory.DefaultMahaUDFRegistrationFactory",
        "udfRegistrationFactoryConfig": [{
          "key": "value"
        }]
      }
    },
    "hive": {
      "factoryClass": "com.yahoo.maha.service.factory.HiveQueryGeneratorFactory",
      "config": {
        "partitionColumnRendererClass": "com.yahoo.maha.service.factory.DefaultPartitionColumnRendererFactory",
        "partitionColumnRendererConfig": [],
        "udfRegistrationFactoryName": "com.yahoo.maha.service.factory.DefaultMahaUDFRegistrationFactory",
        "udfRegistrationFactoryConfig": [{
          "key": "value"
        }]
      }
    }
  },
  "bucketingConfigMap": {
    "commonBucket": {
      "factoryClass": "com.yahoo.maha.service.factory.DefaultBucketingConfigFactory",
      "config": {
        "cube": [{
            "cube": "student_performance",
            "internal": [{
              "revision": 1,
              "percent": 100
            }],
            "external": [{
              "revision": 0,
              "percent": "<%(student_performance.external.rev0.percent, 50)%>"
            },{
              "revision": 1,
              "percent": "<%(student_performance.external.rev1.percent, 50)%>"
            }],
            "dryRun": [{
              "revision": 1,
              "percent": 100,
              "engine": "Oracle"
            }],
            "userWhiteList": [
            ],
            "uriConfig": [
              {
                "uri": "http://localhost:6667/mock/alternative_endpoint",
                "percent": 10
              },
              {
                "uri": "http://localhost:6667/mock/alternative_endpoint_2",
                "percent": 90
              }
            ]
          },
          {
            "cube": "druid_performance",
                "internal": [{
                  "revision": 0,
                  "percent": 100
                }],
                "external": [{
                  "revision": 0,
                  "percent": 100
                }],
                "dryRun": [{
                  "revision": 0,
                  "percent": 100
                }],
                "userWhiteList": [
                ],
                "uriConfig": [
                  {
                    "uri": "http://localhost:6667/mock/alternative_endpoint",
                    "percent": 25
                  },
                  {
                    "uri": "http://localhost:6667/mock/alternative_endpoint_2",
                    "percent": 75
                  }
                ]
          }
        ],
        "queryGenerator": []
      }
    }
  },
   "userTimeZoneProviderMap": {
    "commonUserTimeZone": {
      "factoryClass": "com.yahoo.maha.service.factory.NoopUserTimeZoneProviderFactory",
      "config": {
        "k": "v"
      }
    }
  },
  "utcTimeProviderMap": {
    "commonUTC": {
      "factoryClass": "com.yahoo.maha.service.factory.PassThroughUTCTimeProviderFactory",
      "config": {
        "k": "v"
      }
    }
  },
  "parallelServiceExecutorConfigMap": {
    "commonParallelExec": {
      "factoryClass": "com.yahoo.maha.service.factory.DefaultParallelServiceExecutoryFactory",
      "config": {
        "rejectedExecutionHandlerClass": "com.yahoo.maha.service.factory.DefaultRejectedExecutionHandlerFactory",
        "rejectedExecutionHandlerConfig": "",
        "poolName": "maha-test-pool",
        "defaultTimeoutMillis": 10000,
        "threadPoolSize": 3,
        "queueSize": 20
      }
    }
  },
  "datasourceMap": {
    "oracleDataSource": {
      "factoryClass": "com.yahoo.maha.service.factory.HikariDataSourceFactory",
      "config": {
        "driverClassName": "org.h2.Driver",
        "jdbcUrl": "jdbc:h2:mem:h2dbId;MODE=Oracle;DB_CLOSE_DELAY=-1",
        "username": "sa",
        "passwordProviderFactoryClassName": "com.yahoo.maha.service.factory.PassThroughPasswordProviderFactory",
        "passwordProviderConfig": [{
          "key": "value"
        }],
        "passwordKey": "h2.test.database.password",
        "poolName": "test-pool",
        "maximumPoolSize": 10,
        "minimumIdle": 0,
        "autoCommit": true,
        "connectionTestQuery": "SELECT 1 FROM DUAL",
        "validationTimeout": 1000000,
        "idleTimeout": 1000000,
        "maxLifetime": 10000000,
        "dataSourceProperties": [{
          "key": "propertyKey",
          "value": "propertyValue"
        }]
      }
    }
  },
  "mahaRequestLoggingConfig": {
    "factoryClass": "com.yahoo.maha.service.factory.NoopMahaRequestLogWriterFactory",
    "config": {},
    "isLoggingEnabled": false
  },
  "curatorMap": {
    "default": {
      "factoryClass": "com.yahoo.maha.service.factory.DefaultCuratorFactory",
      "config": {
        "maxRowsLimit": 1000
      }
    }
  }
}""".stripMargin

}
