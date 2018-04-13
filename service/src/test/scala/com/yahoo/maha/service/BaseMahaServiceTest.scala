package com.yahoo.maha.service

import java.util.UUID

import com.google.common.io.Closer
import com.yahoo.maha.core.DailyGrain
import com.yahoo.maha.core.ddl.OracleDDLGenerator
import com.yahoo.maha.jdbc.JdbcConnection
import com.yahoo.maha.service.utils.MahaConstants
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.apache.log4j.MDC
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.FunSuite

/**
 * Created by pranavbhole on 21/03/18.
 */
trait BaseMahaServiceTest extends FunSuite {
  protected var dataSource: Option[HikariDataSource] = None
  protected var jdbcConnection: Option[JdbcConnection] = None
  protected val closer = Closer.create()

  final val REGISTRY = "er"
  protected[this] val fromDate = DailyGrain.toFormattedString(DateTime.now(DateTimeZone.UTC).minusDays(7))
  protected[this] val toDate = DailyGrain.toFormattedString(DateTime.now(DateTimeZone.UTC))

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
                      |   },
                      |   "curatorMap": {
                      |      "fail": {
                      |         "factoryClass": "com.yahoo.maha.service.factory.FailingCuratorFactory",
                      |         "config": {
                      |         }
                      |      },
                      |      "default": {
                      |         "factoryClass": "com.yahoo.maha.service.factory.DefaultCuratorFactory",
                      |         "config": {
                      |         }
                      |      },
                      |      "timeshift": {
                      |         "factoryClass": "com.yahoo.maha.service.factory.TimeShiftCuratorFactory",
                      |         "config": {
                      |         }
                      |      },
                      |      "drilldown": {
                      |         "factoryClass": "com.yahoo.maha.service.factory.DrillDownCuratorFactory",
                      |         "config": {
                      |         }
                      |      },
                      |      "totalmetrics": {
                      |         "factoryClass": "com.yahoo.maha.service.factory.TotalMetricsCuratorFactory",
                      |         "config": {
                      |         }
                      |      }
                      |   }
                      |}
                      |
                       |	""".stripMargin

  initJdbcToH2()
  val mahaServiceResult = MahaServiceConfig.fromJson(jsonString.getBytes("utf-8"))
  assert(mahaServiceResult.isSuccess)

  val mahaServiceConfig = mahaServiceResult.toOption.get
  val mahaService = DefaultMahaService(mahaServiceConfig)

  //For Kafka Logging init
  MDC.put(MahaConstants.REQUEST_ID, "123Request")
  MDC.put(MahaConstants.USER_ID,"abc")

  assert(mahaServiceConfig.registry.get("er").isDefined)
  val erRegistryConfig = mahaServiceConfig.registry.get("er").get
  val erRegistry= erRegistryConfig.registry
  assert(erRegistry.isCubeDefined("student_performance"))
  assert(erRegistry.getDimension("student").isDefined)

  val ddlGenerator = new OracleDDLGenerator
  assert(jdbcConnection.isDefined)

  def createTables(): Unit = {
    // Create Tables
    erRegistry.factMap.values.foreach {
      publicFact =>
        publicFact.factList.foreach {
          fact=>
            val ddl = ddlGenerator.toDDL(fact)
            assert(jdbcConnection.get.executeUpdate(ddl).isSuccess)
        }
    }
  }

}
