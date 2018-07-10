// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory


import cats.instances.uuid
import com.yahoo.maha.core.{DruidEngine, OracleEngine, PrestoEngine}
import com.yahoo.maha.executor.druid.DruidQueryExecutor
import com.yahoo.maha.executor.oracle.OracleQueryExecutor
import com.yahoo.maha.executor.presto.PrestoQueryExecutor
import com.yahoo.maha.service.config.JsonDataSourceConfig
import org.json4s.jackson.JsonMethods._

/**
 * Created by pranavbhole on 01/06/17.
 */
class QueryExecutorFactoryTest extends BaseFactoryTest {
  test("Test Oracle Query Executor Instantiation") {
    val jsonString =
      """
        |{
        |"dataSourceName": "oracleDataSource",
        |"jdbcConnectionFetchSize": 10,
        |"lifecycleListenerFactoryClass": "com.yahoo.maha.service.factory.NoopExecutionLifecycleListenerFactory",
        |"lifecycleListenerFactoryConfig" : [{"key": "value"}]
        |}
        |
      """.stripMargin

    val dataSourceConfigJson =
      s"""
         |{
         |"factoryClass": "com.yahoo.maha.service.factory.HikariDataSourceFactory",
         |"config": {
         |"driverClassName" : "org.h2.Driver",
         |"jdbcUrl" : "jdbc:h2:mem:$uuid;MODE=Oracle;DB_CLOSE_DELAY=-1",
         |"username" : "sa",
         |"passwordProviderFactoryClassName" : "com.yahoo.maha.service.factory.PassThroughPasswordProviderFactory",
         |"passwordProviderConfig" : [{"key" : "value"}],
         |"passwordKey" : "h2.test.database.password",
         |"poolName" : "test-pool",
         |"maximumPoolSize" : 10,
         |"minimumIdle" : 1,
         |"autoCommit": true,
         |"connectionTestQuery" : "SELECT 1 FROM DUAL",
         |"validationTimeout" : 1000000,
         |"idleTimeout" : 1000000,
         |"maxLifetime" : 10000000,
         |"dataSourceProperties": [{"key": "propertyKey" , "value": "propertyValue"}]
         |}
         |}
       """.stripMargin

    val dataSourceConfigResult = JsonDataSourceConfig.parse.read(parse(dataSourceConfigJson))
    assert(dataSourceConfigResult.toOption.isDefined)
    val dataSourceConfig = dataSourceConfigResult.toOption.get
    val dataSourceMap = Map("oracleDataSource".toLowerCase -> dataSourceConfig)

    val factoryResult = getFactory[QueryExecutoryFactory]("com.yahoo.maha.service.factory.OracleQueryExecutoryFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val generatorResult = factory.fromJson(json, dataSourceMap)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[OracleQueryExecutor])
    generatorResult.foreach {
    executor =>
        assert(executor.engine == OracleEngine)
    }
  }

  test("Test Druid Query Executor Instantiation") {

    val jsonString =
      """
        |{
        |"druidQueryExecutorConfigFactoryClassName" : "com.yahoo.maha.service.factory.DefaultDruidQueryExecutorConfigFactory",
        |"druidQueryExecutorConfigJsonConfig" :{
        |"maxConnectionsPerHost" : 100,
        |"maxConnections" : 10000,
        |"connectionTimeout": 140000,
        |"timeoutRetryInterval" : 100,
        |"timeoutThreshold" : 9000,
        |"degradationConfigName" : "TestConfig",
        |"url" : "http://broker.druid.test.maha.com",
        |"headers": {"key": "value"},
        |"readTimeout" : 10000,
        |"requestTimeout" : 10000,
        |"pooledConnectionIdleTimeout" : 10000,
        |"timeoutMaxResponseTimeInMs" : 30000,
        |"enableRetryOn500" : true,
        |"retryDelayMillis" : 1000,
        |"maxRetry" : 3,
        |"enableFallbackOnUncoveredIntervals" : true
        |},
        |"lifecycleListenerFactoryClass" : "com.yahoo.maha.service.factory.NoopExecutionLifecycleListenerFactory",
        |"lifecycleListenerFactoryConfig" :  [{"key": "value"}],
        |"resultSetTransformersFactoryClassName": "com.yahoo.maha.service.factory.DefaultResultSetTransformersFactory",
        |"resultSetTransformersFactoryConfig": [{"key": "value"}],
        |"authHeaderProviderFactoryClassName": "com.yahoo.maha.service.factory.NoopAuthHeaderProviderFactory",
        |"authHeaderProviderFactoryConfig" : {
        |  "domain" : "Maha",
        |  "service" :"MahaProviderService",
        |  "privateKeyName" : "sa",
        |  "privateKeyId" : "sa"
        |}
        |}
      """.stripMargin

    val factoryResult = getFactory[QueryExecutoryFactory]("com.yahoo.maha.service.factory.DruidQueryExecutoryFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val generatorResult = factory.fromJson(json, Map.empty)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[DruidQueryExecutor])
    generatorResult.foreach {
      executor =>
        assert(executor.engine == DruidEngine)
    }
  }

  test("Test Presto Query Executor Instantiation") {
    val jsonString =
      """
        |{
        |"dataSourceName" : "prestoDataSource",
        |"jdbcConnectionFetchSize": 10,
        |"lifecycleListenerFactoryClass": "com.yahoo.maha.service.factory.NoopExecutionLifecycleListenerFactory",
        |"lifecycleListenerFactoryConfig" : [{"key": "value"}],
        |"prestoQueryTemplateFactoryName" : "com.yahoo.maha.service.factory.DefaultPrestoQueryTemplateFactory",
        |"prestoQueryTemplateFactoryConfig" : [{"key": "value"}]
        |}
        |
      """.stripMargin

    val dataSourceConfigJson =
      s"""
         |{
         |"factoryClass": "com.yahoo.maha.service.factory.HikariDataSourceFactory",
         |"config": {
         |"driverClassName" : "org.h2.Driver",
         |"jdbcUrl" : "jdbc:h2:mem:$uuid;MODE=Oracle;DB_CLOSE_DELAY=-1",
         |"username" : "sa",
         |"passwordProviderFactoryClassName" : "com.yahoo.maha.service.factory.PassThroughPasswordProviderFactory",
         |"passwordProviderConfig" : [{"key" : "value"}],
         |"passwordKey" : "h2.test.database.password",
         |"poolName" : "test-pool",
         |"maximumPoolSize" : 10,
         |"minimumIdle" : 1,
         |"autoCommit": true,
         |"connectionTestQuery" : "SELECT 1 FROM DUAL",
         |"validationTimeout" : 1000000,
         |"idleTimeout" : 1000000,
         |"maxLifetime" : 10000000,
         |"dataSourceProperties": [{"key": "propertyKey" , "value": "propertyValue"}]
         |}
         |}
       """.stripMargin

    val dataSourceConfigResult = JsonDataSourceConfig.parse.read(parse(dataSourceConfigJson))
    assert(dataSourceConfigResult.toOption.isDefined)
    val dataSourceConfig = dataSourceConfigResult.toOption.get
    val dataSourceMap = Map("prestoDataSource".toLowerCase -> dataSourceConfig)

    val factoryResult = getFactory[QueryExecutoryFactory]("com.yahoo.maha.service.factory.PrestoQueryExecutoryFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val generatorResult = factory.fromJson(json, dataSourceMap)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[PrestoQueryExecutor])
    generatorResult.foreach {
      executor =>
        assert(executor.engine == PrestoEngine)
    }
  }
}
