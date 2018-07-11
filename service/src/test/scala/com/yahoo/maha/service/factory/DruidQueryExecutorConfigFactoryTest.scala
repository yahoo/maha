// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import com.yahoo.maha.executor.druid.DruidQueryExecutorConfig
import com.yahoo.maha.service.{DefaultMahaServiceConfigContext, MahaServiceConfigContext}
import org.json4s.jackson.JsonMethods._

/**
 * Created by pranavbhole on 01/06/17.
 */
class DruidQueryExecutorConfigFactoryTest extends BaseFactoryTest {
  implicit val context: MahaServiceConfigContext = DefaultMahaServiceConfigContext()

  test("Druid Executor Config") {
    val jsonString =   """
                         |{
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
                         |"enableFallbackOnUncoveredIntervals" : false
                         |}
                       """.stripMargin

    val factoryResult = getFactory[DruidQueryExecutorConfigFactory]("com.yahoo.maha.service.factory.DefaultDruidQueryExecutorConfigFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[DruidQueryExecutorConfig])
    generatorResult.foreach {
      druidExecutorConfig =>
        assert(druidExecutorConfig.maxConnectionsPerHost == 100)
        assert(druidExecutorConfig.maxConnections == 10000)
        assert(druidExecutorConfig.url == "http://broker.druid.test.maha.com")
        assert(druidExecutorConfig.headers.isDefined)
        assert(druidExecutorConfig.headers.get.get("key").get == "value")
        assert(druidExecutorConfig.enableRetryOn500 == true)
    }
  }

}
