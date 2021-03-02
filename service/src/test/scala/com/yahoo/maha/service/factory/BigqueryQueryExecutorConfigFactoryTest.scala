// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import com.yahoo.maha.executor.bigquery.BigqueryQueryExecutorConfig
import com.yahoo.maha.service.{DefaultMahaServiceConfigContext, MahaServiceConfigContext}
import org.json4s.jackson.JsonMethods._

class BigqueryQueryExecutorConfigFactoryTest extends BaseFactoryTest {
  implicit val context: MahaServiceConfigContext = DefaultMahaServiceConfigContext()

  test("Bigquery Executor Config") {
    val jsonString = """
                       |{
                       |"gcpCredentialsFilePath": "/path/to/credentials/file",
                       |"gcpProjectId": "testProjectId",
                       |"enableProxy": false,
                       |"proxyCredentialsFilePath": "/path/to/proxy/file",
                       |"proxyHost": "test.proxy.host.com",
                       |"disableRpc": false,
                       |"connectionTimeoutMs": 30000,
                       |"readTimeoutMs": 60000,
                       |"retries": 5
                       |}
                     """.stripMargin

    val factoryResult = getFactory[BigqueryQueryExecutorConfigFactory](
      "com.yahoo.maha.service.factory.DefaultBigqueryQueryExecutorConfigFactory", closer
    )
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[BigqueryQueryExecutorConfig])
    generatorResult.foreach {
      bigqueryExecutorConfig =>
        assert(bigqueryExecutorConfig.gcpCredentialsFilePath == "/path/to/credentials/file")
        assert(bigqueryExecutorConfig.gcpProjectId == "testProjectId")
        assert(bigqueryExecutorConfig.enableProxy == false)
        assert(bigqueryExecutorConfig.proxyCredentialsFilePath == Some("/path/to/proxy/file"))
        assert(bigqueryExecutorConfig.proxyHost == Some("test.proxy.host.com"))
        assert(bigqueryExecutorConfig.proxyPort == None)
        assert(bigqueryExecutorConfig.disableRpc == Some(false))
        assert(bigqueryExecutorConfig.connectionTimeoutMs == 30000)
        assert(bigqueryExecutorConfig.readTimeoutMs == 60000)
        assert(bigqueryExecutorConfig.retries == 5)
    }
  }
}
