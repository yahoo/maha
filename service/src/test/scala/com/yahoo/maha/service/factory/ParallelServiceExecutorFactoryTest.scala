// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import com.yahoo.maha.parrequest2.future.ParallelServiceExecutor
import com.yahoo.maha.service.{DefaultMahaServiceConfigContext, MahaServiceConfigContext}
import org.json4s.jackson.JsonMethods._

/**
 * Created by pranavbhole on 07/06/17.
 */
class ParallelServiceExecutoryFactoryTest extends BaseFactoryTest {
  implicit val context: MahaServiceConfigContext = DefaultMahaServiceConfigContext()

  test("ParallelServiceExecutoryFactory Test") {
    val jsonString =   """
                         |{
                         |"rejectedExecutionHandlerClass" : "com.yahoo.maha.service.factory.DefaultRejectedExecutionHandlerFactory",
                         |"rejectedExecutionHandlerConfig" : "",
                         |"poolName" : "maha-test-pool",
                         |"defaultTimeoutMillis" : 10000,
                         |"threadPoolSize" : 3,
                         |"queueSize" : 3
                         |}
                         |
                       """.stripMargin

    val factoryResult = getFactory[ParallelServiceExecutoryFactory]("com.yahoo.maha.service.factory.DefaultParallelServiceExecutoryFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[ParallelServiceExecutor])
  }
}
