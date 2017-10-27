// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import com.yahoo.maha.core.query.druid.{SyncDruidQueryOptimizer, AsyncDruidQueryOptimizer}
import org.json4s.jackson.JsonMethods._
import org.scalatest.{Matchers, FunSuite}

/**
 * Created by pranavbhole on 31/05/17.
 */
class DruidQueryOptimizerFactoryTest extends BaseFactoryTest {
  test("Test instantiation of SyncDruidQueryOptimizer from factory") {

    val jsonString =   """
                         |{
                         | "maxSingleThreadedDimCardinality" : 40000,
                         | "maxNoChunkCost": 280000,
                         | "maxChunks": 3,
                         | "timeout": 300000
                         |}
                       """.stripMargin

    val factoryResult = getFactory[DruidQueryOptimizerFactory]("com.yahoo.maha.service.factory.SyncDruidQueryOptimizerFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[SyncDruidQueryOptimizer])
  }

  test("Test instantiation of AsyncDruidQueryOptimizer from factory") {

    val jsonString =   """
                         |{
                         | "maxSingleThreadedDimCardinality" : 40000,
                         | "maxNoChunkCost": 280000,
                         | "maxChunks": 3,
                         | "timeout": 300000
                         |}
                       """.stripMargin

    val factoryResult = getFactory[DruidQueryOptimizerFactory]("com.yahoo.maha.service.factory.AsyncDruidQueryOptimizerFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[AsyncDruidQueryOptimizer])
  }
}
