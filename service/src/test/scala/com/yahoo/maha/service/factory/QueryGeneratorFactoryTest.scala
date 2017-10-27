// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import org.json4s.jackson.JsonMethods._
import org.scalatest.{Matchers, FunSuite}

/**
 * Created by pranavbhole on 31/05/17.
 */
class QueryGeneratorFactoryTest extends BaseFactoryTest {

  test("successfully construct druid query generator from json") {
    val jsonString =   """
                         |{
                         |"queryOptimizerClass": "com.yahoo.maha.service.factory.DefaultDruidQueryOptimizerFactory",
                         |"queryOptimizerConfig" : [{"key": "value"}],
                         |"dimCardinality" : 40000,
                         |"maximumMaxRows" : 5000,
                         |"maximumTopNMaxRows" : 400,
                         |"maximumMaxRowsAsync" : 100000
                         |}
                       """.stripMargin

    val factoryResult = getFactory[DruidQueryGeneratorFactory]("com.yahoo.maha.service.factory.DruidQueryGeneratorFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
  }

  test("successfully construct oracle query generator from json") {
    val jsonString =   """
                         |{
                         |"partitionColumnRendererClass" : "com.yahoo.maha.service.factory.DefaultPartitionColumnRendererFactory",
                         |"partitionColumnRendererConfig" : [{"key": "value"}],
                         |"literalMapperClass" : "com.yahoo.maha.service.factory.DefaultOracleLiteralMapperFactory",
                         |"literalMapperConfig" : [{"key": "value"}]
                         |}
                       """.stripMargin

    val factoryResult = getFactory[OracleQueryGeneratorFactory]("com.yahoo.maha.service.factory.OracleQueryGeneratorFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
  }

}