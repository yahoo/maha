// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import com.yahoo.maha.service.{DefaultMahaServiceConfigContext, MahaServiceConfigContext}
import org.json4s.jackson.JsonMethods._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Created by pranavbhole on 31/05/17.
 */
class QueryGeneratorFactoryTest extends BaseFactoryTest {
  implicit val context: MahaServiceConfigContext = DefaultMahaServiceConfigContext()


  test("successfully construct druid query generator from json") {
    val jsonString =   """
                         |{
                         |"queryOptimizerClass": "com.yahoo.maha.service.factory.DefaultDruidQueryOptimizerFactory",
                         |"queryOptimizerConfig" : [{"key": "value"}],
                         |"dimCardinality" : 40000,
                         |"maximumMaxRows" : 5000,
                         |"maximumTopNMaxRows" : 400,
                         |"maximumMaxRowsAsync" : 100000,
                         |"shouldLimitInnerQueries" : true,
                         |"useCustomRoundingSumAggregator" : true
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

  test("successfully construct Hive query generator from json") {
    val jsonString =   """
                         |{
                         |"partitionColumnRendererClass" : "com.yahoo.maha.service.factory.DefaultPartitionColumnRendererFactory",
                         |"partitionColumnRendererConfig" : [{"key": "value"}],
                         |"udfRegistrationFactoryName" : "com.yahoo.maha.service.factory.DefaultMahaUDFRegistrationFactory",
                         |"udfRegistrationFactoryConfig" : [{"key": "value"}]
                         |}
                       """.stripMargin

    val factoryResult = getFactory[HiveQueryGeneratorFactory]("com.yahoo.maha.service.factory.HiveQueryGeneratorFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert("HiveQueryGenerator".equals(generatorResult.toOption.get.getClass.getSimpleName), generatorResult)
  }

  test("successfully construct Hive query generator V0 from json") {
    val jsonString =   """
                         |{
                         |"partitionColumnRendererClass" : "com.yahoo.maha.service.factory.DefaultPartitionColumnRendererFactory",
                         |"partitionColumnRendererConfig" : [{"key": "value"}],
                         |"udfRegistrationFactoryName" : "com.yahoo.maha.service.factory.DefaultMahaUDFRegistrationFactory",
                         |"udfRegistrationFactoryConfig" : [{"key": "value"}],
                         |"version": 0
                         |}
                       """.stripMargin

    val factoryResult = getFactory[HiveQueryGeneratorFactory]("com.yahoo.maha.service.factory.HiveQueryGeneratorFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert("HiveQueryGenerator".equals(generatorResult.toOption.get.getClass.getSimpleName), generatorResult)
  }

  test("successfully construct Hive query generator V2 from json") {
    val jsonString =   """
                         |{
                         |"partitionColumnRendererClass" : "com.yahoo.maha.service.factory.DefaultPartitionColumnRendererFactory",
                         |"partitionColumnRendererConfig" : [{"key": "value"}],
                         |"udfRegistrationFactoryName" : "com.yahoo.maha.service.factory.DefaultMahaUDFRegistrationFactory",
                         |"udfRegistrationFactoryConfig" : [{"key": "value"}],
                         |"version": 2
                         |}
                       """.stripMargin

    val factoryResult = getFactory[HiveQueryGeneratorFactory]("com.yahoo.maha.service.factory.HiveQueryGeneratorFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val generatorResult = factory.fromJson(json)

    assert(generatorResult.isSuccess, generatorResult)
    assert("HiveQueryGeneratorV2".equals(generatorResult.toOption.get.getClass.getSimpleName), generatorResult)
  }

  test("successfully construct Presto query generator from json") {
    val jsonString =   """
                         |{
                         |"partitionColumnRendererClass" : "com.yahoo.maha.service.factory.DefaultPartitionColumnRendererFactory",
                         |"partitionColumnRendererConfig" : [{"key": "value"}],
                         |"udfRegistrationFactoryName" : "com.yahoo.maha.service.factory.DefaultMahaUDFRegistrationFactory",
                         |"udfRegistrationFactoryConfig" : [{"key": "value"}]
                         |}
                       """.stripMargin

    val factoryResult = getFactory[PrestoQueryGeneratorFactory]("com.yahoo.maha.service.factory.PrestoQueryGeneratorFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
  }

  test("successfully construct Presto query generator V1 from json") {
    val jsonString =   """
                         |{
                         |"partitionColumnRendererClass" : "com.yahoo.maha.service.factory.DefaultPartitionColumnRendererFactory",
                         |"partitionColumnRendererConfig" : [{"key": "value"}],
                         |"udfRegistrationFactoryName" : "com.yahoo.maha.service.factory.DefaultMahaUDFRegistrationFactory",
                         |"udfRegistrationFactoryConfig" : [{"key": "value"}],
                         |"version": 1
                         |}
                       """.stripMargin

    val factoryResult = getFactory[PrestoQueryGeneratorFactory]("com.yahoo.maha.service.factory.PrestoQueryGeneratorFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val generatorResult = factory.fromJson(json)

    assert(generatorResult.isSuccess, generatorResult)
    assert("PrestoQueryGeneratorV1".equals(generatorResult.toOption.get.getClass.getSimpleName), generatorResult)
  }

  test("successfully construct Bigquery query generator from json") {
    val jsonString =   """
                         |{
                         |"partitionColumnRendererClass" : "com.yahoo.maha.service.factory.DefaultPartitionColumnRendererFactory",
                         |"partitionColumnRendererConfig" : [{"key": "value"}],
                         |"udfRegistrationFactoryName" : "com.yahoo.maha.service.factory.DefaultMahaUDFRegistrationFactory",
                         |"udfRegistrationFactoryConfig" : [{"key": "value"}]
                         |}
                       """.stripMargin

    val factoryResult = getFactory[BigqueryQueryGeneratorFactory]("com.yahoo.maha.service.factory.BigqueryQueryGeneratorFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert("BigqueryQueryGenerator".equals(generatorResult.toOption.get.getClass.getSimpleName), generatorResult)
  }

}
