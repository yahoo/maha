// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import com.yahoo.maha.core.dimension.DimCol
import com.yahoo.maha.core._
import com.yahoo.maha.service.{DefaultMahaServiceConfigContext, MahaServiceConfigContext}
import org.json4s.jackson.JsonMethods._

/**
 * Created by pranavbhole on 31/05/17.
 */
class LiteralMapperTest extends BaseFactoryTest {
  implicit val context: MahaServiceConfigContext = DefaultMahaServiceConfigContext()

  test("Test OracleLiteralMapper ") {
    val factoryResult = getFactory[OracleLiteralMapperFactory]("com.yahoo.maha.service.factory.DefaultOracleLiteralMapperFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    assert(factory.supportedProperties.isEmpty)
    val json = parse("{}")
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[OracleLiteralMapper])
  }

  test("Test DruidLiteralMapper ") {
    val factoryResult = getFactory[DruidLiteralMapperFactory]("com.yahoo.maha.service.factory.DefaultDruidLiteralMapperFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    assert(factory.supportedProperties.isEmpty)
    val json = parse("{}")
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[DruidLiteralMapper])
  }

  test("Test DefaultPostgresLiteralMapper") {
    val factoryResult = getFactory[PostgresLiteralMapperFactory]("com.yahoo.maha.service.factory.DefaultPostgresLiteralMapperFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    assert(factory.supportedProperties.isEmpty)
    val json = parse("{}")
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[PostgresLiteralMapper])
  }

  test("Test DefaultPostgresLiteralMapperUsingDriver") {
    val factoryResult = getFactory[PostgresLiteralMapperFactory]("com.yahoo.maha.service.factory.DefaultPostgresLiteralMapperUsingDriverFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    assert(factory.supportedProperties.isEmpty)
    val json = parse("{}")
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[PostgresLiteralMapper])
  }
}
