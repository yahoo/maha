// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import com.yahoo.maha.service.config.PasswordProvider
import com.yahoo.maha.core.{UTCTimeProvider, UserTimeZoneProvider}
import com.yahoo.maha.core.query.ExecutionLifecycleListener
import com.yahoo.maha.service.{DefaultMahaServiceConfigContext, MahaServiceConfigContext}
import org.json4s.jackson.JsonMethods._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Created by pranavbhole on 31/05/17.
 */
class FactoryTest extends BaseFactoryTest {
  implicit val context: MahaServiceConfigContext = DefaultMahaServiceConfigContext()

  test("Test PassThroughPasswordProviderFactory ") {
    val factoryResult = getFactory[PasswordProviderFactory]("com.yahoo.maha.service.factory.PassThroughPasswordProviderFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    assert(factory.supportedProperties.isEmpty)
    val json = parse("{}")
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[PasswordProvider])
  }

  test("Test PassThroughUTCTimeProviderFactory ") {
    val factoryResult = getFactory[UTCTimeProviderFactory]("com.yahoo.maha.service.factory.PassThroughUTCTimeProviderFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    assert(factory.supportedProperties.isEmpty)
    val json = parse("{}")
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[UTCTimeProvider])
  }

  test("Test NoopExecutionLifecycleListenerFactory ") {
    val factoryResult = getFactory[ExecutionLifecycleListenerFactory]("com.yahoo.maha.service.factory.NoopExecutionLifecycleListenerFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    assert(factory.supportedProperties.isEmpty)
    val json = parse("{}")
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[ExecutionLifecycleListener])
  }

  test("Create a BaseUTCTimeProviderFactory") {
    val factoryResult = getFactory[UTCTimeProviderFactory]("com.yahoo.maha.service.factory.BaseUTCTimeProviderFactory", closer)
    factoryResult.toOption.get.fromJson(parse("{}"))
    assert(factoryResult.isSuccess, "should successfully instantiate base factory.")
    assert(factoryResult.toOption.get.supportedProperties == List.empty, "No currently supported properties.")
  }

  test("Test NoopUserTimeZoneProviderFactory ") {
    val factoryResult = getFactory[UserTimeZoneProviderFactory]("com.yahoo.maha.service.factory.NoopUserTimeZoneProviderFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    assert(factory.supportedProperties.isEmpty)
    val json = parse("{}")
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[UserTimeZoneProvider])
  }

}
