// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core.{HiveEngine, PrestoEngine}
import com.yahoo.maha.core.query.hive.HiveQueryGenerator
import org.scalatest.{FunSuite, Matchers}
import org.mockito.Mockito._

class QueryGeneratorRegistryTest extends FunSuite with Matchers {
  test("Successfully register query generator without version") {
    val qgenRegistry = new QueryGeneratorRegistry()
    val hiveQueryGenerator = mock(classOf[HiveQueryGenerator])
    qgenRegistry.register(HiveEngine, hiveQueryGenerator)
    assert(qgenRegistry.isEngineRegistered(HiveEngine, Option(Version.v0)) == true)
    assert(qgenRegistry.isEngineRegistered(HiveEngine, Option(Version.v1)) == false)
    assert(qgenRegistry.isEngineRegistered(PrestoEngine, None) == false)
    assert(qgenRegistry.isEngineRegistered(PrestoEngine, Option(Version.v0)) == false)
  }

  test("Successfully register query generator with version") {
    val qgenRegistry = new QueryGeneratorRegistry()
    val hiveQueryGenerator = mock(classOf[HiveQueryGenerator])
    qgenRegistry.register(HiveEngine, hiveQueryGenerator)
    qgenRegistry.register(HiveEngine, hiveQueryGenerator, Version.v1)
    qgenRegistry.register(HiveEngine, hiveQueryGenerator, Version.v2)
    assert(qgenRegistry.isEngineRegistered(HiveEngine, Option(Version.v0)) == true)
    assert(qgenRegistry.isEngineRegistered(HiveEngine, Option(Version.v1)) == true)
    assert(qgenRegistry.isEngineRegistered(HiveEngine, Option(Version.v2)) == true)
  }

  test("Failed to register query generator repeatedly for the same engine and version") {
    val qgenRegistry = new QueryGeneratorRegistry()
    val hiveQueryGenerator = mock(classOf[HiveQueryGenerator])
    qgenRegistry.register(HiveEngine, hiveQueryGenerator, Version.v1)
    val exception = intercept[IllegalArgumentException] {
      qgenRegistry.register(HiveEngine, hiveQueryGenerator, Version.v1)
    }
    assert(exception.getMessage.contains("Query generator already defined for engine : Hive and version V1"), exception.getMessage)
  }

  test("Test get query generator with version") {
    val qgenRegistry = new QueryGeneratorRegistry()
    val hiveQueryGenerator = mock(classOf[HiveQueryGenerator])
    qgenRegistry.register(HiveEngine, hiveQueryGenerator, Version.v0)
    qgenRegistry.register(HiveEngine, hiveQueryGenerator, Version.v1)
    qgenRegistry.register(HiveEngine, hiveQueryGenerator, Version.v2)
    assert(qgenRegistry.getGenerator(HiveEngine, Some(Version.v0)).get == hiveQueryGenerator)
    assert(qgenRegistry.getGenerator(PrestoEngine, Some(Version.v0)).isEmpty)
  }
}
