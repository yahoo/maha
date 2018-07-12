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
    assert(qgenRegistry.isEngineRegistered(HiveEngine, V0) == true)
    assert(qgenRegistry.isEngineRegistered(HiveEngine, V1) == false)
    assert(qgenRegistry.isEngineRegistered(PrestoEngine) == false)
    assert(qgenRegistry.isEngineRegistered(PrestoEngine, V0) == false)
  }

  test("Successfully register query generator with version") {
    val qgenRegistry = new QueryGeneratorRegistry()
    val hiveQueryGenerator = mock(classOf[HiveQueryGenerator])
    qgenRegistry.register(HiveEngine, hiveQueryGenerator)
    qgenRegistry.register(HiveEngine, hiveQueryGenerator, V1)
    qgenRegistry.register(HiveEngine, hiveQueryGenerator, V2)
    assert(qgenRegistry.isEngineRegistered(HiveEngine, V0) == true)
    assert(qgenRegistry.isEngineRegistered(HiveEngine, V1) == true)
    assert(qgenRegistry.isEngineRegistered(HiveEngine, V2) == true)
  }

  test("Failed to register query generator repeatedly for the same engine and version") {
    val qgenRegistry = new QueryGeneratorRegistry()
    val hiveQueryGenerator = mock(classOf[HiveQueryGenerator])
    qgenRegistry.register(HiveEngine, hiveQueryGenerator, V1)
    val exception = intercept[IllegalArgumentException] {
      qgenRegistry.register(HiveEngine, hiveQueryGenerator, V1)
    }
    assert(exception.getMessage.contains("Query generator already defined for engine : Hive and version V1"), exception.getMessage)
  }

  test("Test get query generator with version") {
    val qgenRegistry = new QueryGeneratorRegistry()
    val hiveQueryGenerator = mock(classOf[HiveQueryGenerator])
    qgenRegistry.register(HiveEngine, hiveQueryGenerator, V0)
    qgenRegistry.register(HiveEngine, hiveQueryGenerator, V1)
    qgenRegistry.register(HiveEngine, hiveQueryGenerator, V2)
    assert(qgenRegistry.getGenerator(HiveEngine, Some(V0)).get == hiveQueryGenerator)
    assert(qgenRegistry.getGenerator(PrestoEngine, Some(V0)).isEmpty)
  }
}
