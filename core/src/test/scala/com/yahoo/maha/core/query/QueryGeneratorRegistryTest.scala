// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core.query.hive.HiveQueryGenerator
import com.yahoo.maha.core.{HiveEngine, PrestoEngine}
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class QueryGeneratorRegistryTest extends AnyFunSuite with Matchers {
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
    assert(qgenRegistry.isEngineRegistered(HiveEngine, Option(Version.v0)) == true)
    assert(qgenRegistry.isEngineRegistered(HiveEngine, Option(Version.v1)) == true)
  }

  test("Failed to register query generator repeatedly for the same engine and version") {
    val qgenRegistry = new QueryGeneratorRegistry()
    val hiveQueryGenerator = mock(classOf[HiveQueryGenerator])
    qgenRegistry.register(HiveEngine, hiveQueryGenerator, Version.v1)
    val exception = intercept[IllegalArgumentException] {
      qgenRegistry.register(HiveEngine, hiveQueryGenerator, Version.v1)
    }
    assert(exception.getMessage.contains("Query generator already defined for engine : Hive and version Version(1)"), exception.getMessage)
  }

  test("Test get query generator with version") {
    val qgenRegistry = new QueryGeneratorRegistry()
    val hiveQueryGenerator = mock(classOf[HiveQueryGenerator])
    qgenRegistry.register(HiveEngine, hiveQueryGenerator, Version.v0)
    qgenRegistry.register(HiveEngine, hiveQueryGenerator, Version.v1)
    qgenRegistry.register(HiveEngine, hiveQueryGenerator, Version.v2)
    assert(qgenRegistry.getValidGeneratorForVersion(HiveEngine, Version.v0, None).get == hiveQueryGenerator)
    assert(qgenRegistry.getValidGeneratorForVersion(PrestoEngine, Version.v0, None).isEmpty)
  }
}
