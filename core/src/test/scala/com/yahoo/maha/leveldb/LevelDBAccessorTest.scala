// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.leveldb

import com.yahoo.maha.serde.StringSerDe
import org.junit.Assert._

import org.scalatest.{ BeforeAndAfterAll, FunSuite, Matchers }

/**
 * Created by surabhip on 3/10/16.
 */
class LevelDBAccessorTest extends FunSuite with Matchers with BeforeAndAfterAll {

  private val levelDBAccessor: LevelDBAccessor[String, String] = new LevelDBAccessorBuilder("testdb", Some("/tmp")).addKeySerDe(StringSerDe).addValSerDe(StringSerDe).toLevelDBAccessor

  override protected def afterAll(): Unit = {
    levelDBAccessor.close()
    levelDBAccessor.destroy()
  }

  test("basic put and get operations should succeed") {
    val testKey = "test-key"
    val testVal = "test-val"

    assertTrue(levelDBAccessor.put(testKey, testVal))
    assertTrue(levelDBAccessor.get(testKey).isDefined)
    assertEquals(testVal, levelDBAccessor.get(testKey).get)
  }
  
  test("successfully perform put batch and should be able to retrieve") {
    val kv = new collection.mutable.HashMap[String, String]
    kv += "one" -> "1"
    kv += "two" -> "2"
    kv += "three" -> "3"
    levelDBAccessor.putBatch(kv)
    
    assert(levelDBAccessor.get("one").get === "1")
    assert(levelDBAccessor.get("two").get === "2")
    assert(levelDBAccessor.get("three").get === "3")
  }
}
