// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.leveldb

import com.yahoo.maha.serde.StringSerDe
import org.junit.Assert._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.mutable

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

  test("Failure put") {
    val testKey = null
    val testVal = null
    assertFalse(levelDBAccessor.put(testKey, testVal))
    assertTrue(levelDBAccessor.putBatch(mutable.Map.empty))
    assertFalse(levelDBAccessor.putBatch(null))
    assertEquals(None, levelDBAccessor.get(null))
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

  test("Set LevelDBAccessorBuilder parameters") {
    val _1MB : Int = 1024 * 1024
    val builder = new LevelDBAccessorBuilder("mutable", Some("/tmp")).addBlockSize(_1MB).addCacheSize(500 * _1MB).addMaxOpenFiles(1000).addWriteBufferSize(10 * _1MB).setCreateIfMissing(true)
    assertTrue(builder.createIfMissing)
    assertEquals(builder.blockSize, _1MB)
    assertEquals(builder.cacheSize, 500 * _1MB)
    assertEquals(builder.maxOpenFiles, 1000)
    assertEquals(builder.writeBufferSize, 10 * _1MB)
  }
}
