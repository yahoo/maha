// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.rocksdb

import java.util.concurrent.TimeUnit

import com.yahoo.maha.serde.StringSerDe
import org.junit.Assert._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.mutable

/**
 * Created by surabhip on 3/10/16.
 */
class RocksDBAccessorTest extends FunSuite with Matchers with BeforeAndAfterAll {

  private val rocksDBAccessor: RocksDBAccessor[String, String] = new RocksDBAccessorBuilder("testdb", Some("/tmp")).addKeySerDe(StringSerDe).addValSerDe(StringSerDe).toRocksDBAccessor

  override protected def afterAll(): Unit = {
    rocksDBAccessor.close()
    rocksDBAccessor.destroy()
  }

  test("basic put and get operations should succeed") {
    val testKey = "test-key"
    val testVal = "test-val"

    assertTrue(rocksDBAccessor.put(testKey, testVal))
    assertTrue(rocksDBAccessor.get(testKey).isDefined)
    assertEquals(testVal, rocksDBAccessor.get(testKey).get)
  }

  test("successfully perform put batch and should be able to retrieve") {
    val kv = new collection.mutable.HashMap[String, String]
    kv += "one" -> "1"
    kv += "two" -> "2"
    kv += "three" -> "3"
    rocksDBAccessor.putBatch(kv)

    assert(rocksDBAccessor.get("one").get === "1")
    assert(rocksDBAccessor.get("two").get === "2")
    assert(rocksDBAccessor.get("three").get === "3")
  }

  test("Test RocksDB Accessor with Time To Live") {

    val rocksDBAccessorTtl: RocksDBAccessor[String, String] = new RocksDBAccessorBuilder("testDbWithTimeToLive", Some("/tmp"), Some(1)).addKeySerDe(StringSerDe).addValSerDe(StringSerDe).toRocksDBAccessor
    val testKey = "test-key"
    val testVal = "test-val"

    assertTrue(rocksDBAccessorTtl.put(testKey, testVal))
    assertTrue(rocksDBAccessorTtl.get(testKey).isDefined)
    assertEquals(testVal, rocksDBAccessorTtl.get(testKey).get)
    TimeUnit.MILLISECONDS.sleep(2000)
    rocksDBAccessorTtl.compactRange()
    TimeUnit.MILLISECONDS.sleep(500)
    val value = rocksDBAccessorTtl.get(testKey)
    assert(value.isDefined == false)
  }

  test("Failure put") {
    val testKey = null
    val testVal = null
    assertFalse(rocksDBAccessor.put(testKey, testVal))
    assertFalse(rocksDBAccessor.putBatch(mutable.Map.empty))
    assertFalse(rocksDBAccessor.putBatch(null))
    assertEquals(None, rocksDBAccessor.get(null))
  }

  test("DB closed, error cases") {
    val _1MB : Int = 1024 * 1024
    val key : String = "key-val"
    val value : String = "value-val"
    val builder : RocksDBAccessor[String, String] = new RocksDBAccessorBuilder("mutable", Some("/tmp"))
      .addBlockSize(_1MB)
      .addCacheSize(500 * _1MB)
      .addMaxOpenFiles(1000)
      .addWriteBufferSize(10 * _1MB)
      .setCreateIfMissing(true)
      .toRocksDBAccessor
    builder.close
    builder.destroy()
    assertFalse(builder.put("key", "value"))
    assertFalse(builder.putBatch(mutable.Map(key->value)))
    assertEquals(None, builder.get(key))
  }

  test("test delete key") {
    val testKey = "test-key"
    val testVal = "test-val"

    assertTrue(rocksDBAccessor.remove(null) == false)

    assertTrue(rocksDBAccessor.put(testKey, testVal))
    assertTrue(rocksDBAccessor.get(testKey).isDefined)
    assertEquals(testVal, rocksDBAccessor.get(testKey).get)

    assertTrue(rocksDBAccessor.remove(testKey))
    assertTrue(rocksDBAccessor.get(testKey).isDefined == false)
    assertTrue(rocksDBAccessor.remove("unknown") == false)
    assertTrue(rocksDBAccessor.remove(testKey) == false)
  }

}
