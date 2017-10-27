// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.leveldb

import java.io._
import java.util.concurrent.atomic.AtomicInteger

import com.yahoo.maha.serde.SerDe
import grizzled.slf4j.Logging
import org.apache.commons.io.FileUtils
import org.fusesource.leveldbjni.JniDBFactory._
import org.iq80.leveldb._

import scala.collection.mutable
import scala.util.Try

object LevelDBAccessor {
  val instanceId = new AtomicInteger(0)
  def incrementAndGetId() : Int = instanceId.incrementAndGet()
}

class LevelDBAccessor[K, V](builder: LevelDBAccessorBuilder[K, V]) extends Logging {

  val baseDir = new File(builder.baseDir)
  baseDir.deleteOnExit()
  val options: Options = new Options()
  options.blockSize(builder.blockSize)
  options.cacheSize(builder.cacheSize)
  options.maxOpenFiles(builder.maxOpenFiles)
  options.writeBufferSize(builder.writeBufferSize)
  options.createIfMissing(builder.createIfMissing)
  options.paranoidChecks(true)
  val dbFileName = s"leveldb_${LevelDBAccessor.incrementAndGetId()}-${System.currentTimeMillis()}-${builder.dbName}"
  val dbFile: File = new File(baseDir,dbFileName)
  info(s"Creating new levelDB : ${dbFile.getAbsolutePath}")
  val db: DB = factory.open(dbFile, options)
  val keySerDe = builder.keySerDe
  val valSerDe = builder.valSerDe

  def put(key: K, value: V): Boolean = {
    if (key != null) {
      try {
        db.put(keySerDe.serialize(key), valSerDe.serialize(value))
        return true
      } catch {
        case e: Exception =>
          error(s"Failed to put '$key => $value' into LevelDB", e)
      }
    }
    false
  }

  def putBatch(inputBatch: mutable.Map[K, V], sync: Boolean = true): Boolean = {
    if (inputBatch != null || inputBatch.nonEmpty) {
      try {
        val writeOptions = new WriteOptions
        val batch: WriteBatch = db.createWriteBatch()
        inputBatch foreach { case (key, value) => batch.put(keySerDe.serialize(key), valSerDe.serialize(value)) }
        writeOptions.sync(sync)
        db.write(batch, writeOptions)
        batch.close()
        return true
      } catch {
        case e: Exception =>
          error(s"Failed to put batch of size '${inputBatch.size}' into Level DB", e)
      }
    }
    false
  }

  def get(key: K): Option[V] = {

    if (key != null) {
      try {
        return Option.apply(valSerDe.deserialize(db.get(keySerDe.serialize(key))))
      } catch {
        case e: Exception =>
        // Don't log if the key is not found, it slows down app startup
        //debug(s"Failed to get value for key: '$key' from LevelDB")
      }
    }
    None
  }

  def close() : Unit = {
    Try(db.close())
  }

  def destroy() : Unit = {
    info(s"Destroying levelDB : ${dbFile.getAbsolutePath}")
    try {
      factory.destroy(dbFile, new Options())
    } catch {
      case e: Exception =>
        warn("Failed to destroy levelDB", e)
    }
    Try(FileUtils.forceDelete(dbFile))
  }
}

class LevelDBAccessorBuilder[K, V](val dbName: String, val baseDirOption: Option[String] = None) {

  private val _1MB: Int = 1024 * 1024
  var blockSize: Int = _1MB
  var cacheSize: Int = 500 * _1MB
  var maxOpenFiles: Int = 1000
  var writeBufferSize: Int = 10 * _1MB
  var createIfMissing: Boolean = true
  var keySerDe: SerDe[K] = null
  var valSerDe: SerDe[V] = null
  val baseDir = baseDirOption.getOrElse("/home/y/tmp")

  def addBlockSize(blockSize: Int) = {
    this.blockSize = blockSize
    this
  }

  def addCacheSize(cacheSize: Int) = {
    this.cacheSize = cacheSize
    this
  }

  def addMaxOpenFiles(maxOpenFiles: Int) = {
    this.maxOpenFiles = maxOpenFiles
    this
  }

  def addWriteBufferSize(writeBufferSize: Int) = {
    this.writeBufferSize = writeBufferSize
    this
  }

  def addKeySerDe(keySerDe: SerDe[K]) = {
    this.keySerDe = keySerDe
    this
  }

  def addValSerDe(valSerDe: SerDe[V]) = {
    this.valSerDe = valSerDe
    this
  }

  def setCreateIfMissing(createIfMissing: Boolean) = {
    this.createIfMissing = createIfMissing
    this
  }

  def toLevelDBAccessor: LevelDBAccessor[K, V] = {
    new LevelDBAccessor(this)
  }
}