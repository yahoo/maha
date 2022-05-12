// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.rocksdb

import com.yahoo.maha.rocksdb.RocksDBAccessor._1MB

import java.io._
import java.util.concurrent.atomic.AtomicInteger
import com.yahoo.maha.serde.SerDe
import grizzled.slf4j.Logging
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter._
import org.rocksdb._

import scala.collection.mutable
import scala.util.Try

object RocksDBAccessor extends Logging {
  val _1MB: Int = 1024 * 1024
  val instanceId = new AtomicInteger(0)
  val prefix = "rocksdb_"
  def incrementAndGetId() : Int = instanceId.incrementAndGet()
  def getDBName[K, V](builder: RocksDBAccessorBuilder[K, V]): String = {
    s"$prefix${RocksDBAccessor.incrementAndGetId()}-${System.currentTimeMillis()}-${builder.dbName}"
  }

  def listDBs(baseDir: String): Iterable[File] = {
    import scala.collection.JavaConverters._
    val filters = FileFilterUtils.and(DirectoryFileFilter.INSTANCE, new PrefixFileFilter(prefix))
    FileUtils.listFilesAndDirs(new File(baseDir), filters, filters).asScala.filter(_.getName.startsWith(prefix))
  }

  def cleanupBaseDir(baseDir: String): Unit = {
    for(dir <- listDBs(baseDir)) {
      info(s"Cleaning up rocksdb base dir path : ${dir.getAbsolutePath}")
      Try(FileUtils.deleteDirectory(dir))
    }
  }
}
class RocksDBAccessor[K, V](builder: RocksDBAccessorBuilder[K, V]) extends Logging {

  val baseDir = new File(builder.baseDir)
  baseDir.deleteOnExit()
  val dbFileName: String = RocksDBAccessor.getDBName(builder)
  val dbFile: File = new File(baseDir, dbFileName)
  info(s"Creating rocksDB : ${dbFile.getAbsolutePath}")
  val db: RocksDB = {
    val options: Options = new Options()
    val blockCacheOptions: BlockBasedTableConfig = new BlockBasedTableConfig
    blockCacheOptions.setBlockSize(builder.blockSize)
    blockCacheOptions.setBlockCacheSize(builder.cacheSize)
    options.setTableFormatConfig(blockCacheOptions)
    options.setMaxOpenFiles(builder.maxOpenFiles)
    options.setWriteBufferSize(builder.writeBufferSize)
    options.setCreateIfMissing(builder.createIfMissing)
    options.setCompressionType(builder.compressionType)
    options.setParanoidChecks(true)
    options.optimizeForPointLookup(builder.cacheSize / _1MB)
    options.setMemTableConfig(new HashSkipListMemTableConfig)
    options.setAllowConcurrentMemtableWrite(false)
    RocksDB.loadLibrary()
    if(builder.timeToLive.isDefined) {
      info(s"Creating rocksDB with timeToLive = ${builder.timeToLive.get} Seconds : ${dbFile.getAbsolutePath}")
      TtlDB.open(options, dbFile.getAbsolutePath, builder.timeToLive.get, false)
    } else {
      RocksDB.open(options, dbFile.getAbsolutePath)
    }
  }
  val keySerDe = builder.keySerDe
  val valSerDe = builder.valSerDe
  var closed = false

  def put(key: K, value: V): Boolean = {
    if (key != null) {
      try {
        db.put(keySerDe.serialize(key), valSerDe.serialize(value))
        return true
      } catch {
        case e: Exception =>
          error(s"Failed to put '$key => $value' into RocksDB", e)
      }
    }
    false
  }

  def remove(key: K): Boolean = {
    if(key != null)  {
      try {
        if(get(key).isDefined) {
          db.singleDelete(keySerDe.serialize(key))
          return true
        }
      } catch {
        case e:Exception=>
          error(s"Failed to delete key $key from RocksDB", e)
      }
    }
    false
  }

/*
Compact Range function discards the deleted or expired entries in the database,
it also push push the level to down as it clears the entries,
it is costly operation and has to be called in periodic or round robin fashion,
thus leaving it public to invoke it as per the implementation
 */
  def compactRange(): Unit = {
    if(db.isInstanceOf[TtlDB]) {
      db.compactRange()
    }
  }

  def putBatch(inputBatch: mutable.Map[K, V], sync: Boolean = true): Boolean = {
    if (inputBatch != null && inputBatch.nonEmpty) {
      val writeOptions: WriteOptions = new WriteOptions
      val batch: WriteBatch =  new WriteBatch()
      try {
        writeOptions.setSync(sync)
        inputBatch foreach { case (key, value) => batch.put(keySerDe.serialize(key), valSerDe.serialize(value)) }
        db.write(writeOptions, batch)
        return true
      } catch {
        case e: Exception =>
          error(s"Failed to put batch of size '${inputBatch.size}' into Level DB", e)
      } finally {
        writeOptions.close()
        batch.close()
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
          //info(s"Failed to get value for key: '$key' from RocksDB")
      }
    }
    None
  }

  def close() : Unit = synchronized {
    if(!closed) {
      Try {
        val flushOptions = new FlushOptions
        flushOptions.setWaitForFlush(true)
        db.flush(flushOptions)
        flushOptions.close()
      }
      Try(db.close())
      closed = true
    }
  }
  
  def destroy() : Unit = synchronized {
    Try(close())
    info(s"Destroying rocksDB: ${dbFile.getAbsolutePath}")
    Try(FileUtils.forceDelete(dbFile))
  }
}

class RocksDBAccessorBuilder[K, V](val dbName: String, val baseDirOption: Option[String] = None, val timeToLive: Option[Int] = None) {
  import RocksDBAccessor._1MB

  var blockSize: Int = _1MB
  var cacheSize: Int = 500 * _1MB
  var maxOpenFiles: Int = 1000
  var writeBufferSize: Int = 10 * _1MB
  var createIfMissing: Boolean = true
  var keySerDe: SerDe[K] = null
  var valSerDe: SerDe[V] = null
  val baseDir = baseDirOption.getOrElse("/home/y/tmp")
  var compressionType:CompressionType = CompressionType.LZ4HC_COMPRESSION

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

  def setCompressionType(compressionType: CompressionType) = {
    this.compressionType  = compressionType
    this
  }

  def toRocksDBAccessor: RocksDBAccessor[K, V] = {
    new RocksDBAccessor(this)
  }
}
