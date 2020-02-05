package com.yahoo.maha.core.query

import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import com.yahoo.maha.core.query.QueryRowList.ROW_COUNT_ALIAS
import com.yahoo.maha.core.request.Parameter.RequestId
import com.yahoo.maha.rocksdb.{RocksDBAccessor, RocksDBAccessorBuilder}
import com.yahoo.maha.serde.LongSerDe
import grizzled.slf4j.Logging
import org.apache.commons.io.{FileUtils, FilenameUtils}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/*
    Created by pranavbhole on 2/3/20
*/
case class OffHeapRowListConfig(tempStoragePathOption:Option[String], inMemRowCountThreshold: Int = 10000)

case class OffHeapRowList(query: Query, config: OffHeapRowListConfig) extends QueryRowList with AutoCloseable with Logging {

  private[this] val tempStoragePath = OffHeapRowList.getAndValidateTempStorage(config.tempStoragePathOption)

  private[this] var rocksDBAccessorOption: Option[RocksDBAccessor[Long, Row]] = None

  private[this] val atomicLongRowCount = new AtomicLong(-1)

  private[this] val rocksDBRowCount = new AtomicLong(0) // To be used for verification and monitoring

  protected[this] val list: collection.mutable.ArrayBuffer[Row] = {
    if(query.queryContext.requestModel.maxRows > 0) {
      new ArrayBuffer[Row](query.queryContext.requestModel.maxRows + 10)
    } else {
      collection.mutable.ArrayBuffer.empty[Row]
    }
  }

  def addRow(r: Row, er: Option[Row] = None) : Unit = {
    postResultRowOperation(r, er)
    if (atomicLongRowCount.get() >= config.inMemRowCountThreshold - 1) {
      persist(r)
    } else {
      list+=r
      atomicLongRowCount.incrementAndGet()
    }
  }

  private[this] def persist(row:Row): Unit =  {
    if(rocksDBAccessorOption.isEmpty) {
      rocksDBAccessorOption = Some(OffHeapRowList.initRocksDB(tempStoragePath, query))
    }
    rocksDBRowCount.incrementAndGet()
    rocksDBAccessorOption.get.put(atomicLongRowCount.incrementAndGet(), row)
  }

  private[this] def rocksDBGet(index:Long): Row = {
    require(index <= atomicLongRowCount.get(), s"Failed to get the index ${index} from rocksdb")
    require(rocksDBAccessorOption.isDefined, "Invalid get call, RocksDB is yet not initialized")
    val rowOption = rocksDBAccessorOption.get.get(index)
    require(rowOption.isDefined, s"Failed to get row at index ${index} from rocksDB")
    rowOption.get
  }

  def isEmpty : Boolean = {
    if (atomicLongRowCount.get() < 0) {
      true
    } else false
  }

  override protected def kill(): Unit = {
    info(s"called killed on OffHeapRowList, Stats:-> inMemRowCountThreshold: ${config.inMemRowCountThreshold}, sizeOfInMemStore: ${sizeOfInMemStore()}, sizeOfRocksDB:${sizeOfRocksDB()}")
    if (rocksDBAccessorOption.isDefined) {
      Try {
        rocksDBAccessorOption.get.destroy()
        rocksDBAccessorOption = None
        atomicLongRowCount.set(-1)
        list.clear()
      }
    }
  }

  def foreach(fn: Row => Unit) : Unit = {
    list.foreach(fn)
    Range.Long.inclusive(config.inMemRowCountThreshold, atomicLongRowCount.get, 1).foreach {
      rc =>
        fn(rocksDBGet(rc))
    }
  }

  def forall(fn: Row => Boolean) : Boolean = {
    list.forall(fn) && Range.Long.inclusive(config.inMemRowCountThreshold, atomicLongRowCount.get, 1).forall {
      rc =>
        fn(rocksDBGet(rc))
    }
  }

  def map[T](fn: Row => T) : Iterable[T] = {
    list.map(fn) ++ Range.Long.inclusive(config.inMemRowCountThreshold, atomicLongRowCount.get, 1).map {
      rc =>
        fn(rocksDBGet(rc))
    }
  }

  def size: Int = {
    if(atomicLongRowCount.get() < 0) {
      0
    } else atomicLongRowCount.intValue() + 1
  }

  //def javaForeach(fn: ParCallable)
  override def getTotalRowCount: Int = {
    var total_count = 0
    val listAttempt = Try {
      val firstRow = if (config.inMemRowCountThreshold > 0) {
        list.head
      } else {
        rocksDBAccessorOption.get.get(0).get
      }
      require(firstRow.aliasMap.contains(ROW_COUNT_ALIAS), "TOTALROWS not defined in alias map")
      val totalrow_col_num = firstRow.aliasMap(ROW_COUNT_ALIAS)
      val current_totalrows = firstRow.cols(totalrow_col_num).toString.toInt
      total_count = current_totalrows
    }

    if (!listAttempt.isSuccess) {
      warn("Failed to get total row count.\n" + listAttempt)
    }

    total_count
  }

  override def length : Int = {
    atomicLongRowCount.intValue() + 1
  }

  override def close(): Unit = {
    kill()
  }

  def isPersistentStoreInitialized(): Boolean = {
    rocksDBAccessorOption.isDefined
  }

  def sizeOfInMemStore(): Int = {
    list.size
  }

  def sizeOfRocksDB(): Long = {
    rocksDBRowCount.longValue()
  }

}

object OffHeapRowList extends Logging {
  final val MAHA_OFF_HEAP_STORAGE_PATH ="MAHA_OFF_HEAP_STORAGE_PATH"
  final val DIRNAME = "off_heap_row_list"

  def getAndValidateTempStorage(tempStoragePathOption:Option[String]): String = {
    val tempStoragePath = if (tempStoragePathOption.isDefined) {
      tempStoragePathOption.get
    } else {
      System.getenv(OffHeapRowList.MAHA_OFF_HEAP_STORAGE_PATH)
    }

    val file = FileUtils.getFile(tempStoragePath)

    require(file.exists(), s"Failed to find the temp storage location $tempStoragePath")
    require(file.isDirectory, s"Temp storage location should be directory")
    val newFile = new File(tempStoragePath, DIRNAME)
    if (!newFile.exists()) {
      newFile.mkdirs()
      info(s"Created tmp dir to store off heap row list ${newFile.getAbsolutePath}")
    }
    require(newFile.exists(), s"Failed to make temp directory to store off heap row list ${newFile.getAbsolutePath}")
    newFile.getAbsolutePath
  }

  def initRocksDB(tempStoragePath:String, query: Query): RocksDBAccessor[Long, Row] = {
    val params = query.queryContext.requestModel.additionalParameters
    val uuid = if(params.contains(RequestId)) {
      params.get(RequestId)
    } else {
      UUID.randomUUID().toString
    }

    new RocksDBAccessorBuilder(s"off-heap-rowlist-${uuid}", Some(tempStoragePath))
      .addKeySerDe(LongSerDe)
      .addValSerDe(RowSerDe)
      .toRocksDBAccessor
  }

}
