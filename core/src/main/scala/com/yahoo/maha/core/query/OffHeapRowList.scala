package com.yahoo.maha.core.query

import java.io.File
import java.util.UUID
import com.twitter.chill.ScalaKryoInstantiator
import com.yahoo.maha.core.query.QueryRowList.ROW_COUNT_ALIAS
import com.yahoo.maha.core.request.Parameter.RequestId
import com.yahoo.maha.rocksdb.{RocksDBAccessor, RocksDBAccessorBuilder}
import com.yahoo.maha.serde.{LongSerDe, SerDe}
import grizzled.slf4j.Logging
import org.apache.commons.io.{FileUtils}
import org.rocksdb.CompressionType

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/*
    Created by pranavbhole on 2/3/20
*/
case class OffHeapRowListConfig(tempStoragePathOption:Option[String],
                                inMemRowCountThreshold: Int = 10000,
                                rocksDBCacheSizeOption: Option[Int] = None,
                                rocksDBWriteBufferOption: Option[Int] = None)

case class RowValue(cols: collection.mutable.ArrayBuffer[Any]) {
  def toRow(aliasMap : Map[String, Int]) : Row = {
    Row(aliasMap, cols)
  }
}

object RowValueSerDe extends SerDe[RowValue] {
  override def serialize(t: RowValue): Array[Byte] = {
    ScalaKryoInstantiator.defaultPool.toBytesWithClass(t)
  }
  override def deserialize(bytes: Array[Byte]): RowValue = {
    ScalaKryoInstantiator.defaultPool.fromBytes(bytes).asInstanceOf[RowValue]
  }
}

case class OffHeapRowList(query: Query, config: OffHeapRowListConfig) extends QueryRowList with AutoCloseable with Logging {

  private[this] val tempStoragePath = OffHeapRowList.getAndValidateTempStorage(config.tempStoragePathOption)

  private[this] var rocksDBAccessorOption: Option[RocksDBAccessor[Long, RowValue]] = None

  private[this] var rowCount:Long = -1

  private[this] var rocksDBRowCount:Long = 0 // To be used for verification and monitoring

  protected[this] val list: collection.mutable.ArrayBuffer[Row] = {
    if(query.queryContext.requestModel.maxRows > 0) {
      new ArrayBuffer[Row](query.queryContext.requestModel.maxRows + 10)
    } else {
      collection.mutable.ArrayBuffer.empty[Row]
    }
  }

  private[this] def incrementAndGet(): Long = {
    rowCount+=1
    rowCount
  }

  def addRow(r: Row, er: Option[Row] = None) : Unit = {
    postResultRowOperation(r, er)
    if (rowCount >= config.inMemRowCountThreshold - 1) {
      persist(r)
    } else {
      list+=r
      incrementAndGet()
    }
  }

  private[this] def persist(row:Row): Unit =  {
    if(rocksDBAccessorOption.isEmpty) {
      rocksDBAccessorOption = Some(OffHeapRowList.initRocksDB(tempStoragePath, query, config))
    }
    rocksDBRowCount+=1
    rocksDBAccessorOption.get.put(incrementAndGet(), RowValue(row.cols))
  }

  private[this] def rocksDBGet(index:Long): Row = {
    require(index <= rowCount, s"Failed to get the index ${index} from rocksdb")
    require(rocksDBAccessorOption.isDefined, "Invalid get call, RocksDB is yet not initialized")
    val rowValueOption = rocksDBAccessorOption.get.get(index)
    require(rowValueOption.isDefined, s"Failed to get row at index ${index} from rocksDB")
    rowValueOption.get.toRow(aliasMap)
  }

  def isEmpty : Boolean = {
    if (rowCount < 0) {
      true
    } else false
  }

  override protected def kill(): Unit = {
    if(isDebugEnabled) {
      logStats
    }
    if (rocksDBAccessorOption.isDefined) {
      logStats()
      Try {
        rocksDBAccessorOption.get.destroy()
        rocksDBAccessorOption = None
        rowCount = -1
        list.clear()
      }
    }
  }

  def logStats(): Unit = {
    info(s"OffHeapRowList, Stats:-> inMemRowCountThreshold: ${config.inMemRowCountThreshold}, sizeOfInMemStore: ${sizeOfInMemStore()}, sizeOfRocksDB:${sizeOfRocksDB()}")
  }

  def foreach(fn: Row => Unit) : Unit = {
    list.foreach(fn)
    Range.Long.inclusive(config.inMemRowCountThreshold, rowCount, 1).foreach {
      rc =>
        fn(rocksDBGet(rc))
    }
  }

  def forall(fn: Row => Boolean) : Boolean = {
    list.forall(fn) && Range.Long.inclusive(config.inMemRowCountThreshold, rowCount, 1).forall {
      rc =>
        fn(rocksDBGet(rc))
    }
  }

  def map[T](fn: Row => T) : Iterable[T] = {
    list.map(fn) ++ Range.Long.inclusive(config.inMemRowCountThreshold, rowCount, 1).map {
      rc =>
        fn(rocksDBGet(rc))
    }
  }

  def size: Int = {
    if(rowCount < 0) {
      0
    } else rowCount.intValue() + 1
  }

  //def javaForeach(fn: ParCallable)
  override def getTotalRowCount: Int = {
    var total_count = 0
    val listAttempt = Try {
      val firstRow = if (config.inMemRowCountThreshold > 0) {
        list.head
      } else {
        rocksDBGet(0)
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
    rowCount.intValue() + 1
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
    rocksDBRowCount
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

  def initRocksDB(tempStoragePath:String, query: Query, config: OffHeapRowListConfig): RocksDBAccessor[Long, RowValue] = {
    val params = query.queryContext.requestModel.additionalParameters
    val uuid = if(params.contains(RequestId)) {
      params.get(RequestId)
    } else {
      UUID.randomUUID().toString
    }
    import RocksDBAccessor._1MB

    new RocksDBAccessorBuilder(s"off-heap-rowlist-${uuid}", Some(tempStoragePath))
      .addKeySerDe(LongSerDe)
      .addValSerDe(RowValueSerDe)
      .addCacheSize(config.rocksDBCacheSizeOption.getOrElse(10 * _1MB))
      .addWriteBufferSize(config.rocksDBWriteBufferOption.getOrElse(1 * _1MB))
      .setCompressionType(CompressionType.LZ4_COMPRESSION)
      .toRocksDBAccessor
  }

}
