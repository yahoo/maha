// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core._
import com.yahoo.maha.parrequest2.future.ParFunction
import com.yahoo.maha.report.{RowCSVWriter, RowCSVWriterProvider}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
 * Created by hiral on 12/22/15.
 */

case class Row(aliasMap: Map[String, Int], cols: collection.mutable.ArrayBuffer[Any]) {
  def addValue(alias: String, value: Any) = {
    if (aliasMap.contains(alias)) {
      cols.update(aliasMap(alias), value)
    } else {
      throw new IllegalArgumentException(s"Failed to find value in aliasMap on addValue for alias=$alias, value: $value, aliasMap:$aliasMap")
    }
  }
  def addValue(index: Int, value: Any) = {
    cols.update(index, value)
  }
  def getValue(alias: String) : Any = {
    if(aliasMap.contains(alias)) {
      cols(aliasMap(alias))
    } else {
      throw new IllegalArgumentException(s"Failed to find value in aliasMap on getValue for alias=$alias")
    }
  }

  def sumValue(index: Int, value: Any, dataType: DataType): Unit = {
    val oldValue = getValue(index)
    dataType match {
      case IntType(_,_,_,_,_) =>
        cols.update(index, oldValue.toString.toInt + value.toString.toInt)
      case DecType(_,_,_,_,_,_) =>
        cols.update(index, oldValue.toString.toDouble + value.toString.toDouble)
      case StrType(_,_,_) =>
        cols.update(index, value)
      case _ =>
        cols.update(index, value)
    }
  }

  def getValue(index: Int) : Any = {
    require(index < cols.size, s"index on getValue must be < size, index=$index, size=${cols.size}")
    cols(index)
  }
  def getColumns : Iterable[Any] = cols

  def pretty : String = {
    val list = new mutable.LinkedHashSet[String]
    Try {
      aliasMap.map(e=> e._2 -> e._1).toList.sortBy(_._1).foreach {
        case (index, alias) =>
          list.add(s"$alias = ${cols(index)}")
      }
    }
    s"(${list.mkString(", ")})"
  }
}

sealed trait RowListLifeCycle {
  protected def start() : Unit
  def nextStage(): Unit = {}
  protected def end() : Unit

  def withLifeCycle[T](fn: => T) : T = {
    start()
    try {
      fn
    } finally {
      end()
    }
  }
}

trait RowList extends RowListLifeCycle {
  val columns: IndexedSeq[ColumnInfo]
  def addRow(r: Row, er: Option[Row] = None) : Unit
  def isEmpty : Boolean
  def foreach(fn: Row => Unit) : Unit
  def map[T](fn: Row => T) : Iterable[T]
  def javaForeach[U](fn: ParFunction[Row, U]) : Unit = {
    foreach(r => fn.apply(r))
  }
  def javaMap[U](fn: ParFunction[Row, U]) : java.lang.Iterable[U] = {
    import collection.JavaConverters._
    map(r => fn.apply(r)).asJava
  }
  def getTotalRowCount : Int = {
    0
  }
  protected def start() : Unit = {
    //do nothing
  }
  protected def end() : Unit = {
    //do nothing
  }
}

object QueryRowList {
  val ROW_COUNT_ALIAS: String = "TOTALROWS"
}

import com.yahoo.maha.core.query.QueryRowList._
trait QueryRowList extends RowList {

  override val columns: IndexedSeq[ColumnInfo] = query.queryContext.requestModel.requestCols

  def query: Query

  def subQuery: IndexedSeq[Query] = IndexedSeq.empty

  def columnNames: IndexedSeq[String] = {
    (query.queryContext.requestModel.requestCols.map(_.alias) ++ query.additionalColumns ++ query.queryContext.indexAliasOption.toIndexedSeq).distinct
  }

  def ephemeralColumnNames: IndexedSeq[String] = {
    query.ephemeralAliasColumnMap.keySet.toIndexedSeq
  }

  protected[this] val constantColMap: Map[String, String] = query.queryContext.requestModel.requestCols.view.filter(_.isInstanceOf[ConstantColumnInfo]).map(ci => ci.asInstanceOf[ConstantColumnInfo]).map(ci => ci.alias -> ci.value).toMap
  protected[this] val aliasMap : Map[String, Int] = columnNames.zipWithIndex.toMap

  protected[this] val ephemeralAliasMap : Map[String, Int] = ephemeralColumnNames.zipWithIndex.toMap

  protected [this] val postResultColumnMap: Map[String, PostResultColumn] = {
    val aMap = new mutable.HashMap[String, PostResultColumn]()
    query.aliasColumnMap.map {
      case (alias, column) => if (column.isInstanceOf[PostResultColumn]) {
        aMap += (alias -> column.asInstanceOf[PostResultColumn])
      }
    }
    aMap.toMap
  }

  def newRow: Row = {
    val r = new Row(aliasMap, ArrayBuffer.fill[Any](columnNames.size)(null))
    constantColMap.foreach {
      case (alias, value) => r.addValue(alias, value)
    }
    r
  }

  def newEphemeralRow: Row = {
    new Row(ephemeralAliasMap, ArrayBuffer.fill[Any](ephemeralColumnNames.size)(null))
  }

  def postResultRowOperation(row:Row, ephemeralRowOption:Option[Row]) : Unit = {

    postResultColumnMap.foreach {
      case (columnAlias, prCol) =>
        val rowData: RowData = new PostResultRowData(row, ephemeralRowOption, columnAlias)
        prCol.postResultFunction.resultApply(rowData)
    }
  }

}

trait InMemRowList extends QueryRowList {

  private val logger = LoggerFactory.getLogger(classOf[InMemRowList])

  protected[this] val list: collection.mutable.ArrayBuffer[Row] = {
    if(query.queryContext.requestModel.maxRows > 0) {
      new ArrayBuffer[Row](query.queryContext.requestModel.maxRows + 10)
    } else {
      collection.mutable.ArrayBuffer.empty[Row]
    }
  }
  def addRow(r: Row, er: Option[Row] = None) : Unit = {
    postResultRowOperation(r, er)
    list += r
  }

  def isEmpty : Boolean = list.isEmpty

  def foreach(fn: Row => Unit) : Unit = {
    list.foreach(fn)
  }

  def map[T](fn: Row => T) : Iterable[T] = {
    list.map(fn)
  }

  def size: Int = list.size

  //def javaForeach(fn: ParCallable)
  override def getTotalRowCount: Int = {
    var total_count = 0
    val listAttempt = Try {
      val firstRow = list.head

      require(firstRow.aliasMap.contains(ROW_COUNT_ALIAS), "TOTALROWS not defined in alias map, only valid in Oracle Queries")
      val totalrow_col_num = firstRow.aliasMap(ROW_COUNT_ALIAS)
      val current_totalrows = firstRow.cols(totalrow_col_num).toString.toInt

      total_count = current_totalrows
    }

    if(!listAttempt.isSuccess){
      logger.warn("Failed to get total row count.\n" + listAttempt)
    }

    total_count
  }

}

case class CompleteRowList(query: Query) extends InMemRowList

sealed trait IndexedRowList extends InMemRowList {
  def indexAlias: String
  def getRowByIndex(indexValue: Any) : scala.collection.Set[Row]
  def updateRow(r: Row) : Unit
  def keys  : Iterable[Any]
  def addSubQuery(query: Query) : Unit
  def updatedSize : Int
  def isUpdatedRowListEmpty: Boolean
}

object DimDrivenIndexedRowList {
  val logger: Logger = LoggerFactory.getLogger(classOf[DimDrivenIndexedRowList])
}
sealed trait DimDrivenIndexedRowList extends IndexedRowList {

  protected[this] val aliasRowMap = new collection.mutable.HashMap[String, Set[(Row, Int)]]

  protected[this] val subQueryList = new ArrayBuffer[Query]()

  protected[this] val updatedRowSet = new collection.mutable.TreeSet[Int]

  override def subQuery: IndexedSeq[Query] = subQueryList

  val logger: Logger = DimDrivenIndexedRowList.logger
  def indexAlias: String

  def getRowByIndex(indexValue: Any) : scala.collection.Set[Row] = {
    val rowSetOption =  aliasRowMap.get(indexValue.toString)
    rowSetOption.fold(scala.collection.Set.empty[Row]) {
      rowSet =>
        rowSet.map {
          row =>
            updatedRowSet += row._2
            row._1
        }
    }
  }

  override def addRow(r: Row, er: Option[Row] = None) : Unit = {
    postResultRowOperation(r, er)
    val primaryKeyAny = r.getValue(indexAlias)
    if(primaryKeyAny == null) {
      logger.error(s"Index alias ($indexAlias) value is null on addRow, dropping row : $r")
    } else {
      val primaryKeyValue = primaryKeyAny.toString
      if (aliasRowMap.contains(primaryKeyValue)) {
        val rowSet = aliasRowMap(primaryKeyValue)
        //perform update
        rowSet.foreach {
          case (existingRow, existingRowIndex) =>
            r.aliasMap.foreach {
              case (alias, index) =>
                val newValue = r.getValue(index)
                if (newValue != null) {
                  existingRow.addValue(index, newValue)
                }
            }
            updatedRowSet += existingRowIndex
        }
      } else {
        //add new row
        val idx = list.size
        list += r
        //since it is dim driven, we should never have more than one value in the set so always overriding with new set
        val rowSet: Set[(Row, Int)] = Set((r, idx))
        aliasRowMap.put(primaryKeyValue, rowSet)
      }
    }
  }

  //used by subsequent query when back filling dim rows
  def updateRow(r: Row) : Unit = {
    val primaryKeyAny = r.getValue(indexAlias)
    if(primaryKeyAny == null) {
      logger.error(s"Index alias ($indexAlias) value is null on updateRow, dropping row : $r")
    } else {
      val primaryKeyValue = primaryKeyAny.toString
      if (aliasRowMap.contains(primaryKeyValue)) {
        val rowSet = aliasRowMap(primaryKeyValue)
        //perform update
        rowSet.foreach {
          case (existingRow, existingRowIndex) =>
            r.aliasMap.foreach {
              case (alias, index) =>
                val newValue = r.getValue(index)
                if (newValue != null) {
                  existingRow.addValue(index, newValue)
                }
            }
            updatedRowSet += existingRowIndex
        }
      } else {
        //add new row
        val idx = list.size
        list += r
        //since it is dim driven, we should never have more than one value in the set so always overriding with new set
        val rowSet: Set[(Row, Int)] = Set((r, idx))
        aliasRowMap.put(primaryKeyValue, rowSet)
        updatedRowSet += idx
      }
    }
  }

  def keys  : Iterable[Any] = aliasRowMap.keys

  def addSubQuery(query: Query) : Unit = subQueryList += query

  def updatedSize : Int = updatedRowSet.size

  def isUpdatedRowListEmpty = updatedRowSet.isEmpty

}

object FactDrivenIndexedRowList {
  val logger: Logger = LoggerFactory.getLogger(classOf[FactDrivenIndexedRowList])
}
sealed trait FactDrivenIndexedRowList extends IndexedRowList {

  protected[this] val aliasRowMap = new collection.mutable.HashMap[String, Set[(Row, Int)]]

  protected[this] val subQueryList = new ArrayBuffer[Query]()

  protected[this] val updatedRowSet = new collection.mutable.TreeSet[Int]

  override def subQuery: IndexedSeq[Query] = subQueryList

  val logger: Logger = FactDrivenIndexedRowList.logger
  def indexAlias: String

  def getRowByIndex(indexValue: Any) : scala.collection.Set[Row] = {
    val rowSetOption =  aliasRowMap.get(indexValue.toString)
    rowSetOption.fold(scala.collection.Set.empty[Row]) {
      rowSet =>
        rowSet.map {
          row =>
            updatedRowSet += row._2
            row._1
        }
    }
  }

  override def addRow(r: Row, er: Option[Row] = None) : Unit = {
    postResultRowOperation(r, er)
      val primaryKeyAny = r.getValue(indexAlias)
      if(primaryKeyAny == null) {
        logger.error(s"Index alias ($indexAlias) value is null on addRow, dropping row : $r")
      } else {
        val primaryKeyValue = primaryKeyAny.toString
        //add new row
        val idx = list.size
        list += r
        val existingSetOption = aliasRowMap.get(primaryKeyValue)
        if (existingSetOption.isDefined) {
          val rowSet: Set[(Row, Int)] = existingSetOption.get ++ Set((r, idx))
          aliasRowMap.put(primaryKeyValue, rowSet)
        } else {
          val rowSet: Set[(Row, Int)] = Set((r, idx))
          aliasRowMap.put(primaryKeyValue, rowSet)
        }
      }
    }

  //used by subsequent query when back filling dim rows : Fact Driven Case
  def updateRow(r: Row) : Unit = {
    val primaryKeyAny = r.getValue(indexAlias)
    if(primaryKeyAny == null) {
      logger.error(s"Index alias ($indexAlias) value is null on updateRow, dropping row : $r")
    } else {
      val primaryKeyValue = primaryKeyAny.toString
      if (aliasRowMap.contains(primaryKeyValue)) {
        aliasRowMap.get(primaryKeyValue).get.foreach {
          entry =>
            val (existingRow, existingRowIndex) = (entry._1, entry._2)
            //perform update
            r.aliasMap.foreach {
              case (alias, index) =>
                val newValue = r.getValue(index)
                if (newValue != null) {
                  existingRow.addValue(index, newValue)
                }
            }
            updatedRowSet += existingRowIndex
        }
      } else {
        //since fact driven, update of row should not add new row, ignore the row
      }
    }
  }

  def keys  : Iterable[Any] = aliasRowMap.keys

  def addSubQuery(query: Query) : Unit = subQueryList += query

  def updatedSize : Int = updatedRowSet.size

  def isUpdatedRowListEmpty = updatedRowSet.isEmpty

}

case class DimDrivenPartialRowList(indexAlias: String, query: Query) extends DimDrivenIndexedRowList
case class FactDrivenPartialRowList(indexAlias: String, query: Query) extends FactDrivenIndexedRowList
case class DimDrivenFactOrderedPartialRowList(indexAlias: String, query: Query) extends DimDrivenIndexedRowList {

  private[this] val model = query.queryContext.requestModel

  override def foreach(fn: Row => Unit) : Unit = {
    if(model.hasNonFKDimFilters) {
      //we request N rows, druid returns 2*N rows, oracle gives us N rows, we return N rows
      //we request N rows, druid returns 2*N rows, oracle gives us N-m rows, we return N-m rows
      //we request N rows, druid returns N-m rows, oracle gives us N-m rows, we return N-m rows
      //we request N rows, druid returns N-m rows, oracle gives us N-m-p rows, we return N-m rows
      //we request N rows, druid returns N-m rows, oracle gives us N rows, we return N rows
      if(updatedRowSet.size == model.maxRows && list.size == model.maxRows) {
        super.foreach(fn)
      } else {
        var count = 0
        list.view.zipWithIndex.filter(tpl => updatedRowSet(tpl._2)).foreach {
          tpl =>
            if(count < model.maxRows) {
              fn(tpl._1)
              count += 1
            }
        }
      }
    } else {
      super.foreach(fn)
    }
  }

  override def map[T](fn: Row => T) : Iterable[T] = {
    if(model.hasNonFKDimFilters) {
      if(updatedRowSet.size == model.maxRows && list.size == model.maxRows) {
        super.map(fn)
      } else {
        var count = 0
        list.view.zipWithIndex.filter(tpl => updatedRowSet(tpl._2)).collect {
          case tpl if count < model.maxRows =>
            count += 1
            fn(tpl._1)
        }
      }
    } else {
      super.map(fn)
    }
  }

  override def isEmpty : Boolean = updatedRowSet.isEmpty
}

object UnionViewRowList {
  val logger = LoggerFactory.getLogger(classOf[UnionViewRowList])
}
case class UnionViewRowList(indexAliasComposite:Set[String]
                            , query: Query
                            , factAliasToDataTypeMap: Map[String, DataType]
                            , constAliasToValueMapList: List[Map[String, String]]) extends InMemRowList {

  protected[this] val aliasRowMap = new collection.mutable.HashMap[Set[String], Set[(Row, Int)]]

  protected[this] val subQueryList = new ArrayBuffer[Query]()

  protected[this] val updatedRowSet = new collection.mutable.TreeSet[Int]

  private val logger = UnionViewRowList.logger

  private var stageCount = 0
  private var allConstAliasToValueMap : Map[String, String] = constAliasToValueMapList(stageCount)
  private var requestedConstAliasToValueMap : Map[String, String] = allConstAliasToValueMap
    .filter(e=> query.queryContext.requestModel.requestColsSet.contains(e._1))

  private val constantFiltersAliasToValuesMap : Map[String, Set[String]] = {
    val constFilterAliastoValuesMapTemp = new mutable.HashMap[String, Set[String]]()
    query.queryContext.requestModel.factFilters.foreach {
      filter =>
        if(requestedConstAliasToValueMap.contains(filter.field)) {
          filter match {
            case EqualityFilter(field, value, _, _) =>
              constFilterAliastoValuesMapTemp += (field-> Set(value))
            case InFilter(field, values, _, _) =>
              constFilterAliastoValuesMapTemp += (field-> values.toSet)
            case f =>
              throw new IllegalArgumentException(s"Unsupported filter operation on constant Field : $f")
          }
        }
    }
    constFilterAliastoValuesMapTemp.toMap
  }

  override def subQuery: IndexedSeq[Query] = subQueryList

  override def nextStage(): Unit = {
    stageCount+=1
    allConstAliasToValueMap = constAliasToValueMapList(stageCount)
    requestedConstAliasToValueMap = allConstAliasToValueMap
      .filter(e=> query.queryContext.requestModel.requestColsSet.contains(e._1))
  }
  /*
   At the end of the RowList LifeCycle, applying constant column filters
    and removing rows from the final list
   */
  override def end() : Unit = {
    val filteredRows = new mutable.ArrayBuffer[Row]
    list.foreach {
      row=>
      constantFiltersAliasToValuesMap.foreach {
        entry=>
          val value = row.getValue(entry._1)
          if(value!= null && !entry._2.contains(value.toString)) {
            filteredRows+=row
          }
      }
    }
    list--=filteredRows
  }

  /*
  def withConstantMap(constAliasToValueMap: Map[String, String]) : RowList = {
    this.constAliasToValueMap = constAliasToValueMap
    this
  }*/

  def getRowByIndexSet(indexAliasKeys: Set[Any]) : scala.collection.Set[Row] = {
    val rowSetOption =  aliasRowMap.get(indexAliasKeys.map(s=> s.toString))
    rowSetOption.fold(scala.collection.Set.empty[Row]) {
      rowSet =>
        rowSet.map {
          row =>
            updatedRowSet += row._2
            row._1
        }
    }
  }

  override def addRow(r: Row, er: Option[Row] = None) : Unit = {
    postResultRowOperation(r, er)
    updateRow(r)
  }

  //used by subsequent query when back filling dim rows : Fact Driven Case
  def updateRow(r: Row) : Unit = {
    //updating Constant Values
    requestedConstAliasToValueMap.foreach {
      entry => r.addValue(entry._1, entry._2)
    }

    val compositeKey = {
      try {
        indexAliasComposite.map(index => r.getValue(index).toString)
      } catch {
        case e:Exception=>
          logger.error(s"Found one of the index aliases as null =$r " +
            s"indexAliasComposite = $indexAliasComposite")
          return
      }
    }

    if(aliasRowMap.contains(compositeKey)) {
      aliasRowMap.get(compositeKey).get.foreach {
        entry =>
          val (existingRow, existingRowIndex) = (entry._1, entry._2)
          //perform update
          r.aliasMap.foreach {
            case (alias, index) =>
              val newValue = r.getValue(index)
              if(newValue != null && !indexAliasComposite.contains(alias)) {
                existingRow.sumValue(index, newValue, factAliasToDataTypeMap(alias))
              }
          }
          updatedRowSet += existingRowIndex
      }
    } else {
      //add new row
      val idx = list.size
      list += r
      val rowSet: Set[(Row, Int)] = Set((r,idx))
      aliasRowMap.put(compositeKey, rowSet)
      updatedRowSet += idx
    }
  }

  def keys  : Iterable[Any] = aliasRowMap.keys

  def addSubQuery(query: Query) : Unit = subQueryList += query

  def updatedSize : Int = updatedRowSet.size

  def isUpdatedRowListEmpty = updatedRowSet.isEmpty

}

case class NoopRowList(query: Query) extends QueryRowList {

  override def addRow(r: Row, er: Option[Row] = None): Unit = {
    throw new UnsupportedOperationException("addRow not implemented!")
  }

  override def isEmpty: Boolean = {
    throw new UnsupportedOperationException("isEmpty not implemented!")
  }

  override def foreach(fn: (Row) => Unit): Unit = {
    throw new UnsupportedOperationException("foreach not implemented!")
  }

  override def map[T](fn: (Row) => T): Iterable[T] = {
    throw new UnsupportedOperationException("map not implemented!")
  }
}

object CSVRowList {
  private final val logger: Logger = LoggerFactory.getLogger(classOf[CSVRowList])
}

/**
  * CSV row list.  Not thread safe
  * @param query
  * @param csvWriterProvider
  * @param writeHeader
  */
class CSVRowList(val query: Query, csvWriterProvider: RowCSVWriterProvider, writeHeader: Boolean) extends QueryRowList {

  private[this] var started = false
  private[this] var ended = false
  private[this] var rowsWritten: Int = 0
  private[this] lazy val csvWriter: RowCSVWriter = csvWriterProvider.newRowCSVWriter

  override protected def start(): Unit = {
    if(!started) {
      started = true
      if (writeHeader) {
        val outputColumnNames = query.queryContext.requestModel.reportingRequest.selectFields.map(f => f.alias.getOrElse(f.field)) ++ query.additionalColumns
        csvWriter.writeColumnNames(outputColumnNames)
      }
    }
  }

  override protected def end(): Unit = {
    if(started && !ended) {
      try {
        csvWriter.close()
      } catch {
        case t: Throwable =>
          CSVRowList.logger.error("Failed on end", t)
          throw t
      } finally {
        ended = true
      }
    }
  }

  override def addRow(r: Row, er: Option[Row] = None): Unit = {
    if(started) {
      postResultRowOperation(r, er)
      csvWriter.writeRow(r, columnNames)
    } else {
      throw new IllegalStateException("Cannot addRow without calling start first")
    }
  }

  override def isEmpty : Boolean = true
  override def foreach(fn: Row => Unit) : Unit = {
    CSVRowList.logger.warn("foreach not supported on CSVRowList")
  }
  override def map[T](fn: Row => T) : Iterable[T] = {
    CSVRowList.logger.warn("map not supported on CSVRowList")
    Iterable.empty
  }
}

class DerivedRowList(override val columns: IndexedSeq[ColumnInfo]
                     , val initialSize: Int = 1
                     , val sortedList: collection.mutable.ArrayBuffer[Row] = ArrayBuffer.empty
                     , val drivingQuery: Query
                    ) extends RowList {

  val list: collection.mutable.ArrayBuffer[Row] = {
    if(sortedList.nonEmpty) {
      sortedList
    } else {
      new ArrayBuffer[Row](initialSize)
    }
  }

  override def addRow(r: Row, er: Option[Row]): Unit = {
    list += r
  }

  override def isEmpty: Boolean = {
    list.isEmpty
  }

  override def foreach(fn: (Row) => Unit): Unit = {
    list.foreach(fn)
  }

  override def map[T](fn: (Row) => T): Iterable[T] = {
    list.map(fn)
  }

}

trait RowData {
  val columnAlias: String
  def getInt(name: String): Option[Int]
  def getLong(name: String): Option[Long]
  def get(name: String): Option[String]
  def setValue(any: Any): Unit
}

case class PostResultRowData(r: Row, er: Option[Row] = None, columnAlias: String) extends RowData {

  override def setValue(any: Any): Unit = {
    r.addValue(columnAlias, any)
  }

  override def getInt(name: String): Option[Int] = {
    if (er.isDefined && er.get.aliasMap.contains(name)) {
      val value = er.get.getValue(name)
      Try(value.toString.toInt).toOption
    } else if (r.aliasMap.contains(name)) {
      val value = r.getValue(name)
      Try(value.toString.toInt).toOption
    } else {
      None
    }
  }

  override def getLong(name: String): Option[Long] = {
    if (er.isDefined && er.get.aliasMap.contains(name)) {
      val value = er.get.getValue(name)
      Try(value.toString.toLong).toOption
    } else if (r.aliasMap.contains(name)) {
      val value = r.getValue(name)
      Try(value.toString.toLong).toOption
    } else {
      None
    }
  }

  override def get(name: String): Option[String] = {
    if (er.isDefined && er.get.aliasMap.contains(name)) {
      Option(er.get.getValue(name).toString)
    } else if (r.aliasMap.contains(name)) {
      Option(r.getValue(name).toString)
    } else {
      None
    }
  }
}

