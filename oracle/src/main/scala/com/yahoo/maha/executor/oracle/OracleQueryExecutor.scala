// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.executor.oracle

import java.math.MathContext
import java.sql.{ResultSet, ResultSetMetaData}

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.yahoo.maha.core._
import com.yahoo.maha.core.query._
import com.yahoo.maha.jdbc.JdbcConnection
import grizzled.slf4j.Logging
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}

import scala.math.BigDecimal.RoundingMode
import scala.util.{Failure, Try}

/**
 * Created by hiral on 12/22/15.
 */
object OracleQueryExecutor {
  final val DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  final val DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd")
}

class OracleQueryExecutor(jdbcConnection: JdbcConnection, lifecycleListener: ExecutionLifecycleListener) extends QueryExecutor with Logging {
  val engine: Engine = OracleEngine

  val mathContextCache = CacheBuilder
    .newBuilder()
    .maximumSize(100)
    .concurrencyLevel(Runtime.getRuntime.availableProcessors())
    .build(new CacheLoader[java.lang.Integer, MathContext]() {
    override def load(key: java.lang.Integer): MathContext = {
      new MathContext(key, java.math.RoundingMode.HALF_EVEN)
    }
  })
  
  private[this] def getBigDecimalSafely(resultSet: ResultSet, index: Int) : BigDecimal = {
    val result: java.math.BigDecimal = resultSet.getBigDecimal(index)
    if(result == null)
      return null
    result
  }
  
  private[this] def getLongSafely(resultSet: ResultSet, index: Int) : Long = {
    val result = getBigDecimalSafely(resultSet, index)
    if(result == null)
      return 0L
    result.longValue()
  }

  /*
  private[this] def getDoubleSafely(resultSet: ResultSet, index: Int) : Double = {
    val result = getBigDecimalSafely(resultSet, index)
    if(result == null)
      return 0D
    result.doubleValue()
  }*/
  
  def getColumnValue(index: Int, column: Column, resultSet: ResultSet) : Any = {
    column.dataType match {
      case IntType(_, sm, _, _, _) =>
        if(sm.isDefined) {
          resultSet.getString(index)
        } else {
          getLongSafely(resultSet, index)
        }
      case DecType(len: Int, scale, _, _, _, _) =>
        val result: java.lang.Double = {
          if (scale > 0 && len > 0) {
            val result = getBigDecimalSafely(resultSet, index)
            if(result != null) {
              val mc = mathContextCache.get(len)
              result.setScale(scale, RoundingMode.HALF_EVEN).round(mc).toDouble
            } else {
              null
            }
          } else if(scale > 0) {
            val result = getBigDecimalSafely(resultSet, index)
            if(result != null) {
              result.setScale(scale, RoundingMode.HALF_EVEN).toDouble
            } else {
              null
            }
          } else if(len > 0) {
            val result = getBigDecimalSafely(resultSet, index)
            if(result != null) {
              val mc = mathContextCache.get(len)
              result.setScale(10, RoundingMode.HALF_EVEN).round(mc).toDouble
            } else {
              null
            }
          } else {
            val result = getBigDecimalSafely(resultSet, index)
            if(result != null) {
              result.setScale(10, RoundingMode.HALF_EVEN).toDouble
            } else {
              null
            }
          }
        }
        result
      case dt: DateType =>
        if(dt.format.isDefined) {
          resultSet.getString(index)
        } else {
          val date = resultSet.getDate(index)
          if (date != null) {
            val dateTime = new DateTime(date)
            dateTime.toString(OracleQueryExecutor.DATE_FORMATTER)
          } else {
            null
          }
        }
      case tt: TimestampType =>
        val ts = resultSet.getTimestamp(index)
        if (ts != null) {
          val dateTime = new DateTime(ts, DateTimeZone.UTC)
          dateTime.toString(OracleQueryExecutor.DATE_TIME_FORMATTER)
        } else {
          null
        }
      case s: StrType =>
        resultSet.getString(index)
      case any => throw new UnsupportedOperationException(s"Unhandled data type for column value extraction : $any")
    }
  }

  def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes) : QueryResult[T] = {
    val acquiredQueryAttributes = lifecycleListener.acquired(query, queryAttributes)
    val debugEnabled = query.queryContext.requestModel.isDebugEnabled
    if(!acceptEngine(query.engine)) {
      throw new UnsupportedOperationException(s"OracleQueryExecutor does not support query with engine=${query.engine}")
    } else {
      var rowCount = 0
      rowList match {
        //if we have a indexed row list that's populated, we need to perform join
        case rl if rl.isInstanceOf[IndexedRowList] =>
          //do default row processing
          val irl = rl.asInstanceOf[IndexedRowList]
          val columnIndexMap = new collection.mutable.HashMap[String, Int]
          val aliasColumnMap = query.aliasColumnMap
          var index = -1
          val indexColumn = aliasColumnMap(irl.indexAlias)

          if(debugEnabled) {
            info(s"Running query : ${query.asString}")
          }
          val result : Try[Unit] = jdbcConnection.queryForObject(query.asString) { resultSet =>
            if (resultSet.next()) {
              var metaData: ResultSetMetaData = null
              if (metaData == null) {
                metaData = resultSet.getMetaData
                var count = 1
                while (count <= metaData.getColumnCount) {
                  //get alias
                  val alias = metaData.getColumnLabel(count)
                  if (alias == irl.indexAlias) {
                    index = count
                    columnIndexMap += alias -> count
                  } else {
                    columnIndexMap += alias -> count
                  }
                  if (debugEnabled) {
                    info(s"metaData index=$count label=$alias")
                  }
                  count += 1
                }
                if (debugEnabled) {
                  info(s"columnIndexMap from metaData ${columnIndexMap.toList.sortBy(_._1).mkString("\n", "\n", "\n")}")
                  info(s"aliasColumnMap from query ${aliasColumnMap.mapValues(_.name).toList.sortBy(_._1).mkString("\n", "\n", "\n")}")
                }
              }


              do {
                //get existing index row or create new one
                val rowSet = {
                  val indexValue = getColumnValue(index, indexColumn, resultSet)
                  val rowSet = irl.getRowByIndex(indexValue)
                  //no row, create one
                  if(rowSet.isEmpty) {
                    val r = irl.newRow
                    r.addValue(irl.indexAlias, indexValue)
                    irl.updateRow(r)
                    Set(r)
                  } else {
                    rowSet
                  }
                }

                val aliasValueMap =
                  for {
                    (alias, column) <- aliasColumnMap
                  } yield {
                    val index = columnIndexMap(alias)
                    val value = getColumnValue(index, column, resultSet)
                    (alias, value)
                  }

                rowSet.foreach {
                  row =>
                    aliasValueMap.foreach {
                      entry =>
                        row.addValue(entry._1, entry._2)
                    }
                }
                rowCount += 1
              } while (resultSet.next())
              if(debugEnabled) {
                info(s"rowCount = $rowCount, rowList.size = ${irl.size}, rowList.updatedSize = ${irl.updatedSize}")
              }
            }
          }
          result match {
            case Failure(e) =>
              Try(lifecycleListener.failed(query, acquiredQueryAttributes, e))
              throw e
            case _ =>
              QueryResult(rl, lifecycleListener.completed(query, acquiredQueryAttributes), QueryResultStatus.SUCCESS)
          }
        case rl =>
          var metaData : ResultSetMetaData = null
          val columnIndexMap = new collection.mutable.HashMap[String, Int]
          val aliasColumnMap = query.aliasColumnMap
          if(debugEnabled) {
            info(s"Running query : ${query.asString}")
          }
          val result : Try[Unit] = jdbcConnection.queryForObject(query.asString) { resultSet =>
            if (resultSet.next()) {
              if (metaData == null) {
                metaData = resultSet.getMetaData
                var count = 1
                while (count <= metaData.getColumnCount) {
                  //get alias
                  val alias = metaData.getColumnLabel(count)
                  columnIndexMap += alias -> count
                  if (debugEnabled) {
                    info(s"metaData index=$count label=$alias")
                  }
                  count += 1
                }
                if (debugEnabled) {
                  info(s"columnIndexMap from metaData ${columnIndexMap.toList.sortBy(_._1).mkString("\n", "\n", "\n")}")
                  info(s"aliasColumnMap from query ${aliasColumnMap.mapValues(_.name).toList.sortBy(_._1).mkString("\n", "\n", "\n")}")
                }
              }

              //process all columns
              do {
                val row = rl.newRow
                for {
                  (alias, column) <- aliasColumnMap
                } {
                  val index = columnIndexMap(alias)
                  val value = getColumnValue(index, column, resultSet)
                  row.addValue(alias, value)
                }
                rl.addRow(row)
                rowCount += 1
              } while (resultSet.next())
              if(debugEnabled) {
                info(s"rowCount = $rowCount")
              }
            }
          }
          result match {
            case Failure(e) =>
              Try(lifecycleListener.failed(query, acquiredQueryAttributes, e))
              throw e
            case _ =>
              QueryResult(rl, lifecycleListener.completed(query, acquiredQueryAttributes), QueryResultStatus.SUCCESS)
          }
      }
    }
  }
}
