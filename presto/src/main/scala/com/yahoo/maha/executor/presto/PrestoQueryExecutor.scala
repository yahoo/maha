// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.executor.presto

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

object PrestoQueryExecutor {
  final val DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  final val DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd")
}

class PrestoQueryExecutor(jdbcConnection: JdbcConnection, lifecycleListener: ExecutionLifecycleListener) extends QueryExecutor with Logging {
  val engine: Engine = PrestoEngine

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
            dateTime.toString(PrestoQueryExecutor.DATE_FORMATTER)
          } else {
            null
          }
        }
      case tt: TimestampType =>
        val ts = resultSet.getTimestamp(index)
        if (ts != null) {
          val dateTime = new DateTime(ts, DateTimeZone.UTC)
          dateTime.toString(PrestoQueryExecutor.DATE_TIME_FORMATTER)
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
      throw new UnsupportedOperationException(s"PrestoQueryExecutor does not support query with engine=${query.engine}")
    } else {
      var rowCount = 0
      var metaData: ResultSetMetaData = null
      val columnIndexMap = new collection.mutable.HashMap[String, Int]
      val aliasColumnMap = query.aliasColumnMap
      if (debugEnabled) {
        info(s"Running query : ${query.asString}")
      }
      val result: Try[Unit] = jdbcConnection.queryForObject(query.asString) { resultSet =>
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
            val row = rowList.newRow
            for {
              (alias, column) <- aliasColumnMap
            } {
              val index = columnIndexMap(alias)
              val value = getColumnValue(index, column, resultSet)
              row.addValue(alias, value)
            }
            rowList.addRow(row)
            rowCount += 1
          } while (resultSet.next())
          if (debugEnabled) {
            info(s"rowCount = $rowCount")
          }
        }
      }
      result match {
        case Failure(e) =>
          Try(lifecycleListener.failed(query, acquiredQueryAttributes, e))
          throw e
        case _ =>
          QueryResult(rowList, lifecycleListener.completed(query, acquiredQueryAttributes), QueryResultStatus.SUCCESS)
      }
    }
  }
}
