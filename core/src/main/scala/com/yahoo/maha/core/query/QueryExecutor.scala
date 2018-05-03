// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import java.math.MathContext
import java.sql.ResultSet

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.yahoo.maha.core.{StrType, TimestampType, _}
import grizzled.slf4j.Logging
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}

import scala.math.BigDecimal.RoundingMode

/**
 * Created by hiral on 12/22/15.
 */

trait QueryExecutor {
  def engine: Engine
  def acceptEngine(engine: Engine) : Boolean = engine == this.engine
  def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes) : QueryResult[T]
}

class QueryExecutorContext {
  //maintain map of executors by engine
  var executorMap : Map[Engine, QueryExecutor] = Map.empty

  def register(executor: QueryExecutor) : Unit = synchronized {
    require(!executorMap.contains(executor.engine), s"QueryExecutor for engine=${executor.engine} already registered!")
    executorMap += executor.engine -> executor
  }
  
  def remove(executor: QueryExecutor) : Unit = synchronized {
    require(executorMap.contains(executor.engine), s"QueryExecutor for engine=${executor.engine} not registered!")
    require(executorMap(executor.engine) == executor, s"QueryExecutor for not found, register first!")
    executorMap -= executor.engine
  }

  def haveExecutorForEngine(engine: Engine): Boolean = executorMap.contains(engine)

  def getExecutor(engine: Engine) : Option[QueryExecutor] = executorMap.get(engine)
}

trait ExecutionLifecycleListener {
  def acquired(query: Query, attributes: QueryAttributes) : QueryAttributes
  def started(query: Query, attributes: QueryAttributes) : QueryAttributes
  def completed(query: Query, attributes: QueryAttributes) : QueryAttributes
  def failed(query: Query, attributes: QueryAttributes, error: Throwable) : QueryAttributes
}

class NoopExecutionLifecycleListener extends ExecutionLifecycleListener with Logging {
  override def acquired(query: Query, attributes: QueryAttributes): QueryAttributes = {
    //do nothing
    if(query.queryContext.requestModel.isDebugEnabled) {
      info(s"acquired : ${query.asString}")
    }
    attributes
  }

  override def started(query: Query, attributes: QueryAttributes): QueryAttributes = {
    //do nothing
    if(query.queryContext.requestModel.isDebugEnabled) {
      info(s"started : ${query.asString}")
    }
    attributes
  }

  override def completed(query: Query, attributes: QueryAttributes): QueryAttributes = {
    //do nothing
    if(query.queryContext.requestModel.isDebugEnabled) {
      info(s"completed : ${query.asString}")
    }
    attributes
  }

  override def failed(query: Query, attributes: QueryAttributes, error: Throwable): QueryAttributes = {
    //do nothing
    if(query.queryContext.requestModel.isDebugEnabled) {
      info(s"failed : ${query.asString}")
    }
    attributes
  }
}

class ColumnValueExtractor {
  val mathContextCache = CacheBuilder
    .newBuilder()
    .maximumSize(100)
    .concurrencyLevel(Runtime.getRuntime.availableProcessors())
    .build(new CacheLoader[java.lang.Integer, MathContext]() {
      override def load(key: java.lang.Integer): MathContext = {
        new MathContext(key, java.math.RoundingMode.HALF_EVEN)
      }
    })

  final val DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  final val DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd")

  def getBigDecimalSafely(resultSet: ResultSet, index: Int) : BigDecimal = {
    val result: java.math.BigDecimal = resultSet.getBigDecimal(index)
    if(result == null)
      return null
    result
  }

  def getLongSafely(resultSet: ResultSet, index: Int) : Long = {
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
            new DateTime(date).toString(DATE_FORMATTER)
          } else {
            null
          }
        }
      case tt: TimestampType =>
        val ts = resultSet.getTimestamp(index)
        if (ts != null) {
          val dateTime = new DateTime(ts, DateTimeZone.UTC)
          dateTime.toString(DATE_TIME_FORMATTER)
        } else {
          null
        }
      case s: StrType =>
        resultSet.getString(index)
      case any => throw new UnsupportedOperationException(s"Unhandled data type for column value extraction : $any")
    }
  }
}

