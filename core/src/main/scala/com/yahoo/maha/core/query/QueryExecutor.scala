// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import java.util.Properties

import com.yahoo.maha.core.Engine
import grizzled.slf4j.Logging

import scala.concurrent.Future

/**
 * Created by hiral on 12/22/15.
 */

trait QueryExecutor {
  def engine: Engine
  def acceptEngine(engine: Engine) : Boolean = engine == this.engine
  def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes) : QueryResult
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

