// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core.{Engine, OracleEngine}
import org.scalatest.{FunSuite, Matchers}

/**
 * Created by hiral on 2/9/16.
 */
class QueryExecutorContextTest extends FunSuite with Matchers {

  test("successfully register a query executor") {
    val qe = new QueryExecutor {
      
      val lifecycleListener : ExecutionLifecycleListener = new NoopExecutionLifecycleListener
      
      override def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes): QueryResult[T] = {
        val acquiredQueryAttributes = lifecycleListener.acquired(query, queryAttributes)
        val startedQueryAttributes = lifecycleListener.started(query, acquiredQueryAttributes)
        val checkFailed = lifecycleListener.failed(query, startedQueryAttributes, null)
        QueryResult(rowList, lifecycleListener.completed(query, queryAttributes), QueryResultStatus.SUCCESS)
      }

      override def engine: Engine = OracleEngine
    }
    
    assert(qe.acceptEngine(OracleEngine))
    
    val qec = new QueryExecutorContext
    qec.register(qe)
    assert(qec.haveExecutorForEngine(OracleEngine))
    assert(qec.getExecutor(OracleEngine).get === qe)
    qec.remove(qe)
    assert(!qec.haveExecutorForEngine(OracleEngine))
    assert(qec.getExecutor(OracleEngine) === None)
  }
}
