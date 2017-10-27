// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.executor

import com.yahoo.maha.core.{HiveEngine, OracleEngine, DruidEngine, Engine}
import com.yahoo.maha.core.query.{QueryAttributes, Query, QueryExecutor, RowList}

/**
 * Created by pranavbhole on 08/04/16.
 */
class MockDruidQueryExecutor(callback: RowList => Unit) extends QueryExecutor {
  override def engine: Engine = DruidEngine

  override def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes): (T, QueryAttributes) = {
    callback(rowList)
    (rowList, queryAttributes)
  }
}

class MockOracleQueryExecutor(callback: RowList => Unit) extends QueryExecutor {
  override def engine: Engine = OracleEngine

  override def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes): (T, QueryAttributes) = {
    callback(rowList)
    (rowList, queryAttributes)
  }
}

class MockHiveQueryExecutor(callback: RowList => Unit) extends QueryExecutor {
  override def engine: Engine = HiveEngine

  override def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes): (T, QueryAttributes) = {
    callback(rowList)
    (rowList, queryAttributes)
  }
}
