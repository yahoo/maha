// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.executor

import com.yahoo.maha.core.query._
import com.yahoo.maha.core.{BigqueryEngine, DruidEngine, Engine, HiveEngine, OracleEngine, PostgresEngine}
import scala.util.Try

/**
 * Created by pranavbhole on 08/04/16.
 */
class MockDruidQueryExecutor(callback: QueryRowList => Unit) extends QueryExecutor {
  override def engine: Engine = DruidEngine

  override def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes): QueryResult[T] = {
    val result = Try(callback(rowList.asInstanceOf[QueryRowList]))
    if(result.isSuccess) {
      QueryResult(rowList, queryAttributes, QueryResultStatus.SUCCESS)
    } else {
      QueryResult(rowList, queryAttributes, QueryResultStatus.FAILURE)
    }
  }
}

class MockOracleQueryExecutor(callback: QueryRowList => Unit) extends QueryExecutor {
  override def engine: Engine = OracleEngine

  override def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes): QueryResult[T] = {
    val result = Try(callback(rowList.asInstanceOf[QueryRowList]))
    if(result.isSuccess) {
      QueryResult(rowList, queryAttributes, QueryResultStatus.SUCCESS)
    } else {
      QueryResult(rowList, queryAttributes, QueryResultStatus.FAILURE)
    }
  }
}

class MockHiveQueryExecutor(callback: QueryRowList => Unit) extends QueryExecutor {
  override def engine: Engine = HiveEngine

  override def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes): QueryResult[T] = {
    val result = Try(callback(rowList.asInstanceOf[QueryRowList]))
    if(result.isSuccess) {
      QueryResult(rowList, queryAttributes, QueryResultStatus.SUCCESS)
    } else {
      QueryResult(rowList, queryAttributes, QueryResultStatus.FAILURE)
    }
  }
}

class MockPostgresQueryExecutor(callback: QueryRowList => Unit) extends QueryExecutor {
  override def engine: Engine = PostgresEngine

  override def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes): QueryResult[T] = {
    val result = Try(callback(rowList.asInstanceOf[QueryRowList]))
    if(result.isSuccess) {
      QueryResult(rowList, queryAttributes, QueryResultStatus.SUCCESS)
    } else {
      QueryResult(rowList, queryAttributes, QueryResultStatus.FAILURE)
    }
  }
}

class MockBigqueryQueryExecutor(callback: QueryRowList => Unit) extends QueryExecutor {
  override def engine: Engine = BigqueryEngine

  override def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes): QueryResult[T] = {
    val result = Try(callback(rowList.asInstanceOf[QueryRowList]))
    if (result.isSuccess) {
      QueryResult(rowList, queryAttributes, QueryResultStatus.SUCCESS)
    } else {
      QueryResult(rowList, queryAttributes, QueryResultStatus.FAILURE)
    }
  }
}
