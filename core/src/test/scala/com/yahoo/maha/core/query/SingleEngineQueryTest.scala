// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core._
import org.scalatest.{FunSuite, Matchers}

import scala.util.Try

/**
 * Created by hiral on 2/18/16.
 */
class SingleEngineQueryTest extends FunSuite with Matchers with BaseQueryGeneratorTest with SharedDimSchema with BaseQueryContextTest with BaseQueryChainTest {


  test("successfully run dim only query") {
    val query = getQuery(OracleEngine, getDimQueryContext(OracleEngine, getRequestModel(dimOnlyQueryJson), None), DimOnlyQuery)
    val qc = new SingleEngineQuery(query)
    val result = qc.execute(queryExecutorContext, (q) => new CompleteRowList(q), QueryAttributes.empty, new EngineQueryStats)
    result.rowList.foreach {
      r =>
        query.queryContext.requestModel.requestCols.filter(_.isInstanceOf[DimColumnInfo]).map(_.asInstanceOf[DimColumnInfo].alias).foreach {
          col => assert(r.getValue(col) === s"$col-value")
        }
    }
    val queryAttribute = result.queryAttributes.getAttribute(QueryAttributes.QueryStats)
    assert(queryAttribute.isInstanceOf[QueryStatsAttribute] && queryAttribute.asInstanceOf[QueryStatsAttribute].stats.getStats.nonEmpty)
  }

  test("successfully run fact only query") {
    val query = getQuery(OracleEngine, getFactQueryContext(OracleEngine, getRequestModel(factOnlyQueryJson), None, QueryAttributes.empty), FactOnlyQuery)
    val qc = new SingleEngineQuery(query)
    val result = qc.execute(queryExecutorContext, (q) => new CompleteRowList(q), QueryAttributes.empty, new EngineQueryStats)
    result.rowList.foreach {
      r =>
        query.queryContext.requestModel.requestCols.filter(_.isInstanceOf[FactColumnInfo]).map(_.asInstanceOf[FactColumnInfo].alias).foreach {
          col => assert(r.getValue(col) === s"$col-value")
        }
    }
    val queryAttribute = result.queryAttributes.getAttribute(QueryAttributes.QueryStats)
    assert(queryAttribute.isInstanceOf[QueryStatsAttribute] && queryAttribute.asInstanceOf[QueryStatsAttribute].stats.getStats.nonEmpty)
  }

  test("successfully run combined query") {
    val query = getQuery(OracleEngine, getCombinedQueryContext(OracleEngine, getRequestModel(combinedQueryJson), None, QueryAttributes.empty), DimFactQuery)
    val qc = new SingleEngineQuery(query)
    val result = qc.execute(queryExecutorContext, (q) => new CompleteRowList(q), QueryAttributes.empty, new EngineQueryStats)
    result.rowList.foreach {
      r =>
        query.queryContext.requestModel.requestCols.filterNot(_.isInstanceOf[ConstantColumnInfo]).map(_.alias).foreach {
          col => assert(r.getValue(col) === s"$col-value")
        }
    }
    val queryAttribute = result.queryAttributes.getAttribute(QueryAttributes.QueryStats)
    assert(queryAttribute.isInstanceOf[QueryStatsAttribute] && queryAttribute.asInstanceOf[QueryStatsAttribute].stats.getStats.nonEmpty)
  }

  test("with forced failing query result and fallback not defined") {
    val query = getQuery(DruidEngine, getFactQueryContext(OracleEngine, getRequestModel(combinedQueryJson), None, QueryAttributes.empty), DimFactQuery)
    val qc = new SingleEngineQuery(query)
    val resultTry = Try(qc.execute(getPartialQueryExecutorContext, (q) => new CompleteRowList(q), QueryAttributes.empty, new EngineQueryStats))
    assert(resultTry.isFailure)
  }
}
