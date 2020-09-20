// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core.{DruidEngine, OracleEngine}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Created by hiral on 2/18/16.
 */
class MultiEngineQueryTest extends AnyFunSuite with Matchers with BaseQueryGeneratorTest with SharedDimSchema with BaseQueryContextTest with BaseQueryChainTest {

  test("successfully run multi engine query") {
    val model = getRequestModel(combinedQueryJson)
    val dimQuery = getQuery(OracleEngine, getDimQueryContext(OracleEngine, model, Option("Advertiser ID"), List.empty), DimOnlyQuery)
    val factQuery = (irl: IndexedRowList, queryAttributes: QueryAttributes) => {
      val query = getQuery(DruidEngine, getFactQueryContext(DruidEngine, model, Option(irl.rowGrouping.indexAlias), irl.rowGrouping.factGroupByCols, QueryAttributes.empty), FactOnlyQuery)
      query
    }
    val qc = new MultiEngineQuery(dimQuery, Set(OracleEngine, DruidEngine), IndexedSeq(factQuery))
    val irlFn = (q : Query) => new DimDrivenPartialRowList(RowGrouping("Advertiser ID", List.empty), q)
    val result = qc.execute(queryExecutorContext, irlFn, QueryAttributes.empty, new EngineQueryStats)
    result.rowList.foreach {
      r =>
        model.requestCols.map(_.alias).foreach {
          col => assert(r.getValue(col) === s"$col-value")
        }
    }
    val queryAttribute = result.queryAttributes.getAttribute(QueryAttributes.QueryStats)
    assert(queryAttribute.isInstanceOf[QueryStatsAttribute] && queryAttribute.asInstanceOf[QueryStatsAttribute].stats.getStats.size > 1)
  }
}
