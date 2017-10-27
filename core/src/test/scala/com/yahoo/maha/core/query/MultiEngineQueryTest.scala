// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core.{DruidEngine, OracleEngine}
import org.scalatest.{Matchers, FunSuite}

/**
 * Created by hiral on 2/18/16.
 */
class MultiEngineQueryTest extends FunSuite with Matchers with BaseQueryGeneratorTest with SharedDimSchema with BaseQueryContextTest with BaseQueryChainTest {

  test("successfully run multi engine query") {
    val model = getRequestModel(combinedQueryJson)
    val dimQuery = getQuery(OracleEngine, getDimQueryContext(OracleEngine, model, Option("Advertiser ID")), DimOnlyQuery)
    val factQuery = (irl: IndexedRowList, queryAttributes: QueryAttributes) => {
      val query = getQuery(DruidEngine, getFactQueryContext(DruidEngine, model, Option(irl.indexAlias), QueryAttributes.empty), FactOnlyQuery)
      query
    }
    val qc = new MultiEngineQuery(dimQuery, Set(OracleEngine, DruidEngine), IndexedSeq(factQuery))
    val irlFn = (q : Query) => new DimDrivenPartialRowList("Advertiser ID", q)
    val result = qc.execute(queryExecutorContext, irlFn, QueryAttributes.empty, new EngineQueryStats)
    result._1.foreach {
      r =>
        model.requestCols.map(_.alias).foreach {
          col => assert(r.getValue(col) === s"$col-value")
        }
    }
    val queryAttribute = result._2.getAttribute(QueryAttributes.QueryStats)
    assert(queryAttribute.isInstanceOf[QueryStatsAttribute] && queryAttribute.asInstanceOf[QueryStatsAttribute].stats.getStats.size > 1)
  }
}
