// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core.{DruidEngine, OracleEngine}
import org.scalatest.{Matchers, FunSuite}

/**
 * Created by pranavbhole on 06/12/16.
 */
class MultiQueryTest extends FunSuite with Matchers with BaseQueryGeneratorTest with SharedDimSchema with BaseQueryContextTest with BaseQueryChainTest {

  test("successfully multi query") {
    val model = getRequestModel(combinedQueryJson)
    val dimQuery = getQuery(OracleEngine, getDimQueryContext(OracleEngine, model, Option("Advertiser ID")), DimOnlyQuery)

    val qc = new MultiQuery(List(dimQuery, dimQuery))
    val irlFn = (q : Query) => new DimDrivenPartialRowList("Advertiser ID", q)
    val result = qc.execute(queryExecutorContext, irlFn, QueryAttributes.empty, new EngineQueryStats)
    result.rowList.foreach {
      r =>
        model.requestCols.map(_.alias).foreach {
          col => assert(r.getValue(col) === s"$col-value")
        }
    }
  }
}

