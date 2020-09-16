// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import com.yahoo.maha.core.OracleEngine
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Created by pranavbhole on 06/12/16.
 */
class MultiQueryTest extends AnyFunSuite with Matchers with BaseQueryGeneratorTest with SharedDimSchema with BaseQueryContextTest with BaseQueryChainTest {

  test("successfully multi query") {
    val model = getRequestModel(combinedQueryJson)
    val dimQuery = getQuery(OracleEngine, getDimQueryContext(OracleEngine, model, Option("Advertiser ID"), List("Source", "Pricing Type")), DimOnlyQuery)

    val qc = new MultiQuery(List(dimQuery, dimQuery))
    val irlFn = (q : Query) => new DimDrivenPartialRowList(RowGrouping("Advertiser ID", List("Source", "Pricing Type")), q)
    val result = qc.execute(queryExecutorContext, irlFn, QueryAttributes.empty, new EngineQueryStats)
    result.rowList.foreach {
      r =>
        model.requestCols.map(_.alias).foreach {
          col => assert(r.getValue(col) === s"$col-value")
        }
    }
  }

  test("MultiQuery with a subsequentQuery to execute.") {
    val model = getRequestModel(combinedQueryJson)
    val dimQuery = getQuery(OracleEngine, getDimQueryContext(OracleEngine, model, Option("Advertiser ID"), List("Source", "Pricing Type")), DimOnlyQuery)
    val dimQuery2 = getQuery(OracleEngine, getDimQueryContext(OracleEngine, model, Option("Advertiser ID"), List("Source", "Destination URL")), DimOnlyQuery)

    val qc = new MultiQuery(List(dimQuery, dimQuery2))
    val irlFn = (q : Query) => new DimDrivenPartialRowList(RowGrouping("Advertiser ID", List("Source", "Destination URL")), q)
    val result = qc.execute(queryExecutorContext, irlFn, QueryAttributes.empty, new EngineQueryStats)
    result.rowList.foreach {
      r =>
        model.requestCols.map(_.alias).foreach {
          col => assert(r.getValue(col) === s"$col-value")
        }
    }
  }

  test("MultiQuery with a valid FallbackQueryOption & empty result rowList - expect a fallBack query to execute in logger.") {
    val model = getRequestModel(combinedQueryJson)
    val dimQuery = getQuery(OracleEngine, getDimQueryContext(OracleEngine, model, Option("Advertiser ID"), List("Source", "Pricing Type")), DimOnlyQuery)
    val dimQuery2 = getQuery(OracleEngine, getDimQueryContext(OracleEngine, model, Option("Advertiser ID"), List("Source", "Destination URL")), DimOnlyQuery)

    val qc = new MultiQuery(List(dimQuery, dimQuery2), Some(dimQuery2, new DimDrivenPartialRowList(RowGrouping("Advertiser ID", List("Source", "Invalid URL")), dimQuery2)))
    val irlFn = (q : Query) => new DimDrivenPartialRowList(RowGrouping("Advertiser ID", List("Source", "Invalid URL")), q)
    val result = qc.execute(queryExecutorContextWithoutReturnedRows, irlFn, QueryAttributes.empty, new EngineQueryStats)
  }

}

