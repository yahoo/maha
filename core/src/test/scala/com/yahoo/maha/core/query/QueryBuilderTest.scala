package com.yahoo.maha.core.query

import org.scalatest.{FunSuiteLike, Matchers}

class QueryBuilderTest extends FunSuiteLike with Matchers{
  test("Create a QueryBuilder") {
    val qb : QueryBuilder = new QueryBuilder(initSize = 100, orderBySize = 10)
    assert(qb.getGroupByClause == "")
    assert(qb.getOuterGroupByClause == "")
    qb.addMultiDimensionJoin("test_join")
    assert(qb.getMultiDimensionJoinExpressions == "test_join")
    qb.addPartitionPredicate("pred")
    qb.addPartitionPredicate("pred2")
    assert(qb.getPartitionPredicates == "pred OR pred2")
    qb.addColumnHeader("header")
    assert(qb.getColumnHeaders == "header")
    qb.addOuterQueryEndClause("endClause")
    assert(qb.getOuterQueryEndClause == "endClause")
  }

}
