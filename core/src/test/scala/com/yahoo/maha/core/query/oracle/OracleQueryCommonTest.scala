package com.yahoo.maha.core.query.oracle

import com.yahoo.maha.core.fact.Fact
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.{Column, ColumnInfo, Engine, OracleEngine}
import org.scalatest.{FunSuite, Matchers}

/**
 * Created by pranavbhole on 15/12/17.
 */
class OracleQueryCommonTest extends FunSuite with Matchers {

  class TestQueryCommon extends OracleQueryCommon {

    override def generateDimensionSql(queryContext: QueryContext, queryBuilderContext: QueryBuilderContext, includePagination: Boolean): DimensionSql = ???

    override def renderOuterColumn(columnInfo: ColumnInfo, queryBuilderContext: QueryBuilderContext, duplicateAliasMapping: Map[String, Set[String]], isFactOnlyQuery: Boolean, isDimOnly: Boolean, queryContext: QueryContext): (String, String) = ???

    override def renderColumnWithAlias(fact: Fact, column: Column, alias: String, requiredInnerCols: Set[String], queryBuilder: QueryBuilder, queryBuilderContext: QueryBuilderContext, queryContext: FactualQueryContext): Unit = ""

    override def generate(queryContext: QueryContext): Query = NoopQuery

    override def engine: Engine = OracleEngine

    def concat(str:String, str1: String) : String = {
      concat((str, str1))
    }
  }

  val test = new TestQueryCommon

  test("Test QueryCommon") {
    assert(test.concat("a", "b") == s"""a "b"""")
    assert(test.concat("a", "") == s"""a""")
  }

}
