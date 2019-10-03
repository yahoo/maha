package com.yahoo.maha.core.query.postgres

import com.yahoo.maha.core.{PostgresEngine, ColumnInfo, Column, Engine}
import com.yahoo.maha.core.fact.Fact
import com.yahoo.maha.core.query._
import org.scalatest.{Matchers, FunSuite}

/**
 * Created by pranavbhole on 15/12/17.
 */
class PostgresQueryCommonTest extends FunSuite with Matchers {

  class TestQueryCommon extends PostgresQueryCommon {

    override def generateDimensionSql(queryContext: QueryContext, queryBuilderContext: QueryBuilderContext, includePagination: Boolean): DimensionSql = ???

    override def renderOuterColumn(columnInfo: ColumnInfo, queryBuilderContext: QueryBuilderContext, duplicateAliasMapping: Map[String, Set[String]], isFactOnlyQuery: Boolean, isDimOnly: Boolean, queryContext: QueryContext): (String, String) = ???

    override def renderColumnWithAlias(fact: Fact, column: Column, alias: String, requiredInnerCols: Set[String], queryBuilder: QueryBuilder, queryBuilderContext: QueryBuilderContext, queryContext: FactualQueryContext): Unit = ""

    override def generate(queryContext: QueryContext): Query = NoopQuery

    override def engine: Engine = PostgresEngine

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
