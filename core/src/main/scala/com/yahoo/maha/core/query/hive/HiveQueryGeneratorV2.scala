// Copyright 2018, Oath Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.hive

import com.yahoo.maha.core._
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.query._
import grizzled.slf4j.Logging

import scala.collection.{SortedSet, mutable}

/**
 * Created by pranavbhole on 10/16/18.
  * This is latest Hive Query Generator to be currently used,
  * which has sorting and dim aggregation feature.
 */
class HiveQueryGeneratorV2(partitionColumnRenderer:PartitionColumnRenderer, udfStatements: Set[UDFRegistration]) extends HiveOuterGroupByQueryGenerator(partitionColumnRenderer, udfStatements) with Logging {

  override val version = Version.v2

  override val engine: Engine = HiveEngine
  override def generate(queryContext: QueryContext): Query = {
    info(s"Generating Hive query using HiveQueryGenerator V2 Outer Group By:, version ${version}")
    queryContext match {
      case context : CombinedQueryContext =>
        generateQuery(context)
      case FactQueryContext(factBestCandidate, model, indexAliasOption, factGroupByKeys, attributes, _) =>
        generateQuery(CombinedQueryContext(SortedSet.empty, factBestCandidate, model, attributes))
      case ogbContext@DimFactOuterGroupByQueryQueryContext(dims, factBestCandidate, model, attributes) =>
        generateOuterGroupByQuery(ogbContext)
      case any => throw new UnsupportedOperationException(s"query context not supported : $any")
    }
  }

  override def validateEngineConstraints(requestModel: RequestModel): Boolean = {
    requestModel.orFilterMeta.isEmpty
  }

  private[this] def generateQuery(queryContext: CombinedQueryContext) : Query = {

    //init vars
    val queryBuilderContext = new QueryBuilderContext
    val queryBuilder: QueryBuilder = new QueryBuilder(
      queryContext.requestModel.requestCols.size + 5
      , queryContext.requestModel.requestSortByCols.size + 1)
    val requestModel = queryContext.requestModel
    val factCandidate = queryContext.factBestCandidate
    val publicFact = queryContext.factBestCandidate.publicFact
    val fact = factCandidate.fact
    val factViewName = fact.underlyingTableName.getOrElse(fact.name)
    val factViewAlias = queryBuilderContext.getAliasForTable(factViewName)
    val dims = queryContext.dims

    val requestedCols = queryContext.requestModel.requestCols
    val columnAliasToColMap = new mutable.HashMap[String, Column]()

    def renderDerivedFactCols(derivedCols: List[(Column, String)]) = {
      val requiredInnerCols: Set[String] =
        derivedCols.view.map(_._1.asInstanceOf[DerivedColumn]).flatMap(dc => dc.derivedExpression.sourceColumns).toSet
      derivedCols.foreach {
        case (column, alias) =>
          renderColumnWithAlias(fact: Fact, column, alias, requiredInnerCols, false, queryContext, queryBuilderContext, queryBuilder, HiveEngine)
      }
    }

    def generateOrderByClause(queryContext: CombinedQueryContext,
                             queryBuilder: QueryBuilder): Unit = {
      val model = queryContext.requestModel
      model.requestSortByCols.map {
        case FactSortByColumnInfo(alias, order) =>
          val colExpression = queryBuilderContext.getFactColNameForAlias(alias)
          s"$colExpression ${order}"
        case DimSortByColumnInfo(alias, order) =>
          val dimColName = queryBuilderContext.getDimensionColNameForAlias(alias)
          s"$dimColName ${order}"
        case a => throw new IllegalArgumentException(s"Unhandled SortByColumnInfo $a")
      }.foreach(queryBuilder.addOrderBy(_))
    }

    def generateConcatenatedColsWithCast(queryContext: QueryContext, queryBuilderContext: QueryBuilderContext): String = {
      def castAsString(finalAlias: String):String = {
        s"CAST($finalAlias AS STRING)"
      }
      val renderedConcateColumns = queryContext.requestModel.requestCols.map {
        case ConstantColumnInfo(alias, _) =>
          val finalAlias = getConstantColAlias(alias)
          s"""NVL(${castAsString(finalAlias)}, '')"""
        case DimColumnInfo(alias) =>
          val finalAlias = queryBuilderContext.getDimensionColNameForAlias(alias)
          s"""NVL(${castAsString(finalAlias)}, '')"""
        case FactColumnInfo(alias)=>
          val finalAlias = queryBuilderContext.getFactColNameForAlias(alias)
          s"""NVL(${castAsString(finalAlias)}, '')"""
      }
      "CONCAT_WS(\",\"," + (renderedConcateColumns).mkString(", ") + ")"
    }


    /**
     * Final Query
     */

    val factQueryFragment = generateFactQueryFragment(queryContext, queryBuilderContext, queryBuilder, renderDerivedFactCols, renderRollupExpression, renderColumnWithAlias)
    generateDimSelects(dims, queryBuilderContext, queryBuilder, requestModel, fact, factViewAlias)

    generateOrderByClause(queryContext, queryBuilder)
    val orderByClause = queryBuilder.getOrderByClause

    val outerCols = generateOuterColumns(queryContext, queryBuilderContext, queryBuilder, renderOuterColumn)
    val concatenatedCols = generateConcatenatedColsWithCast(queryContext, queryBuilderContext)


    val queryAlias = getQueryAliasWithRowLimit(requestModel)

    val parameterizedQuery : String = {
      val dimJoinQuery = queryBuilder.getJoinExpressions

      // factViewAlias => needs to generate abbr from factView name like account_stats_1h_v2 -> as1v0
      // outerCols same cols in concate cols, different expression ???
      s"""SELECT $concatenatedCols
          |FROM(
          |SELECT $outerCols
          |FROM($factQueryFragment)
          |$factViewAlias
          |$dimJoinQuery
          |$orderByClause) $queryAlias
       """.stripMargin
    }

    // generate alias for request columns
    requestedCols.foreach {
      case FactColumnInfo(alias) =>
        columnAliasToColMap += queryBuilderContext.getFactColNameForAlias(alias) -> queryBuilderContext.getFactColByAlias(alias)
      case DimColumnInfo(alias) =>
        columnAliasToColMap += queryBuilderContext.getDimensionColNameForAlias(alias) -> queryBuilderContext.getDimensionColByAlias(alias)
      case ConstantColumnInfo(alias, value) =>
      //TODO: fix this
      //constantColumnsMap += renderColumnAlias(alias) -> value
    }
    val paramBuilder = new QueryParameterBuilder

    new HiveQuery(
      queryContext,
      parameterizedQuery, 
      Option(udfStatements),
      paramBuilder.build(),
      queryContext.requestModel.requestCols.map(_.alias),
      columnAliasToColMap.toMap,
      IndexedSeq.empty,
      queryGenVersion = Some(this.version)
    )
  }
}

object HiveQueryGeneratorV2 extends Logging {
  val ANY_PARTITIONING_SCHEME = HivePartitioningScheme("") //no name needed since class name hashcode

  def register(queryGeneratorRegistry: QueryGeneratorRegistry, partitionDimensionColumnRenderer:PartitionColumnRenderer, udfStatements: Set[UDFRegistration]) = {
    if(!queryGeneratorRegistry.isEngineRegistered(HiveEngine, Option(Version.v2))) {
      val generator = new HiveQueryGeneratorV2(partitionDimensionColumnRenderer, udfStatements)
      queryGeneratorRegistry.register(HiveEngine, generator, generator.version)
    } else {
      throw new IllegalArgumentException(s"Hive Query Generator Version V2 is already registered")
    }
  }
}






