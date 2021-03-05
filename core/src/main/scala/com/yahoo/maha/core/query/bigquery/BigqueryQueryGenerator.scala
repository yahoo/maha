// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.bigquery

import com.yahoo.maha.core._
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.query._
import grizzled.slf4j.Logging
import scala.collection.{SortedSet, mutable}


class BigqueryQueryGenerator(
  partitionColumnRenderer: PartitionColumnRenderer,
  udfStatements: Set[UDFRegistration]
) extends BigqueryOuterGroupByQueryGenerator(partitionColumnRenderer, udfStatements)
  with Logging {
  override val version = Version.v0
  override val engine: Engine = BigqueryEngine

  override def generate(queryContext: QueryContext): Query = {
    queryContext match {
      case context: CombinedQueryContext =>
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

  private[this] def generateQuery(queryContext: CombinedQueryContext): Query = {
    val queryBuilderContext = new QueryBuilderContext
    val queryBuilder: QueryBuilder = new QueryBuilder(
      queryContext.requestModel.requestCols.size + 5,
      queryContext.requestModel.requestSortByCols.size + 1
    )
    val requestModel = queryContext.requestModel
    val factCandidate = queryContext.factBestCandidate
    val fact = factCandidate.fact
    val factViewName = fact.name
    val factViewAlias = queryBuilderContext.getAliasForTable(factViewName)
    val dims = queryContext.dims
    val requestedCols = queryContext.requestModel.requestCols
    val columnAliasToColMap = new mutable.HashMap[String, Column]()

    def renderDerivedFactCols(derivedCols: List[(Column, String)]) = {
      val requiredInnerCols: Set[String] =
        derivedCols.view.map(_._1.asInstanceOf[DerivedColumn]).flatMap(dc => dc.derivedExpression.sourceColumns).toSet
      derivedCols.foreach {
        case (column, alias) =>
          renderColumnWithAlias(fact, column, alias, requiredInnerCols, false, queryContext, queryBuilderContext, queryBuilder, BigqueryEngine)
      }
    }

    def generateOrderByClause(
      queryContext: CombinedQueryContext,
      queryBuilder: QueryBuilder
    ): Unit = {
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

    def generateConcatenatedCols(
      queryContext: QueryContext,
      queryBuilderContext: QueryBuilderContext
    ): String = {
      def castToChar(renderedCol: String): String = {
        s"""IFNULL(CAST($renderedCol AS STRING), '') AS $renderedCol"""
      }

      val renderedConcatenatedColumns = queryContext.requestModel.requestCols.map {
        case ConstantColumnInfo(alias, _) =>
          getConstantColAlias(alias)
        case DimColumnInfo(alias) =>
          queryBuilderContext.getDimensionColNameForAlias(alias)
        case FactColumnInfo(alias)=>
          queryBuilderContext.getFactColNameForAlias(alias)
      }.map(castToChar)

      renderedConcatenatedColumns.mkString(", ")
    }

    val factQueryFragment = generateFactQueryFragment(
      queryContext,
      queryBuilderContext,
      queryBuilder,
      renderDerivedFactCols,
      renderRollupExpression,
      renderColumnWithAlias
    )

    generateDimSelects(dims, queryBuilderContext, queryBuilder, requestModel, fact, factViewAlias)

    generateOrderByClause(queryContext, queryBuilder)
    val orderByClause = s"${queryBuilder.getOrderByClause}"

    val outerCols = generateOuterColumns(queryContext, queryBuilderContext, queryBuilder, renderOuterColumn)

    val concatenatedCols = generateConcatenatedCols(queryContext, queryBuilderContext)

    val queryAlias = getQueryAliasWithRowLimit(requestModel)

    val dimJoinQuery = s"${queryBuilder.getJoinExpressions}"

    val parameterizedQuery =
      s"""SELECT $concatenatedCols
         |FROM (
         |SELECT $outerCols
         |FROM ( $factQueryFragment )
         |$factViewAlias
         |$dimJoinQuery
         |$orderByClause
         |) $queryAlias
       """.stripMargin

    requestedCols.foreach {
      case FactColumnInfo(alias) =>
        columnAliasToColMap += queryBuilderContext.getFactColNameForAlias(alias) -> queryBuilderContext.getFactColByAlias(alias)
      case DimColumnInfo(alias) =>
        columnAliasToColMap += queryBuilderContext.getDimensionColNameForAlias(alias) -> queryBuilderContext.getDimensionColByAlias(alias)
      case ConstantColumnInfo(alias, value) =>
    }

    val paramBuilder = new QueryParameterBuilder

    BigqueryQuery(
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

object BigqueryQueryGenerator extends Logging {
  val ANY_PARTITIONING_SCHEME = BigqueryPartitioningScheme("")

  def register(
    queryGeneratorRegistry: QueryGeneratorRegistry,
    partitionDimensionColumnRenderer: PartitionColumnRenderer,
    udfStatements: Set[UDFRegistration]
  ) = {
    if (!queryGeneratorRegistry.isEngineRegistered(BigqueryEngine, Option(Version.v0))) {
      val generator = new BigqueryQueryGenerator(partitionDimensionColumnRenderer, udfStatements)
      queryGeneratorRegistry.register(BigqueryEngine, generator, generator.version)
    } else {
      throw new IllegalArgumentException(s"Bigquery Query Generator Version V0 is already registered")
    }
  }
}
