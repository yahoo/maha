// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.presto

import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.query._
import grizzled.slf4j.Logging

import scala.collection.{SortedSet, mutable}


class PrestoQueryGeneratorV1(partitionColumnRenderer:PartitionColumnRenderer, udfStatements: Set[UDFRegistration]) extends PrestoOuterGroupByQueryGenerator(partitionColumnRenderer, udfStatements) with Logging {
  override val version = Version.v1

  override val engine: Engine = PrestoEngine
  override def generate(queryContext: QueryContext): Query = {
    info(s"Generating Presto query using PrestoQueryGenerator V1 Outer Group By:, version $version")
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
//  val prestoLiteralMapper = new HiveLiteralMapper() // Reuse hive literal mapper

  private[this] def generateQuery(queryContext: CombinedQueryContext) : Query = {

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
    val partitionCols = new mutable.HashSet[Column]()
    val requestedCols = queryContext.requestModel.requestCols
    val columnAliasToColMap = new mutable.HashMap[String, Column]()

    def renderColumnAlias(colAlias: String) : String = {
      val renderedExp = new StringBuilder
      // Mangle Aliases, Derived expressions except Id's
      if (!colAlias.toLowerCase.endsWith("id") && (Character.isUpperCase(colAlias.charAt(0)) || colAlias.contains(" "))) {
        // All aliases are prefixed with _to relieve namespace collisions with pre-defined columns with same name.
        renderedExp.append("mang_")
      }
      // remove everything that is not a letter, a digit or space
      // replace any whitespace with "_"
      renderedExp.append(colAlias).toString().replaceAll("[^a-zA-Z0-9\\s_]", "").replaceAll("\\s", "_").toLowerCase
    }

    def generateConcatenatedCols(): String = {
      val renderedConcateColumns = queryContext.requestModel.requestCols.map {
        case ConstantColumnInfo(alias, _) =>
          getConstantColAlias(alias)
        case DimColumnInfo(alias) =>
          queryBuilderContext.getDimensionColNameForAlias(alias)
        case FactColumnInfo(alias)=>
          queryBuilderContext.getFactColNameForAlias(alias)
      }.map(castToChar)
      renderedConcateColumns.mkString(", ")
    }

    def castToChar(renderedCol: String) : String = s"""CAST($renderedCol as VARCHAR) AS $renderedCol"""

    /**
      * Fact select
      *
      * factViewCols => I. Non-Derived:
      *                      1. expr -> columnName( "account_id" )
      *                      2. rollup columnName( "sum(impression) impression" )
      *                 II. Derived:
      *                      derivedExpr derivedColumnName
      */

    def generateFactQueryFragment() : String = {
      /**
        *  render fact/dim columns with derived/rollup expression
        */
      def renderStaticMappedDimension(column: Column) : String = {
        val nameOrAlias = renderColumnAlias(column.alias.getOrElse(column.name))
        column.dataType match {
          case IntType(_, sm, _, _, _) if sm.isDefined =>
            val defaultValue = sm.get.default
            val whenClauses = sm.get.tToStringMap.map {
              case (from, to) => s"WHEN ($nameOrAlias IN ($from)) THEN '$to'"
            }
            s"CASE ${whenClauses.mkString(" ")} ELSE '$defaultValue' END"
          case StrType(_, sm, _) if sm.isDefined =>
            val defaultValue = sm.get.default
            val whenClauses = sm.get.tToStringMap.map {
              case (from, to) => s"WHEN ($nameOrAlias IN ('$from')) THEN '$to'"
            }
            s"CASE ${whenClauses.mkString(" ")} ELSE '$defaultValue' END"
          case _ =>
            s"""COALESCE($nameOrAlias, "NA")"""
        }
      }

      val dimCols = queryContext.factBestCandidate.dimColMapping.toList.collect {
        case (dimCol, alias) if queryContext.factBestCandidate.requestCols(dimCol) =>
          val column = fact.columnsByNameMap(dimCol)
          (column, alias)
      }

      //render derived columns last
      val groupDimCols = dimCols.groupBy(_._1.isDerivedColumn)
      groupDimCols.toList.sortBy(_._1).foreach {
        case (_, list) => list.foreach {
          case (column, alias) =>
            val name = column.name
            val nameOrAlias = column.alias.getOrElse(name)
            renderColumnWithAlias(fact, column, alias, Set.empty, isOuterColumn = false, queryContext, queryBuilderContext, queryBuilder, PrestoEngine)
            if (column.isDerivedColumn) {
              val derivedExpressionExpanded: String = column.asInstanceOf[DerivedDimensionColumn].derivedExpression.render(name, Map.empty).asInstanceOf[String]
              queryBuilder.addGroupBy( s"""$derivedExpressionExpanded""")
            } else {
              if(column.dataType.hasStaticMapping) {
                queryBuilder.addGroupBy(renderStaticMappedDimension(column))
              } else {
                queryBuilder.addGroupBy(nameOrAlias)
              }
            }
        }
      }

      val factCols = queryContext.factBestCandidate.factColMapping.toList.collect {
        case (nonFkCol, alias) if queryContext.factBestCandidate.requestCols(nonFkCol) =>
          (fact.columnsByNameMap(nonFkCol), alias)
      }

      val groupedFactCols = factCols.groupBy(_._1.isDerivedColumn)
      //render non derived columns first
      groupedFactCols.get(false).foreach { nonDerivedCols =>
        nonDerivedCols.foreach {
          case (column, alias) =>
            val renderedAlias = s""""$alias""""
            renderColumnWithAlias(fact, column, alias, Set.empty, isOuterColumn = false, queryContext, queryBuilderContext, queryBuilder, PrestoEngine)
        }
      }

      //render derived columns last
      groupedFactCols.get(true).foreach { derivedCols =>
        val requiredInnerCols: Set[String] =
          derivedCols.view.map(_._1.asInstanceOf[DerivedColumn]).flatMap(dc => dc.derivedExpression.sourceColumns).toSet
        derivedCols.foreach {
          case (column, alias) =>
            val renderedAlias = s""""$alias""""
            renderColumnWithAlias(fact, column, alias, requiredInnerCols, isOuterColumn = false, queryContext, queryBuilderContext, queryBuilder, PrestoEngine)
        }
      }

      val hasPartitioningScheme = fact.annotations.contains(PrestoQueryGeneratorV1.ANY_PARTITIONING_SCHEME)

      val factFilters = queryContext.factBestCandidate.filters
      val factForcedFilters = queryContext.factBestCandidate.publicFact.forcedFilters
      val aliasToNameMapFull = queryContext.factBestCandidate.publicFact.aliasToNameColumnMap
      val allFilters = factForcedFilters // ++ factFilters need to append non-forced filters, or otherwise pass them in separately

      val whereFilters = new mutable.LinkedHashSet[String]
      val havingFilters = new mutable.LinkedHashSet[String]


      val unique_filters = removeDuplicateIfForced( factFilters.toSeq, allFilters.toSeq, queryContext )

      unique_filters.sorted map {
        filter =>
          val name = publicFact.aliasToNameColumnMap(filter.field)
          val colRenderFn = (x: Column) =>
            x match {
              case FactCol(_, dt, cc, rollup, _, annotations, _) =>
                s"""${renderRollupExpression(x.name, rollup)}"""
              case PrestoDerFactCol(_, _, dt, cc, de, annotations, rollup, _) =>
                s"""${renderRollupExpression(de.render(x.name, Map.empty), rollup)}"""
              case any =>
                throw new UnsupportedOperationException(s"Found non fact column : $any")
            }
          val result = QueryGeneratorHelper.handleFilterRender(filter, publicFact, fact, aliasToNameMapFull, queryContext, PrestoEngine, prestoLiteralMapper, colRenderFn)

          if (fact.dimColMap.contains(name)) {
            whereFilters += result.filter
          } else if (fact.factColMap.contains(name)) {
            havingFilters += result.filter
          } else {
            throw new IllegalArgumentException(
              s"Unknown fact column: publicFact=${publicFact.name}, fact=${fact.name} alias=${filter.field}, name=$name")
          }
      }

      val dayFilter = FilterSql.renderFilter(
        queryContext.requestModel.localTimeDayFilter,
        queryContext.factBestCandidate.publicFact.aliasToNameColumnMap,
        Map.empty,
        fact.columnsByNameMap,
        PrestoEngine,
        prestoLiteralMapper).filter

      val combinedQueriedFilters = {
        if (hasPartitioningScheme) {
          val partitionFilterOption = partitionColumnRenderer.renderFact(queryContext, prestoLiteralMapper, PrestoEngine)
          if(partitionFilterOption.isDefined) {
            whereFilters += partitionFilterOption.get
            RenderedAndFilter(whereFilters).toString
          } else {
            whereFilters += dayFilter
            RenderedAndFilter(whereFilters).toString
          }
        } else {
          whereFilters += dayFilter
          RenderedAndFilter(whereFilters).toString
        }
      }

      val factWhere = {
        if(combinedQueriedFilters.length > 0) {
          s"""WHERE ${combinedQueriedFilters}"""
        } else {
          ""
        }
      }

      val groupBy = queryBuilder.getGroupByClause
      val havingClause = if (havingFilters.nonEmpty) {
        val havingAndFilters = RenderedAndFilter(havingFilters.toSet)
        s"""HAVING ${havingAndFilters.toString}"""
      } else ""

      s"""SELECT ${queryBuilder.getFactViewColumns}
          |FROM $factViewName
          |$factWhere
          |$groupBy
          |$havingClause
       """.stripMargin
    }

    val factQueryFragment = generateFactQueryFragment()

    dims.foreach {
      dimBundle =>
        dimBundle.fields.foreach {
          alias =>
            val name = {
              if(dimBundle.publicDim.primaryKeyByAlias == alias) {
                dimBundle.dim.primaryKey
              } else {
                dimBundle.publicDim.aliasToNameMap(alias)
              }
            }
            val column = dimBundle.dim.dimensionColumnsByNameMap(name)
            if (column.isInstanceOf[PrestoPartDimCol]) {
              partitionCols += column
            }
            val finalAlias =  {
              if(dimBundle.publicDim.primaryKeyByAlias == alias) {
                getPkFinalAliasForDim(queryBuilderContext, dimBundle)
              } else {
                renderColumnAlias(alias)
              }
            }
            queryBuilderContext.setDimensionColAlias(alias, finalAlias, column, dimBundle.publicDim)
        }
    }


    /**
      * Dimension selects
      */

    dims.foreach {
      dim =>
        queryBuilder.addDimensionJoin(generateDimJoinQuery(queryBuilderContext, dim, fact, requestModel, factViewAlias))
    }

    // generate alias for request columns
    requestedCols.foreach {
      case FactColumnInfo(alias) =>
        columnAliasToColMap += queryBuilderContext.getFactColNameForAlias(alias) -> queryBuilderContext.getFactColByAlias(alias)
      case DimColumnInfo(alias) =>
        columnAliasToColMap += queryBuilderContext.getDimensionColNameForAlias(alias) -> queryBuilderContext.getDimensionColByAlias(alias)
      case ConstantColumnInfo(alias, value) =>
        val pubDimCol = publicFact.dimCols.filter(pubDimCol => pubDimCol.alias.equals(alias))
        val pubFactCol = publicFact.factCols.filter(pubFactCol => pubFactCol.alias.equals(alias))
        val column = if (pubFactCol.isEmpty) fact.dimColMap(pubDimCol.head.name) else fact.factColMap(pubFactCol.head.name)
        columnAliasToColMap += renderColumnAlias(alias) -> column
    }

    val concatenatedCols = generateConcatenatedCols()
    val outerCols = generateOuterColumns(queryContext, queryBuilderContext, queryBuilder)
    val dimJoinQuery = queryBuilder.getJoinExpressions

    generateOrderByClause(queryContext, queryBuilderContext, queryBuilder)

    val outerOrderByClause = queryBuilder.getOrderByClause

    // factViewAlias => needs to generate abbr from factView name like account_stats_1h_v2 -> as1v0
    // outerCols same cols in concate cols, different expression ???
    val parameterizedQuery : String =
      s"""SELECT $concatenatedCols
          |FROM(
          |SELECT $outerCols
          |FROM($factQueryFragment)
          |$factViewAlias
          |$dimJoinQuery
          |$outerOrderByClause
          )
       """.stripMargin

    val parameterizedQueryWithRowLimit = {
      if(queryContext.requestModel.maxRows > 0) {
        s"""$parameterizedQuery queryAlias LIMIT ${queryContext.requestModel.maxRows}"""
      } else {
        s"""$parameterizedQuery queryAlias"""
      }
    }

    val paramBuilder = new QueryParameterBuilder

    new PrestoQuery(
      queryContext,
      parameterizedQueryWithRowLimit,
      Option(udfStatements),
      paramBuilder.build(),
      queryContext.requestModel.requestCols.map(_.alias),
      columnAliasToColMap.toMap,
      IndexedSeq.empty,
      queryGenVersion = Some(this.version)
    )
  }

  def generateOrderByClause(queryContext: CombinedQueryContext,
                            queryBuilderContext:QueryBuilderContext,
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

}

object PrestoQueryGeneratorV1 extends Logging {
  val ANY_PARTITIONING_SCHEME = PrestoPartitioningScheme("") //no name needed since class name hashcode

  def register(queryGeneratorRegistry: QueryGeneratorRegistry, partitionDimensionColumnRenderer:PartitionColumnRenderer, udfStatements: Set[UDFRegistration]) = {
    if(!queryGeneratorRegistry.isEngineRegistered(PrestoEngine, Option(Version.v1))) {
      val generator = new PrestoQueryGeneratorV1(partitionDimensionColumnRenderer:PartitionColumnRenderer, udfStatements)
      queryGeneratorRegistry.register(PrestoEngine, generator, generator.version)
    } else {
      queryGeneratorRegistry.getDefaultGenerator(PrestoEngine).foreach {
        qg =>
          if(!qg.isInstanceOf[PrestoQueryGeneratorV1]) {
            warn(s"Another query generator registered for PrestoEngine : ${qg.getClass.getCanonicalName}")
          }
      }
    }
  }
}