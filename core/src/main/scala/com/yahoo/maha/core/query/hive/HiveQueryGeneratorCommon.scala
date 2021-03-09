package com.yahoo.maha.core.query.hive

import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.query._

import scala.collection.{SortedSet, mutable}

abstract class HiveQueryGeneratorCommon(partitionColumnRenderer:PartitionColumnRenderer, udfStatements: Set[UDFRegistration]) extends BaseQueryGenerator[WithHiveEngine] with BigqueryHivePrestoQueryCommon {

  // render outercols with column expression
  def generateOuterColumns(queryContext: CombinedQueryContext,
                           queryBuilderContext: QueryBuilderContext,
                           queryBuilder: QueryBuilder,
                           renderOuterColumn: (ColumnInfo, QueryBuilderContext, Map[String, Set[String]], FactBestCandidate, Boolean) => (String, String)
  ) : String = {
    queryContext.requestModel.requestCols foreach {
      columnInfo =>
        QueryGeneratorHelper.populateAliasColMapOfRequestCols(columnInfo, queryBuilderContext, queryContext)
        queryBuilder.addOuterColumn(concat(renderOuterColumn(columnInfo, queryBuilderContext, queryContext.factBestCandidate.duplicateAliasMapping, queryContext.factBestCandidate, false)))
    }
    queryBuilder.getOuterColumns
  }

  /**
    * Fact select
    *
    * factViewCols => I. Non-Derived:
    *                      1. expr -> columnName( "account_id" )
    *                      2. rollup columnName( "sum(impression) impression" )
    *                 II. Derived:
    *                      derivedExpr derivedColumnName
    */

  def generateFactQueryFragment(queryContext: CombinedQueryContext,
                                queryBuilderContext: QueryBuilderContext,
                                queryBuilder: QueryBuilder,
                                renderDerivedFactCols: (List[(Column, String)] => Unit),
                                renderRollupExpression: (String, RollupExpression, Option[String]) => String,
                                renderColumnWithAlias: (Fact, Column, String, Set[String], Boolean, QueryContext, QueryBuilderContext, QueryBuilder, Engine) => Unit) : String = {

    val fact = queryContext.factBestCandidate.fact
    val publicFact = queryContext.factBestCandidate.publicFact
    val factViewName = fact.name
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
          renderColumnWithAlias(fact, column, alias, Set.empty, false, queryContext, queryBuilderContext, queryBuilder, HiveEngine)
          val isAggregatedDimCol = isAggregateDimCol(column)
          if (!isAggregatedDimCol) {
            if (column.isDerivedColumn) {
              val derivedExpressionExpanded: String = column.asInstanceOf[DerivedDimensionColumn].derivedExpression.render(name, Map.empty).asInstanceOf[String]
              queryBuilder.addGroupBy( s"""$derivedExpressionExpanded""")
            } else {
              if(column.dataType.hasStaticMapping) {
                queryBuilder.addGroupBy(renderStaticMappedDimension(column, HiveEngine))
              } else {
                queryBuilder.addGroupBy(nameOrAlias)
              }
            }
          }
      }
    }

    def isAggregateDimCol(column: Column) : Boolean = {
      if(column.isInstanceOf[BaseDerivedDimCol]) {
        column.asInstanceOf[BaseDerivedDimCol].isAggregateColumn
      } else false
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
          renderColumnWithAlias(fact, column, alias, Set.empty, false, queryContext, queryBuilderContext, queryBuilder, HiveEngine)
      }
    }

    //render derived columns last
    groupedFactCols.get(true).foreach { renderDerivedFactCols  }


    val hasPartitioningScheme = fact.annotations.contains(HiveQueryGenerator.ANY_PARTITIONING_SCHEME)

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
              s"""${renderRollupExpression(x.name, rollup, None)}"""
            case OracleDerFactCol(_, _, dt, cc, de, annotations, rollup, _) => //This never gets used, otherwise errors would be thrown before the Generator.
              s"""${renderRollupExpression(de.render(x.name, Map.empty), rollup, None)}"""
            case any =>
              throw new UnsupportedOperationException(s"Found non fact column : $any")
          }
        val result = QueryGeneratorHelper.handleFilterSqlRender(filter, publicFact, fact, aliasToNameMapFull, queryContext, HiveEngine, hiveLiteralMapper, colRenderFn)

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
      HiveEngine,
      hiveLiteralMapper).filter

    val combinedQueriedFilters = {
      if (hasPartitioningScheme) {
        val partitionFilterOption = partitionColumnRenderer.renderFact(queryContext, hiveLiteralMapper, HiveEngine)
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

  def generateDimJoinQuery(queryBuilderContext: QueryBuilderContext, dimBundle: DimensionBundle, fact: Fact, requestModel: RequestModel, factViewAlias: String) : String = {

    /**
      *  render fact/dim columns with derived/rollup expression
      */
    def renderColumn(column: Column, alias: String): String = {
      val name = column.alias.getOrElse(column.name)
      column match {
        case DimCol(_, dt, _, _, _, _) =>
          name
        case HiveDerDimCol(_, dt, _, de, _, _, _) =>
          s"""${de.render(name, Map.empty)}"""
        case other => throw new IllegalArgumentException(s"Unhandled column type for dimension cols : $other")
      }
    }

    val requestDimCols = dimBundle.fields
    val publicDimName = dimBundle.publicDim.name
    val dimTableName = dimBundle.dim.name
    val dimFilters = dimBundle.filters
    val fkColName = fact.publicDimToForeignKeyMap(publicDimName)
    val fkCol = fact.columnsByNameMap(fkColName)
    val pkColName = dimBundle.dim.primaryKey
    val dimAlias = queryBuilderContext.getAliasForTable(publicDimName)

    val dimCols = requestDimCols map {
      colAlias =>
        if (dimBundle.publicDim.isPrimaryKeyAlias(colAlias)) {
          s"""$pkColName ${getPkFinalAliasForDim(queryBuilderContext, dimBundle)}"""
        } else {
          val colName = dimBundle.publicDim.aliasToNameMapFull(colAlias)
          val column = dimBundle.dim.dimensionColumnsByNameMap(colName)
          val finalAlias : String = queryBuilderContext.getDimensionColNameForAlias(colAlias)
          s"${renderColumn(column, finalAlias)} AS $finalAlias"
        }
    }

    val aliasToNameMapFull = dimBundle.publicDim.aliasToNameMapFull
    val columnsByNameMap = dimBundle.dim.columnsByNameMap

    val wheres = dimFilters map {
      filter =>
        FilterSql.renderFilter(
          filter,
          aliasToNameMapFull,
          Map.empty,
          columnsByNameMap,
          HiveEngine,
          hiveLiteralMapper
        ).filter
    }

    val partitionFilters = partitionColumnRenderer.renderDim(requestModel, dimBundle, hiveLiteralMapper, HiveEngine)
    val renderedFactFk = renderColumn(fkCol, "")

    val dimWhere = s"""WHERE ${RenderedAndFilter(wheres + partitionFilters).toString}"""

    val joinType = if (requestModel.anyDimHasNonFKNonForceFilter) {
      "JOIN"
    } else {
      "LEFT OUTER JOIN"
    }
    // pkColName ?? alias ?? cc3_id
    s"""$joinType (
       |SELECT ${dimCols.mkString(", ")}
       |FROM $dimTableName
       |$dimWhere
       |)
       |$dimAlias
       |ON
       |$factViewAlias.$renderedFactFk = $dimAlias.${dimAlias}_$pkColName
       """.stripMargin

  }

  def generateDimSelects(dims: SortedSet[DimensionBundle], queryBuilderContext: QueryBuilderContext, queryBuilder: QueryBuilder, requestModel: RequestModel, fact: Fact, factViewAlias: String) = {
    val partitionCols = new mutable.HashSet[Column]()
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
            if (column.isInstanceOf[HivePartDimCol]) {
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

  }

  protected[this] def getQueryAliasWithRowLimit(requestModel: RequestModel) : String = {
    val QueryAlias = "queryAlias"
    if(requestModel.maxRows > 0) {
      s"""$QueryAlias LIMIT ${requestModel.maxRows}"""
    } else {
      s"""$QueryAlias"""
    }
  }
}
