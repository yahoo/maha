// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.presto

import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.query.QueryGeneratorHelper.{getAdditionalColData, overrideRenderedCol}
import com.yahoo.maha.core.query._

import grizzled.slf4j.Logging

import scala.collection.{SortedSet, mutable}


class PrestoQueryGenerator(partitionColumnRenderer:PartitionColumnRenderer, udfStatements: Set[UDFRegistration]) extends BaseQueryGenerator[WithPrestoEngine] with Logging {

  override val engine: Engine = PrestoEngine
  override def generate(queryContext: QueryContext): Query = {
    queryContext match {
      case context : CombinedQueryContext =>
        generateQuery(context)
      case FactQueryContext(factBestCandidate, model, indexAliasOption, factGroupByKeys, attributes, _) =>
        generateQuery(CombinedQueryContext(SortedSet.empty, factBestCandidate, model, attributes))
      case DimFactOuterGroupByQueryQueryContext(dims, factBestCandidate, model, attributes) =>
        generateQuery(CombinedQueryContext(dims, factBestCandidate, model, attributes))
      case any => throw new UnsupportedOperationException(s"query context not supported : $any")
    }
  }
  val prestoLiteralMapper = new HiveLiteralMapper() // Reuse hive literal mapper

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

    def renderRollupExpression(expression: String, rollupExpression: RollupExpression, renderedColExp: Option[String] = None) : String = {
      rollupExpression match {
        case SumRollup => s"SUM($expression)"
        case MaxRollup => s"MAX($expression)"
        case MinRollup => s"MIN($expression)"
        case AverageRollup => s"AVG($expression)"
        case PrestoCustomRollup(exp) => s"(${exp.render(expression, Map.empty, renderedColExp)})"
        case NoopRollup => s"($expression)"
        case CountRollup => s"COUNT(*)"
        case any => throw new UnsupportedOperationException(s"Unhandled rollup expression : $any")
      }
    }

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

    def getConstantColAlias(alias: String) : String = {
      renderColumnAlias(alias.replaceAll("[^a-zA-Z0-9_]", ""))
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

    // render outercols with column expression
    def generateOuterColumns() : String = {
      queryContext.requestModel.requestCols foreach {
        columnInfo =>
          /*if (!columnInfo.isInstanceOf[ConstantColumnInfo] && queryBuilderContext.aliasColumnMap.contains(columnInfo.alias)) {
            //aliasColumnMapOfRequestCols += (columnInfo.alias -> queryBuilderContext.aliasColumnMap(columnInfo.alias))
          } else if (queryContext.factBestCandidate.duplicateAliasMapping.contains(columnInfo.alias)) {
            val sourceAliases = queryContext.factBestCandidate.duplicateAliasMapping(columnInfo.alias)
            val sourceAlias = sourceAliases.find(queryBuilderContext.aliasColumnMap.contains)
            require(sourceAlias.isDefined
              , s"Failed to find source column for duplicate alias mapping : ${queryContext.factBestCandidate.duplicateAliasMapping(columnInfo.alias)}")
            //aliasColumnMapOfRequestCols += (columnInfo.alias -> queryBuilderContext.aliasColumnMap(sourceAlias.get))
          }*/
          QueryGeneratorHelper.populateAliasColMapOfRequestCols(columnInfo, queryBuilderContext, queryContext)
          queryBuilder.addOuterColumn(renderOuterColumn(columnInfo, queryBuilderContext, queryContext.factBestCandidate.duplicateAliasMapping, factCandidate))
      }
      queryBuilder.getOuterColumns

    }

    def renderOuterColumn(columnInfo: ColumnInfo, queryBuilderContext: QueryBuilderContext, duplicateAliasMapping: Map[String, Set[String]], factCandidate: FactBestCandidate): String = {

      def renderFactCol(alias: String, finalAliasOrExpression: String, col: Column, finalAlias: String): (String,String) = {
        val postFilterAlias = renderNormalOuterColumn(col, finalAliasOrExpression)
        (postFilterAlias,finalAlias)
      }

      columnInfo match {
        case FactColumnInfo(alias) =>
          QueryGeneratorHelper.concat(QueryGeneratorHelper.handleOuterFactColInfo(queryBuilderContext, alias, factCandidate, renderFactCol, duplicateAliasMapping, factCandidate.fact.underlyingTableName.getOrElse(factCandidate.fact.name), false))
          /*if (queryBuilderContext.containsFactColNameForAlias(alias)) {
            val col = queryBuilderContext.getFactColByAlias(alias)
            val finalAlias = queryBuilderContext.getFactColNameForAlias(alias)
            val finalAliasOrExpression = {
              if(queryBuilderContext.isDimensionCol(alias)) {
                val factAlias = queryBuilderContext.getAliasForTable(factCandidate.fact.underlyingTableName.getOrElse(factCandidate.fact.name))
                val factExp = queryBuilderContext.getFactColExpressionOrNameForAlias(alias)
                s"$factAlias.$factExp"
              } else  {
                queryBuilderContext.getFactColExpressionOrNameForAlias(alias)
              }
            }
            renderFactCol(alias, finalAliasOrExpression, col, finalAlias)
          } else if (duplicateAliasMapping.contains(alias)) {
            val duplicateAliases = duplicateAliasMapping(alias)
            val renderedDuplicateAlias = duplicateAliases.collectFirst {
              case duplicateAlias if queryBuilderContext.containsFactColNameForAlias(duplicateAlias) =>
                val col = queryBuilderContext.getFactColByAlias(duplicateAlias)
                val finalAliasOrExpression = queryBuilderContext.getFactColExpressionOrNameForAlias(duplicateAlias)
                val finalAlias = queryBuilderContext.getFactColNameForAlias(duplicateAlias)
                renderFactCol(alias, finalAliasOrExpression, col, finalAlias)
            }
            require(renderedDuplicateAlias.isDefined, s"Failed to render column : $alias")
            renderedDuplicateAlias.get
          } else {
            throw new IllegalArgumentException(s"Could not find inner alias for outer column : $alias")
          }*/
        case DimColumnInfo(alias) =>
          val col = queryBuilderContext.getDimensionColByAlias(alias)
          val finalAlias = queryBuilderContext.getDimensionColNameForAlias(alias)
          val publicDim = queryBuilderContext.getDimensionForColAlias(alias)
          val referredAlias = s"${queryBuilderContext.getAliasForTable(publicDim.name)}.$finalAlias"
          val postFilterAlias = renderNormalOuterColumn(col, referredAlias)
          s"""$postFilterAlias $finalAlias"""
        case ConstantColumnInfo(alias, value) =>
          val finalAlias = getConstantColAlias(alias)
          s"""'$value' $finalAlias"""
        case _ => throw new UnsupportedOperationException("Unsupported Column Type")
      }
    }

    def renderNormalOuterColumn(column: Column, finalAlias: String) : String = {
      val renderedCol = column.dataType match {
        case DecType(_, _, Some(default), Some(min), Some(max), _) =>
          val minMaxClause = s"CASE WHEN (($finalAlias >= ${min}) AND ($finalAlias <= ${max})) THEN $finalAlias ELSE ${default} END"
          s"""ROUND(COALESCE($minMaxClause, ${default}), 10)"""
        case DecType(_, _, Some(default), _, _, _) =>
          s"""ROUND(COALESCE($finalAlias, ${default}), 10)"""
        case DecType(_, _, _, _, _, _) =>
          s"""ROUND(COALESCE($finalAlias, 0), 10)"""
        case IntType(_,sm,df,_,_) =>
          if (sm.isDefined) {
            s"""COALESCE(CAST($finalAlias as varchar), 'NA')"""
          } else {
            s"""COALESCE(CAST($finalAlias as bigint), ${df.getOrElse(0)})"""
          }
        case DateType(_) => s"""getFormattedDate($finalAlias)"""
        case StrType(_, sm, df, isBinary) =>
          val defaultValue = df.getOrElse("NA")
          if(!isBinary) {
            s"""COALESCE(CAST($finalAlias as VARCHAR), '$defaultValue')"""
          } else {
            s"""CAST($finalAlias as BINARY)"""
          }
        case _ => s"""COALESCE(cast($finalAlias as VARCHAR), 'NA')"""
      }
      if (column.annotations.contains(EscapingRequired)) {
        s"""getCsvEscapedString(CAST(COALESCE($finalAlias, '') AS VARCHAR))"""
      } else {
        renderedCol
      }
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
          case StrType(_, sm, _, _) if sm.isDefined =>
            val defaultValue = sm.get.default
            val whenClauses = sm.get.tToStringMap.map {
              case (from, to) => s"WHEN ($nameOrAlias IN ('$from')) THEN '$to'"
            }
            s"CASE ${whenClauses.mkString(" ")} ELSE '$defaultValue' END"
          case _ =>
            s"""COALESCE($nameOrAlias, "NA")"""
        }
      }

      def renderColumnWithAlias(fact: Fact, column: Column, alias: String, requiredInnerCols: Set[String]): Unit = {
        val name = column.alias.getOrElse(column.name)
        val exp = column match {
          case any if queryBuilderContext.containsColByNameAndAlias(name,alias) =>
            //do nothing, we've already processed it
            ""
          case DimCol(_, dt, _, _, _, _) if dt.hasStaticMapping =>
            val renderedAlias = renderColumnAlias(alias)
            queryBuilderContext.setFactColAliasAndExpression(alias, renderedAlias, column, Option(column.name))
            s"${renderStaticMappedDimension(column)} ${column.name}"
          case DimCol(_, dt, _, _, _, _) =>
            val renderedAlias = renderColumnAlias(alias)
            queryBuilderContext.setFactColAliasAndExpression(alias, renderedAlias, column, Option(name))
            name
          case PrestoDerDimCol(_, dt, _, de, _, _, _) =>
            val renderedAlias = renderColumnAlias(alias)
            val overriddenCol = overrideRenderedCol(false, getAdditionalColData(queryContext.requestModel.reportingRequest), column.asInstanceOf[PrestoDerDimCol], name)
            queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
            s"""${overriddenCol} $renderedAlias"""
          case FactCol(_, dt, _, rollup, _, _, _) =>
            dt match {
              case DecType(_, _, Some(default), Some(min), Some(max), _) =>
                val renderedAlias = renderColumnAlias(alias)
                val minMaxClause = s"CASE WHEN (($name >= $min) AND ($name <= $max)) THEN $name ELSE $default END"
                queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
                s"""${renderRollupExpression(name, rollup, Option(minMaxClause))} $renderedAlias"""
              case IntType(_, _, Some(default), Some(min), Some(max)) =>
                val renderedAlias = renderColumnAlias(alias)
                val minMaxClause = s"CASE WHEN (($name >= $min) AND ($name <= $max)) THEN $name ELSE $default END"
                queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
                s"""${renderRollupExpression(name, rollup, Option(minMaxClause))} $renderedAlias"""
              case _ =>
                val renderedAlias = renderColumnAlias(alias)
                queryBuilderContext.setFactColAliasAndExpression(alias, renderedAlias, column, Option(name))
                s"""${renderRollupExpression(name, rollup)} $name"""
            }
          case PrestoDerFactCol(_, _, dt, cc, de, annotations, rollup, _)
            if queryContext.factBestCandidate.filterCols.contains(name) || de.expression.hasRollupExpression || requiredInnerCols(name)
              || de.isDimensionDriven =>
            val renderedAlias = renderColumnAlias(alias)
            val overriddenCol = overrideRenderedCol(false, getAdditionalColData(queryContext.requestModel.reportingRequest), column.asInstanceOf[PrestoDerFactCol], name)
            queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
            s"""${renderRollupExpression(overriddenCol, rollup)} $renderedAlias"""

          case PrestoDerFactCol(_, _, dt, cc, de, annotations, _, _) =>
            //means no fact operation on this column, push expression outside
            de.sourceColumns.foreach {
              case src if src != name =>
                val sourceCol = fact.columnsByNameMap(src)
                //val renderedAlias = renderColumnAlias(sourceCol.name)
                val renderedAlias = sourceCol.alias.getOrElse(sourceCol.name)

                renderColumnWithAlias(fact, sourceCol, renderedAlias, requiredInnerCols)
              case _ => //do nothing if we reference ourselves
            }
            val renderedAlias = renderColumnAlias(alias)
            val overriddenCol = overrideRenderedCol(false, getAdditionalColData(queryContext.requestModel.reportingRequest), column.asInstanceOf[PrestoDerFactCol], renderedAlias, queryBuilderContext.getColAliasToFactColNameMap, expandDerivedExpression = false)
            queryBuilderContext.setFactColAliasAndExpression(alias, renderedAlias, column, Option(s"""(${overriddenCol})"""))
            ""
          case ConstFactCol(_, _, v, _, _, _, _, _) =>
            val renderedAlias = renderColumnAlias(alias)
            queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
            s"'$v' $name"
        }
        queryBuilder.addFactViewColumn(exp)
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
            renderColumnWithAlias(fact, column, alias, Set.empty)
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
            renderColumnWithAlias(fact, column, alias, Set.empty)
        }
      }

      //render derived columns last
      groupedFactCols.get(true).foreach { derivedCols =>
        val requiredInnerCols: Set[String] =
          derivedCols.view.map(_._1.asInstanceOf[DerivedColumn]).flatMap(dc => dc.derivedExpression.sourceColumns).toSet
        derivedCols.foreach {
          case (column, alias) =>
            val renderedAlias = s""""$alias""""
            renderColumnWithAlias(fact, column, alias, requiredInnerCols)
        }
      }

      val hasPartitioningScheme = fact.annotations.contains(PrestoQueryGenerator.ANY_PARTITIONING_SCHEME)

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
                val overriddenCol = overrideRenderedCol(false, getAdditionalColData(queryContext.requestModel.reportingRequest), x.asInstanceOf[PrestoDerFactCol], name)
                s"""${renderRollupExpression(overriddenCol, rollup)}"""
              case any =>
                throw new UnsupportedOperationException(s"Found non fact column : $any")
            }
          val result = QueryGeneratorHelper.handleFilterSqlRender(filter, publicFact, fact, aliasToNameMapFull, queryContext, PrestoEngine, prestoLiteralMapper, colRenderFn)

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

    def getPkFinalAliasForDim (dimBundle: DimensionBundle) : String = {
      val pkColName = dimBundle.dim.primaryKey
      val dimAlias = queryBuilderContext.getAliasForTable(dimBundle.publicDim.name)
      s"${dimAlias}_$pkColName"
    }

    def generateDimJoinQuery(dimBundle: DimensionBundle, requestModel: RequestModel) : String = {

      /**
        *  render fact/dim columns with derived/rollup expression
        */
      def renderColumn(column: Column, alias: String): String = {
        val name = column.alias.getOrElse(column.name)
        column match {
          case DimCol(_, dt, _, _, _, _) =>
            name
          case PrestoDerDimCol(_, dt, _, de, _, _, _) =>
            val overriddenCol = overrideRenderedCol(false, getAdditionalColData(queryContext.requestModel.reportingRequest), column.asInstanceOf[PrestoDerDimCol], name)
            s"""${overriddenCol}"""
          case other => throw new IllegalArgumentException(s"Unhandled column type for dimension cols : $other")
        }
      }

      val requestDimCols = dimBundle.fields
      val publicDimName = dimBundle.publicDim.name
      val dimTableName = dimBundle.dim.underlyingTableName.getOrElse(dimBundle.dim.name)
      val dimFilters = dimBundle.filters
      val fkColName = fact.publicDimToForeignKeyMap(publicDimName)
      val fkCol = fact.columnsByNameMap(fkColName)
      val pkColName = dimBundle.dim.primaryKey
      val dimAlias = queryBuilderContext.getAliasForTable(publicDimName)

      val dimCols = requestDimCols map {
        colAlias =>
          if (dimBundle.publicDim.isPrimaryKeyAlias(colAlias)) {
            s"""$pkColName ${getPkFinalAliasForDim(dimBundle)}"""
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
            PrestoEngine,
            prestoLiteralMapper
          ).filter
      }

      val partitionFilters = partitionColumnRenderer.renderDim(requestModel, dimBundle, prestoLiteralMapper, PrestoEngine)
      val renderedFactFk = renderColumn(fkCol, "")

      val dimWhere = s"""WHERE ${RenderedAndFilter(wheres + partitionFilters).toString}"""

      val joinType = if (requestModel.anyDimHasNonFKNonForceFilter) {
        "JOIN"
      } else {
        "LEFT OUTER JOIN"
      }

      // Columns on which join is done must be of the same type. Add explicit CAST otherwise.
      val joinCondition = {
        require(dimBundle.dim.columnsByNameMap.contains(pkColName), s"Dim: ${dimBundle.dim.name} does not contain $pkColName")
        if (fkCol.dataType.getClass.equals(dimBundle.dim.columnsByNameMap(pkColName).dataType.getClass)) {
          s"$factViewAlias.$renderedFactFk = $dimAlias.${dimAlias}_$pkColName"
        } else {
          s"CAST($factViewAlias.$renderedFactFk AS VARCHAR) = CAST($dimAlias.${dimAlias}_$pkColName AS VARCHAR)"
        }
      }

      s"""$joinType (
         |SELECT ${dimCols.mkString(", ")}
         |FROM $dimTableName
         |$dimWhere
         |)
         |$dimAlias
         |ON
         |$joinCondition
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
                getPkFinalAliasForDim(dimBundle)
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
        queryBuilder.addDimensionJoin(generateDimJoinQuery(dim, requestModel))
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

    val concatenatedCols = generateConcatenatedCols()
    val outerCols = generateOuterColumns()
    val dimJoinQuery = queryBuilder.getJoinExpressions

    generateOrderByClause(queryContext, queryBuilderContext, queryBuilder);

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
      IndexedSeq.empty
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

object PrestoQueryGenerator extends Logging {
  val ANY_PARTITIONING_SCHEME = PrestoPartitioningScheme("") //no name needed since class name hashcode

  def register(queryGeneratorRegistry: QueryGeneratorRegistry, partitionDimensionColumnRenderer:PartitionColumnRenderer, udfStatements: Set[UDFRegistration]) = {
    if(!queryGeneratorRegistry.isEngineRegistered(PrestoEngine, Option(Version.DEFAULT))) {
      val generator = new PrestoQueryGenerator(partitionDimensionColumnRenderer:PartitionColumnRenderer, udfStatements)
      queryGeneratorRegistry.register(PrestoEngine, generator)
    } else {
      queryGeneratorRegistry.getDefaultGenerator(PrestoEngine).foreach {
        qg =>
          if(!qg.isInstanceOf[PrestoQueryGenerator]) {
            warn(s"Another query generator registered for PrestoEngine : ${qg.getClass.getCanonicalName}")
          }
      }
    }
  }
}