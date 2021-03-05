package com.yahoo.maha.core.query.hive

import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.query._
import grizzled.slf4j.Logging

import scala.collection.mutable

/*
    Created by pranavbhole on 5/28/19
*/
abstract case class HiveOuterGroupByQueryGenerator(partitionColumnRenderer:PartitionColumnRenderer, udfStatements: Set[UDFRegistration]) extends HiveQueryGeneratorCommon(partitionColumnRenderer, udfStatements) with BigqueryHivePrestoQueryCommon with Logging {

  protected def generateOuterGroupByQuery(queryContext: DimFactOuterGroupByQueryQueryContext): Query = {

    //init vars
    val queryBuilderContext = new QueryBuilderContext
    val queryBuilder: QueryBuilder = new QueryBuilder(
      queryContext.requestModel.requestCols.size + 5
      , queryContext.requestModel.requestSortByCols.size + 1)
    val requestModel = queryContext.requestModel
    val factCandidate = queryContext.factBestCandidate
    val publicFact = queryContext.factBestCandidate.publicFact
    val fact = factCandidate.fact
    val factViewName = fact.name
    val factViewAlias = queryBuilderContext.getAliasForTable(factViewName)
    val dims = queryContext.dims

    val requestedCols = queryContext.requestModel.requestCols
    val columnAliasToColMap = new mutable.HashMap[String, Column]()

    val primitiveColsSet = new mutable.LinkedHashSet[(String, Column)]()
    val noopRollupColSet = new mutable.LinkedHashSet[(String, Column)]()

    val aliasColumnMapOfRequestCols = new mutable.HashMap[String, Column]()

    /**
      * Inner most fact query
      */
    val factQueryFragment = generateFactOGBQuery(queryContext, queryBuilder, queryBuilderContext, renderRollupExpression, renderColumnWithAlias, primitiveColsSet, noopRollupColSet)

    generateDimSelects(dims, queryBuilderContext, queryBuilder, requestModel, fact, factViewAlias)

    ogbGeneratePreOuterColumns(primitiveColsSet.toMap, noopRollupColsMap = noopRollupColSet.toMap, queryContext.factBestCandidate, queryContext, queryBuilder, queryBuilderContext)

    generateOrderByClause(queryContext, queryBuilder, queryBuilderContext)
    val orderByClause = queryBuilder.getOrderByClause

    val outerCols = queryBuilder.getPreOuterColumns  //generateOuterColumns(queryContext, queryBuilderContext, queryBuilder, renderOuterColumn)

    ogbGenerateOuterColumns(queryContext, queryBuilder, queryBuilderContext, aliasColumnMapOfRequestCols)

    val concatenatedCols = queryBuilder.getOuterColumns //generateConcatenatedColsWithCast(queryContext, queryBuilderContext)

    val projectedConcatWs:String = {
      concat_ws(requestModel.requestCols.map(colInfo =>
        to_string(nvl(renderColumnAlias(colInfo.alias)))).mkString(",")
      )
    }

    val queryAlias = getQueryAliasWithRowLimit(requestModel)

    val parameterizedQuery : String = {
      val dimJoinQuery = queryBuilder.getJoinExpressions

      // factViewAlias => needs to generate abbr from factView name like account_stats_1h_v2 -> as1v0
      // outerCols same cols in concate cols, different expression ???
      s"""SELECT ${projectedConcatWs}
         |FROM(
         |SELECT $concatenatedCols
         |FROM(
         |SELECT $outerCols
         |FROM($factQueryFragment)
         |$factViewAlias
         |$dimJoinQuery
         |${queryBuilder.getOuterGroupByClause}
         |$orderByClause) OgbQueryAlias
         |) $queryAlias
       """.stripMargin
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

  /**
    * Inner Fact query generator which select only primitive columns from the fact.
    *
    * Primitive columns: Primitive columns are fact cols rendered in inner select which are base columns(non-derived) of all the derived columns
    * Derived columns with dependencies are not rendered in ogbGenerateFactViewColumns
    * Primitive columns are also rendered in preOuterSelect rendering
    */
  def generateFactOGBQuery(queryContext: DimFactOuterGroupByQueryQueryContext,
                                queryBuilder: QueryBuilder,
                                queryBuilderContext: QueryBuilderContext,
                                renderRollupExpression: (String, RollupExpression, Option[String]) => String,
                                renderColumnWithAlias: (Fact, Column, String, Set[String], Boolean, QueryContext, QueryBuilderContext, QueryBuilder, Engine) => Unit,
                                primitiveColsSet: mutable.LinkedHashSet[(String, Column)],
                                noopRollupColSet: mutable.LinkedHashSet[(String, Column)]) : String = { // base: query common

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
                queryBuilder.addGroupBy(nameOrAlias)
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
        case (column:FactCol, alias) if !column.rollupExpression.isInstanceOf[HiveCustomRollup] =>
          val name = column.alias.getOrElse(column.name)
          primitiveColsSet.add((name, column))
        case _=>
      }
    }

    groupedFactCols.get(true).map {
      derivedFactCols =>
        dfsGetPrimitiveCols(fact, derivedFactCols.map(_._1).toIndexedSeq, primitiveColsSet, HiveEngine)
    }

    //
    val customRollupSet = getCustomRollupColsSet(groupedFactCols, queryBuilderContext)
    if(customRollupSet.nonEmpty) {
      dfsGetPrimitiveCols(fact, customRollupSet.map(_._1).toIndexedSeq, primitiveColsSet, HiveEngine)
    }

    // Find out all the NoopRollup cols recursively
    dfsNoopRollupCols(fact, factCols.toSet, List.empty, noopRollupColSet)

    // Render Primitive columns
    primitiveColsSet.foreach {
      case (alias:String, column: Column) =>
        renderColumnWithAlias(fact, column, alias, Set.empty, false, queryContext, queryBuilderContext, queryBuilder, HiveEngine)
        val colName= column.alias.getOrElse(column.name)
        // If recursively found primitive col is dimension column then add it to group by clause
        if (fact.dimColMap.contains(colName)) {
          queryBuilder.addGroupBy(colName)
        }
    }

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
            case HiveDerFactCol(_, _, dt, cc, de, annotations, rollup, _) => //This never gets used, otherwise errors would be thrown before the Generator.
              s"""${renderRollupExpression(de.render(x.name, Map.empty), rollup, None)}"""
            case any =>
              throw new UnsupportedOperationException(s"Found non fact column : $any")
          }
        val result = QueryGeneratorHelper.handleFilterSqlRender(filter, publicFact, fact, aliasToNameMapFull, null, HiveEngine, hiveLiteralMapper, colRenderFn)

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


  def ogbGeneratePreOuterColumns(primitiveInnerAliasColMap: Map[String, Column],
                                 noopRollupColsMap: Map[String, Column],
                                 factBest:FactBestCandidate,
                                 queryContext:DimFactOuterGroupByQueryQueryContext,
                                 queryBuilder: QueryBuilder,
                                 queryBuilderContext:QueryBuilderContext): Unit = {
    // add requested dim and fact columns, this should include constants
    val preOuterRenderedColAliasMap = new mutable.HashMap[Column, String]()

    val isFactOnlyQuery = queryContext.dims.isEmpty

    queryContext.requestModel.requestCols.foreach {
      case columnInfo@FactColumnInfo(alias) if factBest.publicFact.aliasToNameColumnMap.contains(alias) =>
        val colName = factBest.publicFact.aliasToNameColumnMap(alias)
        val col = factBest.fact.columnsByNameMap(colName)
        val aliasOrColName = col.alias.getOrElse(colName)
        // Check if alias is rendered in inner selection or not
        if(factBest.factColMapping.contains(colName)) {
          if(queryBuilderContext.containsFactAliasToColumnMap(aliasOrColName)) {
            if (primitiveInnerAliasColMap.contains(aliasOrColName)) {
              val innerSelectCol = queryBuilderContext.getFactColByAlias(aliasOrColName)
              val qualifiedColInnerAlias = if (queryContext.shouldQualifyFactsInPreOuter) {
                queryBuilderContext.getFactColNameForAlias(aliasOrColName)
              } else aliasOrColName
              renderPreOuterFactCol(qualifiedColInnerAlias, aliasOrColName, alias, innerSelectCol)
            } else {
              val col = queryBuilderContext.getFactColByAlias(aliasOrColName)
              if (col.isInstanceOf[FactCol] && col.asInstanceOf[FactCol].rollupExpression.isInstanceOf[HiveCustomRollup]) {
                renderPreOuterFactCol(aliasOrColName, aliasOrColName, alias, col)
              }
            }
          } else {
            // handle custom rollup fo
            val col = factBest.fact.columnsByNameMap(aliasOrColName)
            if (col.isInstanceOf[FactCol] && col.asInstanceOf[FactCol].rollupExpression.isInstanceOf[HiveCustomRollup]) {
              renderPreOuterFactCol(aliasOrColName, aliasOrColName, alias, col)
            }
          }
        } else {
          // Condition to handle dimCols mapped to FactColumnInfo in requestModel
          if(queryBuilderContext.containsFactAliasToColumnMap(alias)) {
            val (renderedCol, renderedAlias) = renderOuterColumn(columnInfo, queryBuilderContext, queryContext.factBestCandidate.duplicateAliasMapping, factBest, true)
            queryBuilder.addPreOuterColumn(concat(renderedCol, renderedAlias))
            queryBuilder.addOuterGroupByExpressions(renderedCol)
            queryBuilderContext.setPreOuterAliasToColumnMap(renderedCol, renderedAlias, col)
            preOuterRenderedColAliasMap.put(queryBuilderContext.getFactColByAlias(alias), renderedAlias)
          }
        }

      case columnInfo@DimColumnInfo(alias) =>
        val (renderedCol, renderedAlias) = renderOuterColumn(columnInfo, queryBuilderContext, queryContext.factBestCandidate.duplicateAliasMapping, factBest, true)
        queryBuilder.addPreOuterColumn(concat(renderedCol, renderedAlias))
        queryBuilder.addOuterGroupByExpressions(renderedCol)
        queryBuilderContext.setPreOuterAliasToColumnMap(renderedCol, renderedAlias, queryBuilderContext.getDimensionColByAlias(alias))
      case ConstantColumnInfo(alias, value) =>
      // rendering constant columns only in outer columns
      case _ => throw new UnsupportedOperationException("Unsupported Column Type")
    }
    // Render primitive cols
    primitiveInnerAliasColMap.foreach {
      // if primitive col is not already rendered
      case (alias, col) if !preOuterRenderedColAliasMap.contains(col) =>
        col match {
          case dimCol:DimensionColumn =>
            //dim col which are dependent upon the DerFact cols
            val (renderedCol, renderedAlias) = renderOuterColumn(FactColumnInfo(alias), queryBuilderContext, queryContext.factBestCandidate.duplicateAliasMapping, factBest, true)
            queryBuilder.addPreOuterColumn(concat(s"$renderedCol $renderedAlias",""))
            queryBuilder.addOuterGroupByExpressions(renderedCol)
            queryBuilderContext.setPreOuterAliasToColumnMap(renderedCol, renderedAlias, col)
          case _=>
            val colInnerAlias = col.alias.getOrElse(col.name)
            val qualifiedColInnerAlias = if(queryContext.shouldQualifyFactsInPreOuter) {
              queryBuilderContext.getFactColNameForAlias(colInnerAlias)
            } else colInnerAlias
            renderPreOuterFactCol(qualifiedColInnerAlias, colInnerAlias, alias, col)
        }
      // if primitive col is already rendered as Public alias, render it as inner alias for the outer derived cols
      case (alias, col) if preOuterRenderedColAliasMap.contains(col) =>
        // check is the alias is not already rendered
        if (!preOuterRenderedColAliasMap.values.toSet.contains(alias)) {
          col match  {
            case DimCol(_, dt, cc, _, annotations, _) =>
              val name = col.alias.getOrElse(col.name)
              queryBuilder.addPreOuterColumn(s"""$name AS $alias""")
              queryBuilder.addOuterGroupByExpressions(name)
              queryBuilderContext.setPreOuterAliasToColumnMap(name, alias, col)
            case _=> // cant be the case for primitive cols
          }
        }
      case _=>
    }

    // Render NoopRollup cols
    noopRollupColsMap.foreach {
      case (alias, col) if !preOuterRenderedColAliasMap.keySet.contains(col) =>
        val colInnerAlias = renderColumnAlias(col.name)
        val qualifiedColInnerAlias = if(queryContext.shouldQualifyFactsInPreOuter) {
          queryBuilderContext.getFactColNameForAlias(colInnerAlias)
        } else col.alias.getOrElse(col.name)
        renderPreOuterFactCol(qualifiedColInnerAlias, colInnerAlias, alias, col)
      case _=> // ignore as it col is already rendered
    }

    def renderPreOuterFactCol(qualifiedColInnerAlias: String, colInnerAlias: String, finalAlias: String, innerSelectCol: Column): Unit = {
      val preOuterFactColRendered = innerSelectCol match {
        case FactCol(_, dt, cc, rollup, _, annotations, _) =>
          s"""${renderRollupExpression(qualifiedColInnerAlias, rollup)} AS $colInnerAlias"""
        case HiveDerFactCol(_, _, dt, cc, de, annotations, rollup, _) =>
          s"""${renderRollupExpression(de.render(qualifiedColInnerAlias, Map.empty), rollup)} AS $colInnerAlias"""
        case _=> throw new IllegalArgumentException(s"Unexpected Col $innerSelectCol found in FactColumnInfo ")
      }
      val colInnerAliasQuoted = if(innerSelectCol.isDerivedColumn) {
        s"""$colInnerAlias"""
      } else colInnerAlias

      preOuterRenderedColAliasMap.put(innerSelectCol, colInnerAlias)
      queryBuilderContext.setPreOuterAliasToColumnMap(colInnerAliasQuoted, finalAlias, innerSelectCol)
      queryBuilder.addPreOuterColumn(preOuterFactColRendered)
    }

  } //end ogbGeneratePreOuterColumns


  def ogbGenerateOuterColumns(queryContext: DimFactOuterGroupByQueryQueryContext,
                              queryBuilder: QueryBuilder,
                              queryBuilderContext: QueryBuilderContext, aliasColumnMapOfRequestCols:mutable.HashMap[String, Column]): Unit = {
    // add requested dim and fact columns, this should include constants
    val factBest = queryContext.factBestCandidate
    queryContext.requestModel.requestCols foreach {
      columnInfo =>
        if (!columnInfo.isInstanceOf[ConstantColumnInfo] && queryBuilderContext.containsFactAliasToColumnMap(columnInfo.alias)) {
          aliasColumnMapOfRequestCols += (columnInfo.alias -> queryBuilderContext.getFactColByAlias(columnInfo.alias))
        } else if (queryContext.factBestCandidate.duplicateAliasMapping.contains(columnInfo.alias)) {
          val sourceAliases = queryContext.factBestCandidate.duplicateAliasMapping(columnInfo.alias)
          val sourceAlias = sourceAliases.find(queryBuilderContext.aliasColumnMap.contains)
          require(sourceAlias.isDefined
            , s"Failed to find source column for duplicate alias mapping : ${queryContext.factBestCandidate.duplicateAliasMapping(columnInfo.alias)}")
          aliasColumnMapOfRequestCols += (columnInfo.alias -> queryBuilderContext.aliasColumnMap(sourceAlias.get))
        } else if (queryBuilderContext.isDimensionCol(columnInfo.alias)) {
          aliasColumnMapOfRequestCols += (columnInfo.alias -> queryBuilderContext.getDimensionColByAlias(columnInfo.alias))
        } else if (queryBuilderContext.containsPreOuterAlias(columnInfo.alias)) {
          aliasColumnMapOfRequestCols += (columnInfo.alias -> queryBuilderContext.getPreOuterAliasToColumnMap(columnInfo.alias).get)
        }

        val renderedAlias = renderColumnAlias(columnInfo.alias)

        val renderedCol = columnInfo match {
          case FactColumnInfo(alias) if queryBuilderContext.containsPreOuterAlias(alias) =>
            val preOuterAliasOption = queryBuilderContext.getPreOuterFinalAliasToAliasMap(alias)
            if(preOuterAliasOption.isDefined) {
              val preOuterAlias = preOuterAliasOption.get
              s"""$preOuterAlias AS $renderedAlias"""
            } else {
              s"""$renderedAlias"""
            }
          case FactColumnInfo(alias) =>
            val column:Column  = if(queryBuilderContext.containsFactAliasToColumnMap(alias)) {
              queryBuilderContext.getFactColByAlias(alias)
            } else {
              // Case to handle CustomRollup Columns
              val aliasToColNameMap: Map[String, String] = factBest.factColMapping.map {
                case (factColName, factAlias) =>
                  val col = factBest.fact.columnsByNameMap(factColName)
                  val name:String = col.alias.getOrElse(col.name)
                  factAlias -> name
              }
              require(aliasToColNameMap.contains(alias), s"Can not find the alias $alias in aliasToColNameMap")
              val colName = aliasToColNameMap(alias)
              factBest.fact.columnsByNameMap(colName)
            }
            renderParentOuterDerivedFactCols(queryBuilderContext, renderedAlias, column)
          case DimColumnInfo(alias) => {
            val renderedAlias = renderColumnAlias(alias)
            val colName = queryBuilderContext.getDimensionColNameForAlias(alias)
            s"""$colName AS $renderedAlias"""
          }
          case ConstantColumnInfo(alias, value) =>
            val renderedAlias = renderColumnAlias(alias)
            s"""'$value' AS $renderedAlias"""
          case _ => throw new UnsupportedOperationException("Unsupported Column Type")
        }
        queryBuilder.addOuterColumn(renderedCol)
    }

/*
    val outerWhereClause = generateOuterWhereClause(queryContext, queryBuilderContext)
    queryBuilder.setOuterWhereClause(outerWhereClause.toString)
*/

  }

  /*
   Find the Custom Rollup columns, and also sets non dependent primitive cols in the query builder context
   */
  private def getCustomRollupColsSet(groupedFactCols: Map[Boolean, List[(Column, String)]], queryBuilderContext:QueryBuilderContext) : mutable.LinkedHashSet[(Column, String)] = {
    val customRollupSet = new mutable.LinkedHashSet[(Column, String)]
    for {
      groupedFactCols <- groupedFactCols.get(false)
    } yield {
      groupedFactCols.foreach {
        case (f:FactCol, colAlias: String) if f.rollupExpression.isInstanceOf[HiveCustomRollup] =>
          customRollupSet.add((f,colAlias))
          //if custom rollup depends on itself as a primitive col, then don't add to context
          if(!f.rollupExpression.asInstanceOf[HiveCustomRollup].expression.sourceColumns(f.name)) {
            queryBuilderContext.setFactColAlias(f.alias.getOrElse(f.name), colAlias, f)
          }
        case _=> // ignore for all the other cases as they are handled
      }
      customRollupSet
    }
    customRollupSet
  }

  /*
  Commonly used method
 */
  def renderOuterColumn(columnInfo: ColumnInfo,
                        queryBuilderContext: QueryBuilderContext,
                        duplicateAliasMapping: Map[String, Set[String]],
                        factCandidate: FactBestCandidate,
                        isOuterGroupBy: Boolean = false): (String, String) = {

      def renderNormalOuterColumnWithoutCasting(column: Column, finalAlias: String) : String = {
        val renderedCol = column.dataType match {
          case DecType(_, _, Some(default), Some(min), Some(max), _) =>
            val minMaxClause = s"CASE WHEN (($finalAlias >= ${min}) AND ($finalAlias <= ${max})) THEN $finalAlias ELSE ${default} END"
            s"""ROUND(COALESCE($minMaxClause, ${default}), 10)"""
          case DecType(_, _, Some(default), _, _, _) =>
            s"""ROUND(COALESCE($finalAlias, ${default}), 10)"""
          case DecType(_, _, _, _, _, _) =>
            s"""ROUND(COALESCE($finalAlias, 0L), 10)"""
          case IntType(_,sm,df,_,_) =>
            if (sm.isDefined && isOuterGroupBy) {
              handleStaticMappingInt(sm, finalAlias)
            }
            else {
              s"""COALESCE($finalAlias, ${df.getOrElse(0)}L)"""
            }
          case DateType(_) => s"""getFormattedDate($finalAlias)"""
          case StrType(_, sm, df) =>
            val defaultValue = df.getOrElse("NA")
            if (sm.isDefined && isOuterGroupBy) {
              handleStaticMappingString(sm, finalAlias, defaultValue)
            }
            else {
              s"""COALESCE($finalAlias, '$defaultValue')"""
            }
          case _ => s"""COALESCE($finalAlias, 'NA')"""
        }
        if (column.annotations.contains(EscapingRequired)) {
          s"""getCsvEscapedString(CAST(NVL($finalAlias, '') AS STRING))"""
        } else {
          renderedCol
        }
      }

      def renderFactCol(alias: String, finalAliasOrExpression: String, col: Column, finalAlias: String): (String,String) = {
        val postFilterAlias = renderNormalOuterColumnWithoutCasting(col, finalAliasOrExpression)
        (postFilterAlias,finalAlias)
      }
      def renderDimCol(alias:String) : (String, String) = {
        val col = queryBuilderContext.getDimensionColByAlias(alias)
        val finalAlias = queryBuilderContext.getDimensionColNameForAlias(alias)
        val publicDim = queryBuilderContext.getDimensionForColAlias(alias)
        val referredAlias = s"${queryBuilderContext.getAliasForTable(publicDim.name)}.$finalAlias"
        val postFilterAlias = renderNormalOuterColumnWithoutCasting(col, referredAlias)
        (postFilterAlias, finalAlias)
      }

      columnInfo match {
        case FactColumnInfo(alias) =>
          if (queryBuilderContext.isDimensionCol(alias) && isOuterGroupBy) {
            // Render ID Cols from dimensions with table and alias it with fact col as it is FactColumnInfo
            val col = queryBuilderContext.getDimensionColByAlias(alias)
            val dimAlias = queryBuilderContext.getDimensionColNameForAlias(alias)
            val publicDim = queryBuilderContext.getDimensionForColAlias(alias)
            val referredAlias = s"${queryBuilderContext.getAliasForTable(publicDim.name)}.$dimAlias"
            val postFilterAlias = renderNormalOuterColumnWithoutCasting(col, referredAlias)

            val factAlias = queryBuilderContext.getFactColNameForAlias(alias)
            (postFilterAlias, factAlias)
          } else {
            QueryGeneratorHelper.handleOuterFactColInfo(queryBuilderContext, alias, factCandidate, renderFactCol, duplicateAliasMapping, factCandidate.fact.name, isOuterGroupBy)
          }
        case DimColumnInfo(alias) =>
          renderDimCol(alias)
        case ConstantColumnInfo(alias, value) =>
          val finalAlias = getConstantColAlias(alias)
          (s"'$value'", finalAlias)
        case _ => throw new UnsupportedOperationException("Unsupported Column Type")
      }
  }
}
