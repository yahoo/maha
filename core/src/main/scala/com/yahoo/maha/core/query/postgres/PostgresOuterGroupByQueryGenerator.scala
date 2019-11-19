// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.postgres

import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact.{FactCol, NoopRollup, PostgresCustomRollup, PostgresDerFactCol}
import com.yahoo.maha.core.query._
import grizzled.slf4j.Logging
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by pranavbhole on 22/11/17.
 */
abstract class PostgresOuterGroupByQueryGenerator(partitionColumnRenderer:PartitionColumnRenderer, literalMapper: PostgresLiteralMapper = new PostgresLiteralMapper) extends PostgresQueryCommon with Logging {

  protected[this] def generateOuterWhereClause(queryContext: QueryContext, queryBuilderContext: QueryBuilderContext) : WhereClause = {
    val outerFilters = new mutable.LinkedHashSet[String]
    val requestModel =  queryContext.requestModel
    requestModel.outerFilters.foreach {
      filter =>
        val f = FilterSql.renderOuterFilter(filter, queryBuilderContext.aliasColumnMap, PostgresEngine, literalMapper)
        outerFilters += f.filter
    }

    WhereClause(RenderedAndFilter(outerFilters))
  }

  /*
  Method to create an OGB(Outer Group By) query
 */
  protected[this] def generateDimFactOuterGroupByQuery(queryContext: DimFactOuterGroupByQueryQueryContext): Query = {
    val paramBuilder = new QueryParameterBuilder
    val queryBuilderContext = new QueryBuilderContext
    val queryBuilder: QueryBuilder = new QueryBuilder(
      queryContext.requestModel.requestCols.size + 5
      , queryContext.requestModel.requestSortByCols.size + 1)
    val requestModel = queryContext.requestModel
    val optionalHint: String = getFactOptionalHint(queryContext, requestModel).map(toComment).getOrElse("")
    val aliasColumnMapOfRequestCols = new mutable.HashMap[String, Column]()
    val isFactOnlyQuery = requestModel.isFactDriven && queryContext.dims.forall {
      db => (db.fields.filterNot(db.publicDim.isPrimaryKeyAlias).isEmpty && !db.hasNonFKSortBy
        && queryContext.factBestCandidate.publicFact.foreignKeyAliases(db.publicDim.primaryKeyByAlias))
    }
    val requestColAliasesSet = requestModel.requestCols.map(_.alias).toIndexedSeq
    val factOnlySubqueryFields : IndexedSeq[String] = if(isFactOnlyQuery) {
      queryContext.dims.view.filter(!_.hasNonFKNonForceFilters).map(_.publicDim.primaryKeyByAlias).filterNot(requestColAliasesSet.toSet).toIndexedSeq
    } else IndexedSeq.empty

    val factBest = queryContext.factBestCandidate

    def ogbGenerateDimJoin(): Unit = {
      if (queryContext.dims.nonEmpty) {
        val dsql = generateDimensionSql(queryContext, queryBuilderContext, true)
        queryBuilder.addDimensionJoin(dsql.drivingDimensionSql)
        //TODO: add support for optimal mutli dimension sort by metric query
        //TODO: right now it just does join with driving table
        dsql.multiDimensionJoinSql.foreach(queryBuilder.addMultiDimensionJoin)
      }
    }

    def ogbGenerateOrderBy(): Unit = {
      if(requestModel.isDimDriven && requestModel.hasDrivingDimNonFKNonPKSortBy) {
        // In Dim driven case, if driving dimension has orderBy then we do not want to orderBy again in the outer as it mess up the order
      } else {
        requestModel.requestSortByCols.foreach {
          ci =>
            queryBuilder.addOrderBy(renderSortByColumn(ci, queryBuilderContext))
        }
      }
    }

    def ogbGenerateWhereAndHavingClause(): Unit = {
      // inner fact where clauses
      val fact = queryContext.factBestCandidate.fact
      val publicFact = queryContext.factBestCandidate.publicFact
      val filters = queryContext.factBestCandidate.filters
      val allFilters = publicFact.forcedFilters //++ filters  need to append regular filters or pass in
      val whereFilters = new mutable.LinkedHashSet[String]
      val havingFilters = new mutable.LinkedHashSet[String]
      val hasPartitioningScheme = fact.annotations.contains(PostgresQueryGenerator.ANY_PARTITIONING_SCHEME)

      if (requestModel.isFactDriven || requestModel.dimensionsCandidates.isEmpty || requestModel.hasNonFKFactFilters || requestModel.hasFactSortBy || fact.forceFilters.nonEmpty) {
        val unique_filters = removeDuplicateIfForced( filters.toSeq, allFilters.toSeq, queryContext )
        unique_filters.sorted.foreach {
          filter =>
            val isMultiField : Boolean = filter.isInstanceOf[MultiFieldForcedFilter]
            val names: mutable.ArrayBuffer[String] = new ArrayBuffer[String]()
            names += publicFact.aliasToNameColumnMap(filter.field)
            if(isMultiField)
              names += publicFact.aliasToNameColumnMap(filter.asInstanceOf[MultiFieldForcedFilter].compareTo)

            val name1 = names.remove(0)
            if (fact.dimColMap.contains(name1)) {
              if(isMultiField){
                val name2 = names.remove(0)
                require(fact.dimColMap.contains(name2), "Dim-Metric Comparison Failed: Can only compare dim-dim or metric-metric")
              }

              val f = FilterSql.renderFilter(
                filter,
                queryContext.factBestCandidate.publicFact.aliasToNameColumnMap,
                Map.empty,
                fact.columnsByNameMap,
                PostgresEngine,
                literalMapper)
              whereFilters += f.filter
            } else if (fact.factColMap.contains(name1)) {
              val column = fact.columnsByNameMap(name1)
              val alias = queryContext.factBestCandidate.factColMapping(name1)
              val nameOrAlias = column.alias.getOrElse(name1)
              val exp = column match {
                case FactCol(_, dt, cc, rollup, _, annotations, _) =>
                  s"""${renderRollupExpression(nameOrAlias, rollup, isOuterGroupBy = true)}"""
                case PostgresDerFactCol(_, _, dt, cc, de, annotations, rollup, _) =>
                  s"""${renderRollupExpression(de.render(nameOrAlias, Map.empty), rollup, isOuterGroupBy = true)}"""
                case any =>
                  throw new UnsupportedOperationException(s"Found non fact column : $any")
              }
              if(!isMultiField) {
                val f = FilterSql.renderFilter(
                  filter,
                  queryContext.factBestCandidate.publicFact.aliasToNameColumnMap,
                  Map(alias -> (name1, exp)),
                  fact.columnsByNameMap,
                  PostgresEngine,
                  literalMapper
                )
                havingFilters += f.filter
              } else {
                val name2 = names.remove(0)
                require(fact.factColMap.contains(name2), "Metric-Dim Comparison Failed: Can only compare dim-dim or metric-metric")
                val column2 = fact.columnsByNameMap(name2)
                val alias2 = queryContext.factBestCandidate.factColMapping(name2)
                val nameOrAlias2 = column2.alias.getOrElse(name2)
                val exp2 = column2 match {
                  case FactCol(_, dt, cc, rollup, _, annotations, _) =>
                    s"""${renderRollupExpression(nameOrAlias2, rollup, isOuterGroupBy = true)}"""
                  case PostgresDerFactCol(_, _, dt, cc, de, annotations, rollup, _) =>
                    s"""${renderRollupExpression(de.render(nameOrAlias2, Map.empty), rollup, isOuterGroupBy = true)}"""
                  case any =>
                    throw new UnsupportedOperationException(s"Found non fact column : $any")
                }
                val f = FilterSql.renderFilter(
                  filter,
                  queryContext.factBestCandidate.publicFact.aliasToNameColumnMap,
                  Map(alias -> (name2, exp), alias2 -> (name2, exp2)),
                  fact.columnsByNameMap,
                  PostgresEngine,
                  literalMapper
                )
                havingFilters += f.filter
              }
            } else {
              throw new IllegalArgumentException(
                s"Unknown fact column: publicFact=${publicFact.name}, fact=${fact.name} alias=${filter.field}, name=$name1")
            }
        }
      }
      val dayFilter = FilterSql.renderFilter(
        requestModel.localTimeDayFilter,
        queryContext.factBestCandidate.publicFact.aliasToNameColumnMap,
        Map.empty,
        fact.columnsByNameMap,
        PostgresEngine,
        literalMapper).filter

      val combinedQueriedFilters = {
        if (hasPartitioningScheme) {
          val partitionFilters = new mutable.LinkedHashSet[String]
          val partitionFilterOption = partitionColumnRenderer.renderFact(queryContext, literalMapper, PostgresEngine)
          if(partitionFilterOption.isDefined) {
            partitionFilters += partitionFilterOption.get
            RenderedAndFilter(partitionFilters ++ whereFilters)
          } else {
            RenderedAndFilter(whereFilters + dayFilter)
          }
        } else {
          RenderedAndFilter(whereFilters + dayFilter)
        }
      }

      val whereClauseExpression = s"""WHERE ${combinedQueriedFilters.toString} """
      queryBuilder.setWhereClause(whereClauseExpression)

      if (havingFilters.nonEmpty) {
        val havingAndFilters = RenderedAndFilter(havingFilters.toIndexedSeq)
        val havingClauseExpression = s"""HAVING ${havingAndFilters.toString}"""
        queryBuilder.setHavingClause(havingClauseExpression)
      }

    }

    /*
     Primitive columns are fact cols rendered in inner select which are dependent on the derived columns
     Derived columns with dependencies are not rendered in ogbGenerateFactViewColumns
     Primitive columns are also rendered in preOuterSelect rendering
      */
    val primitiveColsSet = new mutable.LinkedHashSet[(String, Column)]()

    /*
     Nooprollup parent cols storage set used by preOuter column renderer
    */
    val noopRollupColSet = new mutable.LinkedHashSet[(String, Column)]()

    def ogbGenerateFactViewColumns(): Unit = {
      val factTableAlias = queryBuilderContext.getAliasForTable(queryContext.factBestCandidate.fact.name)
      val fact = queryContext.factBestCandidate.fact

      val dimCols = queryContext.factBestCandidate.dimColMapping.toList.collect {
        case (dimCol, alias) if (queryContext.factBestCandidate.requestCols(dimCol)
          || queryContext.factBestCandidate.publicFact.foreignKeyAliases(alias)) =>
          val column = fact.columnsByNameMap(dimCol)
          (column, alias)
      }

      val groupDimCols = dimCols.groupBy(_._1.isDerivedColumn)
      groupDimCols.toList.sortBy(_._1).foreach {
        case (_, list) => list.foreach {
          case (column, alias) =>
            val name = column.name
            val nameOrAlias = column.alias.getOrElse(name)
            if(!factOnlySubqueryFields.contains(alias)) {
              renderColumnWithAlias(fact, column, alias, Set.empty, queryBuilder, queryBuilderContext, queryContext)
            }
            if (column.isDerivedColumn) {
              val derivedExpressionExpanded: String = column.asInstanceOf[DerivedDimensionColumn].derivedExpression.render(name, Map.empty).asInstanceOf[String]
              queryBuilder.addGroupBy( s"""$derivedExpressionExpanded""")
            } else {
              if(!factOnlySubqueryFields.contains(alias)) {
                if(column.dataType.hasStaticMapping) {
                  queryBuilder.addGroupBy(renderStaticMappedDimension(column))
                } else {
                  queryBuilder.addGroupBy(nameOrAlias)
                }
              }
            }
        }
      }

      val factCols = queryContext.factBestCandidate.factColMapping.toList.collect {
        case (nonFkCol, alias) if queryContext.factBestCandidate.requestCols(nonFkCol) =>
          (fact.columnsByNameMap(nonFkCol), alias)
      }

      val groupedFactCols = factCols.groupBy(_._1.isDerivedColumn)

      // Findout if there is any customRollup fact/primitive column
      val customRollupColsOption = for {
        groupedFactCols <- groupedFactCols.get(false)
      } yield {
          val customRollupSet = new mutable.LinkedHashSet[(Column, String)]
          groupedFactCols.foreach {
            case (f:FactCol, colAlias: String) if f.rollupExpression.isInstanceOf[PostgresCustomRollup] =>
              customRollupSet.add((f,colAlias))
              //if custom rollup depends on itself as a primitive col, then don't add to context
              if(!f.rollupExpression.asInstanceOf[PostgresCustomRollup].expression.sourceColumns(f.name)) {
                queryBuilderContext.setFactColAlias(f.alias.getOrElse(f.name), colAlias, f)
              }
            case _=> // ignore for all the other cases as they are handled
          }
          customRollupSet
      }

      val (customRollupAliasSet, customRollupColSet) = if (customRollupColsOption.isDefined) {
        (customRollupColsOption.get.map(_._2).toIndexedSeq, customRollupColsOption.get.map(_._1).toIndexedSeq)
      } else (IndexedSeq.empty[String], IndexedSeq.empty[Column])

      // Find out all primitive cols recursively in non derived CustomRollup cols
      if(customRollupColSet.nonEmpty) {
        dfsGetPrimitiveCols(customRollupColSet, primitiveColsSet)
      }

      // Find out all the NoopRollup cols recursively
      dfsNoopRollupCols(factCols.toSet, List.empty, noopRollupColSet)

      // Find out all primitive cols recursively in derived cols
      for {
        groupedFactDerCols <- groupedFactCols.get(true)
      } {
        val derivedColsSet = groupedFactDerCols.map(_._1).toIndexedSeq

        dfsGetPrimitiveCols(derivedColsSet, primitiveColsSet)
        // Set all Derived Fact Cols in context
        groupedFactDerCols.foreach {
          case (column, alias) =>
            queryBuilderContext.setFactColAlias(alias, alias, column)
        }
      }

      for {
        groupedFactCols <- groupedFactCols.get(false)
      } {
        groupedFactCols.foreach {
          case(col, alias) =>
            if (!customRollupAliasSet.contains(alias)) {
              primitiveColsSet.add((col.alias.getOrElse(col.name), col))
            }
        }
      }

      //render non derived columns/primitive cols first
      primitiveColsSet.foreach {
        case (alias, column) =>
          val nameOrAlias = column.alias.getOrElse(column.name)
          column match {
            case col: DimCol =>
              if (column.isDerivedColumn) {
                val derivedExpressionExpanded: String = column.asInstanceOf[DerivedDimensionColumn].derivedExpression.render(column.name, Map.empty).asInstanceOf[String]
                queryBuilder.addGroupBy( s"""$derivedExpressionExpanded""")
              } else {
                if(column.dataType.hasStaticMapping) {
                  queryBuilder.addGroupBy(renderStaticMappedDimension(column))
                } else {
                  queryBuilder.addGroupBy(nameOrAlias)
                }
              }
            case _ => // no action on the all other types of cols
          }
          renderColumnWithAlias(fact, column, alias, Set.empty, queryBuilder, queryBuilderContext, queryContext)
      }

      /*
      method to crawl the NoopRollup fact cols recursively and fill up the parent column
       whose dependent source columns is/are NoopRollup column.
       All such parent noop rollup columns has to be rendered at OuterGroupBy layer
       */
      def dfsNoopRollupCols(cols: Set[(Column, String)], parentList: List[(Column, String)], noopRollupColSet: mutable.LinkedHashSet[(String, Column)]): Unit = {
        cols.foreach {
          case (col, alias)=>
            col match {
              case factCol@FactCol(_, dt, cc, rollup, _, annotations, _) =>
                rollup match {
                  case PostgresCustomRollup(e) =>
                   parseCustomRollup(e, col, alias)
                  case NoopRollup =>
                    pickupLeaf(col, alias)
                  case _=> //ignore all other rollup cases
                }
              case derCol@PostgresDerFactCol(_, _, dt, cc, de, annotations, rollup, _) =>
                rollup match {
                  case PostgresCustomRollup(e) =>
                    parseCustomRollup(e, col, alias)
                  case NoopRollup =>
                    pickupLeaf(col, alias)
                  case _=> //ignore all other rollup cases
                }
                if (rollup != NoopRollup) {
                  de.sourceColumns.toList.sorted.foreach {
                    sourceColName =>
                      val colOption = fact.columnsByNameMap.get(sourceColName)
                      require(colOption.isDefined, s"Failed to find the sourceColumn $sourceColName in fact ${fact.name}")
                      val sourceCol = colOption.get
                      val sourceColAlias = sourceCol.alias.getOrElse(sourceCol.name)
                      if (col.alias.getOrElse(col.name) != sourceColAlias) {
                        // avoid adding self dependent columns
                        dfsNoopRollupCols(Set((sourceCol, sourceColAlias)), parentList++List((col, alias)), noopRollupColSet)
                      }
                  }
                }
              case _=>
              //ignore all dim cols cases
            }
        }
        def parseCustomRollup(expression: PostgresDerivedExpression, col : Column, alias : String): Unit = {
          expression.sourceColumns.toList.sorted.foreach {
            case sourceColName =>
              val colOption = fact.columnsByNameMap.get(sourceColName)
              require(colOption.isDefined, s"Failed to find the sourceColumn $sourceColName in fact ${fact.name}")
              val sourceCol = colOption.get
              val sourceColAlias = sourceCol.alias.getOrElse(sourceCol.name)
              if (col.alias.getOrElse(col.name) != sourceColAlias) {
                // avoid adding self dependent columns
                dfsNoopRollupCols(Set((sourceCol, sourceColAlias)), parentList++List((col, alias)), noopRollupColSet)
              }
          }
        }
        /*
           Pick up the root of the NoopRollup dependent column
         */
        def pickupLeaf(col : Column, alias : String): Unit = {
          val parentCol =  parentList.reverse.headOption
          if(parentCol.isDefined) {
            noopRollupColSet.add(parentCol.get._2, parentCol.get._1)
          } else {
            noopRollupColSet.add(alias, col)
          }
        }
      }

      def dfsGetPrimitiveCols(derivedCols: IndexedSeq[Column], primitiveColsSet:mutable.LinkedHashSet[(String, Column)]): Unit = {
        derivedCols.foreach {
          case derCol:DerivedColumn =>
            derCol.derivedExpression.sourceColumns.toList.sorted.foreach {
              sourceCol =>
                val colOption = fact.columnsByNameMap.get(sourceCol)
                require(colOption.isDefined, s"Failed to find the sourceColumn $sourceCol in fact ${fact.name}")
                val col = colOption.get
                if(col.isDerivedColumn) {
                  dfsGetPrimitiveCols(IndexedSeq(col.asInstanceOf[DerivedColumn]), primitiveColsSet)
                } else {
                  val name = col.alias.getOrElse(col.name)
                  if (!primitiveColsSet.contains((name, col))) {
                    primitiveColsSet.add((name, col))
                  }
                }
            }
          case derCol : FactCol =>
            require(derCol.rollupExpression.isInstanceOf[PostgresCustomRollup], s"Unexpected Rollup expression ${derCol.rollupExpression} in finding primitive cols")
            val customRollup = derCol.rollupExpression.asInstanceOf[PostgresCustomRollup]
            customRollup.expression.sourceColumns.toList.sorted.foreach {
              sourceCol =>
                val colOption = fact.columnsByNameMap.get(sourceCol)
                require(colOption.isDefined, s"Failed to find the sourceColumn $sourceCol in fact ${fact.name}")
                val col = colOption.get
                if(col.isDerivedColumn) {
                  dfsGetPrimitiveCols(IndexedSeq(col.asInstanceOf[DerivedColumn]), primitiveColsSet)
                } else {
                  val name = col.alias.getOrElse(col.name)
                  if(!primitiveColsSet.contains((name, col))) {
                    primitiveColsSet.add((name, col))
                  }
                }
            }

          case e =>
            throw new IllegalArgumentException(s"Unexpected column case found in the dfsGetPrimitiveCols $e")
        }
      }

      if (requestModel.includeRowCount && requestModel.isFactDriven) {
        queryBuilder.addFactViewColumn(PAGINATION_ROW_COUNT)
      }
    }

    def renderParentOuterDerivedFactCols(projectedAlias:String, column:Column): String = {
      column match {
        case PostgresDerDimCol(_, dt, cc, de, _, annotations, _) =>
          val renderedAlias = s""""$projectedAlias""""
          queryBuilderContext.setFactColAlias(projectedAlias, s"""$renderedAlias""", column)
          s"""$renderedAlias"""
        case PostgresPartDimCol(_, dt, cc, _, annotations, _) =>
          val renderedAlias = s""""$projectedAlias""""
          queryBuilderContext.setFactColAlias(projectedAlias, s"""$renderedAlias""", column)
          s"""$renderedAlias"""
        case DimCol(_, dt, cc, _, annotations, _) =>
          val renderedAlias = s""""$projectedAlias""""
          queryBuilderContext.setFactColAlias(projectedAlias, s"""$renderedAlias""", column)
          s"""$renderedAlias"""
        case FactCol(_, dt, cc, rollup, _, annotations, _) =>
          val renderedAlias = s""""$projectedAlias""""
          queryBuilderContext.setFactColAlias(projectedAlias, s"""$renderedAlias""", column)
            s"""$renderedAlias"""
        case PostgresDerFactCol(_, _, dt, cc, de, annotations, rollup, _) =>
          val renderedAlias = s""""$projectedAlias""""
          val name = column.alias.getOrElse(column.name)
          queryBuilderContext.setFactColAlias(projectedAlias, s"""$renderedAlias""", column)
          s"""${de.render(name, Map.empty)} AS $renderedAlias"""
        case _=> throw new IllegalArgumentException(s"Unexpected fact derived column found in outer select $column")
      }
    }

    def ogbGenerateOuterColumns(): Unit = {
      // add requested dim and fact columns, this should include constants
      queryContext.requestModel.requestCols foreach {
        columnInfo =>
          if (!columnInfo.isInstanceOf[ConstantColumnInfo] && queryBuilderContext.containsFactAliasToColumnMap(columnInfo.alias)) {
            aliasColumnMapOfRequestCols += (columnInfo.alias -> queryBuilderContext.getFactColByAlias(columnInfo.alias))
          } else if (queryBuilderContext.isDimensionCol(columnInfo.alias)) {
            aliasColumnMapOfRequestCols += (columnInfo.alias -> queryBuilderContext.getDimensionColByAlias(columnInfo.alias))
          } else if (queryBuilderContext.containsPreOuterAlias(columnInfo.alias)) {
            aliasColumnMapOfRequestCols += (columnInfo.alias -> queryBuilderContext.getPreOuterAliasToColumnMap(columnInfo.alias).get)
          }

          val renderedCol = columnInfo match {
            case FactColumnInfo(alias) if queryBuilderContext.containsPreOuterAlias(alias) =>
              val preOuterAliasOption = queryBuilderContext.getPreOuterFinalAliasToAliasMap(alias)
              if(preOuterAliasOption.isDefined) {
                val preOuterAlias = preOuterAliasOption.get
                  s"""$preOuterAlias AS "$alias""""
              } else {
                s""""$alias"""
              }
            case FactColumnInfo(alias) if queryBuilderContext.containsFactAliasToColumnMap(alias) =>
              val column = queryBuilderContext.getFactColByAlias(alias)
              renderParentOuterDerivedFactCols(alias, column)
//              val column  = if(queryBuilderContext.containsFactAliasToColumnMap(alias)) {
//                queryBuilderContext.getFactColByAlias(alias)
//              } else {
//                // Case to handle CustomRollup Columns
//                val aliasToColNameMap: Map[String, String] = factBest.factColMapping.map {
//                  case (factColName, factAlias) =>
//                    val col = factBest.fact.columnsByNameMap(factColName)
//                    val name:String = col.alias.getOrElse(col.name)
//                    factAlias -> name
//                }
//                require(aliasToColNameMap.contains(alias), s"Can not find the alias $alias in aliasToColNameMap")
//                val colName = aliasToColNameMap(alias)
//                queryBuilderContext.getFactColByAlias(colName)
//              }
//              renderParentOuterDerivedFactCols(alias, column)
            case DimColumnInfo(alias) => s""""$alias""""
            case ConstantColumnInfo(alias, value) =>
              s"""'$value' AS "$alias""""
            case _ => throw new UnsupportedOperationException("Unsupported Column Type")
          }
          queryBuilder.addOuterColumn(renderedCol)
      }

      if (queryContext.requestModel.includeRowCount) {
        queryBuilder.addOuterColumn(s""""${PostgresQueryGenerator.ROW_COUNT_ALIAS}"""")
        aliasColumnMapOfRequestCols += (PostgresQueryGenerator.ROW_COUNT_ALIAS -> PAGINATION_ROW_COUNT_COL)
      }

      val outerWhereClause = generateOuterWhereClause(queryContext, queryBuilderContext)
      queryBuilder.setOuterWhereClause(outerWhereClause.toString)

    }

    def ogbGeneratePreOuterColumns(primitiveInnerAliasColMap: Map[String, Column], noopRollupColsMap: Map[String, Column]): Unit = {
      // add requested dim and fact columns, this should include constants
      val preOuterRenderedColAliasMap = new mutable.HashMap[Column, String]()
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
                if (col.isInstanceOf[FactCol] && col.asInstanceOf[FactCol].rollupExpression.isInstanceOf[PostgresCustomRollup]) {
                  renderPreOuterFactCol(aliasOrColName, aliasOrColName, alias, col)
                } else {
                  if(!col.isDerivedColumn) {
                    throw new IllegalArgumentException(s"Failed to find inner fact alias for $alias")
                  }
                }
              }
            } else {
              //what to do if no in query builder context?  Must be error
              if(!col.isDerivedColumn) {
                throw new IllegalArgumentException(s"Failed to find inner fact alias for $alias")
              }
            }
          } else {
            // Condition to handle dimCols mapped to FactColumnInfo in requestModel
            if(queryBuilderContext.containsFactAliasToColumnMap(alias)) {
              val (renderedCol, renderedAlias) = renderOuterColumn(columnInfo, queryBuilderContext, queryContext.factBestCandidate.duplicateAliasMapping, isFactOnlyQuery, false, queryContext)
              queryBuilder.addPreOuterColumn(concat(renderedCol, renderedAlias))
              queryBuilder.addOuterGroupByExpressions(renderedCol)
              preOuterRenderedColAliasMap.put(queryBuilderContext.getFactColByAlias(alias), renderedAlias)
            } else {
              throw new IllegalArgumentException(s"Failed to find inner fact alias for $alias")
            }
          }

        case columnInfo@DimColumnInfo(alias) =>
          val (renderedCol, renderedAlias) = renderOuterColumn(columnInfo, queryBuilderContext, queryContext.factBestCandidate.duplicateAliasMapping, isFactOnlyQuery, false, queryContext)
          queryBuilder.addPreOuterColumn(concat(renderedCol, renderedAlias))
          queryBuilder.addOuterGroupByExpressions(renderedCol)
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
              val (renderedCol, renderedAlias) = renderOuterColumn(FactColumnInfo(alias), queryBuilderContext, queryContext.factBestCandidate.duplicateAliasMapping, isFactOnlyQuery, false, queryContext)
              queryBuilder.addPreOuterColumn(concat(s"$renderedCol $renderedAlias",""))
              queryBuilder.addOuterGroupByExpressions(renderedCol)
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
              case _=> // cant be the case for primitive cols
            }
          }
        case _=>
      }

      // Render NoopRollup cols
      noopRollupColsMap.foreach {
        case (alias, col) if !preOuterRenderedColAliasMap.keySet.contains(col) =>
          val colInnerAlias = col.name
          val qualifiedColInnerAlias = if(queryContext.shouldQualifyFactsInPreOuter) {
            queryBuilderContext.getFactColNameForAlias(colInnerAlias)
          } else col.alias.getOrElse(col.name)
          renderPreOuterFactCol(qualifiedColInnerAlias, colInnerAlias, alias, col)
        case _=> // ignore as it col is already rendered
      }

      if(queryBuilder.containsFactViewColumns(PAGINATION_ROW_COUNT)) {
        queryBuilder.addPreOuterColumn(PAGINATION_ROW_COUNT)
      }

      def renderPreOuterFactCol(qualifiedColInnerAlias: String, colInnerAlias: String, finalAlias: String, innerSelectCol: Column): Unit = {
        val preOuterFactColRendered = innerSelectCol match {
          case FactCol(_, dt, cc, rollup, _, annotations, _) =>
            s"""${renderRollupExpression(qualifiedColInnerAlias, rollup, isOuterGroupBy = true)} AS $colInnerAlias"""
          case PostgresDerFactCol(_, _, dt, cc, de, annotations, rollup, _) =>
            s"""${renderRollupExpression(de.render(qualifiedColInnerAlias, Map.empty), rollup, isOuterGroupBy = true)} AS "$colInnerAlias""""
          case _=> throw new IllegalArgumentException(s"Unexpected Col $innerSelectCol found in FactColumnInfo ")
        }
        val colInnerAliasQuoted = if(innerSelectCol.isDerivedColumn) {
          s""""$colInnerAlias""""
        } else colInnerAlias

        preOuterRenderedColAliasMap.put(innerSelectCol, colInnerAlias)
        queryBuilderContext.setPreOuterAliasToColumnMap(colInnerAliasQuoted, finalAlias, innerSelectCol)
        queryBuilder.addPreOuterColumn(preOuterFactColRendered)
      }

    } //end ogbGeneratePreOuterColumns


    /*
     1. generate query builder
     */
    ogbGenerateFactViewColumns()
    ogbGenerateWhereAndHavingClause()
    ogbGenerateDimJoin()
    ogbGeneratePreOuterColumns(primitiveColsSet.toMap, noopRollupColSet.toMap)
    ogbGenerateOuterColumns()
    ogbGenerateOrderBy()
    /*
     2. query parameters
     */

    val orderByClause = {
      if (requestModel.requestSortByCols.nonEmpty) {
        queryBuilder.getOrderByClause
      } else StringUtils.EMPTY
    }

    val queryStringPaginated = {
      // fill out query string
      val queryString =
        s"""SELECT ${queryBuilder.getOuterColumns}
FROM (SELECT ${queryBuilder.getPreOuterColumns}
      FROM (SELECT $optionalHint
                   ${queryBuilder.getFactViewColumns}
            FROM ${getFactAlias(queryContext.factBestCandidate.fact.name, queryContext.dims.map(_.dim).toSet)}
            ${queryBuilder.getWhereClause}
            ${queryBuilder.getGroupByClause}
            ${queryBuilder.getHavingClause}
           ) ${queryBuilderContext.getAliasForTable(queryContext.factBestCandidate.fact.name)}
          ${queryBuilder.getJoinExpressions}
          ${queryBuilder.getOuterGroupByClause}
) ${queryBuilderContext.getSubqueryAlias} ${queryBuilder.getOuterWhereClause}
   $orderByClause"""

      if (requestModel.isSyncRequest && (requestModel.isFactDriven || requestModel.hasFactSortBy)) {
        addPaginationWrapper(queryString, queryContext.requestModel.maxRows, queryContext.requestModel.startIndex, true, queryBuilderContext)
      } else {
        queryString
      }
    }

    new PostgresQuery(
      queryContext,
      queryStringPaginated,
      paramBuilder.build(),
      aliasColumnMapOfRequestCols.toMap,
      additionalColumns(queryContext)
    )
  } // Outer Group By Query End
}
