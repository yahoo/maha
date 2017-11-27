package com.yahoo.maha.core.query.oracle

import com.yahoo.maha.core.dimension.{DerivedDimensionColumn, OracleDerDimCol, OraclePartDimCol, DimCol}
import com.yahoo.maha.core.fact.{Fact, OracleDerFactCol, FactCol}
import com.yahoo.maha.core._
import com.yahoo.maha.core.query._
import grizzled.slf4j.Logging
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable

/**
 * Created by pranavbhole on 22/11/17.
 */
abstract class OuterGroupByQueryGenerator(partitionColumnRenderer:PartitionColumnRenderer, literalMapper: OracleLiteralMapper = new OracleLiteralMapper) extends OracleQueryCommon with Logging {

  protected[this] def generateOuterWhereClause(queryContext: QueryContext, queryBuilderContext: QueryBuilderContext) : WhereClause = {
    val outerFilters = new mutable.LinkedHashSet[String]
    val requestModel =  queryContext.requestModel
    requestModel.outerFilters.foreach {
      filter =>
        val f = FilterSql.renderOuterFilter(filter, queryBuilderContext.aliasColumnMap, OracleEngine, literalMapper)
        outerFilters += f.filter
    }

    WhereClause(AndFilter(outerFilters))
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
    val optionalHint: String = getFactOptionalHint(queryContext.factBestCandidate.fact, requestModel).map(toComment).getOrElse("")
    val aliasColumnMapOfRequestCols = new mutable.HashMap[String, Column]()
    val isFactOnlyQuery = requestModel.isFactDriven && queryContext.dims.forall {
      db => (db.fields.filterNot(db.publicDim.isPrimaryKeyAlias).isEmpty && !db.hasNonFKSortBy
        && queryContext.factBestCandidate.publicFact.foreignKeyAliases(db.publicDim.primaryKeyByAlias))
    }
    val requestColAliasesSet = requestModel.requestCols.map(_.alias).toSet
    val factOnlySubqueryFields : Set[String] = if(isFactOnlyQuery) {
      queryContext.dims.view.map(_.publicDim.primaryKeyByAlias).filterNot(requestColAliasesSet).toSet
    } else Set.empty

    val factBest = queryContext.factBestCandidate

    def ogb_generateDimJoin(): Unit = {
      if (queryContext.dims.nonEmpty) {
        val dsql = generateDimensionSql(queryContext, queryBuilderContext, true)
        queryBuilder.addDimensionJoin(dsql.drivingDimensionSql)
        //TODO: add support for optimal mutli dimension sort by metric query
        //TODO: right now it just does join with driving table
        dsql.multiDimensionJoinSql.foreach(queryBuilder.addMultiDimensionJoin)
      }
    }

    def ogb_generateOrderBy(): Unit = {
      if(requestModel.isDimDriven && requestModel.hasDrivingDimNonFKNonPKSortBy) {
        // In Dim driven case, if driving dimension has orderBy then we do not want to orderBy again in the outer as it mess up the order
      } else {
        requestModel.requestSortByCols.foreach {
          ci =>
            queryBuilder.addOrderBy(renderSortByColumn(ci, queryBuilderContext))
        }
      }
    }

    def ogb_generateWhereAndHavingClause(): Unit = {
      // inner fact where clauses
      val fact = queryContext.factBestCandidate.fact
      val publicFact = queryContext.factBestCandidate.publicFact
      val filters = queryContext.factBestCandidate.filters
      val allFilters = publicFact.forcedFilters //++ filters  need to append regular filters or pass in
      val whereFilters = new mutable.LinkedHashSet[String]
      val havingFilters = new mutable.LinkedHashSet[String]
      var escaped = false
      val hasPartitioningScheme = fact.annotations.contains(OracleQueryGenerator.ANY_PARTITIONING_SCHEME)

      if (requestModel.isFactDriven || requestModel.dimensionsCandidates.isEmpty || requestModel.hasNonFKFactFilters || requestModel.hasFactSortBy || fact.forceFilters.nonEmpty) {
        val unique_filters = removeDuplicateIfForced( filters.toSeq, allFilters.toSeq, queryContext )
        unique_filters.sorted.foreach {
          filter =>
            val name = publicFact.aliasToNameColumnMap(filter.field)
            if (fact.dimColMap.contains(name)) {
              val f = FilterSql.renderFilter(
                filter,
                queryContext.factBestCandidate.publicFact.aliasToNameColumnMap,
                fact.columnsByNameMap,
                OracleEngine,
                literalMapper)
              escaped |= f.escaped
              whereFilters += f.filter
            } else if (fact.factColMap.contains(name)) {
              val column = fact.columnsByNameMap(name)
              val alias = queryContext.factBestCandidate.factColMapping(name)
              val exp = column match {
                case FactCol(_, dt, cc, rollup, _, annotations, _) =>
                  s"""${renderRollupExpression(name, rollup)}"""
                case OracleDerFactCol(_, _, dt, cc, de, annotations, rollup, _) =>
                  s"""${renderRollupExpression(de.render(name, Map.empty), rollup)}"""
                case any =>
                  throw new UnsupportedOperationException(s"Found non fact column : $any")
              }
              val f = FilterSql.renderFilter(
                filter,
                queryContext.factBestCandidate.publicFact.aliasToNameColumnMap,
                fact.columnsByNameMap,
                OracleEngine,
                literalMapper,
                Option(exp)
              )
              escaped |= f.escaped
              havingFilters += f.filter
            } else {
              throw new IllegalArgumentException(
                s"Unknown fact column: publicFact=${publicFact.name}, fact=${fact.name} alias=${filter.field}, name=$name")
            }
        }
      }
      val dayFilter = FilterSql.renderFilter(
        requestModel.localTimeDayFilter,
        queryContext.factBestCandidate.publicFact.aliasToNameColumnMap,
        fact.columnsByNameMap,
        OracleEngine,
        literalMapper).filter

      val combinedQueriedFilters = {
        if (hasPartitioningScheme) {
          val partitionFilters = new mutable.LinkedHashSet[String]
          val partitionFilterOption = partitionColumnRenderer.renderFact(queryContext, literalMapper, OracleEngine)
          if(partitionFilterOption.isDefined) {
            partitionFilters += partitionFilterOption.get
            AndFilter(partitionFilters ++ whereFilters)
          } else {
            AndFilter(whereFilters + dayFilter)
          }
        } else {
          AndFilter(whereFilters + dayFilter)
        }
      }

      val whereClauseExpression = s"""WHERE ${combinedQueriedFilters.toString} """
      queryBuilder.setWhereClause(whereClauseExpression)

      if (havingFilters.nonEmpty) {
        val havingAndFilters = AndFilter(havingFilters.toSet)
        val havingClauseExpression = s"""HAVING ${havingAndFilters.toString}"""
        queryBuilder.setHavingClause(havingClauseExpression)
      }

    }

    /*
     Primitive columns are fact cols rendered in inner select which are dependent on the derived columns
     Derived columns with dependencies are not rendered in ogb_generateFactViewColumns
     Primitive columns are also rendered in preOuterSelect rendering
      */
    val primitiveColsSet = new mutable.HashSet[(Column, String)]()

    def ogb_generateFactViewColumns(): Unit = {
      val factTableAlias = queryBuilderContext.getAliasForTable(queryContext.factBestCandidate.fact.name)
      val fact = queryContext.factBestCandidate.fact

      def renderColumnName(column: Column): String = {
        //column.alias.fold(column.name)(alias => s"""$alias AS ${column.name}""")
        column.alias.getOrElse(column.name)
      }

      def renderStaticMappedDimension(column: Column) : String = {
        val nameOrAlias = renderColumnName(column)
        column.dataType match {
          case IntType(_, sm, _, _, _) if sm.isDefined =>
            val defaultValue = sm.get.default
            val whenClauses = sm.get.tToStringMap.map {
              case (from, to) => s"WHEN (${nameOrAlias} IN ($from)) THEN '$to'"
            }
            s"CASE ${whenClauses.mkString(" ")} ELSE '$defaultValue' END"
          case StrType(_, sm, _) if sm.isDefined =>
            val defaultValue = sm.get.default
            val decodeValues = sm.get.tToStringMap.map {
              case (from, to) => s"'$from', '$to'"
            }
            s"""DECODE(${nameOrAlias}, ${decodeValues.mkString(", ")}, '$defaultValue')"""
          case _ =>
            nameOrAlias
        }
      }

      def renderColumnWithAlias(fact: Fact, column: Column, alias: String, requiredInnerCols: Set[String]): Unit = {
        val name = column.alias.getOrElse(column.name)
        val exp = column match {
          case any if queryBuilderContext.containsColByName(name) =>
            //do nothing, we've already processed it
            ""
          case DimCol(_, dt, cc, _, annotations, _) if dt.hasStaticMapping =>
            queryBuilderContext.setFactColAlias(alias, s"""$factTableAlias.$name""", column)
            s"${renderStaticMappedDimension(column)} $name"
          case DimCol(_, dt, cc, _, annotations, _) =>
            queryBuilderContext.setFactColAlias(alias, s"""$factTableAlias.$name""", column)
            renderColumnName(column)
          case OraclePartDimCol(_, dt, cc, _, annotations, _) =>
            queryBuilderContext.setFactColAlias(alias, s"""$factTableAlias.$name""", column)
            renderColumnName(column)
          case OracleDerDimCol(_, dt, cc, de, _, annotations, _) =>
            val renderedAlias = s""""$alias""""
            queryBuilderContext.setFactColAlias(alias, s"""$factTableAlias.$renderedAlias""", column)
            s"""${de.render(name, Map.empty)} AS $renderedAlias"""
          case FactCol(_, dt, cc, rollup, _, annotations, _) =>
            val nameOrAlias = column.alias.getOrElse(name)
            column.dataType match {
              case DecType(_, _, Some(default), Some(min), Some(max), _) =>
                val renderedAlias = s""""$name""""
                val minMaxClause = s"CASE WHEN (($nameOrAlias >= $min) AND ($nameOrAlias <= $max)) THEN $nameOrAlias ELSE $default END"
                queryBuilderContext.setFactColAlias(alias, s"""$factTableAlias.$renderedAlias""", column)
                s"${renderRollupExpression(nameOrAlias, rollup, Option(minMaxClause))} AS $renderedAlias"
              case IntType(_, _, Some(default), Some(min), Some(max)) =>
                val renderedAlias = s""""$name""""
                val minMaxClause = s"CASE WHEN (($nameOrAlias >= $min) AND ($nameOrAlias <= $max)) THEN $nameOrAlias ELSE $default END"
                queryBuilderContext.setFactColAlias(alias, s"""$factTableAlias.$renderedAlias""", column)
                s"${renderRollupExpression(nameOrAlias, rollup, Option(minMaxClause))} AS $renderedAlias"
              case _ =>
                val renderedAlias = s""""$name""""
                queryBuilderContext.setFactColAlias(alias, s"""$factTableAlias.$renderedAlias""", column)
                s"""${renderRollupExpression(nameOrAlias, rollup)} AS $renderedAlias"""
            }
          case OracleDerFactCol(_, _, dt, cc, de, annotations, rollup, _)
            if queryContext.factBestCandidate.filterCols.contains(name) || de.expression.hasRollupExpression || requiredInnerCols(name) =>
            val renderedAlias = s""""$alias""""
            queryBuilderContext.setFactColAlias(alias, s"""$factTableAlias.$renderedAlias""", column)
            s"""${renderRollupExpression(de.render(name, Map.empty), rollup)} AS $renderedAlias"""
          case OracleDerFactCol(_, _, dt, cc, de, annotations, _, _) =>
            //means no fact operation on this column, push expression outside
            de.sourceColumns.foreach {
              case src if src != name =>
                val sourceCol = fact.columnsByNameMap(src)
                renderColumnWithAlias(fact, sourceCol, sourceCol.name, requiredInnerCols)
              case _ => //do nothing if we reference ourselves
            }
            queryBuilderContext.setFactColAlias(alias, s"""(${de.render(name
              , queryBuilderContext.getColAliasToFactColNameMap
              , columnPrefix = Option(s"$factTableAlias.")
              , expandDerivedExpression = false)})""", column)
            ""
          case any =>
            throw new UnsupportedOperationException(s"Found non unhandled column : $any")
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
            if(!factOnlySubqueryFields(alias)) {
              renderColumnWithAlias(fact, column, alias, Set.empty)
            }
            if (column.isDerivedColumn) {
              val derivedExpressionExpanded: String = column.asInstanceOf[DerivedDimensionColumn].derivedExpression.render(name, Map.empty).asInstanceOf[String]
              queryBuilder.addGroupBy( s"""$derivedExpressionExpanded""")
            } else {
              if(!factOnlySubqueryFields(alias)) {
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

      // Find out all primitive cols recursively
      for {
        groupedFactDerCols <- groupedFactCols.get(true)
      } yield {
        getPrimitiveCols(groupedFactDerCols.map(_._1.asInstanceOf[DerivedColumn]).toSet, primitiveColsSet)
        // Set all Derived Fact Cols in context
        groupedFactDerCols.foreach {
          case (column, alias) =>
            queryBuilderContext.setFactColAlias(alias, alias, column)
        }
      }

      for {
        groupedFactCols <- groupedFactCols.get(false)
      } yield {
        primitiveColsSet++=groupedFactCols.toSet
      }

      //render non derived columns/primitive cols first
      primitiveColsSet.foreach {
        case (column, alias) =>
          renderColumnWithAlias(fact, column, alias, Set.empty)
      }

      /*
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
      */

      def getPrimitiveCols(derivedCols: Set[DerivedColumn], primitiveColsSet:mutable.HashSet[(Column, String)]): Unit = {
        derivedCols.foreach {
          derCol=>
            derCol.derivedExpression.sourceColumns.foreach {
              sourceCol =>
                val colOption = fact.columnsByNameMap.get(sourceCol)
                require(colOption.isDefined, s"Failed to find the sourceColumn $sourceCol in fact ${fact.name}")
                val col = colOption.get
                if(col.isDerivedColumn) {
                  getPrimitiveCols(Set(col.asInstanceOf[DerivedColumn]), primitiveColsSet)
                } else {
                  primitiveColsSet.add((col, col.alias.getOrElse(col.name)))
                }
            }
        }
      }

      if (requestModel.includeRowCount && requestModel.isFactDriven) {
        queryBuilder.addFactViewColumn(PAGINATION_ROW_COUNT)
      }
    }

    def renderParentOuterDerivedFactCols(projectedAlias:String, column:Column): String = {
      column match {
        case OracleDerFactCol(_, _, dt, cc, de, annotations, rollup, _) =>
          val renderedAlias = s""""$projectedAlias""""
          val name = column.alias.getOrElse(column.name)
          queryBuilderContext.setFactColAlias(projectedAlias, s"""$renderedAlias""", column)
          s"""${de.render(name, Map.empty)} AS $renderedAlias"""
        case _=> throw new IllegalArgumentException("Unexpected fact derived column found in outer select")
      }
    }

    def ogb_generateOuterColumns(): Unit = {
      // add requested dim and fact columns, this should include constants
      queryContext.requestModel.requestCols foreach {
        columnInfo =>
          if (!columnInfo.isInstanceOf[ConstantColumnInfo] && queryBuilderContext.aliasColumnMap.contains(columnInfo.alias)) {
            aliasColumnMapOfRequestCols += (columnInfo.alias -> queryBuilderContext.aliasColumnMap(columnInfo.alias))
          } else if (queryContext.factBestCandidate.duplicateAliasMapping.contains(columnInfo.alias)) {
            val sourceAliases = queryContext.factBestCandidate.duplicateAliasMapping(columnInfo.alias)
            val sourceAlias = sourceAliases.find(queryBuilderContext.aliasColumnMap.contains)
            require(sourceAlias.isDefined
              , s"Failed to find source column for duplicate alias mapping : ${queryContext.factBestCandidate.duplicateAliasMapping(columnInfo.alias)}")
            aliasColumnMapOfRequestCols += (columnInfo.alias -> queryBuilderContext.aliasColumnMap(sourceAlias.get))
          }
          val renderedCol = columnInfo match {
            case FactColumnInfo(alias) if queryBuilderContext.containsPreOuterAlias(alias) =>
              val preOuterAliasOption = queryBuilderContext.getPreOuterFinalAliasToAliasMap(alias)
              if(preOuterAliasOption.isDefined) {
                val preOuterAlias = s""""${preOuterAliasOption.get}""""
                s"""$preOuterAlias AS "$alias""""
              } else {
                s""""$alias"""
              }
            case FactColumnInfo(alias) =>
              val column = queryBuilderContext.getFactColByAlias(alias)
              renderParentOuterDerivedFactCols(alias, column)
            case DimColumnInfo(alias) => s""""$alias""""
            case ConstantColumnInfo(alias, value) =>
              s"""'$value' AS "$alias""""
            case _ => throw new UnsupportedOperationException("Unsupported Column Type")
          }
          queryBuilder.addOuterColumn(renderedCol)
      }

      if (queryContext.requestModel.includeRowCount) {
        queryBuilder.addOuterColumn(OracleQueryGenerator.ROW_COUNT_ALIAS)
        aliasColumnMapOfRequestCols += (OracleQueryGenerator.ROW_COUNT_ALIAS -> PAGINATION_ROW_COUNT_COL)
      }

      val outerWhereClause = generateOuterWhereClause(queryContext, queryBuilderContext)
      queryBuilder.setOuterWhereClause(outerWhereClause.toString)

    }

    def ogb_generatePreOuterColumns(primitiveInnerAliasColMap: Map[String, Column]): Unit = {
      // add requested dim and fact columns, this should include constants
      val preOuterRenderedColAlias = new mutable.HashSet[String]()
      queryContext.requestModel.requestCols foreach {
        columnInfo =>

          columnInfo match {
            case FactColumnInfo(alias) =>
              val colAliasOption = factBest.factColMapping.map(_ swap).get(alias)
              // Check if alias is rendered in inner selection or not
              if(colAliasOption.isDefined && queryBuilderContext.containsFactAliasToColumnMap(colAliasOption.get)) {
                val colInnerAlias = colAliasOption.get
                if(primitiveInnerAliasColMap.contains(colInnerAlias)) {
                  val innerSelectCol = queryBuilderContext.getFactColByAlias(colInnerAlias)
                  renderPreOuterFactCol(colInnerAlias, alias, innerSelectCol)
                }
              }

            case DimColumnInfo(alias) =>
              queryBuilder.addPreOuterColumn(renderOuterColumn(columnInfo, queryBuilderContext, queryContext.factBestCandidate.duplicateAliasMapping, isFactOnlyQuery, false, queryContext))
              queryBuilder.addOuterGroupByExpressions(s""""$alias"""")
          }
      }
      // Render primitive cols
      primitiveInnerAliasColMap.foreach {
        case (alias, col) if !preOuterRenderedColAlias.contains(alias)=>
          renderPreOuterFactCol(col.alias.getOrElse(col.name), alias, col)
        case _=> // ignore as it col is already rendered
      }

      def renderPreOuterFactCol(colInnerAlias: String, finalAlias: String, innerSelectCol: Column): Unit = {
        val preOuterFactColRendered = innerSelectCol match {
          case FactCol(_, dt, cc, rollup, _, annotations, _) =>
            s"""${renderRollupExpression(colInnerAlias, rollup)} AS $colInnerAlias"""
          case OracleDerFactCol(_, _, dt, cc, de, annotations, rollup, _) =>
            s"""${renderRollupExpression(de.render(colInnerAlias, Map.empty), rollup)} AS $colInnerAlias"""
          case _=> throw new IllegalArgumentException(s"Unexpected Col $innerSelectCol found in FactColumnInfo ")
        }
        preOuterRenderedColAlias += colInnerAlias
        queryBuilderContext.setPreOuterAliasToColumnMap(colInnerAlias, finalAlias, innerSelectCol)
        queryBuilder.addPreOuterColumn(preOuterFactColRendered)
      }

    } //end ogb_generatePreOuterColumns


    /*
     1. generate query builder
     */
    ogb_generateFactViewColumns()
    ogb_generateWhereAndHavingClause()
    //only generate dim if we are not fact only
    if(!isFactOnlyQuery) {
      ogb_generateDimJoin()
    }

    ogb_generatePreOuterColumns(primitiveColsSet.map(e=> e._2 -> e._1).toMap)
    ogb_generateOuterColumns()
    ogb_generateOrderBy()
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
) ${queryBuilder.getOuterWhereClause}
   $orderByClause"""

      if (requestModel.isSyncRequest && (requestModel.isFactDriven || requestModel.hasFactSortBy)) {
        addPaginationWrapper(queryString, queryContext.requestModel.maxRows, queryContext.requestModel.startIndex, true)
      } else {
        queryString
      }
    }

    new OracleQuery(
      queryContext,
      queryStringPaginated,
      paramBuilder.build(),
      aliasColumnMapOfRequestCols.toMap,
      additionalColumns(queryContext)
    )
  } // Outer Group By Query End
}
