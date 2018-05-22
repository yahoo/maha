// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.hive

import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.query._
import grizzled.slf4j.Logging

import scala.collection.mutable.LinkedHashSet
import scala.collection.{SortedSet, mutable}

/**
 * Created by shengyao on 12/16/15.
 */
class HiveQueryGenerator(partitionColumnRenderer:PartitionColumnRenderer, udfStatements: Set[UDFRegistration]) extends BaseQueryGenerator[WithHiveEngine] {

  override val engine: Engine = HiveEngine
  override def generate(queryContext: QueryContext): Query = {
    queryContext match {
      case context : CombinedQueryContext =>
        generateQuery(context)
      case FactQueryContext(factBestCandidate, model, indexAliasOption, attributes) =>
        generateQuery(CombinedQueryContext(SortedSet.empty, factBestCandidate, model, attributes))
      case DimFactOuterGroupByQueryQueryContext(dims, factBestCandidate, model, attributes) =>
        generateQuery(CombinedQueryContext(dims, factBestCandidate, model, attributes), true)
      case any => throw new UnsupportedOperationException(s"query context not supported : $any")
    }
  }
  override def validateEngineConstraints(requestModel: RequestModel): Boolean = {
    requestModel.orFilterMeta.isEmpty
  }
  val hiveLiteralMapper = new HiveLiteralMapper()
  val outerGroupByAlias = "outergroupby"

  private[this] def generateQuery(queryContext: CombinedQueryContext, isOuterGroupBy: Boolean = false) : Query = {

    println(s"isOuterGroupBy: $isOuterGroupBy")
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
    val partitionCols = new mutable.HashSet[Column]()
    val requestedCols = queryContext.requestModel.requestCols
    val columnAliasToColMap = new mutable.HashMap[String, Column]()
    //val constantColumnsMap = new mutable.HashMap[String, String]()


    def renderRollupExpression(expression: String, rollupExpression: RollupExpression, renderedColExp: Option[String] = None) : String = {
      rollupExpression match {
        case SumRollup => s"SUM($expression)"
        case MaxRollup => s"MAX($expression)"
        case MinRollup => s"MIN($expression)"
        case AverageRollup => s"AVG($expression)"
        case HiveCustomRollup(exp) => {
          if (isOuterGroupBy) {
            exp.sourceColumns.foreach(col => {
              renderColumnWithAlias(fact, fact.columnsByNameMap.get(col).get, col, Set.empty, false)
            })
          }
          s"(${exp.render(expression, Map.empty, renderedColExp)})"
        }
        case NoopRollup => s"($expression)"
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
          val finalAlias = getConstantColAlias(alias)
          s"""NVL($finalAlias, '')"""
        case DimColumnInfo(alias) =>
          val finalAlias = queryBuilderContext.getDimensionColNameForAlias(alias)
          s"""NVL($finalAlias, '')"""
        case FactColumnInfo(alias)=>
          val finalAlias = queryBuilderContext.getFactColNameForAlias(alias)
          s"""NVL($finalAlias, '')"""
      }
      "CONCAT_WS(\",\"," + (renderedConcateColumns).mkString(", ") + ")"
    }

    // render outercols with column expression
    def generateOuterColumns() : String = {
      queryContext.requestModel.requestCols foreach {
        columnInfo =>
          QueryGeneratorHelper.populateAliasColMapOfRequestCols(columnInfo, queryBuilderContext, queryContext)
          queryBuilder.addOuterColumn(renderOuterColumn(columnInfo, queryBuilderContext, queryContext.factBestCandidate.duplicateAliasMapping, factCandidate))
      }
      queryBuilder.getOuterColumns

    }

    def renderOuterColumn(columnInfo: ColumnInfo, queryBuilderContext: QueryBuilderContext, duplicateAliasMapping: Map[String, Set[String]], factCandidate: FactBestCandidate): String = {

      def renderFactCol(alias: String, finalAliasOrExpression: String, col: Column, finalAlias: String): String = {
        val finalExp = {
          if (col.isDerivedColumn && isOuterGroupBy) {
            col.asInstanceOf[DerivedFactColumn].derivedExpression.render(finalAlias, queryBuilderContext.getColAliasToFactColNameMap).toString
          } else {
            finalAliasOrExpression
          }
        }
        val postFilterAlias = renderNormalOuterColumn(col, finalExp)
        val finalOuterAlias = renderColumnAlias(alias)
        queryBuilderContext.setFactColAlias(alias, finalOuterAlias, col)
        s"""$postFilterAlias $finalOuterAlias"""
      }

      columnInfo match {
        case FactColumnInfo(alias) =>
          if (isOuterGroupBy) {
            val column = fact.columnsByNameMap.get(publicFact.aliasToNameColumnMap.get(alias).get).get
            if (!queryBuilderContext.containsFactColNameForAlias(alias)) {
              renderColumnWithAlias(factCandidate.fact, column, alias, Set.empty, true)
            }
          }
          QueryGeneratorHelper.handleFactColInfo(queryBuilderContext, alias, factCandidate, renderFactCol, duplicateAliasMapping, factCandidate.fact.name, isOuterGroupBy)
        case DimColumnInfo(alias) =>
          val col = queryBuilderContext.getDimensionColByAlias(alias)
          val finalAlias = queryBuilderContext.getDimensionColNameForAlias(alias)
          val referredAlias = {
            if (isOuterGroupBy) {
              s"$outerGroupByAlias.$finalAlias"
            } else {
              val publicDim = queryBuilderContext.getDimensionForColAlias(alias)
              s"${queryBuilderContext.getAliasForTable(publicDim.name)}.$finalAlias"
            }
          }
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
          s"""CAST(ROUND(COALESCE($minMaxClause, ${default}), 10) as STRING)"""
        case DecType(_, _, Some(default), _, _, _) =>
          s"""CAST(ROUND(COALESCE($finalAlias, ${default}), 10) as STRING)"""
        case DecType(_, _, _, _, _, _) =>
          s"""CAST(ROUND(COALESCE($finalAlias, 0L), 10) as STRING)"""
        case IntType(_,sm,_,_,_) =>
            s"""CAST(COALESCE($finalAlias, 0L) as STRING)"""
        case DateType(_) => s"""getFormattedDate($finalAlias)"""
        case StrType(_, sm, df) =>
          val defaultValue = df.getOrElse("NA")
          s"""COALESCE($finalAlias, "$defaultValue")"""
        case _ => s"""COALESCE($finalAlias, "NA")"""
      }
      if (column.annotations.contains(EscapingRequired)) {
        s"""getCsvEscapedString(CAST(NVL($finalAlias, '') AS STRING))"""
      } else {
        renderedCol
      }
    }


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
          val decodeValues = sm.get.tToStringMap.map {
            case (from, to) => s"'$from', '$to'"
          }
          s"""decodeUDF($nameOrAlias, ${decodeValues.mkString(", ")}, '$defaultValue')"""
        case _ =>
          s"""COALESCE($nameOrAlias, "NA")"""
      }
    }

    def renderColumnWithAlias(fact: Fact, column: Column, alias: String, requiredInnerCols: Set[String], renderOuterColumn: Boolean): Unit = {
      val name = column.alias.getOrElse(column.name)
      val exp = column match {
        case any if queryBuilderContext.containsColByName(name) =>
          //do nothing, we've already processed it
          ""
        case DimCol(_, dt, _, _, _, _) if dt.hasStaticMapping =>
          val renderedAlias = renderColumnAlias(alias)
          queryBuilderContext.setFactColAliasAndExpression(alias, renderedAlias, column, Option(name))
          s"${renderStaticMappedDimension(column)} $name"
        case DimCol(_, dt, _, _, _, _) =>
          val renderedAlias = renderColumnAlias(alias)
          queryBuilderContext.setFactColAliasAndExpression(alias, renderedAlias, column, Option(name))
          name
        case HiveDerDimCol(_, dt, _, de, _, _, _) =>
          val renderedAlias = renderColumnAlias(alias)
          queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
          s"""${de.render(name, Map.empty)} $renderedAlias"""
        case FactCol(_, dt, _, rollup, _, _, _) =>
          dt match {
            case DecType(_, _, Some(default), Some(min), Some(max), _) =>
              val renderedAlias = if (isOuterGroupBy && !renderOuterColumn) renderColumnAlias(name) else renderColumnAlias(alias)
              val minMaxClause = s"CASE WHEN (($name >= $min) AND ($name <= $max)) THEN $name ELSE $default END"
              queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
              s"""${renderRollupExpression(name, rollup, Option(minMaxClause))} $renderedAlias"""
            case IntType(_, _, Some(default), Some(min), Some(max)) =>
              val renderedAlias = if (isOuterGroupBy && !renderOuterColumn) renderColumnAlias(name) else renderColumnAlias(alias)
              val minMaxClause = s"CASE WHEN (($name >= $min) AND ($name <= $max)) THEN $name ELSE $default END"
              queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
              s"""${renderRollupExpression(name, rollup, Option(minMaxClause))} $renderedAlias"""
            case _ =>
              val renderedAlias = if (isOuterGroupBy && !renderOuterColumn) renderColumnAlias(name) else renderColumnAlias(alias)
              queryBuilderContext.setFactColAliasAndExpression(alias, renderedAlias, column, Option(name))
              s"""${renderRollupExpression(name, rollup)} $name"""
          }
        case HiveDerFactCol(_, _, dt, cc, de, annotations, rollup, _)
          if queryContext.factBestCandidate.filterCols.contains(name) || de.expression.hasRollupExpression || requiredInnerCols(name)
            || de.isDimensionDriven =>
          val renderedAlias = if (isOuterGroupBy && !renderOuterColumn) renderColumnAlias(name) else renderColumnAlias(alias)
          println("renderedAlias1: " + renderedAlias)
          queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
          val exp1 = s"""${renderRollupExpression(de.render(name, Map.empty), rollup)} $renderedAlias"""
          println("exp1: " + exp1)
          exp1

        case HiveDerFactCol(_, _, dt, cc, de, annotations, _, _) =>
          //means no fact operation on this column, push expression outside
          de.sourceColumns.foreach {
            case src if src != name =>
              val sourceCol = fact.columnsByNameMap(src)
              //val renderedAlias = renderColumnAlias(sourceCol.name)
              val renderedAlias = sourceCol.alias.getOrElse(sourceCol.name)
              println("renderedAlias2: " + renderedAlias)

              renderColumnWithAlias(fact, sourceCol, renderedAlias, requiredInnerCols, renderOuterColumn)
            case _ => //do nothing if we reference ourselves
          }
          //val renderedAlias = renderColumnAlias(alias)
          val renderedAlias = if (isOuterGroupBy && !renderOuterColumn) renderColumnAlias(name) else renderColumnAlias(alias)
          println("renderedAlias3: " + renderedAlias)
          queryBuilderContext.setFactColAliasAndExpression(alias, renderedAlias, column, Option(s"""(${de.render(renderedAlias, queryBuilderContext.getColAliasToFactColNameMap, expandDerivedExpression = isOuterGroupBy)})"""))
          ""
      }

      queryBuilder.addFactViewColumn(exp)
      queryBuilderContext.setColAliasToRenderedFactColExp(alias, exp)

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
            renderColumnWithAlias(fact, column, alias, Set.empty, false)
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
            renderColumnWithAlias(fact, column, alias, Set.empty, false)
        }
      }

      //render derived columns last
      groupedFactCols.get(true).foreach { derivedCols =>
        val requiredInnerCols: Set[String] =
          derivedCols.view.map(_._1.asInstanceOf[DerivedColumn]).flatMap(dc => dc.derivedExpression.sourceColumns).toSet
        derivedCols.foreach {
          case (column, alias) =>
            if (isOuterGroupBy) {
              requiredInnerCols.foreach(innerColName => {
                val innerCol = queryContext.factBestCandidate.fact.columnsByNameMap.get(innerColName).get
                if (!innerCol.isDerivedColumn && !innerCol.isInstanceOf[DimCol]) {
                  val rollup = column.asInstanceOf[FactColumn].rollupExpression
                  val renderedInnerCol = s"${renderRollupExpression(innerColName, rollup)} $innerColName"
                  queryBuilder.addFactViewColumn(renderedInnerCol)
                }
              })
            } else {
              renderColumnWithAlias(fact, column, alias, requiredInnerCols, false)
            }
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
          if (fact.dimColMap.contains(name)) {
            val sqlResult = FilterSql.renderFilter(
              filter,
              aliasToNameMapFull,
              fact.columnsByNameMap,
              HiveEngine,
              hiveLiteralMapper
            )
            whereFilters += sqlResult.filter
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
              HiveEngine,
              hiveLiteralMapper,
              Option(exp)
            )
            havingFilters += f.filter
          } else {
            throw new IllegalArgumentException(
              s"Unknown fact column: publicFact=${publicFact.name}, fact=${fact.name} alias=${filter.field}, name=$name")
          }
      }

      val dayFilter = FilterSql.renderFilter(
        queryContext.requestModel.localTimeDayFilter,
        queryContext.factBestCandidate.publicFact.aliasToNameColumnMap,
        fact.columnsByNameMap,
        HiveEngine,
        hiveLiteralMapper).filter

      val combinedQueriedFilters = {
        if (hasPartitioningScheme) {
          val partitionFilterOption = partitionColumnRenderer.renderFact(queryContext, hiveLiteralMapper, HiveEngine)
          if(partitionFilterOption.isDefined) {
            whereFilters += partitionFilterOption.get
            AndFilter(whereFilters).toString
          } else {
            whereFilters += dayFilter
            AndFilter(whereFilters).toString
          }
        } else {
          whereFilters += dayFilter
          AndFilter(whereFilters).toString
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
        val havingAndFilters = AndFilter(havingFilters.toSet)
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
            columnsByNameMap,
            HiveEngine,
            hiveLiteralMapper
          ).filter
      }

      val partitionFilters = partitionColumnRenderer.renderDim(requestModel, dimBundle, hiveLiteralMapper, HiveEngine)
      val renderedFactFk = renderColumn(fkCol, "")

      val dimWhere = s"""WHERE ${AndFilter(wheres + partitionFilters).toString}"""

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


    /**
     * Final Query
     */

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
            if (column.isInstanceOf[HivePartDimCol]) {
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


    val columnsForGroupBySelect: LinkedHashSet[String] = new mutable.LinkedHashSet[String]
    val columnsForGroupBy: LinkedHashSet[String] = new mutable.LinkedHashSet[String]
    def generateOuterGroupByColumns = {
      def addDimGroupByColumns(alias: String, isFromFact: Boolean) = {
        val referredAlias = {
          if (isFromFact) {
            s"${queryBuilderContext.getAliasForTable(fact.name)}.${queryBuilderContext.getFactColByAlias(alias).name}"
          } else {
            val finalAlias = queryBuilderContext.getDimensionColNameForAlias(alias)
            val tableName = queryBuilderContext.getDimensionForColAlias(alias).name
            s"${queryBuilderContext.getAliasForTable(tableName)}.$finalAlias"
          }
        }
        columnsForGroupBySelect += referredAlias
        columnsForGroupBy += referredAlias
      }

      requestedCols.foreach {
        case FactColumnInfo(alias) =>
          if (queryBuilderContext.containsFactAliasToColumnMap(alias) && queryBuilderContext.getFactColByAlias(alias).isInstanceOf[DimCol]) {
            addDimGroupByColumns(alias, true)
          } else {
            if (!queryBuilderContext.containsFactAliasToColumnMap(alias)) {
              println(s"$alias is a derived column")
              val colName = queryContext.factBestCandidate.publicFact.aliasToNameColumnMap.get(alias).get
              val col = queryContext.factBestCandidate.fact.factColMap.get(colName).get
              val sourceColumnNames = col.asInstanceOf[DerivedFactColumn].derivedExpression.sourceColumns
              val rollup = col.asInstanceOf[DerivedFactColumn].rollupExpression
              sourceColumnNames.foreach(colName => {
                println("Source ColName" + colName)
                val sourceCol = queryContext.factBestCandidate.fact.columnsByNameMap.get(colName).get
                if (!sourceCol.isDerivedColumn && !sourceCol.isInstanceOf[DimCol]) {
                  val finalAlias = colName
                  val rollupExp = s"${renderRollupExpression(finalAlias, rollup)} $finalAlias"
                  columnsForGroupBySelect += rollupExp
                }
              })
            } else {
              val rollup = queryBuilderContext.getFactColByAlias(alias).asInstanceOf[FactColumn].rollupExpression
              val finalAlias = queryBuilderContext.getFactColExpressionOrNameForAlias(alias)
              val rollupExp = s"${renderRollupExpression(finalAlias, rollup)} $finalAlias"
              columnsForGroupBySelect += rollupExp
            }
          }
        case DimColumnInfo(alias) =>
          addDimGroupByColumns(alias, false)
      }
      println("cols select: " + columnsForGroupBySelect)
      println("cols groupby: " + columnsForGroupBy)
    }

    if (isOuterGroupBy) generateOuterGroupByColumns
    val outerCols = generateOuterColumns()
    val concatenatedCols = generateConcatenatedCols()

    val dimJoinQuery = queryBuilder.getJoinExpressions

    val parameterizedQuery : String = {
      if (isOuterGroupBy) {
        val groupByColumnSelect = columnsForGroupBySelect.mkString(",")
        val groupByColumns = columnsForGroupBy.mkString(",")

        s"""SELECT $concatenatedCols
            |FROM(
            |SELECT $outerCols
            |FROM(
            |SELECT $groupByColumnSelect
            |FROM($factQueryFragment)
            |$factViewAlias
            |$dimJoinQuery
            |GROUP BY $groupByColumns) $outerGroupByAlias
            |)
         """.stripMargin
      } else {
        // factViewAlias => needs to generate abbr from factView name like account_stats_1h_v2 -> as1v0
        // outerCols same cols in concate cols, different expression ???
        s"""SELECT $concatenatedCols
            |FROM(
            |SELECT $outerCols
            |FROM($factQueryFragment)
            |$factViewAlias
            |$dimJoinQuery)
         """.stripMargin
      }
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
      IndexedSeq.empty
    )
  }
}

object HiveQueryGenerator extends Logging {
  val ANY_PARTITIONING_SCHEME = HivePartitioningScheme("") //no name needed since class name hashcode

  def register(queryGeneratorRegistry: QueryGeneratorRegistry, partitionDimensionColumnRenderer:PartitionColumnRenderer, udfStatements: Set[UDFRegistration]) = {
    if(!queryGeneratorRegistry.isEngineRegistered(HiveEngine)) {
      val generator = new HiveQueryGenerator(partitionDimensionColumnRenderer:PartitionColumnRenderer, udfStatements)
      queryGeneratorRegistry.register(HiveEngine, generator)
    } else {
      queryGeneratorRegistry.getGenerator(HiveEngine).foreach {
        qg =>
          if(!qg.isInstanceOf[HiveQueryGenerator]) {
            warn(s"Another query generator registered for HiveEngine : ${qg.getClass.getCanonicalName}")
          }
      }
    }
  }
}
