// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.hive

import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact.{DerivedFactColumn, _}
import com.yahoo.maha.core.query._
import grizzled.slf4j.Logging

import scala.collection.mutable.LinkedHashSet
import scala.collection.{SortedSet, mutable}

/**
 * Created by shengyao on 12/16/15.
 */
class HiveQueryGenerator(partitionColumnRenderer:PartitionColumnRenderer, udfStatements: Set[UDFRegistration]) extends HiveQueryGeneratorCommon(partitionColumnRenderer, udfStatements) {

  override val engine: Engine = HiveEngine
  override def generate(queryContext: QueryContext): Query = {
    queryContext match {
      case context : CombinedQueryContext =>
        generateQuery(context)
      case FactQueryContext(factBestCandidate, model, indexAliasOption, attributes) =>
        generateQuery(CombinedQueryContext(SortedSet.empty, factBestCandidate, model, attributes))
      case DimFactOuterGroupByQueryQueryContext(dims, factBestCandidate, model, attributes) =>
        //generateOuterGroupByQuery(CombinedQueryContext(dims, factBestCandidate, model, attributes))
        generateQuery(CombinedQueryContext(dims, factBestCandidate, model, attributes))
      case any => throw new UnsupportedOperationException(s"query context not supported : $any")
    }
  }

  override def validateEngineConstraints(requestModel: RequestModel): Boolean = {
    requestModel.orFilterMeta.isEmpty
  }

  private[this] def generateOuterGroupByQuery(queryContext: CombinedQueryContext) : Query = {

    val queryBuilderContext = new QueryBuilderContext
    val queryBuilder: QueryBuilder = new QueryBuilder(
      queryContext.requestModel.requestCols.size + 5,
      queryContext.requestModel.requestSortByCols.size + 1)
    val requestModel = queryContext.requestModel
    val publicFact = queryContext.factBestCandidate.publicFact
    val fact = queryContext.factBestCandidate.fact
    val factViewAlias = queryBuilderContext.getAliasForTable(fact.name)
    val requestedCols = queryContext.requestModel.requestCols
    val columnAliasToColMap = new mutable.HashMap[String, Column]()
    val columnNamesInOuterGroupBy: LinkedHashSet[String] = new mutable.LinkedHashSet[String]
    val outerGroupByAlias = "outergroupby"

    def renderOuterColumn(columnInfo: ColumnInfo, queryBuilderContext: QueryBuilderContext, duplicateAliasMapping: Map[String, Set[String]], factCandidate: FactBestCandidate): String = {

      def renderFactCol(alias: String, finalAliasOrExpression: String, col: Column, finalAlias: String): String = {
        val finalExp = {
          if (col.isInstanceOf[FactColumn] && col.isDerivedColumn) {
            val rollupExpression = col.asInstanceOf[DerivedFactColumn].rollupExpression
            if (rollupExpression.equals(NoopRollup) && columnNamesInOuterGroupBy.contains(finalAlias)) {
              finalAlias
            } else {
              val renderedDerived = col.asInstanceOf[DerivedFactColumn].derivedExpression.render(finalAlias, queryBuilderContext.getColAliasToFactColNameMap).toString
              renderedDerived
            }
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
          val column = fact.columnsByNameMap.get(publicFact.aliasToNameColumnMap.get(alias).get).get
          if (!queryBuilderContext.containsFactColNameForAlias(alias)) {
            renderColumnWithAlias(factCandidate.fact, column, alias, Set.empty, true)
          }
          QueryGeneratorHelper.handleOuterFactColInfo(queryBuilderContext, alias, factCandidate, renderFactCol, duplicateAliasMapping, factCandidate.fact.name, true)
        case DimColumnInfo(alias) =>
          val col = queryBuilderContext.getDimensionColByAlias(alias)
          val finalAlias = queryBuilderContext.getDimensionColNameForAlias(alias)
          val referredAlias = s"$outerGroupByAlias.$finalAlias"
          val postFilterAlias = renderNormalOuterColumn(col, referredAlias)
          s"""$postFilterAlias $finalAlias"""
        case ConstantColumnInfo(alias, value) =>
          val finalAlias = getConstantColAlias(alias)
          s"""'$value' $finalAlias"""
        case _ => throw new UnsupportedOperationException("Unsupported Column Type")
      }
    }

    def renderRollupExpression(expression: String, rollupExpression: RollupExpression, renderedColExp: Option[String] = None) : String = {
      rollupExpression match {
        case SumRollup => s"SUM($expression)"
        case MaxRollup => s"MAX($expression)"
        case MinRollup => s"MIN($expression)"
        case AverageRollup => s"AVG($expression)"
        case HiveCustomRollup(exp) => {
          exp.sourceColumns.foreach(col => {
            renderColumnWithAlias(fact, fact.columnsByNameMap.get(col).get, col, Set.empty, false)
          })
          s"(${exp.render(expression, Map.empty, renderedColExp)})"
        }
        case NoopRollup => s"($expression)"
        case any => throw new UnsupportedOperationException(s"Unhandled rollup expression : $any")
      }
    }

    def renderDerivedFactCols(derivedCols: List[(Column, String)]):Unit = {
        val requiredInnerCols: Set[String] =
          derivedCols.view.map(_._1.asInstanceOf[DerivedColumn]).flatMap(dc => dc.derivedExpression.sourceColumns).toSet
        derivedCols.foreach {
          case (column, alias) =>
            requiredInnerCols.foreach(innerColName => {
              val innerCol = queryContext.factBestCandidate.fact.columnsByNameMap.get(innerColName).get
              innerCol match {
                case d:DimensionColumn =>
                  renderColumnWithAlias(fact, innerCol, innerColName, Set.empty, false)
                  queryBuilder.addGroupBy(innerColName)
                case f:FactColumn =>
                  if (f.isDerivedColumn) {
                    renderDerivedFactCols(List((f, f.alias.getOrElse(f.name))))
                  } else {
                    val rollupExpression = column.asInstanceOf[FactColumn].rollupExpression
                    val rollup = rollupExpression match {
                      case NoopRollup => SumRollup // Need to sum facts in the inner select for outergroupby
                      case _ => rollupExpression
                    }
                    val renderedInnerCol = s"${renderRollupExpression(innerColName, rollup)} $innerColName"
                    queryBuilder.addFactViewColumn(renderedInnerCol)
                  }
              }
            })
        }
    }

    def generateOuterGroupByColumns: (String, String) = {
      val columnsForGroupBySelect: LinkedHashSet[String] = new mutable.LinkedHashSet[String]
      val columnsForGroupBy: LinkedHashSet[String] = new mutable.LinkedHashSet[String]

      def addDimGroupByColumns(colNameOrAlias: String, isFromFact: Boolean, queryBuilderContext: QueryBuilderContext) = {
        val referredAlias = {
          if (isFromFact) {
            if (queryBuilderContext.containsFactColNameForAlias(colNameOrAlias)) {
              s"${queryBuilderContext.getAliasForTable(fact.name)}.${queryBuilderContext.getFactColNameForAlias(colNameOrAlias)}"
            } else {
              // It's not a requested column
              s"${queryBuilderContext.getAliasForTable(fact.name)}.$colNameOrAlias"
            }
          } else {
            val finalAlias = queryBuilderContext.getDimensionColNameForAlias(colNameOrAlias)
            val tableName = queryBuilderContext.getDimensionForColAlias(colNameOrAlias).name
            s"${queryBuilderContext.getAliasForTable(tableName)}.$finalAlias"
          }
        }
        columnsForGroupBySelect += referredAlias
        columnsForGroupBy += referredAlias
        columnNamesInOuterGroupBy += referredAlias
      }

      def addDerivedFactColOuterGroupBy(alias: String, col: FactColumn): Unit = {
        val sourceColumnNames = col.asInstanceOf[DerivedFactColumn].derivedExpression.sourceColumns
        sourceColumnNames.foreach(colName => {
          val sourceCol = queryContext.factBestCandidate.fact.columnsByNameMap.get(colName).get
          sourceCol match {
            case d:DimensionColumn =>
              addDimGroupByColumns(sourceCol.alias.getOrElse(sourceCol.name), true, queryBuilderContext)
            case f:FactColumn =>
              val rollupExpression = sourceCol.asInstanceOf[FactColumn].rollupExpression
              if (sourceCol.isDerivedColumn) {
                if (rollupExpression.equals(NoopRollup)) { // Noop rollups should be expanded in outergroupby clause
                  renderColumnWithAlias(fact, sourceCol, alias, Set.empty, false)
                  val finalAlias = renderColumnAlias(sourceCol.name)
                  val renderedDerived = s"${queryBuilderContext.getFactColExpressionOrNameForAlias(alias)} $finalAlias"
                  columnsForGroupBySelect.add(renderedDerived)
                  columnNamesInOuterGroupBy += finalAlias
                } else {
                  addDerivedFactColOuterGroupBy(alias, sourceCol.asInstanceOf[DerivedFactColumn])
                }
              } else {
                val finalAlias = colName
                val rollup = rollupExpression match {
                  case NoopRollup => SumRollup // Need to sum facts in the groupby
                  case _ => rollupExpression
                }
                val rollupExp = s"${renderRollupExpression(finalAlias, rollup)} $finalAlias"
                columnsForGroupBySelect += rollupExp
                columnNamesInOuterGroupBy += finalAlias
              }
          }
        })
      }

      requestedCols.foreach {
        case FactColumnInfo(alias) =>
          if (queryBuilderContext.containsFactAliasToColumnMap(alias) && queryBuilderContext.getFactColByAlias(alias).isInstanceOf[DimensionColumn]) {
            addDimGroupByColumns(alias, true, queryBuilderContext)
          } else {
            val colName = queryContext.factBestCandidate.publicFact.aliasToNameColumnMap.get(alias).get
            val col = queryContext.factBestCandidate.fact.factColMap.get(colName).get
            if (col.isDerivedColumn) {
              addDerivedFactColOuterGroupBy(alias, col)
            } else {
              val rollup = queryBuilderContext.getFactColByAlias(alias).asInstanceOf[FactColumn].rollupExpression
              val finalAlias = queryBuilderContext.getFactColExpressionOrNameForAlias(alias)
              val rollupExp = s"${renderRollupExpression(finalAlias, rollup)} $finalAlias"
              columnsForGroupBySelect += rollupExp
              columnNamesInOuterGroupBy += finalAlias
            }
          }
        case DimColumnInfo(alias) =>
          addDimGroupByColumns(alias, false, queryBuilderContext)
      }
      (columnsForGroupBySelect.mkString(","), columnsForGroupBy.mkString(","))
    }

    def renderColumnWithAlias(fact: Fact,
                              column: Column,
                              alias: String,
                              requiredInnerCols: Set[String],
                              renderOuterColumn: Boolean): Unit = {
      val name = column.alias.getOrElse(column.name)
      val exp = column match {
        case any if queryBuilderContext.containsColByName(name) && queryBuilderContext.aliasColumnMap.contains(alias) =>
          //do nothing, we've already processed it
          ""
        case DimCol(_, dt, _, _, _, _) if dt.hasStaticMapping =>
          val renderedAlias = if (!renderOuterColumn) renderColumnAlias(name) else renderColumnAlias(alias)
          queryBuilderContext.setFactColAliasAndExpression(alias, renderedAlias, column, Option(name))
          s"${renderStaticMappedDimension(column)} $name"
        case DimCol(_, dt, _, _, _, _) =>
          val renderedAlias = if (!renderOuterColumn) renderColumnAlias(name) else renderColumnAlias(alias)
          queryBuilderContext.setFactColAliasAndExpression(alias, renderedAlias, column, Option(name))
          name
        case HiveDerDimCol(_, dt, _, de, _, _, _) =>
          val renderedAlias = if (!renderOuterColumn) renderColumnAlias(name) else renderColumnAlias(alias)
          queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
          s"""${de.render(name, Map.empty)} $renderedAlias"""
        case FactCol(_, dt, _, rollup, _, _, _) =>
          dt match {
            case DecType(_, _, Some(default), Some(min), Some(max), _) =>
              val renderedAlias = if (!renderOuterColumn) renderColumnAlias(name) else renderColumnAlias(alias)
              val minMaxClause = s"CASE WHEN (($name >= $min) AND ($name <= $max)) THEN $name ELSE $default END"
              queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
              s"""${renderRollupExpression(name, rollup, Option(minMaxClause))} $renderedAlias"""
            case IntType(_, _, Some(default), Some(min), Some(max)) =>
              val renderedAlias = if (!renderOuterColumn) renderColumnAlias(name) else renderColumnAlias(alias)
              val minMaxClause = s"CASE WHEN (($name >= $min) AND ($name <= $max)) THEN $name ELSE $default END"
              queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
              s"""${renderRollupExpression(name, rollup, Option(minMaxClause))} $renderedAlias"""
            case _ =>
              val renderedAlias = if (!renderOuterColumn) renderColumnAlias(name) else renderColumnAlias(alias)
              queryBuilderContext.setFactColAliasAndExpression(alias, renderedAlias, column, Option(name))
              s"""${renderRollupExpression(name, rollup)} $name"""
          }
        case HiveDerFactCol(_, _, dt, cc, de, annotations, rollup, _)
          if renderOuterColumn && (queryContext.factBestCandidate.filterCols.contains(name) || de.expression.hasRollupExpression || requiredInnerCols(name)
            || de.isDimensionDriven) =>
          val renderedAlias = if (!renderOuterColumn) renderColumnAlias(name) else renderColumnAlias(alias)
          queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
          s"""${renderRollupExpression(de.render(name, Map.empty), rollup)} $renderedAlias"""

        case HiveDerFactCol(_, _, dt, cc, de, annotations, _, _) =>
          //means no fact operation on this column, push expression outside
          de.sourceColumns.foreach {
            case src if src != name =>
              val sourceCol = fact.columnsByNameMap(src)
              //val renderedAlias = renderColumnAlias(sourceCol.name)
              val renderedAlias = sourceCol.alias.getOrElse(sourceCol.name)
              renderColumnWithAlias(fact, sourceCol, renderedAlias, requiredInnerCols, renderOuterColumn)
            case _ => //do nothing if we reference ourselves
          }
          //val renderedAlias = renderColumnAlias(alias)
          val renderedAlias = if (!renderOuterColumn) renderColumnAlias(name) else renderColumnAlias(alias)
          queryBuilderContext.setFactColAliasAndExpression(alias, renderedAlias, column, Option(s"""(${de.render(renderedAlias, queryBuilderContext.getColAliasToFactColNameMap, expandDerivedExpression = true)})"""))
          ""
      }

      queryBuilder.addFactViewColumn(exp)
    }

    val factQueryFragment = generateFactQueryFragment(queryContext, queryBuilder, renderDerivedFactCols, renderRollupExpression, renderColumnWithAlias)
    generateDimSelects(queryContext.dims, queryBuilderContext, queryBuilder, requestModel, fact, factViewAlias)
    val (columnsForGroupBySelect, columnsForGroupBy) = generateOuterGroupByColumns
    val outerCols = generateOuterColumns(queryContext, queryBuilderContext, queryBuilder, renderOuterColumn)
    val concatenatedCols = generateConcatenatedCols(queryContext, queryBuilderContext)

    val parameterizedQuery : String = {
      val dimJoinQuery = queryBuilder.getJoinExpressions
      s"""SELECT $concatenatedCols
         |FROM(
         |SELECT $outerCols
         |FROM(
         |SELECT $columnsForGroupBySelect
         |FROM($factQueryFragment)
         |$factViewAlias
         |$dimJoinQuery
         |GROUP BY $columnsForGroupBy) $outerGroupByAlias
         |)
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
      IndexedSeq.empty
    )
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
    val factViewName = fact.name
    val factViewAlias = queryBuilderContext.getAliasForTable(factViewName)
    val dims = queryContext.dims

    val requestedCols = queryContext.requestModel.requestCols
    val columnAliasToColMap = new mutable.HashMap[String, Column]()

    def renderOuterColumn(columnInfo: ColumnInfo, queryBuilderContext: QueryBuilderContext, duplicateAliasMapping: Map[String, Set[String]], factCandidate: FactBestCandidate): String = {

      def renderFactCol(alias: String, finalAliasOrExpression: String, col: Column, finalAlias: String): String = {
        val postFilterAlias = renderNormalOuterColumn(col, finalAliasOrExpression)
        s"""$postFilterAlias $finalAlias"""
      }

      columnInfo match {
        case FactColumnInfo(alias) =>
          QueryGeneratorHelper.handleOuterFactColInfo(queryBuilderContext, alias, factCandidate, renderFactCol, duplicateAliasMapping, factCandidate.fact.name, false)
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

    def renderRollupExpression(expression: String, rollupExpression: RollupExpression, renderedColExp: Option[String] = None) : String = {
      rollupExpression match {
        case SumRollup => s"SUM($expression)"
        case MaxRollup => s"MAX($expression)"
        case MinRollup => s"MIN($expression)"
        case AverageRollup => s"AVG($expression)"
        case HiveCustomRollup(exp) => {
          s"(${exp.render(expression, Map.empty, renderedColExp)})"
        }
        case NoopRollup => s"($expression)"
        case any => throw new UnsupportedOperationException(s"Unhandled rollup expression : $any")
      }
    }

    def renderDerivedFactCols(derivedCols: List[(Column, String)]) = {
      val requiredInnerCols: Set[String] =
        derivedCols.view.map(_._1.asInstanceOf[DerivedColumn]).flatMap(dc => dc.derivedExpression.sourceColumns).toSet
      derivedCols.foreach {
        case (column, alias) =>
          renderColumnWithAlias(fact: Fact, column, alias, requiredInnerCols, false)
      }
    }

    def renderColumnWithAlias(fact: Fact,
                              column: Column,
                              alias: String,
                              requiredInnerCols: Set[String],
                              isOuterColumn: Boolean): Unit = {
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
        case HiveDerFactCol(_, _, dt, cc, de, annotations, rollup, _)
          if queryContext.factBestCandidate.filterCols.contains(name) || de.expression.hasRollupExpression || requiredInnerCols(name)
            || de.isDimensionDriven =>
          val renderedAlias = renderColumnAlias(alias)
          queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
          s"""${renderRollupExpression(de.render(name, Map.empty), rollup)} $renderedAlias"""
        case HiveDerFactCol(_, _, dt, cc, de, annotations, _, _) =>
          //means no fact operation on this column, push expression outside
          de.sourceColumns.foreach {
            case src if src != name =>
              val sourceCol = fact.columnsByNameMap(src)
              //val renderedAlias = renderColumnAlias(sourceCol.name)
              val renderedAlias = sourceCol.alias.getOrElse(sourceCol.name)
              renderColumnWithAlias(fact, sourceCol, renderedAlias, requiredInnerCols, isOuterColumn)
            case _ => //do nothing if we reference ourselves
          }
          //val renderedAlias = renderColumnAlias(alias)
          val renderedAlias = renderColumnAlias(alias)
          queryBuilderContext.setFactColAliasAndExpression(alias, renderedAlias, column, Option(s"""(${de.render(renderedAlias, queryBuilderContext.getColAliasToFactColNameMap, expandDerivedExpression = false)})"""))
          ""
      }

      queryBuilder.addFactViewColumn(exp)
    }

    /**
     * Final Query
     */

    val factQueryFragment = generateFactQueryFragment(queryContext, queryBuilder, renderDerivedFactCols, renderRollupExpression, renderColumnWithAlias)
    generateDimSelects(dims, queryBuilderContext, queryBuilder, requestModel, fact, factViewAlias)
    val outerCols = generateOuterColumns(queryContext, queryBuilderContext, queryBuilder, renderOuterColumn)
    val concatenatedCols = generateConcatenatedCols(queryContext, queryBuilderContext)

    val parameterizedQuery : String = {
      val dimJoinQuery = queryBuilder.getJoinExpressions

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
