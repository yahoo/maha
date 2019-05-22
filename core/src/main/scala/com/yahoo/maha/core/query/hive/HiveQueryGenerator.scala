// Copyright 2018, Oath Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query.hive

import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.query._
import grizzled.slf4j.Logging
import scala.collection.{SortedSet, mutable}

/**
 * Created by pranavbhole on 10/16/18.
  * This is latest Hive Query Generator to be currently used,
  * which has sorting and dim aggregation feature.
 */
class HiveQueryGenerator(partitionColumnRenderer:PartitionColumnRenderer, udfStatements: Set[UDFRegistration]) extends HiveQueryGeneratorCommon(partitionColumnRenderer, udfStatements) with Logging {

  override val version = Version.v0

  override val engine: Engine = HiveEngine
  override def generate(queryContext: QueryContext): Query = {
    info(s"Generating Hive query using HiveQueryGenerator V0:, version ${version}")
    queryContext match {
      case context : CombinedQueryContext =>
        generateQuery(context)
      case FactQueryContext(factBestCandidate, model, indexAliasOption, factGroupByKeys, attributes) =>
        generateQuery(CombinedQueryContext(SortedSet.empty, factBestCandidate, model, attributes))
      case DimFactOuterGroupByQueryQueryContext(dims, factBestCandidate, model, attributes) =>
        generateQuery(CombinedQueryContext(dims, factBestCandidate, model, attributes))
        //generateOuterGroupByQuery(CombinedQueryContext(dims, factBestCandidate, model, attributes))
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
    val factViewName = fact.name
    val factViewAlias = queryBuilderContext.getAliasForTable(factViewName)
    val dims = queryContext.dims

    val requestedCols = queryContext.requestModel.requestCols
    val columnAliasToColMap = new mutable.HashMap[String, Column]()

    def renderOuterColumn(columnInfo: ColumnInfo, queryBuilderContext: QueryBuilderContext, duplicateAliasMapping: Map[String, Set[String]], factCandidate: FactBestCandidate): String = {

      def renderNormalOuterColumnWithoutCasting(column: Column, finalAlias: String) : String = {
        val renderedCol = column.dataType match {
          case DecType(_, _, Some(default), Some(min), Some(max), _) =>
            val minMaxClause = s"CASE WHEN (($finalAlias >= ${min}) AND ($finalAlias <= ${max})) THEN $finalAlias ELSE ${default} END"
            s"""ROUND(COALESCE($minMaxClause, ${default}), 10)"""
          case DecType(_, _, Some(default), _, _, _) =>
            s"""ROUND(COALESCE($finalAlias, ${default}), 10)"""
          case DecType(_, _, _, _, _, _) =>
            s"""ROUND(COALESCE($finalAlias, 0L), 10)"""
          case IntType(_,sm,_,_,_) =>
            s"""COALESCE($finalAlias, 0L)"""
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

      def renderFactCol(alias: String, finalAliasOrExpression: String, col: Column, finalAlias: String): String = {
        val postFilterAlias = renderNormalOuterColumnWithoutCasting(col, finalAliasOrExpression)
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
          val postFilterAlias = renderNormalOuterColumnWithoutCasting(col, referredAlias)
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
        case HiveDerDimAggregateCol(_, dt, cc, de, _, _, _) =>
          // this col always has rollup expresion in derived expression as requirement
          val renderedAlias = renderColumnAlias(alias)
          queryBuilderContext.setFactColAlias(alias, renderedAlias, column)
          s"""${renderRollupExpression(de.render(name, Map.empty), NoopRollup)} $renderedAlias"""
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

    val factQueryFragment = generateFactQueryFragment(queryContext, queryBuilder, renderDerivedFactCols, renderRollupExpression, renderColumnWithAlias)
    generateDimSelects(dims, queryBuilderContext, queryBuilder, requestModel, fact, factViewAlias)

    generateOrderByClause(queryContext, queryBuilder)
    val orderByClause = queryBuilder.getOrderByClause

    val outerCols = generateOuterColumns(queryContext, queryBuilderContext, queryBuilder, renderOuterColumn)
    val concatenatedCols = generateConcatenatedColsWithCast(queryContext, queryBuilderContext)

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
          |$orderByClause)
       """.stripMargin
    }

    val parameterizedQueryWithRowLimit = {
      if(queryContext.requestModel.maxRows > 0) {
        s"""$parameterizedQuery queryAlias LIMIT ${queryContext.requestModel.maxRows}"""
      } else {
        s"""$parameterizedQuery queryAlias"""
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
      parameterizedQueryWithRowLimit,
      Option(udfStatements),
      paramBuilder.build(),
      queryContext.requestModel.requestCols.map(_.alias),
      columnAliasToColMap.toMap,
      IndexedSeq.empty,
      queryGenVersion = Some(this.version)
    )
  }
}

object HiveQueryGenerator extends Logging {
  val ANY_PARTITIONING_SCHEME = HivePartitioningScheme("") //no name needed since class name hashcode

  def register(queryGeneratorRegistry: QueryGeneratorRegistry, partitionDimensionColumnRenderer:PartitionColumnRenderer, udfStatements: Set[UDFRegistration]) = {
    if(!queryGeneratorRegistry.isEngineRegistered(HiveEngine, Option(Version.v0))) {
      val generator = new HiveQueryGenerator(partitionDimensionColumnRenderer:PartitionColumnRenderer, udfStatements)
      queryGeneratorRegistry.register(HiveEngine, generator, Version.v0)
    } else {
      queryGeneratorRegistry.getDefaultGenerator(HiveEngine).foreach {
        qg =>
          if(!qg.isInstanceOf[HiveQueryGenerator]) {
            warn(s"Another query generator registered for HiveEngine : ${qg.getClass.getCanonicalName}")
          }
      }
    }
  }
}



