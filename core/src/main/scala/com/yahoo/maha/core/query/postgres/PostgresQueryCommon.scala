package com.yahoo.maha.core.query.postgres

import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension.{DimCol, Dimension, PostgresHashPartitioning, PostgresPKCompositeIndex}
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.query._

import scala.collection.SortedSet
import scala.collection.mutable.ListBuffer

/**
 * Created by pranavbhole on 14/11/17.
 */

case class WhereClause(filters: CombiningFilter) {
  override def toString: String = {
    if (filters.isEmpty) {
      ""
    } else {
      s"WHERE ${filters.toString}"
    }
  }
}

case class RenderedDimension(dimAlias: String, sql: String, onCondition: Option[String], supportingRenderedDimension: Option[RenderedDimension] = None, hasPagination: Boolean, hasTotalRows: Boolean)

case class DimensionSql(drivingDimensionSql: String, multiDimensionJoinSql: Option[String], hasPagination: Boolean, hasTotalRows: Boolean)

trait PostgresQueryCommon extends  BaseQueryGenerator[WithPostgresEngine] {

  final protected[this] val MAX_SNAPSHOT_TS_ALIAS: String = "max_snapshot_ts_"
  final protected[this] val ADDITIONAL_PAGINATION_COLUMN: IndexedSeq[String] = IndexedSeq(PostgresQueryGenerator.ROW_COUNT_ALIAS)
  final protected[this] val PAGINATION_ROW_COUNT: String = s"""Count(*) OVER() "${PostgresQueryGenerator.ROW_COUNT_ALIAS}""""
  final protected[this] val supportingDimPostfix: String = "_indexed"
  final protected[this] val PAGINATION_WRAPPER: String = "SELECT * FROM (SELECT D.*, ROW_NUMBER() OVER() AS ROWNUM FROM (SELECT * FROM (%s) %s %s) D ) %s WHERE %s"
  final protected[this] val OUTER_PAGINATION_WRAPPER: String = "%s WHERE %s"
  final protected[this] val OUTER_PAGINATION_WRAPPER_WITH_FILTERS: String = "%s AND %s"
  final protected[this] val PAGINATION_WRAPPER_UNION: String = "SELECT * FROM (SELECT D.*, ROW_NUMBER() OVER() AS ROWNUM FROM (%s) D ) %s"
  final protected[this] val PAGINATION_ROW_COUNT_COL = ColumnContext.withColumnContext { implicit cc =>
    DimCol(PostgresQueryGenerator.ROW_COUNT_ALIAS, IntType())
  }
  final protected[this] val ROW_NUMBER_ALIAS = "ROW_NUMBER() OVER() AS ROWNUM"

  // Definition Prototypes
  def generateDimensionSql(queryContext: QueryContext, queryBuilderContext: QueryBuilderContext, includePagination: Boolean): DimensionSql
  def renderOuterColumn(columnInfo: ColumnInfo, queryBuilderContext: QueryBuilderContext, duplicateAliasMapping: Map[String, Set[String]], isFactOnlyQuery: Boolean, isDimOnly: Boolean, queryContext: QueryContext): (String, String)
  def renderColumnWithAlias(fact: Fact, column: Column, alias: String, requiredInnerCols: Set[String], queryBuilder: QueryBuilder, queryBuilderContext: QueryBuilderContext, queryContext: FactualQueryContext): Unit

  protected[this] val factAlias: String = "FactAlias"

  override def validateEngineConstraints(requestModel: RequestModel): Boolean = {
    val filters: SortedSet[Filter] = requestModel.factFilters ++ requestModel.dimFilters
    !filters.exists(f => {
      f match {
        case pdf@PushDownFilter(filter) => filter match {
          case inf@InFilter(field,values,_,_) => if(values.size > PostgresEngine.MAX_SIZE_IN_FILTER) true else false
          case _ => false
        }
        case inf@InFilter(field,values,_,_) => if(values.size > PostgresEngine.MAX_SIZE_IN_FILTER) true else false
        case _ => false
      }
    }) && requestModel.orFilterMeta.isEmpty
  }

  protected[this] def getFactAlias(name: String, dims: Set[Dimension]): String = {
    // if hash partition supported
    if (dims.exists(_.annotations.contains(PostgresHashPartitioning))) {
      s"$name $factAlias"
    } else {
      s"$name"
    }
  }

  protected[this] def getDimOptionalPkIndex(dim: Dimension): Option[PostgresPKCompositeIndex] = {
    dim.annotations.find(_.isInstanceOf[PostgresPKCompositeIndex]).map(_.asInstanceOf[PostgresPKCompositeIndex])
  }

  protected[this] def getFactOptionalHint(factualContext: FactualQueryContext, requestModel: RequestModel): Option[String] = {
    val fact = factualContext.factBestCandidate.fact
    val legacyHint = fact.annotations.foldLeft(Option.empty[String]) {
      (optionalHint, annotation) =>
        if (annotation.isInstanceOf[PostgresFactDimDrivenHint] && requestModel.isDimDriven) {
          Option(annotation.asInstanceOf[PostgresFactDimDrivenHint].hint)
        } else {
          if (annotation.isInstanceOf[PostgresFactStaticHint] && optionalHint.isEmpty) {
            Option(annotation.asInstanceOf[PostgresFactStaticHint].hint)
          } else {
            optionalHint
          }
        }
    }
    val conditionalHintsOption = if(factualContext.factConditionalHints.nonEmpty) {
      Option(factualContext.factConditionalHints.mkString(" "))
    } else None
    if(legacyHint.isDefined) {
      conditionalHintsOption.fold(legacyHint)(ch => legacyHint.map(lh => s"$lh $ch"))
    } else conditionalHintsOption
  }

  protected[this] def additionalColumns(queryContext: QueryContext): IndexedSeq[String] = {
    if (queryContext.requestModel.includeRowCount) {
      ADDITIONAL_PAGINATION_COLUMN
    } else {
      IndexedSeq.empty[String]
    }
  }

  protected[this] def concat(tuple: (String, String)): String = {
    if (tuple._2.isEmpty) {
      s"""${tuple._1}"""
    } else {
      s"""${tuple._1} "${tuple._2}""""
    }
  }

  protected[this] def renderSortByColumn(columnInfo: SortByColumnInfo, queryBuilderContext: QueryBuilderContext): String = {
    columnInfo match {
      case FactSortByColumnInfo(alias, order) =>
        s""""${columnInfo.alias}" ${columnInfo.order.toString} NULLS LAST"""
      case DimSortByColumnInfo(alias, order) =>
        val column = queryBuilderContext.getDimensionColByAlias(alias)
        if (column.isKey) {
          s""""${columnInfo.alias}" ${columnInfo.order.toString}"""
        } else {
          s""""${columnInfo.alias}" ${columnInfo.order.toString} NULLS LAST"""
        }
      case _ => throw new UnsupportedOperationException("Unsupported Sort By Column Type")
    }
  }

  protected[this] def renderRollupExpression(expression: String, rollupExpression: RollupExpression
                                             , renderedColExp: Option[String] = None, isOuterGroupBy: Boolean = false): String = {
    rollupExpression match {
      case SumRollup => s"SUM(${renderedColExp.getOrElse(expression)})"
      case MaxRollup => s"MAX(${renderedColExp.getOrElse(expression)})"
      case MinRollup => s"MIN(${renderedColExp.getOrElse(expression)})"
      case AverageRollup => s"AVG(${renderedColExp.getOrElse(expression)})"
      case PostgresCustomRollup(exp) => s"(${exp.render(expression, Map.empty, renderedColExp)})"
      case NoopRollup => s"(${renderedColExp.getOrElse(expression)})"
      case CountRollup =>
        if(isOuterGroupBy) {
          s"SUM(${renderedColExp.getOrElse(expression)})"
        } else {
          s"COUNT(*)"
        }
      case any => throw new UnsupportedOperationException(s"Unhandled rollup expression : $any")
    }
  }

  protected[this] def toComment(hint: String): String = {
    s"/*+ $hint */"
  }

  protected[this] def addPaginationWrapper(queryString: String, mr: Int, si: Int, includePagination: Boolean
                                           , queryBuilderContext: QueryBuilderContext): String = {
    if(includePagination) {
      val paginationPredicates: ListBuffer[String] = new ListBuffer[String]()
      val minPosition: Int = if (si < 0) 1 else si + 1
      paginationPredicates += ("ROWNUM >= " + minPosition)
      val stopKeyPredicate: String =  {
        if (mr > 0) {
          val maxPosition: Int = if (si <= 0) mr else minPosition - 1 + mr
          paginationPredicates += ("ROWNUM <= " + maxPosition)
          //TODO: fix me
          s"LIMIT $maxPosition"
        } else  s""
      }
      //"SELECT * FROM (SELECT D.*, ROW_NUMBER() OVER() AS ROWNUM FROM (SELECT * FROM (%s) %s %s) D ) %s WHERE %s"
      String.format(PAGINATION_WRAPPER, queryString, queryBuilderContext.getSubqueryAlias, stopKeyPredicate
        , queryBuilderContext.getSubqueryAlias, paginationPredicates.toList.mkString(" AND "))
    } else {
      queryString
    }
  }

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
        val exp = nameOrAlias
        val default = s"ELSE '${sm.get.default}' "

        val builder = new StringBuilder
        builder.append("CASE ")
        sm.get.tToStringMap.foreach {
          case (search, result) =>
            builder.append(s"WHEN $exp = '$search' THEN '$result' ")
        }
        builder.append(default)
        builder.append("END")
        builder.mkString
      case _ =>
        nameOrAlias
    }
  }

}

