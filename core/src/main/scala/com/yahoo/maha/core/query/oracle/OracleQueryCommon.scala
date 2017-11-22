package com.yahoo.maha.core.query.oracle

import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension.{DimCol, Dimension, OracleAdvertiserHashPartitioning, PKCompositeIndex}
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.query.{BaseQueryGenerator, QueryBuilderContext, QueryContext}

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

trait OracleQueryCommon extends  BaseQueryGenerator[WithOracleEngine] {

  final protected[this] val MAX_SNAPSHOT_TS_ALIAS: String = "max_snapshot_ts_"
  final protected[this] val ADDITIONAL_PAGINATION_COLUMN: IndexedSeq[String] = IndexedSeq(OracleQueryGenerator.ROW_COUNT_ALIAS)
  final protected[this] val PAGINATION_ROW_COUNT: String = s"""Count(*) OVER() ${OracleQueryGenerator.ROW_COUNT_ALIAS}"""
  final protected[this] val supportingDimPostfix: String = "_indexed"
  final protected[this] val PAGINATION_WRAPPER: String = "SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (SELECT * FROM (%s) %s) D ) WHERE %s"
  final protected[this] val OUTER_PAGINATION_WRAPPER: String = "%s WHERE %s"
  final protected[this] val OUTER_PAGINATION_WRAPPER_WITH_FILTERS: String = "%s AND %s"
  final protected[this] val PAGINATION_WRAPPER_UNION: String = "SELECT * FROM (SELECT D.*, ROWNUM AS ROW_NUMBER FROM (%s) D )"
  final protected[this] val PAGINATION_ROW_COUNT_COL = ColumnContext.withColumnContext { implicit cc =>
    DimCol(OracleQueryGenerator.ROW_COUNT_ALIAS, IntType())
  }

  // Definition Prototypes
  def generateDimensionSql(queryContext: QueryContext, queryBuilderContext: QueryBuilderContext, includePagination: Boolean): DimensionSql
  def renderOuterColumn(columnInfo: ColumnInfo, queryBuilderContext: QueryBuilderContext, duplicateAliasMapping: Map[String, Set[String]], isFactOnlyQuery: Boolean, isDimOnly: Boolean, queryContext: QueryContext): String

  protected[this] val factAlias: String = "FactAlias"

  protected[this] def getFactAlias(name: String, dims: Set[Dimension]): String = {
    // if hash partition supported
    if (dims.exists(_.annotations.contains(OracleAdvertiserHashPartitioning))) {
      s"$name $factAlias"
    } else {
      s"$name"
    }
  }

  protected[this] def getDimOptionalPkIndex(dim: Dimension): Option[PKCompositeIndex] = {
    dim.annotations.find(_.isInstanceOf[PKCompositeIndex]).map(_.asInstanceOf[PKCompositeIndex])
  }

  protected[this] def getFactOptionalHint(fact: Fact, requestModel: RequestModel): Option[String] = {
    fact.annotations.foldLeft(Option.empty[String]) {
      (optionalHint, annotation) =>
        if (annotation.isInstanceOf[OracleFactDimDrivenHint] && requestModel.isDimDriven) {
          Option(annotation.asInstanceOf[OracleFactDimDrivenHint].hint)
        } else {
          if (annotation.isInstanceOf[OracleFactStaticHint] && optionalHint.isEmpty) {
            Option(annotation.asInstanceOf[OracleFactStaticHint].hint)
          } else {
            optionalHint
          }
        }
    }
  }

  protected[this] def additionalColumns(queryContext: QueryContext): IndexedSeq[String] = {
    if (queryContext.requestModel.includeRowCount) {
      ADDITIONAL_PAGINATION_COLUMN
    } else {
      IndexedSeq.empty[String]
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

  protected[this] def renderRollupExpression(expression: String, rollupExpression: RollupExpression, renderedColExp: Option[String] = None): String = {
    rollupExpression match {
      case SumRollup => s"SUM(${renderedColExp.getOrElse(expression)})"
      case MaxRollup => s"MAX(${renderedColExp.getOrElse(expression)})"
      case MinRollup => s"MIN(${renderedColExp.getOrElse(expression)})"
      case AverageRollup => s"AVG(${renderedColExp.getOrElse(expression)})"
      case OracleCustomRollup(exp) => s"(${exp.render(expression, Map.empty, renderedColExp)})"
      case NoopRollup => s"(${renderedColExp.getOrElse(expression)})"
      case any => throw new UnsupportedOperationException(s"Unhandled rollup expression : $any")
    }
  }

  protected[this] def toComment(hint: String): String = {
    s"/*+ $hint */"
  }

  protected[this] def addPaginationWrapper(queryString: String, mr: Int, si: Int, includePagination: Boolean): String = {
    if(includePagination) {
      val paginationPredicates: ListBuffer[String] = new ListBuffer[String]()
      val minPosition: Int = if (si < 0) 1 else si + 1
      paginationPredicates += ("ROW_NUMBER >= " + minPosition)
      val stopKeyPredicate: String =  {
        if (mr > 0) {
          val maxPosition: Int = if (si <= 0) mr else minPosition - 1 + mr
          paginationPredicates += ("ROW_NUMBER <= " + maxPosition)
          s"WHERE ROWNUM <= $maxPosition"
        } else  s""
      }
      String.format(PAGINATION_WRAPPER, queryString, stopKeyPredicate, paginationPredicates.toList.mkString(" AND "))
    } else {
      queryString
    }
  }

}

