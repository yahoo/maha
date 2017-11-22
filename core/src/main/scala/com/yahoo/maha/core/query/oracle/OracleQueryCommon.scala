package com.yahoo.maha.core.query.oracle

import com.yahoo.maha.core.dimension.{PKCompositeIndex, OracleAdvertiserHashPartitioning, Dimension, DimCol}
import com.yahoo.maha.core.fact.{OracleFactStaticHint, OracleFactDimDrivenHint, Fact}
import com.yahoo.maha.core.query.QueryContext
import com.yahoo.maha.core.{RequestModel, CombiningFilter, ColumnContext, IntType}

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

trait OracleQueryCommon {

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

}

