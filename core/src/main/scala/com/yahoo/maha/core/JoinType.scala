package com.yahoo.maha.core

import com.yahoo.maha.core.fact.FactBestCandidate
import com.yahoo.maha.core.query.DimensionBundle
import grizzled.slf4j.Logging

trait JoinType
case object LeftOuterJoin extends JoinType
case object RightOuterJoin extends JoinType
case object InnerJoin extends JoinType


trait JoinTypeHelper {
  def defaultJoinType : Option[JoinType]

  def getJoinString(joinType: JoinType) (implicit engine : Engine): String = {
    engine match {
      case OracleEngine => getJoinSqlString(joinType)
      case HiveEngine => getJoinSqlString(joinType)
      case _=> "" //TO_DO
    }
  }

  def getJoinSqlString(joinType: JoinType) : String = {
    joinType match {
      case LeftOuterJoin => "LEFT OUTER JOIN"
      case RightOuterJoin => "RIGHT OUTER JOIN"
      case InnerJoin => "INNER JOIN"
      case _ => throw new IllegalArgumentException(s"Unexpected Join Type $joinType")
    }
  }
}

object NoopJoinTypeHelper extends JoinTypeHelper {
  override def defaultJoinType: Option[JoinType] = None
}

case class DimFactJoinTypeHelper(factBest: FactBestCandidate, requestModel: RequestModel) extends JoinTypeHelper with Logging {

  val factHasSchemaRequiredFields: Boolean = factBest.schemaRequiredAliases.forall(factBest.publicFact.columnsByAlias.apply)
  val hasAllDimsNonFKNonForceFilterAsync = requestModel.isAsyncRequest && requestModel.hasAllDimsNonFKNonForceFilter

  val defaultJoinType: Option[JoinType] = {
    if (requestModel.forceDimDriven) {
      Some(RightOuterJoin)
    } else {
      if (hasAllDimsNonFKNonForceFilterAsync) {
        Some(InnerJoin)
      } else
      if (factHasSchemaRequiredFields) {
        Some(LeftOuterJoin)
      } else {
        None
      }
    }
  }

  def getJoinType(dimBundle: DimensionBundle): JoinType = {
    if (factBest.schemaRequiredAliases.forall(dimBundle.publicDim.columnsByAlias)) {
      if (requestModel.isDebugEnabled) {
        info(s"dimBundle.dim.isDerivedDimension: ${dimBundle.dim.name} ${dimBundle.dim.isDerivedDimension} hasNonPushDownFilters: ${dimBundle.hasNonPushDownFilters}")
      }
      if (dimBundle.dim.isDerivedDimension) {
        if (dimBundle.hasNonPushDownFilters) { // If derived dim has filter, then use inner join, otherwise use left outer join
          InnerJoin
        } else {
          LeftOuterJoin
        }
      } else {
        RightOuterJoin
      }
    } else {
      LeftOuterJoin
    }
  }

}