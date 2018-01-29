package com.yahoo.maha.core

import com.yahoo.maha.core.fact.FactBestCandidate
import com.yahoo.maha.core.query.DimensionBundle
import grizzled.slf4j.Logging

trait JoinType
case object LeftOuterJoin extends JoinType
case object RightOuterJoin extends JoinType
case object InnerJoin extends JoinType


object JoinTypeHelper {
  def getJoinString(joinType: JoinType, engine : Engine): String = {
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