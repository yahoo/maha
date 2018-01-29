package com.yahoo.maha.core

trait JoinType
case object LeftOuterJoin extends JoinType
case object RightOuterJoin extends JoinType
case object InnerJoin extends JoinType

object JoinTypeHelper {
  def getJoinString(joinType: JoinType, engine : Engine): String = {
    engine match {
      case OracleEngine => getJoinSqlString(joinType)
      case HiveEngine => getJoinSqlString(joinType)
      case PrestoEngine => getJoinSqlString(joinType)
      case _=>  throw new IllegalArgumentException(s"Unexpected Engine $engine for Join Type $joinType")
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