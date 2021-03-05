package com.yahoo.maha.core.helper

import com.yahoo.maha.core.query.{InnerJoin, JoinType, LeftOuterJoin, RightOuterJoin}
import com.yahoo.maha.core.{BigqueryEngine, Engine, HiveEngine, OracleEngine, PrestoEngine, PostgresEngine}

object SqlHelper {
  def getJoinString(joinType: JoinType, engine : Engine): String = {
    engine match {
      case BigqueryEngine => getJoinSqlString(joinType)
      case OracleEngine => getJoinSqlString(joinType)
      case PostgresEngine => getJoinSqlString(joinType)
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