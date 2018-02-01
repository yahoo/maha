package com.yahoo.maha.core.query

sealed trait JoinType
case object LeftOuterJoin extends JoinType
case object RightOuterJoin extends JoinType
case object InnerJoin extends JoinType