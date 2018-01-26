package com.yahoo.maha.core

trait JoinType {
  def joinStr : String
}
case object LeftOuterJoin extends JoinType {
  val joinStr = "LEFT OUTER JOIN"
}
case object RightOuterJoin extends JoinType {
  val joinStr = "RIGHT OUTER JOIN"
}
case object InnerJoin extends JoinType {
  val joinStr = "INNER JOIN"
}

