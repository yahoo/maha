package com.yahoo.maha.core.query

import com.yahoo.maha.core.query.QueryResultStatus.QueryResultStatus

case class QueryResult(rowList: RowList,
                       queryAttributes: QueryAttributes,
                       queryResultStatus: QueryResultStatus,
                       message: String = "") {

  def isFailure: Boolean = {
    queryResultStatus == QueryResultStatus.FAILURE
  }
}

object QueryResultStatus extends Enumeration {
  type QueryResultStatus = Value
  val SUCCESS, FAILURE = Value
}
