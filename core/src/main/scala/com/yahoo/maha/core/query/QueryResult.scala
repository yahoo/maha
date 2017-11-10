package com.yahoo.maha.core.query

import com.yahoo.maha.core.query.QueryResultStatus.QueryResultStatus

case class QueryResult[T <: RowList](rowList: T,
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
