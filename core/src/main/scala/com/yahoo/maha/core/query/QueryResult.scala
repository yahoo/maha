package com.yahoo.maha.core.query

import com.yahoo.maha.core.query.QueryResultStatus.QueryResultStatus
import org.json4s.JValue

case class QueryResult[T <: RowList](rowList: T
                                     , queryAttributes: QueryAttributes
                                     , queryResultStatus: QueryResultStatus
                                     , exception: Option[Throwable] = None
                                     , pagination: Option[JValue] = None
                                    ) {

  def isFailure: Boolean = {
    queryResultStatus == QueryResultStatus.FAILURE
  }

  def requireSuccess(errMessage: String): Unit = {
    if(isFailure) {
      if(exception.isDefined) {
        throw new RuntimeException(s"$errMessage : ${exception.get.getMessage}", exception.get)
      } else {
        throw new RuntimeException(errMessage)
      }
    }
  }
}

object QueryResultStatus extends Enumeration {
  type QueryResultStatus = Value
  val SUCCESS, FAILURE = Value
}
