package com.yahoo.maha.core.model.meta

import com.yahoo.maha.core.request.ReportingRequest

/**
  *
  * - cube
  * - reportingRequest
  * - requestedDaysWindow
  * - requestedDaysLookBack
  * - startIndex
  * - maxRows
  */
case class ModelReportingRequestMeta (
                                     cube: String
                                     , reportingRequest: ReportingRequest
                                     , requestedDaysWindow: Int
                                     , requestedDaysLookBack: Int
                                     , startIndex: Int
                                     , maxRows: Int
                                     ) {
  override def toString: String =
    s"""${this.getClass.getSimpleName}:
       cube: $cube
       reportingRequest: $reportingRequest
       requestedDaysWindow: $requestedDaysWindow
       requestedDaysLookBack: $requestedDaysLookBack
       startIndex: $startIndex
       maxRows: $maxRows""".stripMargin
}
