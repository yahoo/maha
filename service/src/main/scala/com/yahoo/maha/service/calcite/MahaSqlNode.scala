package com.yahoo.maha.service.calcite

import com.yahoo.maha.core.request.ReportingRequest

trait MahaSqlNode {
  def stringValue: String
}

case class SelectSqlNode(reportingRequest: ReportingRequest) extends MahaSqlNode {
  val stringValue = "select"
}

case class DescribeSqlNode(cube: String) extends MahaSqlNode {
  val stringValue = "describe"

}