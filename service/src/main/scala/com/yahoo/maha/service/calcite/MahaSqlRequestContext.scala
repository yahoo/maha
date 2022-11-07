package com.yahoo.maha.service.calcite

import com.yahoo.maha.core.{DailyGrain, HourlyGrain}
import org.joda.time.{DateTime, DateTimeZone}

case class MahaSqlRequestContext() {
  var fromDate,fromHour : String = _
  var toDate : String = DailyGrain.toFormattedString(DateTime.now(DateTimeZone.UTC))
  var toHour : String = HourlyGrain.toFormattedString(DateTime.now(DateTimeZone.UTC))
  val queryAliasToColumnNameMap = scala.collection.mutable.Map[String, String]()
}
