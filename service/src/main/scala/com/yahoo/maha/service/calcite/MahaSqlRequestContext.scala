package com.yahoo.maha.service.calcite

import com.yahoo.maha.core.{DailyGrain, HourlyGrain}
import org.joda.time.{DateTime, DateTimeZone}

case class MahaSqlRequestContext() {

  private var fromDate,fromHour : String = _
  private var toDate : String = DailyGrain.toFormattedString(DateTime.now(DateTimeZone.UTC))
  private var toHour : String = HourlyGrain.toFormattedString(DateTime.now(DateTimeZone.UTC))
  private val queryAliasToColumnNameMap = scala.collection.mutable.Map[String, String]()

  def getFromDate = fromDate
  def getFromHour = fromHour
  def getToDate = toDate
  def getToHour = toHour

  def setFromDateHour(date: String, hour: String) = {
    fromDate = date
    fromHour = hour
  }

  def setToDateHour(date: String, hour: String) = {
    toDate = date
    toHour = hour
  }

  def setAliasMap(alias: String, columnName: String): Unit = queryAliasToColumnNameMap += (alias -> columnName)

  def getAliasMap(alias: String) = queryAliasToColumnNameMap.getOrElse(alias,alias)

  def checkValueAliasMap(columnName: String) = queryAliasToColumnNameMap.exists(_._2 == columnName)
}
