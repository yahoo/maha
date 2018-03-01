// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import com.yahoo.maha.core.fact.DruidDerFactCol
import com.yahoo.maha.core.query.RowData
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.Try

sealed trait PostResultFunction {

  def expression: String

  def sourceColumns: Set[String]

  def resultApply(rowData: RowData): Unit

  def validate()
}

abstract class BasePostResultFunction (implicit cc: ColumnContext) extends PostResultFunction {
  override def validate(): Unit = {
    sourceColumns.foreach {
      sc => cc.render(sc, Map.empty)
    }
  }
}

sealed trait DruidPostResultFunction extends BasePostResultFunction with PostResultFunction



object DruidPostResultFunction {

  case class POST_RESULT_DECODE(expression: String,
                                args: String*)(implicit cc: ColumnContext) extends DruidPostResultFunction {

    if (args.length < 2) throw new IllegalArgumentException("Usage: DECODE( fieldName , search , result [, search , result]... [, default] )")

    val colName = expression.replaceAll("[}{]","")

    val (sourceColumns:Set[String], map: Map[Any,Any]) = {
      val mutableSet: collection.mutable.Set[String] = collection.mutable.Set[String]()
      mutableSet+=colName

      val constructMap: Map[Any,Any] = {
        args.grouped(2).filter(_.size == 2).map(seq => {

          if (seq(1).toString contains "{") {
            val sourceColName = seq(1).toString.replaceAll("[}{]", "")
            mutableSet+=sourceColName
            val column = cc.getColumnByName(sourceColName)


            require(column.isDefined, s"Failed to find column $sourceColName")
            (seq.head, column)
          }
          else {
            (seq.head, seq(1))
          }
        }).toMap
      }

      (mutableSet.toSet , constructMap)
    }


    override def resultApply(rowData: RowData): Unit = {
      val valueToCheck = rowData.get(colName)
      if (valueToCheck.isDefined && map.contains(valueToCheck.get)) {

        val mapValue = map(valueToCheck.get)

        if(mapValue.isInstanceOf[Some[Column]]) {

          val column = mapValue.asInstanceOf[Some[Column]].get


          val newValue = rowData.get(column.name)
          if (newValue.isDefined) {
            rowData.setValue(newValue.get)
          } else {
            //see if there is default value defined, and set default value, otherwise error
            column.dataType match {
              case i: IntType if i.default.isDefined => rowData.setValue(i.default.get)
              case s: StrType if s.default.isDefined => rowData.setValue(s.default.get)
              case d: DecType if d.default.isDefined => rowData.setValue(d.default.get)
              case _ => require(newValue.isDefined, s"Failed to get source column value : ${column.name}")

            }
          }
        }
        else {
          rowData.setValue(mapValue)
        }
      }
    }

  }

  case class START_OF_THE_WEEK(expression: String)
                              (implicit cc: ColumnContext) extends DruidPostResultFunction {

    val yearAndWeekOfTheYearFormat: String = "YYYY-w"
    private[this] val datetimeFormat = DateTimeFormat.forPattern(yearAndWeekOfTheYearFormat)

    val colName = expression.replaceAll("[}{]","")

    val sourceColumns = Set(colName)

    def toFormattedString(value: String) : String = datetimeFormat.print(DailyGrain.fromFormattedString(value))

    override def resultApply(rowData: RowData): Unit = {
      val valueToCheck = rowData.get(rowData.columnAlias)
      if (valueToCheck.isDefined) {
        val dt: Try[DateTime] = Try(datetimeFormat.parseDateTime(valueToCheck.get))
        if(dt.isSuccess)
          rowData.setValue(DailyGrain.toFormattedString(dt.get))
      }
    }

  }

  case class START_OF_THE_MONTH(expression: String)
                              (implicit cc: ColumnContext) extends DruidPostResultFunction {

    val colName = expression.replaceAll("[}{]","")

    val sourceColumns = Set(colName)

    val startOfTheMonthFormat: String = "yyyy-MM-01"

    override def resultApply(rowData: RowData): Unit = {
    }

  }

}
