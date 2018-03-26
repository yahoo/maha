// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.curators

import com.yahoo.maha.core._
import com.yahoo.maha.core.bucketing.BucketParams
import com.yahoo.maha.core.query.{DerivedRowList, Row, RowList}
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import com.yahoo.maha.service.{MahaService, RequestResult}
import grizzled.slf4j.Logging

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object TimeShift {
  val name: String = "timeshift"
  val PREV_STRING: String = " Prev"
  val PCT_CHANGE_STRING: String = " Pct Change"
}

class TimeShift extends Curator with Logging {

  override val name: String = TimeShift.name
  override val level: Int = 1
  override val priority: Int = 0

  private[this] def getRequestModelForPreviousWindow(registryName: String,
                                                     bucketParams: BucketParams,
                                                     reportingRequest: ReportingRequest,
                                                     mahaService: MahaService,
                                                     mahaRequestLogHelper: MahaRequestLogHelper,
                                                     dimensionAndItsValues: List[(String, Set[String])]) : Try[RequestModelResult] = {

    val updatedReportingRequest: ReportingRequest = reportingRequest.dayFilter match {
      case BetweenFilter(field, from, to) => {
        val fromDateTime = DailyGrain.fromFormattedString(from)
        val betweenDays = DailyGrain.getDaysBetween(from , to)

        val fromForPreviousWindow: String = DailyGrain.toFormattedString(fromDateTime.minusDays(betweenDays).minusDays(1))
        val toForPreviousWindow: String = DailyGrain.toFormattedString(fromDateTime.minusDays(1))
        val previousWindow: BetweenFilter = BetweenFilter(field, fromForPreviousWindow, toForPreviousWindow)
        reportingRequest.copy(dayFilter = previousWindow)
      }
      case _ => reportingRequest
    }

    val newFilters: IndexedSeq[Filter] = updatedReportingRequest.filterExpressions ++ dimensionAndItsValues.map { e =>
      if(e._2.size == 1) {
        EqualityFilter(e._1, e._2.head)
      } else {
        InFilter(e._1, e._2.toList)
      }
    }.toIndexedSeq

    val filterUpdatedReportingRequest: ReportingRequest = updatedReportingRequest.copy(filterExpressions = newFilters)

    val requestModelResultTry: Try[RequestModelResult] = mahaService.generateRequestModel(registryName, filterUpdatedReportingRequest, bucketParams , mahaRequestLogHelper)
    requestModelResultTry
  }

  override def process(registryName: String,
                       bucketParams: BucketParams,
                       reportingRequest: ReportingRequest,
                       mahaService: MahaService,
                       mahaRequestLogHelper: MahaRequestLogHelper,
                       curatorResultMapAtCurrentLevel: Map[String, (Try[RequestModelResult], Try[RequestResult])],
                       curatorResultMapAcrossAllLevel: Map[Int, (Try[RequestModelResult], Try[RequestResult])]): (Try[RequestModelResult], Try[RequestResult]) = {

    require(curatorResultMapAtCurrentLevel.contains(DefaultCurator.name) &&
      curatorResultMapAtCurrentLevel(DefaultCurator.name)._1.isSuccess &&
      curatorResultMapAtCurrentLevel(DefaultCurator.name)._1.get.model.bestCandidates.isDefined &&
      curatorResultMapAtCurrentLevel(DefaultCurator.name)._2.isSuccess)

    val defaultCuratorRequestModel = curatorResultMapAtCurrentLevel(DefaultCurator.name)._1.get.model
    val defaultCuratorRequestResult = curatorResultMapAtCurrentLevel(DefaultCurator.name)._2.get
    val defaultCuratorRowList: RowList = defaultCuratorRequestResult.rowList

    val dimensionKeySet: Set[String] = defaultCuratorRequestModel.bestCandidates.get.publicFact.dimCols.map(_.alias)
      .intersect(defaultCuratorRequestModel.reportingRequest.selectFields.map(_.field).toSet)

    val dimensionAndItsValuesMap: mutable.Map[String, mutable.Set[String]] = new mutable.HashMap[String, mutable.Set[String]]()

    defaultCuratorRowList.map { defaultRow => {
      dimensionKeySet.map { dim => {
        dimensionAndItsValuesMap.put(dim,dimensionAndItsValuesMap.getOrElse(dim, new mutable.HashSet[String]()) += defaultRow.getValue(dim).toString)
          }
        }
      }
    }

    val timeShiftCuratorRequestModelResultTry: Try[RequestModelResult] =
      getRequestModelForPreviousWindow(registryName,
        bucketParams,
        reportingRequest,
        mahaService,
        mahaRequestLogHelper,
        dimensionAndItsValuesMap.map(e => (e._1, e._2.toSet)).toList)

    require(timeShiftCuratorRequestModelResultTry.isSuccess)

    val timeShiftCuratorRequestResultTry = mahaService.processRequestModel(registryName, timeShiftCuratorRequestModelResultTry.get.model, mahaRequestLogHelper)
    require(timeShiftCuratorRequestResultTry.isSuccess)

    val timeShiftCuratorRowList: RowList = timeShiftCuratorRequestResultTry.get.rowList

    val injectableColumns: collection.mutable.ArrayBuffer[ColumnInfo] = new collection.mutable.ArrayBuffer[ColumnInfo]()
    defaultCuratorRequestModel.bestCandidates.get.factColAliases
      .foreach { colAlias =>
        injectableColumns += FactColumnInfo(colAlias + TimeShift.PREV_STRING)
        injectableColumns += FactColumnInfo(colAlias + TimeShift.PCT_CHANGE_STRING)
      }

    val columns: IndexedSeq[ColumnInfo] = defaultCuratorRequestModel.requestCols ++ injectableColumns
    val aliasMap : Map[String, Int] = columns.map(_.alias).zipWithIndex.toMap
    val primaryKeyToRowMap = new collection.mutable.HashMap[String, Row]

    timeShiftCuratorRowList.foreach(timeShiftRow => {
      val primaryKey = dimensionKeySet.map(alias => alias + timeShiftRow.getValue(alias)).mkString
      primaryKeyToRowMap.put(primaryKey, timeShiftRow)
    })

    val derivedRowList: DerivedRowList = new DerivedRowList(columns)

    defaultCuratorRowList.foreach(defaultRow => {
      val primaryKey: String = dimensionKeySet.map(alias => alias + defaultRow.getValue(alias)).mkString
      val timeShiftRowOption: Option[Row] = primaryKeyToRowMap.get(primaryKey)
      val row: Row = new Row(aliasMap, ArrayBuffer.fill[Any](aliasMap.size)(null))
      aliasMap.foreach {
        case(alias, pos) => {
          if(alias.contains(TimeShift.PREV_STRING)) {
            val originalAlias: String = alias.substring(0, alias.indexOf(TimeShift.PREV_STRING))
            val value = if(timeShiftRowOption.isDefined) timeShiftRowOption.get.getValue(originalAlias) else 0
            row.addValue(pos, value)

          } else if(alias.contains(TimeShift.PCT_CHANGE_STRING)) {
            val originalAlias: String = alias.substring(0, alias.indexOf(TimeShift.PCT_CHANGE_STRING))
            val prevValue = if(timeShiftRowOption.isDefined) timeShiftRowOption.get.getValue(originalAlias) else 0
            val currentValue = defaultRow.getValue(originalAlias)

            if(prevValue.isInstanceOf[Number] && currentValue.isInstanceOf[Number]) {
              val prevValueDouble = prevValue.asInstanceOf[Number].doubleValue()
              val currentValueDouble = currentValue.asInstanceOf[Number].doubleValue()
              val pctChange = if (prevValueDouble == 0) BigDecimal(100) else BigDecimal(((currentValueDouble - prevValueDouble)/prevValueDouble) * 100)
              val pctChangeRounded = pctChange
                .setScale(2, BigDecimal.RoundingMode.HALF_UP)
                .toDouble
              row.addValue(pos, pctChangeRounded)
            }

          } else {
            row.addValue(pos, defaultRow.getValue(alias))
          }
        }
      }
      derivedRowList.addRow(row)

    })

    (timeShiftCuratorRequestModelResultTry,
      Try(RequestResult(derivedRowList,
        timeShiftCuratorRequestResultTry.get.queryAttributes,
        timeShiftCuratorRequestResultTry.get.totalRowsOption)
      )
    )
  }
}
