// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.curators

import com.yahoo.maha.core._
import com.yahoo.maha.core.bucketing.BucketParams
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.request._
import com.yahoo.maha.parrequest2.GeneralError
import com.yahoo.maha.parrequest2.future.{ParFunction, ParRequest}
import com.yahoo.maha.service.error.{MahaServiceBadRequestException, MahaServiceExecutionException}
import com.yahoo.maha.service.utils.CuratorMahaRequestLogBuilder
import com.yahoo.maha.service.{CuratorInjector, MahaRequestContext, MahaService, RequestResult}
import grizzled.slf4j.Logging
import org.json4s.{DefaultFormats, JValue}
import org.json4s.scalaz.JsonScalaz

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scalaz.{NonEmptyList, Validation}

object TimeShiftConfig extends Logging {
  implicit val formats: DefaultFormats.type = DefaultFormats

  def parse(curatorJsonConfig: CuratorJsonConfig) : JsonScalaz.Result[TimeShiftConfig] = {
    val config: JValue = curatorJsonConfig.json

    val sortByResult: JsonScalaz.Result[Option[SortBy]] = fieldExtended[Option[SortBy]]("sortBy")(config)
    val daysOffsetResult: JsonScalaz.Result[Option[Int]] = fieldExtended[Option[Int]]("daysOffset")(config)

    import _root_.scalaz.syntax.applicative._
    (sortByResult |@| daysOffsetResult)((a, b) => TimeShiftConfig(a, b))
  }

  def from(curatorConfig: CuratorConfig): Option[TimeShiftConfig] = {
    curatorConfig match {
      case c:TimeShiftConfig =>
        Option(c)
      case a =>
        error(s"Cannot convert ${a.getClass.getSimpleName} to TimeShiftConfig")
        None
    }
  }
}

case class TimeShiftConfig(sortBy: Option[SortBy], daysOffset: Option[Int]) extends CuratorConfig

object TimeShiftCurator {
  val name: String = "timeshift"
  val PREV_STRING: String = " Prev"
  val PCT_CHANGE_STRING: String = " Pct Change"
  val descOrdering = Ordering.fromLessThan((a: Double, b:Double) => a > b)
  val ascOrdering = Ordering.fromLessThan((a: Double, b:Double) => a < b)
}

class TimeShiftCurator (override val requestModelValidator: CuratorRequestModelValidator = NoopCuratorRequestModelValidator) extends Curator with Logging {

  override val name: String = TimeShiftCurator.name
  override val level: Int = 1
  override val priority: Int = 0
  override val isSingleton: Boolean = true
  override def requiresDefaultCurator: Boolean = false

  override def parseConfig(config: CuratorJsonConfig): Validation[NonEmptyList[JsonScalaz.Error], CuratorConfig] = {
    val timeshiftConfigTry : JsonScalaz.Result[TimeShiftConfig] = TimeShiftConfig.parse(config)
    Validation
      .fromTryCatchNonFatal{
        require(timeshiftConfigTry.isSuccess, "Must succeed in creating a timeshiftConfig " + timeshiftConfigTry)
        timeshiftConfigTry.toOption.get}
      .leftMap[JsonScalaz.Error](t => JsonScalaz.UncategorizedError("parseTimeShiftConfigValidation", t.getMessage, List.empty)).toValidationNel
  }
  private[this] def getRequestModelForPreviousWindow(registryName: String,
                                                     bucketParams: BucketParams,
                                                     reportingRequest: ReportingRequest,
                                                     mahaService: MahaService,
                                                     mahaRequestLogBuilder: CuratorMahaRequestLogBuilder,
                                                     dimensionAndItsValues: List[(String, Set[String])],
                                                     config: Option[TimeShiftConfig]
                                                    ) : Try[RequestModelResult] = {

    val updatedReportingRequest: ReportingRequest = reportingRequest.dayFilter match {
      case BetweenFilter(field, from, to) => {
        val fromDateTime = DailyGrain.fromFormattedString(from)
        val betweenDays = DailyGrain.getDaysBetween(from , to)
        val offset = 1 + config.flatMap(_.daysOffset).getOrElse(0)

        val fromForPreviousWindow: String = DailyGrain.toFormattedString(fromDateTime.minusDays(betweenDays).minusDays(offset))
        val toForPreviousWindow: String = DailyGrain.toFormattedString(fromDateTime.minusDays(offset))
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

    if(reportingRequest.isDebugEnabled) {
      info(s"previous period day filter : ${filterUpdatedReportingRequest.dayFilter}")
      info(s"previous period filter expressions : ${filterUpdatedReportingRequest.filterExpressions}")
    }
    val requestModelResultTry: Try[RequestModelResult] = mahaService
      .generateRequestModel(registryName, filterUpdatedReportingRequest, bucketParams)
    requestModelResultTry
  }

  override def process(resultMap: Map[String, Either[CuratorError, IndexedSeq[ParRequest[CuratorResult]]]]
                       , mahaRequestContext: MahaRequestContext
                       , mahaService: MahaService
                       , mahaRequestLogBuilder: CuratorMahaRequestLogBuilder
                       , curatorConfig: CuratorConfig
                       , curatorInjector: CuratorInjector
                      ) : Either[CuratorError, IndexedSeq[ParRequest[CuratorResult]]] = {

    val parallelServiceExecutor = mahaService.getParallelServiceExecutor(mahaRequestContext)
    val parRequestLabel = "processTimeshiftCurator"

    val defaultWindowRequestModelResultTry: Try[RequestModelResult] = mahaService
      .generateRequestModel(mahaRequestContext.registryName
        , mahaRequestContext.reportingRequest, mahaRequestContext.bucketParams)
    if(defaultWindowRequestModelResultTry.isFailure) {
      val message = defaultWindowRequestModelResultTry.failed.get.getMessage
      mahaRequestLogBuilder.logFailed(message)
      withError(curatorConfig, GeneralError.from(parRequestLabel
        , message, MahaServiceBadRequestException(message, defaultWindowRequestModelResultTry.failed.toOption)))
    } else {
      try {
        val defaultWindowRequestModelResult = defaultWindowRequestModelResultTry.get
        requestModelValidator.validate(mahaRequestContext, defaultWindowRequestModelResult)

        val parRequestResult = mahaService.executeRequestModelResult(mahaRequestContext.registryName
          , defaultWindowRequestModelResult, mahaRequestLogBuilder)

        val defaultWindowRequestModel = defaultWindowRequestModelResult.model
        val finalRequestResult: ParRequest[RequestResult] = parRequestResult.prodRun.map(
          "previousWindowTimeshiftCurator"
          , ParFunction.fromScala {
            defaultWindowRequestResult =>
              val defaultWindowRowListTry: Try[InMemRowList] = Try {
                defaultWindowRequestResult.queryPipelineResult.rowList match {
                  case inMemRowList: InMemRowList => inMemRowList
                  case rl =>
                    throw new UnsupportedOperationException(s"Unsupported row list ${Option(rl).map(_.getClass.getSimpleName)}")
                }
              }

              if(defaultWindowRowListTry.isFailure) {
                val exception = defaultWindowRowListTry.failed.get
                val message = exception.getMessage
                mahaRequestLogBuilder.logFailed(message)
                withRequestResultError(curatorConfig
                  , GeneralError.from("defaultWindowRowList"
                    , message
                  ))
              } else {
                val defaultWindowRowList = defaultWindowRowListTry.get
                val dimensionKeySet: Set[String] = defaultWindowRequestModel.bestCandidates.get.publicFact.dimCols.map(_.alias)
                  .intersect(defaultWindowRequestModel.reportingRequest.selectFields.map(_.field).toSet)

                val dimensionAndItsValuesMap: mutable.Map[String, mutable.Set[String]] = new mutable.HashMap[String, mutable.Set[String]]()

                defaultWindowRowList.map {
                  defaultRow =>
                    dimensionKeySet.map {
                      dim =>
                        dimensionAndItsValuesMap.put(dim
                          , dimensionAndItsValuesMap.getOrElse(dim, new mutable.HashSet[String]()) += defaultRow.getValue(dim).toString
                        )
                    }
                }

                val timeshiftConfig = TimeShiftConfig.from(curatorConfig)
                val previousWindowRequestModelResultTry: Try[RequestModelResult] =
                  getRequestModelForPreviousWindow(mahaRequestContext.registryName,
                    mahaRequestContext.bucketParams,
                    mahaRequestContext.reportingRequest,
                    mahaService,
                    mahaRequestLogBuilder,
                    dimensionAndItsValuesMap.map(e => (e._1, e._2.toSet)).toList,
                    timeshiftConfig
                  )

                if (previousWindowRequestModelResultTry.isFailure) {
                  val message = previousWindowRequestModelResultTry.failed.get.getMessage
                  mahaRequestLogBuilder.logFailed(message)
                  val result: Either[GeneralError, RequestResult] = withRequestResultError(curatorConfig
                    , GeneralError.from(parRequestLabel, message, new MahaServiceBadRequestException(message)))
                  result
                } else {

                  val previousWindowRequestResultEither = mahaService
                    .processRequestModel(mahaRequestContext.registryName, previousWindowRequestModelResultTry.get.model, mahaRequestLogBuilder)
                  if (previousWindowRequestResultEither.isLeft) {
                    processError(curatorConfig, parRequestLabel, previousWindowRequestResultEither, mahaRequestLogBuilder)
                  } else {

                    val previousWindowRequestResult = previousWindowRequestResultEither.right.get
                    val previousWindowRowList: InMemRowList = {
                      previousWindowRequestResult.queryPipelineResult.rowList match {
                        case inMemRowList: InMemRowList => inMemRowList
                        case rl =>
                          val message = s"Unsupported row list ${Option(rl).map(_.getClass.getSimpleName)}"
                          mahaRequestLogBuilder.logFailed(message)
                          return withError(curatorConfig
                            , GeneralError.from("previousWindowRowList"
                              , message))
                      }
                    }

                    val derivedRowList: DerivedRowList = createDerivedRowList(
                      defaultWindowRequestResult.queryPipelineResult
                      , defaultWindowRequestModel
                      , defaultWindowRowList
                      , previousWindowRowList
                      , dimensionKeySet, timeshiftConfig)

                    mahaRequestLogBuilder.logSuccess()
                    new Right(RequestResult(defaultWindowRequestResult.queryPipelineResult.copy(rowList = derivedRowList)))
                  }
                }
              }
          })
        withResult(
          parRequestLabel
          , parallelServiceExecutor
          , CuratorResult(this, curatorConfig, Option(parRequestResult.copy(prodRun = finalRequestResult)), defaultWindowRequestModelResult))
      }
      catch {
        case e: Exception =>
          mahaRequestLogBuilder.logFailed(e.getMessage)
          withError(curatorConfig, GeneralError.from(parRequestLabel
            , e.getMessage, MahaServiceBadRequestException(e.getMessage, Option(e))))

      }
    }
  }

  private[this] def processError(curatorConfig: CuratorConfig
                                 , parRequestLabel: String, resultEither: Either[GeneralError, RequestResult]
                                 , mahaRequestLogBuilder: CuratorMahaRequestLogBuilder): Either[CuratorError, RequestResult] = {
    val ge = resultEither.left.get
    val message = ge.throwableOption.map(_.getMessage).getOrElse(ge.message)
    mahaRequestLogBuilder.logFailed(message)
    withRequestResultError(curatorConfig, GeneralError.from(parRequestLabel
      , message, MahaServiceExecutionException(message, ge.throwableOption)))
  }

  private[this] def createDerivedRowList(defaultWindowResult: QueryPipelineResult
                                         , defaultWindowRequestModel: RequestModel
                                         , defaultWindowRowList: InMemRowList
                                         , previousWindowRowList: InMemRowList
                                         , dimensionKeySet: Set[String]
                                         , timeshiftConfig: Option[TimeShiftConfig]) : DerivedRowList = {

    val injectableColumns: ArrayBuffer[ColumnInfo] = new collection.mutable.ArrayBuffer[ColumnInfo]()
    defaultWindowRequestModel.bestCandidates.get.factColAliases
      .foreach { colAlias =>
        injectableColumns += FactColumnInfo(colAlias + TimeShiftCurator.PREV_STRING)
        injectableColumns += FactColumnInfo(colAlias + TimeShiftCurator.PCT_CHANGE_STRING)
      }

    val columns: IndexedSeq[ColumnInfo] = defaultWindowRequestModel.requestCols ++ injectableColumns
    val aliasMap : Map[String, Int] = columns.map(_.alias).zipWithIndex.toMap
    val primaryKeyToRowMap = new collection.mutable.HashMap[String, Row]

    previousWindowRowList.foreach(timeShiftRow => {
      val primaryKey = dimensionKeySet.map(alias => alias + timeShiftRow.getValue(alias)).mkString
      primaryKeyToRowMap.put(primaryKey, timeShiftRow)
    })

    val unsortedRows: ArrayBuffer[Row] = new ArrayBuffer[Row](defaultWindowRowList.size)

    defaultWindowRowList.foreach(defaultRow => {
      val primaryKey: String = dimensionKeySet.map(alias => alias + defaultRow.getValue(alias)).mkString
      val timeShiftRowOption: Option[Row] = primaryKeyToRowMap.get(primaryKey)
      val row: Row = new Row(aliasMap, ArrayBuffer.fill[Any](aliasMap.size)(null))
      aliasMap.foreach {
        case (alias, pos) => {
          if (alias.contains(TimeShiftCurator.PREV_STRING)) {
            val originalAlias: String = alias.substring(0, alias.indexOf(TimeShiftCurator.PREV_STRING))
            val value = if (timeShiftRowOption.isDefined) timeShiftRowOption.get.getValue(originalAlias) else 0
            row.addValue(pos, value)

          } else if (alias.contains(TimeShiftCurator.PCT_CHANGE_STRING)) {
            val originalAlias: String = alias.substring(0, alias.indexOf(TimeShiftCurator.PCT_CHANGE_STRING))
            val prevValue = if (timeShiftRowOption.isDefined) timeShiftRowOption.get.getValue(originalAlias) else 0
            val currentValue = defaultRow.getValue(originalAlias)

            if (prevValue.isInstanceOf[Number] && currentValue.isInstanceOf[Number]) {
              val prevValueDouble = prevValue.asInstanceOf[Number].doubleValue()
              val currentValueDouble = currentValue.asInstanceOf[Number].doubleValue()
              val diff = currentValueDouble - prevValueDouble
              val pctChange = if(diff == 0) BigDecimal(0) else {
                if (prevValueDouble == 0) {
                  BigDecimal(100)
                } else {
                  BigDecimal((diff / prevValueDouble) * 100)
                }
              }
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
      unsortedRows+=row

    })
    //sort by first metric's pct change
    var ordering = TimeShiftCurator.descOrdering
    val sortByAliasOption = {
      if(timeshiftConfig.isDefined && timeshiftConfig.get.sortBy.isDefined) {
        val sortBy = timeshiftConfig.get.sortBy.get
        val metricAlias = sortBy.field
        if(defaultWindowRequestModel.requestColsSet(metricAlias)) {
          sortBy.order match {
            case ASC =>
              ordering = TimeShiftCurator.ascOrdering
            case _ =>
          }
          Option(metricAlias + TimeShiftCurator.PCT_CHANGE_STRING)
        } else {
          injectableColumns.find(_.alias.contains(TimeShiftCurator.PCT_CHANGE_STRING)).map(_.alias)
        }
      } else {
        injectableColumns.find(_.alias.contains(TimeShiftCurator.PCT_CHANGE_STRING)).map(_.alias)
      }
    }
    val sortedList = {
      if(sortByAliasOption.isDefined && aliasMap.contains(sortByAliasOption.get)) {
        val sortByAlias = sortByAliasOption.get
        val pos = aliasMap(sortByAlias)
        unsortedRows.sortBy {
          row =>
            val value = row.getValue(pos)
            if(value != null && value.isInstanceOf[Double]) {
              value.asInstanceOf[Double]
            } else {
              0D
            }
        }(ordering)
      } else {
        unsortedRows
      }
    }
    new DerivedRowList(columns
      , sortedList = sortedList
      , drivingQuery = defaultWindowResult.queryChain.drivingQuery
    )
  }

}
