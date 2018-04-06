// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.curators

import java.util.concurrent.Callable

import com.yahoo.maha.core._
import com.yahoo.maha.core.bucketing.BucketParams
import com.yahoo.maha.core.query.{DerivedRowList, Row, RowList}
import com.yahoo.maha.core.request.{Field, ReportingRequest}
import com.yahoo.maha.parrequest2.{GeneralError, ParCallable}
import com.yahoo.maha.parrequest2.future.ParRequest
import com.yahoo.maha.service.error.{MahaServiceBadRequestException, MahaServiceExecutionException}
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import com.yahoo.maha.service.{MahaService, RequestResult}
import grizzled.slf4j.Logging

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object DrilldownCurator {
  val name: String = "drilldown"
  val PREV_STRING: String = " Prev"
  val PCT_CHANGE_STRING: String = " Pct Change"
}

class DrilldownCurator (override val requestModelValidator: CuratorRequestModelValidator = NoopCuratorRequestModelValidator) extends Curator with Logging {

  override val name: String = DrilldownCurator.name
  override val level: Int = 1
  override val priority: Int = 0

  /**
    * Verify the input reportingRequest generates a valid requestModel.
    * If so, return this requestModel for the primary request.
    * @param registryName
    * @param bucketParams
    * @param reportingRequest
    * @param mahaService
    * @param mahaRequestLogHelper
    * @return requestModel
    */
  private def validateReportingRequest(registryName: String,
                                       bucketParams: BucketParams,
                                       reportingRequest: ReportingRequest,
                                       mahaService: MahaService,
                                       mahaRequestLogHelper: MahaRequestLogHelper): (RequestModel, IndexedSeq[Field]) = {
    val requestModelResultTry: Try[RequestModelResult] = mahaService.generateRequestModel(registryName, reportingRequest, bucketParams, mahaRequestLogHelper)
    require(requestModelResultTry.isSuccess, "Input ReportingRequest was invalid due to " + requestModelResultTry.failed.get.getMessage)
    (requestModelResultTry.get.model,
      (for(col <- requestModelResultTry.get.model.bestCandidates.get.requestCols;
          if requestModelResultTry.get.model.bestCandidates.get.factColMapping.contains(col))
            yield Field(requestModelResultTry.get.model.bestCandidates.get.factColMapping(col), None, None)).toIndexedSeq)
  }

  /**
    * Check if generated requestModel requestCols includes
    * the primary key for its own most granular requested table.
    * If this primary key exists in the request, return its alias
    * to be used in the secondary request's included columns.
    * @param requestModel
    * @return primaryKeyAlias
    */
  private def validateSelectedGranularPrimaryKey(requestModel: RequestModel): Field = {
    val bestCandidates = requestModel.bestCandidates.get
    var primaryKeyAliasSet : Set[String] = Set.empty
    for(col <- bestCandidates.facts.head._2.fact.dimCols; if col.isKey && !col.isForeignKey) {
      primaryKeyAliasSet = bestCandidates.facts.head._2.publicFact.nameToAliasColumnMap(col.name) //get the alias set of the primary key
    }

    //Check the current granular primary key against requested columns.
    //Be virtue of RM generation, only one of its aliases will be requested however multiple
    //may be defined to check against.
    val primaryAliasRequest = requestModel.requestColsSet.intersect(primaryKeyAliasSet)
    require(primaryAliasRequest != Set.empty, "Requested cols should include primary key of most granular request table.")

    Field(primaryAliasRequest.head, None, None)
  }

  /**
    * Copy the current ReportingRequest with:
    * - Requested Drilldown Dim as primary.
    * - Primary key of primary table.
    * - All metrics (facts).
    * @param reportingRequest
    * @param factFields
    * @param primaryKeyField
    */
  private def newRequestWithFactsAndRequestedDrilldown(reportingRequest: ReportingRequest,
                                                       factFields: IndexedSeq[Field],
                                                       primaryKeyField: Field): ReportingRequest = {
    val allSelectedFields : IndexedSeq[Field] = factFields ++ IndexedSeq(primaryKeyField, extractField(reportingRequest))
    reportingRequest.copy(selectFields = allSelectedFields)
  }

  /**
    * With the returned values on the drilldown, create a
    * new reporting request.
    * @param reportingRequest
    * @param drilldownDimName
    * @param inputFieldValues
    */
  def insertValuesIntoDrilldownRequest(reportingRequest: ReportingRequest,
                                       drilldownDimName: String,
                                       inputFieldValues: List[String]): Unit = {
    reportingRequest.copy(filterExpressions = reportingRequest.filterExpressions ++ IndexedSeq(InFilter(drilldownDimName, inputFieldValues)))
  }

  def extractField(reportingRequest: ReportingRequest): Field = {
    val config = reportingRequest.curatorJsonConfigMap("drilldown")
    val field = config.json.values.asInstanceOf[Map[String, String]]("field")
    Field(field, None, None)
  }

  def implementDrilldownRequestMinimization(registryName: String, //TODO: Implement parrequest and return CuratorResult
              bucketParams: BucketParams,
              reportingRequest: ReportingRequest,
              mahaService: MahaService,
              mahaRequestLogHelper: MahaRequestLogHelper): ReportingRequest = {
    val (rm, fields) : (RequestModel, IndexedSeq[Field]) = validateReportingRequest(registryName, bucketParams, reportingRequest, mahaService, mahaRequestLogHelper)
    val primaryField = validateSelectedGranularPrimaryKey(rm)
    val rr = newRequestWithFactsAndRequestedDrilldown(reportingRequest, fields, primaryField)
    rr
  }


  override def process(registryName: String,
                       bucketParams: BucketParams,
                       reportingRequest: ReportingRequest,
                       mahaService: MahaService,
                       mahaRequestLogHelper: MahaRequestLogHelper): ParRequest[CuratorResult] = {

    val registryConfig = mahaService.getMahaServiceConfig.registry.get(registryName).get
    val parallelServiceExecutor = registryConfig.parallelServiceExecutor
    val parRequestLabel = "processDrilldownCurator"

    val parRequest = parallelServiceExecutor.parRequestBuilder[CuratorResult].setLabel(parRequestLabel).
      setParCallable(ParCallable.from[Either[GeneralError, CuratorResult]](
        new Callable[Either[GeneralError, CuratorResult]](){
          override def call(): Either[GeneralError, CuratorResult] = {

            val defaultWindowRequestModelResultTry: Try[RequestModelResult] = mahaService.generateRequestModel(registryName, reportingRequest, bucketParams , mahaRequestLogHelper)
            if(defaultWindowRequestModelResultTry.isFailure) {
              val message = defaultWindowRequestModelResultTry.failed.get.getMessage
              mahaRequestLogHelper.logFailed(message)
              return GeneralError.either[CuratorResult](parRequestLabel, message, new MahaServiceBadRequestException(message, defaultWindowRequestModelResultTry.failed.toOption))
            } else {
              requestModelValidator.validate(defaultWindowRequestModelResultTry.get)
            }

            val defaultWindowRequestModel: RequestModel = defaultWindowRequestModelResultTry.get.model
            val defaultWindowRequestResultTry = mahaService.processRequestModel(registryName, defaultWindowRequestModel, mahaRequestLogHelper)
            if(defaultWindowRequestResultTry.isFailure) {
              val message = defaultWindowRequestResultTry.failed.get.getMessage
              mahaRequestLogHelper.logFailed(message)
              return GeneralError.either[CuratorResult](parRequestLabel, message, new MahaServiceExecutionException(message))
            }

            val defaultWindowRowList: RowList = defaultWindowRequestResultTry.get.rowList

            val dimensionKeySet: Set[String] = defaultWindowRequestModel.bestCandidates.get.publicFact.dimCols.map(_.alias)
              .intersect(defaultWindowRequestModel.reportingRequest.selectFields.map(_.field).toSet)

            val dimensionAndItsValuesMap: mutable.Map[String, mutable.Set[String]] = new mutable.HashMap[String, mutable.Set[String]]()

            defaultWindowRowList.map { defaultRow => {
              dimensionKeySet.map { dim => {
                dimensionAndItsValuesMap.put(dim,dimensionAndItsValuesMap.getOrElse(dim, new mutable.HashSet[String]()) += defaultRow.getValue(dim).toString)
              }
              }
            }
            }

            /*val previousWindowRequestModelResultTry: Try[RequestModelResult] =
              getRequestModelForPreviousWindow(registryName,
                bucketParams,
                reportingRequest,
                mahaService,
                mahaRequestLogHelper,
                dimensionAndItsValuesMap.map(e => (e._1, e._2.toSet)).toList)

            if(previousWindowRequestModelResultTry.isFailure) {
              val message = previousWindowRequestModelResultTry.failed.get.getMessage
              mahaRequestLogHelper.logFailed(message)
              return GeneralError.either[CuratorResult](parRequestLabel, message, new MahaServiceBadRequestException(message))
            }

            val previousWindowRequestResultTry = mahaService.processRequestModel(registryName, previousWindowRequestModelResultTry.get.model, mahaRequestLogHelper)
            if(previousWindowRequestResultTry.isFailure) {
              val message = previousWindowRequestResultTry.failed.get.getMessage
              mahaRequestLogHelper.logFailed(message)
              return GeneralError.either[CuratorResult](parRequestLabel, message, new MahaServiceExecutionException(message))
            }

            val previousWindowRowList: RowList = previousWindowRequestResultTry.get.rowList

            val derivedRowList: DerivedRowList = createDerivedRowList(defaultWindowRequestModel, defaultWindowRowList, previousWindowRowList, dimensionKeySet)

            return new Right[GeneralError, CuratorResult](CuratorResult(Try(RequestResult(derivedRowList, defaultWindowRequestResultTry.get.queryAttributes, defaultWindowRequestResultTry.get.totalRowsOption)), defaultWindowRequestModelResultTry.get))*/
            return new Right[GeneralError, CuratorResult](null)
          }
        }
      )).build()
    parRequest


  }

  private[this] def createDerivedRowList(defaultWindowRequestModel: RequestModel,
                                         defaultWindowRowList: RowList,
                                         previousWindowRowList: RowList,
                                         dimensionKeySet: Set[String]) : DerivedRowList = {

    val injectableColumns: ArrayBuffer[ColumnInfo] = new collection.mutable.ArrayBuffer[ColumnInfo]()
    defaultWindowRequestModel.bestCandidates.get.factColAliases
      .foreach { colAlias =>
        injectableColumns += FactColumnInfo(colAlias + DrilldownCurator.PREV_STRING)
        injectableColumns += FactColumnInfo(colAlias + DrilldownCurator.PCT_CHANGE_STRING)
      }

    val columns: IndexedSeq[ColumnInfo] = defaultWindowRequestModel.requestCols ++ injectableColumns
    val aliasMap : Map[String, Int] = columns.map(_.alias).zipWithIndex.toMap
    val primaryKeyToRowMap = new collection.mutable.HashMap[String, Row]

    previousWindowRowList.foreach(drillDownRow => {
      val primaryKey = dimensionKeySet.map(alias => alias + drillDownRow.getValue(alias)).mkString
      primaryKeyToRowMap.put(primaryKey, drillDownRow)
    })

    val derivedRowList: DerivedRowList = new DerivedRowList(columns)

    defaultWindowRowList.foreach(defaultRow => {
      val primaryKey: String = dimensionKeySet.map(alias => alias + defaultRow.getValue(alias)).mkString
      val drillDownRowOption: Option[Row] = primaryKeyToRowMap.get(primaryKey)
      val row: Row = new Row(aliasMap, ArrayBuffer.fill[Any](aliasMap.size)(null))
      aliasMap.foreach {
        case (alias, pos) => {
          if (alias.contains(DrilldownCurator.PREV_STRING)) {
            val originalAlias: String = alias.substring(0, alias.indexOf(DrilldownCurator.PREV_STRING))
            val value = if (drillDownRowOption.isDefined) drillDownRowOption.get.getValue(originalAlias) else 0
            row.addValue(pos, value)

          } else if (alias.contains(DrilldownCurator.PCT_CHANGE_STRING)) {
            val originalAlias: String = alias.substring(0, alias.indexOf(DrilldownCurator.PCT_CHANGE_STRING))
            val prevValue = if (drillDownRowOption.isDefined) drillDownRowOption.get.getValue(originalAlias) else 0
            val currentValue = defaultRow.getValue(originalAlias)

            if (prevValue.isInstanceOf[Number] && currentValue.isInstanceOf[Number]) {
              val prevValueDouble = prevValue.asInstanceOf[Number].doubleValue()
              val currentValueDouble = currentValue.asInstanceOf[Number].doubleValue()
              val pctChange = if (prevValueDouble == 0) BigDecimal(100) else BigDecimal(((currentValueDouble - prevValueDouble) / prevValueDouble) * 100)
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
    derivedRowList
  }

}
