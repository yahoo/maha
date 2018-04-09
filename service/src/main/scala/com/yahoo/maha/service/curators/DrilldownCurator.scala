// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.curators

import java.util.concurrent.Callable

import com.yahoo.maha.core._
import com.yahoo.maha.core.bucketing.BucketParams
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.request.{Field, ReportingRequest}
import com.yahoo.maha.parrequest2.{GeneralError, ParCallable}
import com.yahoo.maha.parrequest2.future.ParRequest
import com.yahoo.maha.service.error.{MahaServiceBadRequestException, MahaServiceExecutionException}
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import com.yahoo.maha.service.{MahaRequestContext, MahaService, RequestResult}
import grizzled.slf4j.Logging

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object DrilldownCurator {
  val name: String = "drilldown"
}

class DrilldownCurator (override val requestModelValidator: CuratorRequestModelValidator = NoopCuratorRequestModelValidator) extends Curator with Logging {

  override val name: String = DrilldownCurator.name
  override val level: Int = 1
  override val priority: Int = 0
  override val isSingleton: Boolean = true
  private val INCLUDE_ROW_COUNT_DRILLDOWN = false

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
    require(DrilldownConfig.validCubes.contains(reportingRequest.cube), "Cannot drillDown using given source cube " + reportingRequest.cube)

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
  private def mostGranularPrimaryKey(requestModel: RequestModel): Field = {
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
    * - Requested DrillDown Dim as primary.
    * - Primary key of primary table.
    * - All metrics (facts).
    * @param reportingRequest
    * @param factFields
    * @param primaryKeyField
    */
  private def drilldownReportingRequest(reportingRequest: ReportingRequest,
                                                       factFields: IndexedSeq[Field],
                                                       primaryKeyField: Field): ReportingRequest = {
    val drilldownConfig: DrilldownConfig = DrilldownConfig.parse(reportingRequest)
    val allSelectedFields : IndexedSeq[Field] = (IndexedSeq(DrilldownConfig.parse(reportingRequest).dimension) ++ factFields ++ IndexedSeq(primaryKeyField)).distinct
    reportingRequest.copy(cube = drilldownConfig.cube
      , selectFields = allSelectedFields
      , sortBy = drilldownConfig.ordering
      , rowsPerPage = drilldownConfig.maxRows.toInt
    , filterExpressions = reportingRequest.filterExpressions
    , includeRowCount = INCLUDE_ROW_COUNT_DRILLDOWN)
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
                                       inputFieldValues: List[String]): ReportingRequest = {
    reportingRequest.copy(filterExpressions = (reportingRequest.filterExpressions ++ IndexedSeq(InFilter(drilldownDimName, inputFieldValues))).distinct
    , includeRowCount = INCLUDE_ROW_COUNT_DRILLDOWN)
  }

  def implementDrilldownRequestMinimization(registryName: String, //TODO: Implement parrequest and return CuratorResult
              bucketParams: BucketParams,
              reportingRequest: ReportingRequest,
              mahaService: MahaService,
              mahaRequestLogHelper: MahaRequestLogHelper): ReportingRequest = {

    //Validate reportingRequest is valid,
    //Grab its primary key, and
    //Grab the most granular table's primary key.
    val (rm, fields) : (RequestModel, IndexedSeq[Field]) = validateReportingRequest(registryName, bucketParams, reportingRequest, mahaService, mahaRequestLogHelper)
    val primaryField = mostGranularPrimaryKey(rm)

    //Generate a new reporting request with
    //drillDown specs.
    val rr = drilldownReportingRequest(reportingRequest, fields, primaryField)
    rr

    //Generate a final reportingRequest with
    //drillDown col's values from first request.
  }

  def verifyRequestModelResult(requestModelResultTry: Try[RequestModelResult],
                               mahaRequestLogHelper: MahaRequestLogHelper,
                               mahaRequestContext: MahaRequestContext,
                               mahaService: MahaService,
                               parRequestLabel: String) : Either[GeneralError, CuratorResult] = {
    if(requestModelResultTry.isFailure) {
      val message = requestModelResultTry.failed.get.getMessage
      mahaRequestLogHelper.logFailed(message)
      GeneralError.either[CuratorResult](parRequestLabel, message, new MahaServiceBadRequestException(message, requestModelResultTry.failed.toOption))
    } else {
      requestModelValidator.validate(mahaRequestContext, requestModelResultTry.get)
      val requestResultTry = mahaService.processRequestModel(mahaRequestContext.registryName
        , requestModelResultTry.get.model, mahaRequestLogHelper)
      new Right[GeneralError, CuratorResult](CuratorResult(requestResultTry, requestModelResultTry.get))
    }
  }


  override def process(mahaRequestContext: MahaRequestContext
                       , mahaService: MahaService
                       , mahaRequestLogHelper: MahaRequestLogHelper): ParRequest[CuratorResult] = {

    val registryConfig = mahaService.getMahaServiceConfig.registry.get(mahaRequestContext.registryName).get
    val parallelServiceExecutor = registryConfig.parallelServiceExecutor
    val parRequestLabel = "processDrillDownCurator"

    val parRequest = parallelServiceExecutor.parRequestBuilder[CuratorResult].setLabel(parRequestLabel).
      setParCallable(ParCallable.from[Either[GeneralError, CuratorResult]](
        new Callable[Either[GeneralError, CuratorResult]](){
          override def call(): Either[GeneralError, CuratorResult] = {

            val requestModelResultTry: Try[RequestModelResult] = mahaService.generateRequestModel(
              mahaRequestContext.registryName, mahaRequestContext.reportingRequest, mahaRequestContext.bucketParams
              , mahaRequestLogHelper)

            verifyRequestModelResult(requestModelResultTry, mahaRequestLogHelper, mahaRequestContext, mahaService, parRequestLabel)
          }
        }
      )).build()
    val firstRequest : ParRequest[CuratorResult] = parRequest

    val parRequest2 : ParRequest[CuratorResult] = parallelServiceExecutor.parRequestBuilder[CuratorResult].setLabel(parRequestLabel).
      setParCallable(ParCallable.from[Either[GeneralError, CuratorResult]](
        new Callable[Either[GeneralError, CuratorResult]](){
          override def call(): Either[GeneralError, CuratorResult] = {

            require(firstRequest.get.isRight, "First par request failed, cannot build the second! " + firstRequest.get.left.get.message)

            val drillDownConfig = DrilldownConfig.parse(mahaRequestContext.reportingRequest)

            //pick the drilldown col values to use as filters in the second request.
            val rowList = firstRequest.get.right.get.requestResultTry.get.queryPipelineResult.rowList
            var values : Set[String] = Set.empty
            rowList.foreach{
              row => values = values ++ List(row.cols(row.aliasMap(drillDownConfig.dimension.field)).toString)
            }

            val newReportingRequest = implementDrilldownRequestMinimization(mahaRequestContext.registryName, mahaRequestContext.bucketParams, mahaRequestContext.reportingRequest, mahaService, mahaRequestLogHelper)

            val newRequestWithInsertedFilter = insertValuesIntoDrilldownRequest(newReportingRequest, drillDownConfig.dimension.field, values.toList)

            val requestModelResultTry: Try[RequestModelResult] = mahaService.generateRequestModel(
              mahaRequestContext.registryName, newRequestWithInsertedFilter, mahaRequestContext.bucketParams
              , mahaRequestLogHelper)

            verifyRequestModelResult(requestModelResultTry, mahaRequestLogHelper, mahaRequestContext, mahaService, parRequestLabel)
          }
        }
      )).build()

    parRequest2
  }

  private def createDerivedRowList(originalRowList : RowList,
                                   secondRowList : RowList) : RowList = {
    secondRowList.foreach(
      row => originalRowList.addRow(row)
    )
    originalRowList
  }

}
