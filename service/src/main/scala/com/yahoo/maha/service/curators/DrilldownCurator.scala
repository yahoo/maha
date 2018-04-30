// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.curators

import com.yahoo.maha.core._
import com.yahoo.maha.core.bucketing.{BucketParams, BucketSelected, BucketSelector}
import com.yahoo.maha.core.registry.Registry
import com.yahoo.maha.core.request.{CuratorJsonConfig, Field, ReportingRequest}
import com.yahoo.maha.parrequest2.GeneralError
import com.yahoo.maha.parrequest2.future.{ParFunction, ParRequest}
import com.yahoo.maha.service.error.MahaServiceBadRequestException
import com.yahoo.maha.service.utils.CuratorMahaRequestLogBuilder
import com.yahoo.maha.service.{CuratorInjector, MahaRequestContext, MahaService, MahaServiceConfig, RegistryConfig}
import grizzled.slf4j.Logging
import org.json4s.scalaz.JsonScalaz

import scala.util.Try
import scalaz.{NonEmptyList, Validation}

/**
  * DrilldownCurator : Given an input Request with a Drilldown Json config,
  * create a new Request using the input Drilldown primary dimension.
  * @author ryanwagner
  */

/**
  *
  */
object DrilldownCurator {
  val name: String = "drilldown"
}

/**
  *
  * @param requestModelValidator: Used to validate the input RequestModel
  */
class DrilldownCurator (override val requestModelValidator: CuratorRequestModelValidator = NoopCuratorRequestModelValidator) extends Curator with Logging {

  override val name: String = DrilldownCurator.name
  override val level: Int = 10
  override val priority: Int = 0
  override val isSingleton: Boolean = false
  private val INCLUDE_ROW_COUNT_DRILLDOWN : Boolean = false
  override val requiresDefaultCurator : Boolean = true

  override def parseConfig(config: CuratorJsonConfig): Validation[NonEmptyList[JsonScalaz.Error], CuratorConfig] = {
    val drilldownConfigTry : JsonScalaz.Result[DrilldownConfig] = DrilldownConfig.parse(config)
    Validation
      .fromTryCatchNonFatal{
        require(drilldownConfigTry.isSuccess, "Must succeed in creating a drilldownConfig " + drilldownConfigTry)
        drilldownConfigTry.toOption.get}
      .leftMap[JsonScalaz.Error](t => JsonScalaz.UncategorizedError("parseDrillDownConfigValidation", t.getMessage, List.empty)).toValidationNel
  }

  /**
    * Verify the input reportingRequest generates a valid requestModel.
    * If so, return this requestModel for the primary request.
    * @param registryName: Name of the current registry
    * @param bucketParams: Request bucketing configuration
    * @param reportingRequest: Input reporting request
    * @param mahaService: Service used to generate the request model
    * @param mahaRequestLogBuilder: For error logging
    * @return requestModel
    */
  private def validateReportingRequest(registryName: String,
                                       bucketParams: BucketParams,
                                       reportingRequest: ReportingRequest,
                                       mahaService: MahaService,
                                       mahaRequestLogBuilder: CuratorMahaRequestLogBuilder): (RequestModel, IndexedSeq[Field]) = {
    val requestModelResultTry: Try[RequestModelResult] = mahaService.generateRequestModel(registryName, reportingRequest, bucketParams, mahaRequestLogBuilder)
    require(requestModelResultTry.isSuccess, "Input ReportingRequest was invalid due to " + requestModelResultTry.failed.get.getMessage)
    (requestModelResultTry.get.model,
      (for(col <- requestModelResultTry.get.model.bestCandidates.get.requestCols
           if requestModelResultTry.get.model.bestCandidates.get.factColMapping.contains(col))
        yield Field(requestModelResultTry.get.model.bestCandidates.get.factColMapping(col), None, None)).toIndexedSeq)
  }

  /**
    * Check if generated requestModel requestCols includes
    * the primary key for its own most granular requested table.
    * If this primary key exists in the request, return its alias
    * to be used in the secondary request's included columns.
    * @param requestModel: Request model with tree of granular tables
    * @return primaryKeyAlias
    */
  protected def mostGranularPrimaryKeyAlias(registry: Registry, requestModel: RequestModel): Option[String] = {
    if (requestModel.dimensionsCandidates.nonEmpty) {
      val alias = requestModel.dimensionsCandidates.last.dim.primaryKeyByAlias
      require(requestModel.requestColsSet.contains(alias), s"Primary key of most granular dim MUST be present in requested cols to join against : $alias")
      Option(alias)
    } else if (requestModel.bestCandidates.nonEmpty) {
      val bestCandidates = requestModel.bestCandidates.get
      val fkAliases = bestCandidates.fkCols.flatMap(bestCandidates.dimColMapping.get)
      val dimRevisionOption = Option(bestCandidates.publicFact.dimRevision)
      val dimensions = fkAliases.flatMap(alias => registry.getDimensionByPrimaryKeyAlias(alias, dimRevisionOption))
      if (dimensions.nonEmpty) {
        Option(dimensions.maxBy(_.dimLevel.level).primaryKeyByAlias)
      } else None
    }
    else None
  }

  /**
    * Copy the current ReportingRequest with:
    * - Requested DrillDown Dim as primary.
    * - Primary key of primary table.
    * - All metrics (facts).
    * @param reportingRequest: Original reporting request to transform
    * @param factFields: All fact fields from the request model
    * @param primaryKeyField: Primary key from most granular table
    */
  private def drilldownReportingRequest(reportingRequest: ReportingRequest,
                                        factFields: IndexedSeq[Field],
                                        primaryKeyField: Field,
                                        drilldownConfig: DrilldownConfig): ReportingRequest = {
    val cube: String = if (drilldownConfig.cube.nonEmpty) drilldownConfig.cube else reportingRequest.cube
    val allSelectedFields : IndexedSeq[Field] = (IndexedSeq(drilldownConfig.dimension, primaryKeyField).filter{_!=null} ++ factFields).distinct
    reportingRequest.copy(cube = cube
      , selectFields = allSelectedFields
      , sortBy = if (drilldownConfig.ordering != IndexedSeq.empty) drilldownConfig.ordering else reportingRequest.sortBy
      , rowsPerPage = drilldownConfig.maxRows.toInt
      , filterExpressions = reportingRequest.filterExpressions
      , includeRowCount = INCLUDE_ROW_COUNT_DRILLDOWN)
  }

  /**
    * With the returned values on the drilldown, create a
    * new reporting request.
    * @param reportingRequest: ReportingRequest to add primary key filter
    * @param primaryKeyAlias: Name of primary drilldown dimension
    * @param inputFieldValues: All values found in the initial request
    */
  private def insertValuesIntoDrilldownRequest(reportingRequest: ReportingRequest,
                                               primaryKeyAlias: String,
                                               inputFieldValues: List[String]): ReportingRequest = {
    if(reportingRequest.filterExpressions.exists(_.field == primaryKeyAlias)) {
      reportingRequest.copy(includeRowCount = INCLUDE_ROW_COUNT_DRILLDOWN)
    } else {
      reportingRequest.copy(filterExpressions = reportingRequest.filterExpressions ++ IndexedSeq(InFilter(primaryKeyAlias, inputFieldValues))
        , includeRowCount = INCLUDE_ROW_COUNT_DRILLDOWN)
    }
  }

  /**
    *
    * @param registryName: Name of current reporting registry
    * @param bucketParams: Bucket configuration parameters
    * @param reportingRequest: Original reporting request to modify
    * @param mahaService: Service with registry and all initial parameters
    * @param mahaRequestLogBuilder: Error logging
    * @return Modified reporting request with drilldown
    */
  def implementDrilldownRequestMinimization(registryName: String,
                                            bucketParams: BucketParams,
                                            reportingRequest: ReportingRequest,
                                            mahaService: MahaService,
                                            mahaRequestLogBuilder: CuratorMahaRequestLogBuilder,
                                            primaryKeyAlias: String,
                                            drilldownConfig: DrilldownConfig,
                                            mahaRequestContext: MahaRequestContext): ReportingRequest = {
    val (_, fields) : (RequestModel, IndexedSeq[Field]) = validateReportingRequest(registryName, bucketParams, reportingRequest, mahaService, mahaRequestLogBuilder)
    val primaryField : Field = Field(primaryKeyAlias, None, None)

    val fields_reduced : IndexedSeq[Field] = removeInvalidFactAliases(registryName
    , mahaService.getMahaServiceConfig
    , drilldownConfig
    , fields
    , mahaRequestContext)

    val rr = drilldownReportingRequest(reportingRequest, fields_reduced, primaryField, drilldownConfig)
    rr
  }

  def removeInvalidFactAliases(registryName: String,
                               mahaServiceConfig: MahaServiceConfig,
                               drilldownConfig: DrilldownConfig,
                               factFields: IndexedSeq[Field],
                               context: MahaRequestContext): IndexedSeq[Field] = {
    if(drilldownConfig.cube.isEmpty){
      factFields
    }
    else{
      val registryConfig: RegistryConfig = mahaServiceConfig.registry(registryName)
      val selector: BucketSelector = registryConfig.bucketSelector
      val bucketSelected : BucketSelected = selector.selectBuckets(
        drilldownConfig.cube
        , context.bucketParams.copy(forceRevision = None)
      ).get
      val factFieldsReduced : IndexedSeq[Field] = factFields.filter(field =>
        registryConfig
          .registry
          .factMap(
            (drilldownConfig.cube, bucketSelected.revision)
          ).columnsByAlias.contains(field.field)
      )

      val factFieldsRemoved : IndexedSeq[Field] = factFields.filterNot(field =>
        registryConfig
          .registry
          .factMap(
            (drilldownConfig.cube, bucketSelected.revision)
          ).columnsByAlias.contains(field.field)
      )

      if(context.reportingRequest.isDebugEnabled)
        logger.info("Removed fact fields: " + factFieldsRemoved)

      factFieldsReduced
    }
  }

  override def process(resultMap: Map[String, Either[CuratorError, ParRequest[CuratorResult]]]
                       , mahaRequestContext: MahaRequestContext
                       , mahaService: MahaService
                       , mahaRequestLogBuilder: CuratorMahaRequestLogBuilder
                       , curatorConfig: CuratorConfig
                       , curatorInjector: CuratorInjector
                      ) : Either[CuratorError, ParRequest[CuratorResult]] = {

    val registryConfig = mahaService.getMahaServiceConfig.registry(mahaRequestContext.registryName)
    val parallelServiceExecutor = registryConfig.parallelServiceExecutor
    val parRequestLabel = "processDrillDownCurator"

    if(!resultMap.contains(DefaultCurator.name)) {
      withError(curatorConfig, GeneralError.from(parRequestLabel, "default curator required!"))
    } else {
      val generalErrorOrResult = resultMap(DefaultCurator.name)

      if (generalErrorOrResult.isLeft) {
        generalErrorOrResult
      } else {
        val parResult = generalErrorOrResult.right.get
        //we need to take the result of default curator and its request result to produce our request and result
        val finalResult: ParRequest[CuratorResult] = parResult.flatMap(parRequestLabel, ParFunction.fromScala {
          defaultCuratorResult =>
            //we need to use the results of default curator to generate our query and execute it in non-blocking manner
            //and produce the drill down curator result
            //so we map over the result of default curator's par request result's prod run
            //this means we define a function from ParRequest[RequestResult] to ParRequest[CuratorResult]
            val defaultParRequestResultOption = defaultCuratorResult.parRequestResultOption

            if (defaultParRequestResultOption.isEmpty) {
              parallelServiceExecutor.immediateResult(parRequestLabel,
                GeneralError.either(parRequestLabel, "no result from default curator, cannot continue")
              )
            } else {
              val defaultParRequestResult = defaultParRequestResultOption.get
              val innerResult: ParRequest[CuratorResult] = defaultParRequestResult.prodRun.map(
                parRequestLabel
                , ParFunction.fromScala {
                  defaultRequestResult =>
                    try {
                      val fieldAliasOption = mostGranularPrimaryKeyAlias(
                        registryConfig.registry, defaultCuratorResult.requestModelReference.model)
                      if (fieldAliasOption.isEmpty) {
                        withParRequestError(curatorConfig, GeneralError.from(parRequestLabel, "no primary key alias found in request", MahaServiceBadRequestException("No primary key alias found in request")))
                      } else {
                        val fieldAlias = fieldAliasOption.get
                        val rowList = defaultRequestResult.queryPipelineResult.rowList
                        var values: Set[String] = Set.empty
                        rowList.foreach {
                          row => values = values ++ List(row.cols(row.aliasMap(fieldAlias)).toString)
                        }

                        val newReportingRequest = implementDrilldownRequestMinimization(
                          mahaRequestContext.registryName
                          , mahaRequestContext.bucketParams
                          , mahaRequestContext.reportingRequest
                          , mahaService
                          , mahaRequestLogBuilder
                          , fieldAlias
                          , curatorConfig.asInstanceOf[DrilldownConfig]
                          , mahaRequestContext
                        )

                        val newRequestWithInsertedFilter = insertValuesIntoDrilldownRequest(newReportingRequest
                          , fieldAlias, values.toList)



                        if (mahaRequestContext.reportingRequest.isDebugEnabled) {
                          logger.info(s"drilldown request : $newRequestWithInsertedFilter")
                        }

                        val requestModelResultTry = mahaService.generateRequestModel(
                          mahaRequestContext.registryName, newRequestWithInsertedFilter, mahaRequestContext.bucketParams
                          , mahaRequestLogBuilder)

                        if (requestModelResultTry.isFailure) {
                          val message = requestModelResultTry.failed.get.getMessage
                          mahaRequestLogBuilder.logFailed(message)
                          withParRequestError(curatorConfig, GeneralError.from(parRequestLabel, message, MahaServiceBadRequestException(message, requestModelResultTry.failed.toOption)))
                        } else {
                          try {
                            val requestModelResult = requestModelResultTry.get
                            requestModelValidator.validate(mahaRequestContext, requestModelResult)
                            val parRequestResult = mahaService.executeRequestModelResult(mahaRequestContext.registryName
                              , requestModelResultTry.get, mahaRequestLogBuilder)
                            new Right(CuratorResult(DrilldownCurator.this, NoConfig, Option(parRequestResult), requestModelResult))
                          } catch {
                            case e: Exception =>
                              withParRequestError(curatorConfig, GeneralError.from(parRequestLabel
                                , e.getMessage, MahaServiceBadRequestException(e.getMessage, Option(e))))
                          }
                        }
                      }
                    } catch {
                      case e: Exception =>
                        withParRequestError(curatorConfig, GeneralError.from(parRequestLabel
                          , e.getMessage, MahaServiceBadRequestException(e.getMessage, Option(e))))
                    }
                }
              )
              innerResult
            }
        })
        withParResult(parRequestLabel, finalResult)
      }
    }
  }

}
