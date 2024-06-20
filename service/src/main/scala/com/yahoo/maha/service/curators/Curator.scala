// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.curators

import com.yahoo.maha.core.RequestModelResult
import com.yahoo.maha.core.request.{CuratorJsonConfig, Field, ReportingRequest, RowCountQuery, fieldExtended}
import com.yahoo.maha.parrequest2.GeneralError
import com.yahoo.maha.parrequest2.future.{ParFunction, ParRequest, ParallelServiceExecutor}
import com.yahoo.maha.service.error.MahaServiceBadRequestException
import com.yahoo.maha.service.utils.CuratorMahaRequestLogBuilder
import com.yahoo.maha.service.{CuratorInjector, MahaRequestContext, MahaService, ParRequestResult, RequestResult}
import grizzled.slf4j.Logging
import org.json4s.{DefaultFormats, JValue}
import org.json4s.scalaz.JsonScalaz

import scala.util.Try
import scalaz.{NonEmptyList, Validation}

case class CuratorError(curator: Curator, curatorConfig: CuratorConfig, error: GeneralError, index: Option[Int] = None)
  extends GeneralError(error.stage, error.message, error.throwableOption)
case class CuratorResult(curator: Curator
                         , curatorConfig: CuratorConfig
                         , parRequestResultOption: Option[ParRequestResult]
                         , requestModelReference: RequestModelResult
                         , index: Option[Int] = None
                        )

trait CuratorConfig
object NoConfig extends CuratorConfig

trait Curator extends Ordered[Curator] {
  def name: String

  def level: Int

  def priority: Int

  def process(resultMap: Map[String, Either[CuratorError, IndexedSeq[ParRequest[CuratorResult]]]]
              , mahaRequestContext: MahaRequestContext
              , mahaService: MahaService
              , mahaRequestLogBuilder: CuratorMahaRequestLogBuilder
              , curatorConfig: CuratorConfig
              , curatorInjector: CuratorInjector
             ): Either[CuratorError, IndexedSeq[ParRequest[CuratorResult]]]

  def compare(that: Curator) = {
    if (this.level == that.level) {
      Integer.compare(this.priority, that.priority)
    } else Integer.compare(this.level, that.level)
  }

  def isSingleton: Boolean

  def requiresDefaultCurator: Boolean

  def parseConfig(config: CuratorJsonConfig): Validation[NonEmptyList[JsonScalaz.Error], CuratorConfig] = {
    import scalaz.syntax.validation._
    NoConfig.successNel
  }

  protected def requestModelValidator: CuratorRequestModelValidator

  protected def curatorResultPostProcessor: CuratorResultPostProcessor = NoopCuratorResultPostProcessor

  protected def withResult(label: String, parallelServiceExecutor: ParallelServiceExecutor
                           , curatorResult: CuratorResult): Either[CuratorError, IndexedSeq[ParRequest[CuratorResult]]] = {
    new Right(IndexedSeq(parallelServiceExecutor.immediateResult(label, new Right(curatorResult))))
  }

  protected def withError(curatorConfig: CuratorConfig, error: GeneralError): Either[CuratorError, IndexedSeq[ParRequest[CuratorResult]]] = {
    new Left(CuratorError(this, curatorConfig, error))
  }

  protected def withRequestResultError(curatorConfig: CuratorConfig, error: GeneralError): Either[CuratorError, RequestResult] = {
    new Left(CuratorError(this, curatorConfig, error))
  }

  protected def withParResult(label: String
                              , parResult: ParRequest[CuratorResult]): Either[CuratorError, IndexedSeq[ParRequest[CuratorResult]]] = {
    new Right(IndexedSeq(parResult))
  }

  protected def withParRequestError[T](curatorConfig: CuratorConfig, error: GeneralError): Either[GeneralError, T] = {
    new Left(CuratorError(this, curatorConfig, error))
  }

  protected def withParRequestError[T](curatorConfig: CuratorConfig, error: GeneralError, idx: Int): Either[GeneralError, T] = {
    new Left(CuratorError(this, curatorConfig, error, Option(idx)))
  }
  def checkCuratorResponse(responseLength: Int, result: RequestResult ): Int = {responseLength}
}


object DefaultCurator {
  val name: String = "default"
}

trait CuratorRequestModelValidator {
  def validate(mahaRequestContext: MahaRequestContext, requestModelResult: RequestModelResult) : Unit
}

object NoopCuratorRequestModelValidator extends CuratorRequestModelValidator {
  def validate(mahaRequestContext: MahaRequestContext, requestModelResult: RequestModelResult) : Unit = {
    //do nothing
  }
}

trait CuratorResultPostProcessor {
  def process(mahaRequestContext: MahaRequestContext, requestResult: RequestResult) : Either[GeneralError, RequestResult]
}

object NoopCuratorResultPostProcessor extends CuratorResultPostProcessor {
  override def process(mahaRequestContext: MahaRequestContext
                       , requestResult: RequestResult) : Either[GeneralError, RequestResult] = {
    new Right(requestResult)
  }
}

case class DefaultCurator(protected val requestModelValidator: CuratorRequestModelValidator = NoopCuratorRequestModelValidator,
                          override val curatorResultPostProcessor: CuratorResultPostProcessor = NoopCuratorResultPostProcessor) extends Curator with Logging {

  override val name: String = DefaultCurator.name
  override val level: Int = 0
  override val priority: Int = 0
  override val isSingleton: Boolean = false
  override val requiresDefaultCurator: Boolean = false

  override def process(resultMap: Map[String, Either[CuratorError, IndexedSeq[ParRequest[CuratorResult]]]]
                       , mahaRequestContext: MahaRequestContext
                       , mahaService: MahaService
                       , mahaRequestLogBuilder: CuratorMahaRequestLogBuilder
                       , curatorConfig: CuratorConfig
                       , curatorInjector: CuratorInjector
                      ) : Either[CuratorError, IndexedSeq[ParRequest[CuratorResult]]] = {

    val parallelServiceExecutor = mahaService.getParallelServiceExecutor(mahaRequestContext)
    val parRequestLabel = "processDefaultCurator"

    val requestModelResultTry = mahaService.generateRequestModel(mahaRequestContext.registryName
      , mahaRequestContext.reportingRequest
      , mahaRequestContext.bucketParams)

    if(requestModelResultTry.isFailure) {
      val message = requestModelResultTry.failed.get.getMessage
      mahaRequestLogBuilder.logFailed(message, Some(400))
      withError(curatorConfig,
        GeneralError.from(parRequestLabel
          , message, new MahaServiceBadRequestException(message, requestModelResultTry.failed.toOption))
      )
    } else {
      try {
        val requestModelResult = requestModelResultTry.get
        requestModelValidator.validate(mahaRequestContext, requestModelResult)
        val parRequestResult: ParRequestResult = mahaService.executeRequestModelResult(mahaRequestContext.registryName
          , requestModelResult, mahaRequestLogBuilder)

        if (parRequestResult.queryPipeline.isSuccess) {
          val queryPipeline = parRequestResult.queryPipeline.get
          //detect row count injection
          //for dim driven multi engine, inject row count
          //for non dim driven, inject row count if requested
          val injectRowCountCurator =
          mahaRequestContext.reportingRequest.includeRowCount &&
            (
              (mahaRequestContext.reportingRequest.forceDimensionDriven
                && queryPipeline.bestDimCandidates.nonEmpty
                && queryPipeline.bestDimCandidates.head.dim.engine != queryPipeline.queryChain.drivingQuery.engine)
              ||
              (!mahaRequestContext.reportingRequest.forceDimensionDriven)
            )

          if (injectRowCountCurator) {
            curatorInjector.injectCurator(RowCountCurator.name, resultMap, mahaRequestContext, NoConfig)
          }
        }
        val postProcessorResult = parRequestResult.prodRun.map("postProcess", ParFunction.fromScala {
          requestResult =>
            try {
              val result = curatorResultPostProcessor.process(mahaRequestContext, requestResult)
              if (result.isRight) {
                mahaRequestLogBuilder.logSuccess()
              } else {
                val ge = result.left.get
                mahaRequestLogBuilder.logFailed(ge.throwableOption.map(_.getMessage).getOrElse(ge.message))
              }
              result
            } catch {
              case e: Exception =>
                val message = "error in post processor, returning original result"
                logger.error(message, e)
                mahaRequestLogBuilder.logFailed(s"$message - ${e.getMessage}")
                new Right(requestResult)
            }
        })
        withResult(parRequestLabel
          , parallelServiceExecutor
          , CuratorResult(this
            , curatorConfig
            , Option(parRequestResult.copy(prodRun = postProcessorResult))
            , requestModelResult))
      }
      catch {
        case e: Exception =>
          mahaRequestLogBuilder.logFailed(e.getMessage)
          withError(curatorConfig, GeneralError.from(parRequestLabel
            , e.getMessage, new MahaServiceBadRequestException(e.getMessage, Option(e))))

      }
    }
  }

  override def checkCuratorResponse(responseLength: Int, result: RequestResult) = {
    // this method returns the number of rows in the Default Curator Response
    var respLength = 0
    val getResponseLength = Try {
      if (responseLength < result.queryPipelineResult.rowList.length) {
        respLength = result.queryPipelineResult.rowList.length
      }
    }
    if(!getResponseLength.isSuccess){
      logger.warn("result response length could not be extracted. \n")
    }
    respLength
  }
}
object RowCountConfig extends Logging {
  implicit val formats: DefaultFormats.type = DefaultFormats

  def parse(curatorJsonConfig: CuratorJsonConfig) : JsonScalaz.Result[RowCountConfig] = {
    val config: JValue = curatorJsonConfig.json
    val isFactDriven: JsonScalaz.Result[Option[Boolean]] = fieldExtended[Option[Boolean]]("isFactDriven")(config)
    isFactDriven.map(isFactDriven => RowCountConfig(isFactDriven))
  }
}

case class RowCountConfig(isFactDriven: Option[Boolean]) extends CuratorConfig

object RowCountCurator {
  val name: String = "rowcount"

  def getRowCount(mahaRequestContext: MahaRequestContext): Option[Int] = {
    mahaRequestContext.mutableState.get(name) match {
      case Some(i: Int) => Option(i)
      case _ => None
    }
  }
}

case class RowCountCurator(protected val requestModelValidator: CuratorRequestModelValidator = NoopCuratorRequestModelValidator,
                           override val curatorResultPostProcessor: CuratorResultPostProcessor = NoopCuratorResultPostProcessor,
                           private val FACT_ONLY_LIMIT: Int = 5000
                                ) extends Curator with Logging {
  override def name: String = RowCountCurator.name

  override def level: Int = 1

  override def priority: Int = 1

  override def parseConfig(config: CuratorJsonConfig): Validation[NonEmptyList[JsonScalaz.Error], CuratorConfig] = {
    val rowCountConfigTry : JsonScalaz.Result[RowCountConfig] = RowCountConfig.parse(config)
    Validation
      .fromTryCatchNonFatal{
        require(rowCountConfigTry.isSuccess, "Must succeed in creating a rowCountConfig " + rowCountConfigTry)
        rowCountConfigTry.toOption.get}
      .leftMap[JsonScalaz.Error](t => JsonScalaz.UncategorizedError("parseRowCountConfigValidation", t.getMessage, List.empty)).toValidationNel
  }

  override def process(resultMap: Map[String, Either[CuratorError, IndexedSeq[ParRequest[CuratorResult]]]]
                     , mahaRequestContext: MahaRequestContext
                     , mahaService: MahaService
                     , mahaRequestLogBuilder: CuratorMahaRequestLogBuilder
                     , curatorConfig: CuratorConfig
                     , curatorInjector: CuratorInjector
                    ): Either[CuratorError, IndexedSeq[ParRequest[CuratorResult]]] = {
    val parallelServiceExecutor = mahaService.getParallelServiceExecutor(mahaRequestContext)
    val parRequestLabel = "processTotalRows"

    val requestModelResultTry = mahaService.generateRequestModel(mahaRequestContext.registryName
      , mahaRequestContext.reportingRequest
      , mahaRequestContext.bucketParams)

    if(requestModelResultTry.isFailure) {
      val message = requestModelResultTry.failed.get.getMessage
      mahaRequestLogBuilder.logFailed(message, Some(400))
      withError(curatorConfig,
        GeneralError.from(parRequestLabel
          , message, new MahaServiceBadRequestException(message, requestModelResultTry.failed.toOption))
      )
    } else {
      try {
        val requestModelResult = requestModelResultTry.get
        requestModelValidator.validate(mahaRequestContext, requestModelResult)
        if(mahaRequestContext.reportingRequest.forceDimensionDriven) {
          val sourcePipelineTry = mahaService.generateQueryPipelines(mahaRequestContext.registryName
            , requestModelResultTry.get.model
          , mahaRequestContext.bucketParams)._1

          if (sourcePipelineTry.isFailure) {
            val exception = sourcePipelineTry.failed.get
            val message = "source pipeline failed"
            mahaRequestLogBuilder.logFailed(s"$message - ${exception.getMessage}")
            withError(curatorConfig, GeneralError.from(parRequestLabel, message, exception))
          } else {
            val sourcePipeline = sourcePipelineTry.get
            //no filters except fk filters
            val totalRowsCountRequestTry =
              Try {
                require(
                  sourcePipeline.bestDimCandidates.nonEmpty
                  , s"Invalid total rows request, no best dim candidates! : ${sourcePipeline.requestModel}")

                //force dim driven
                //remove all fields except primary key
                //remove all sorts
                val primaryKeyAliasFields = sourcePipeline.bestDimCandidates.map(dim => Field(dim.publicDim.primaryKeyByAlias, None, None)).toIndexedSeq
                sourcePipeline.requestModel.reportingRequest.copy(
                  selectFields = primaryKeyAliasFields
                  , sortBy = IndexedSeq.empty
                  , includeRowCount = true
                  , forceDimensionDriven = true
                  , forceFactDriven = false
                  , paginationStartIndex = 0
                  , rowsPerPage = 1
                  , curatorJsonConfigMap = Map.empty
                )
              }

            executeRowCountRequest(
              mahaRequestContext
              , mahaService
              , mahaRequestLogBuilder
              , parallelServiceExecutor
              , parRequestLabel
              , totalRowsCountRequestTry
              , curatorConfig
              , requestModelResult)
          }
        } else {
          val sourcePipelineTry = mahaService.generateQueryPipelines(mahaRequestContext.registryName
            , requestModelResultTry.get.model
            , mahaRequestContext.bucketParams)._1

          if (sourcePipelineTry.isFailure) {
            val exception = sourcePipelineTry.failed.get
            val message = "source pipeline failed"
            mahaRequestLogBuilder.logFailed(s"$message - ${exception.getMessage}")
            withError(curatorConfig, GeneralError.from(parRequestLabel, message, exception))
          } else {

            val sourcePipeline = sourcePipelineTry.get

            val requiredRowCountFlag: Boolean = {
              curatorConfig match {
                case RowCountConfig(isFactDriven) if isFactDriven.isDefined => isFactDriven.get
                case _ => false
              }
            }

            if (!requiredRowCountFlag) { // use legacy code if didn't specific fact driven query
              val model = requestModelResult.model
              val curatorResult = CuratorResult(this, curatorConfig, None, requestModelResult)
              if (model.dimCardinalityEstimate.nonEmpty) {
                if(model.dimCardinalityEstimate.get.intValue <= FACT_ONLY_LIMIT) {
                  val count = model.dimCardinalityEstimate.get.intValue
                  mahaRequestContext.mutableState.put(RowCountCurator.name, count)
                  mahaRequestLogBuilder.logSuccess()
                  withResult(parRequestLabel, parallelServiceExecutor, curatorResult)
                } else {
                  mahaRequestContext.mutableState.put(RowCountCurator.name, FACT_ONLY_LIMIT)
                  mahaRequestLogBuilder.logSuccess()
                  withResult(parRequestLabel, parallelServiceExecutor, curatorResult)
                }
              } else {
                val message = "No row count can be estimated without dim cardinality estimate"
                mahaRequestLogBuilder.logFailed(message, Option(400))
                withError(curatorConfig, GeneralError.from(parRequestLabel, message))
              }
            } else { // if isFactDriven = true is specified in config, queryType = RowCountQuery
              val totalRowsCountRequestTry =
                Try {
                  //force fact driven
                  //added row count field
                  //remove all sorts
                  sourcePipeline.requestModel.reportingRequest.copy(
                    selectFields = sourcePipeline.requestModel.reportingRequest.selectFields
                    , queryType = RowCountQuery
                    , sortBy = IndexedSeq.empty
                    , forceDimensionDriven = false
                    , forceFactDriven = true
                    , paginationStartIndex = 0
                    , rowsPerPage = 1
                    , curatorJsonConfigMap = Map.empty
                  )
                }

              executeRowCountRequest(
                mahaRequestContext
                , mahaService
                , mahaRequestLogBuilder
                , parallelServiceExecutor
                , parRequestLabel
                , totalRowsCountRequestTry
                , curatorConfig
                , requestModelResult)
            }
          }
        }
      }
      catch {
        case e: Exception =>
          mahaRequestLogBuilder.logFailed(e.getMessage)
          withError(curatorConfig, GeneralError.from(parRequestLabel
            , e.getMessage, MahaServiceBadRequestException(e.getMessage, Option(e))))

      }
    }
  }

  override def isSingleton: Boolean = false

  override def requiresDefaultCurator: Boolean = true

  private def executeRowCountRequest(mahaRequestContext: MahaRequestContext
                                     , mahaService: MahaService
                                     , mahaRequestLogBuilder: CuratorMahaRequestLogBuilder
                                     , parallelServiceExecutor: ParallelServiceExecutor
                                     , parRequestLabel: String
                                     , rowCountRequestTry: Try[ReportingRequest]
                                     , curatorConfig: CuratorConfig
                                     , requestModelResult: RequestModelResult
                                    ): Either[CuratorError, IndexedSeq[ParRequest[CuratorResult]]] = {
    if (rowCountRequestTry.isFailure) {
      val exception = rowCountRequestTry.failed.get
      val message = "total rows request failed to generate"
      mahaRequestLogBuilder.logFailed(s"${message} - ${exception.getMessage}")
      withError(curatorConfig, GeneralError.from(parRequestLabel, message, exception))
    } else {
      val totalRowsRequest = rowCountRequestTry.get
      val parRequestResult: ParRequestResult = mahaService.executeRequest(mahaRequestContext.registryName
        , totalRowsRequest, mahaRequestContext.bucketParams, mahaRequestLogBuilder)

      val totalRowsRequestModel = parRequestResult.queryPipeline.get.requestModel
      if(totalRowsRequest.isDebugEnabled) {
        info(s"Unfiltered request should not generate any fact candidates!  " +
          s" : Request fields : ${totalRowsRequestModel.reportingRequest.selectFields.foreach(field => field.toString + "\t")} " +
          s" : generated Model columns and candidate names : ${totalRowsRequestModel.requestCols.foreach(colInfo => colInfo.toString + "\t")} " +
          s" : ${totalRowsRequestModel.bestCandidates.foreach(candidate => candidate.requestCols.toString())}")
      }

      val populateRowCount: ParRequest[RequestResult] = parRequestResult.prodRun.map(parRequestLabel, ParFunction.fromScala {
        requestResult =>
          val count = requestResult.queryPipelineResult.rowList.getTotalRowCount
          mahaRequestContext.mutableState.put(RowCountCurator.name, count)
          mahaRequestLogBuilder.logSuccess()
          new Right(requestResult)
      })

      val finalParRequestResult = parRequestResult.copy(prodRun = populateRowCount)
      val curatorResult = CuratorResult(this, curatorConfig, Option(finalParRequestResult), requestModelResult.copy(model = totalRowsRequestModel))
      withResult(parRequestLabel, parallelServiceExecutor, curatorResult)
    }
  }

  override def checkCuratorResponse(responseLength: Int, result: RequestResult) = {
    // if the totalRowCount of rowCount curator is lesser than number of rows returned in DefaultCurator Response, this method modifies
    // rowCountCurator response to match the number of rows returned in DefaultCurator Response
    val modifyTotalRowCount = Try {
      val rowCountRowList = result.queryPipelineResult.rowList
      if (rowCountRowList.getTotalRowCount > 1 && rowCountRowList.getTotalRowCount < responseLength) {
        rowCountRowList.setTotalRowCount(responseLength)
      }
    }
    if (!modifyTotalRowCount.isSuccess) {
      logger.warn("Failed to modify total row count.\n")
    }
    responseLength
  }
}
