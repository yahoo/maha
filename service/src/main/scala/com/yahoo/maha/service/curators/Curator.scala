// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.curators

import com.yahoo.maha.core.RequestModelResult
import com.yahoo.maha.core.request.{CuratorJsonConfig, Field}
import com.yahoo.maha.parrequest2.GeneralError
import com.yahoo.maha.parrequest2.future.{ParFunction, ParRequest, ParallelServiceExecutor}
import com.yahoo.maha.service.error.MahaServiceBadRequestException
import com.yahoo.maha.service.utils.CuratorMahaRequestLogBuilder
import com.yahoo.maha.service.{CuratorInjector, MahaRequestContext, MahaService, ParRequestResult, RequestResult}
import grizzled.slf4j.Logging
import org.json4s.scalaz.JsonScalaz

import scala.util.Try
import scalaz.{NonEmptyList, Validation}

case class CuratorError(curator: Curator, curatorConfig: CuratorConfig, error: GeneralError)
  extends GeneralError(error.stage, error.message, error.throwableOption)
case class CuratorResult(curator: Curator
                         , curatorConfig: CuratorConfig
                         , parRequestResultOption: Option[ParRequestResult]
                         , requestModelReference: RequestModelResult
                        )

trait CuratorConfig
object NoConfig extends CuratorConfig

trait Curator extends Ordered[Curator] {
  def name: String
  def level: Int
  def priority: Int
  def process(resultMap: Map[String, Either[CuratorError, ParRequest[CuratorResult]]]
              , mahaRequestContext: MahaRequestContext
              , mahaService: MahaService
              , mahaRequestLogBuilder: CuratorMahaRequestLogBuilder
              , curatorConfig: CuratorConfig
              , curatorInjector: CuratorInjector
             ) : Either[CuratorError, ParRequest[CuratorResult]]
  def compare(that: Curator) = {
    if(this.level == that.level) {
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
                                , curatorResult: CuratorResult): Either[CuratorError, ParRequest[CuratorResult]] = {
    new Right(parallelServiceExecutor.immediateResult(label, new Right(curatorResult)))
  }
  protected def withError(curatorConfig: CuratorConfig, error: GeneralError): Either[CuratorError, ParRequest[CuratorResult]] = {
    new Left(CuratorError(this, curatorConfig, error))
  }
  protected def withParResult(label: String
                              , parResult: ParRequest[CuratorResult]): Either[CuratorError, ParRequest[CuratorResult]] = {
    new Right(parResult)
  }
  protected def withParRequestError(curatorConfig: CuratorConfig, error: GeneralError): Either[GeneralError, CuratorResult] = {
    new Left(CuratorError(this, curatorConfig, error))
  }
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

  override def process(resultMap: Map[String, Either[CuratorError, ParRequest[CuratorResult]]]
                       , mahaRequestContext: MahaRequestContext
                       , mahaService: MahaService
                       , mahaRequestLogBuilder: CuratorMahaRequestLogBuilder
                       , curatorConfig: CuratorConfig
                       , curatorInjector: CuratorInjector
                      ) : Either[CuratorError, ParRequest[CuratorResult]] = {

    val parallelServiceExecutor = mahaService.getParallelServiceExecutor(mahaRequestContext)
    val parRequestLabel = "processDefaultCurator"

    val requestModelResultTry = mahaService.generateRequestModel(mahaRequestContext.registryName
      , mahaRequestContext.reportingRequest
      , mahaRequestContext.bucketParams)

    if(requestModelResultTry.isFailure) {
      val message = requestModelResultTry.failed.get.getMessage
      mahaRequestLogBuilder.logFailed(message)
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
}

object RowCountCurator {
  val name: String = "rowcount"

  def getRowCount(mahaRequestContext: MahaRequestContext) : Option[Int] = {
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

  override def process(resultMap: Map[String, Either[CuratorError, ParRequest[CuratorResult]]]
                         , mahaRequestContext: MahaRequestContext
                         , mahaService: MahaService
                         , mahaRequestLogBuilder: CuratorMahaRequestLogBuilder
                         , curatorConfig: CuratorConfig
                         , curatorInjector: CuratorInjector
                        ) : Either[CuratorError, ParRequest[CuratorResult]] = {
    val parallelServiceExecutor = mahaService.getParallelServiceExecutor(mahaRequestContext)
    val parRequestLabel = "processTotalRows"

    val requestModelResultTry = mahaService.generateRequestModel(mahaRequestContext.registryName
      , mahaRequestContext.reportingRequest
      , mahaRequestContext.bucketParams)

    if(requestModelResultTry.isFailure) {
      val message = requestModelResultTry.failed.get.getMessage
      mahaRequestLogBuilder.logFailed(message)
      withError(curatorConfig,
        GeneralError.from(parRequestLabel
          , message, new MahaServiceBadRequestException(message, requestModelResultTry.failed.toOption))
      )
    } else {
      try {
        val requestModelResult = requestModelResultTry.get
        requestModelValidator.validate(mahaRequestContext, requestModelResult)
        if(mahaRequestContext.reportingRequest.forceDimensionDriven) {
          val sourcePipelineTry = mahaService.generateQueryPipeline(mahaRequestContext.registryName
            , requestModelResultTry.get.model)

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
                )
              }
            if (totalRowsCountRequestTry.isFailure) {
              val exception = totalRowsCountRequestTry.failed.get
              val message = "total rows request failed to generate"
              mahaRequestLogBuilder.logFailed(s"${message} - ${exception.getMessage}")
              withError(curatorConfig, GeneralError.from(parRequestLabel, message, exception))
            } else {
              val totalRowsRequest = totalRowsCountRequestTry.get
              val parRequestResult: ParRequestResult = mahaService.executeRequest(mahaRequestContext.registryName
                , totalRowsRequest, mahaRequestContext.bucketParams, mahaRequestLogBuilder)
              val populateRowCount:ParRequest[RequestResult] = parRequestResult.prodRun.map(parRequestLabel, ParFunction.fromScala {
                requestResult =>
                  val count = requestResult.queryPipelineResult.rowList.getTotalRowCount
                  mahaRequestContext.mutableState.put(RowCountCurator.name, count)
                  mahaRequestLogBuilder.logSuccess()
                  new Right(requestResult)
              })

              val finalParRequestResult = parRequestResult.copy(prodRun = populateRowCount)
              val curatorResult = CuratorResult(this, curatorConfig, Option(finalParRequestResult), requestModelResult)
              withResult(parRequestLabel, parallelServiceExecutor, curatorResult)
            }
          }
        } else {
          val model = requestModelResult.model
          val curatorResult = CuratorResult(this, curatorConfig, None, requestModelResult)
          if (model.dimCardinalityEstimate.isEmpty) {
            val message = "No way to estimate dim cardinality for fact driven request!"
            mahaRequestLogBuilder.logFailed(message)
            withError(curatorConfig, GeneralError.from(parRequestLabel, message))
          }
          else if (model.dimCardinalityEstimate.get.intValue <= FACT_ONLY_LIMIT) {
            val count = model.dimCardinalityEstimate.get.intValue
            mahaRequestContext.mutableState.put(RowCountCurator.name, count)
            mahaRequestLogBuilder.logSuccess()
            withResult(parRequestLabel, parallelServiceExecutor, curatorResult)
          } else {
            mahaRequestContext.mutableState.put(RowCountCurator.name, FACT_ONLY_LIMIT)
            mahaRequestLogBuilder.logSuccess()
            withResult(parRequestLabel, parallelServiceExecutor, curatorResult)
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

  override def requiresDefaultCurator: Boolean = false
}
