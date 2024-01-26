// Copyright 2018, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.curators

import com.yahoo.maha.core._
import com.yahoo.maha.core.bucketing.BucketParams
import com.yahoo.maha.core.request.{ReportingRequest, _}
import com.yahoo.maha.parrequest2.GeneralError
import com.yahoo.maha.parrequest2.future.{ParFunction, ParRequest}
import com.yahoo.maha.service.error.MahaServiceBadRequestException
import com.yahoo.maha.service.utils.CuratorMahaRequestLogBuilder
import com.yahoo.maha.service.{CuratorInjector, MahaRequestContext, MahaService, ParRequestResult}
import grizzled.slf4j.Logging
import org.json4s.scalaz.JsonScalaz
import org.json4s.{DefaultFormats, JValue}

import scala.util.Try
import scalaz.{NonEmptyList, Validation}

/**
 * Created by pranavbhole on 09/04/18.
 */

object TotalMetricsConfig extends Logging {
  implicit val formats: DefaultFormats.type = DefaultFormats

  def parse(curatorJsonConfig: CuratorJsonConfig) : JsonScalaz.Result[TotalMetricsConfig] = {

    val config: JValue = curatorJsonConfig.json

    val forceRevision: JsonScalaz.Result[Option[Int]] = extractForceRevision(config)

    forceRevision.map(forceRevision => TotalMetricsConfig(forceRevision))
  }

  private def extractForceRevision(config: JValue): JsonScalaz.Result[Option[Int]] = {
    fieldExtended[Option[Int]]("forceRevision")(config)
  }

}

case class TotalMetricsConfig(forceRevision: Option[Int]) extends CuratorConfig
object TotalMetricsCurator {
  val name = "totalmetrics"
}

case class TotalMetricsCurator(override val requestModelValidator: CuratorRequestModelValidator = NoopCuratorRequestModelValidator) extends Curator with Logging {

  override val name: String = TotalMetricsCurator.name
  override val level: Int = 1
  override val priority: Int = 2
  override val isSingleton: Boolean = false
  override val requiresDefaultCurator = true

  override def parseConfig(config: CuratorJsonConfig): JsonScalaz.Result[CuratorConfig] = {
    val totalMetricsConfigTry : JsonScalaz.Result[TotalMetricsConfig] = TotalMetricsConfig.parse(config)
    totalMetricsConfigTry.map(_.asInstanceOf[CuratorConfig])
  }

  override def process(resultMap: Map[String, Either[CuratorError, IndexedSeq[ParRequest[CuratorResult]]]]
                       , mahaRequestContext: MahaRequestContext
                       , mahaService: MahaService
                       , mahaRequestLogBuilder: CuratorMahaRequestLogBuilder
                       , curatorConfig: CuratorConfig
                       , curatorInjector: CuratorInjector
                      ) : Either[CuratorError, IndexedSeq[ParRequest[CuratorResult]]] = {

    val registryConfig = mahaService.getMahaServiceConfig.registry.get(mahaRequestContext.registryName).get
    val parallelServiceExecutor = registryConfig.parallelServiceExecutor
    val parRequestLabel = s"processTotalMetricsCurator"
    val reportingRequest = mahaRequestContext.reportingRequest
    val registry = registryConfig.registry
    val publicFactOption = registry.getFact(reportingRequest.cube)
    if (publicFactOption.isEmpty) {
      val message = s"Failed to find the cube ${reportingRequest.cube} in registry"
      mahaRequestLogBuilder.logFailed(message)
      return withError(curatorConfig
        , GeneralError.from(parRequestLabel, message, new MahaServiceBadRequestException(message)))
    }
    val publicFact = publicFactOption.get
    val factColsSet = publicFact.factCols.map(_.alias)

    val totalMetricsReportingRequest = reportingRequest.copy(
      selectFields = reportingRequest.selectFields.filter(f=> factColsSet.contains(f.field)),
      forceDimensionDriven = false,
      forceFactDriven = true,
      includeRowCount = false,
      sortBy = IndexedSeq.empty,
      paginationStartIndex = 0,
      rowsPerPage = -1)

    if(isDebugEnabled) {
      logger.info(s"totalmetrics request : ${new String(ReportingRequest.serialize(totalMetricsReportingRequest))}")
    }

    val bucketParams: BucketParams = {
      curatorConfig match {
        case TotalMetricsConfig(totalMetricsForceRevision) if totalMetricsForceRevision.isDefined =>
          mahaRequestContext.bucketParams.copy(forceRevision = totalMetricsForceRevision)
        case _ =>
          mahaRequestContext.bucketParams
      }
    }
    val totalMetricsRequestModelResultTry: Try[RequestModelResult] = mahaService.generateRequestModel(
      mahaRequestContext.registryName
      , totalMetricsReportingRequest
      , bucketParams)
    if(totalMetricsRequestModelResultTry.isFailure) {
      val message = totalMetricsRequestModelResultTry.failed.get.getMessage
      mahaRequestLogBuilder.logFailed(message)
      withError(curatorConfig, GeneralError.from(parRequestLabel, message
        , MahaServiceBadRequestException(message, totalMetricsRequestModelResultTry.failed.toOption)))
    } else {
      try {
        val requestModelResult = totalMetricsRequestModelResultTry.get
        requestModelValidator.validate(mahaRequestContext, requestModelResult)
        val parRequestResult: ParRequestResult = mahaService.executeRequestModelResult(mahaRequestContext.registryName
          , requestModelResult, mahaRequestLogBuilder)
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