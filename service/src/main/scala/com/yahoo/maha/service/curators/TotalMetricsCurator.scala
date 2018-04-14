package com.yahoo.maha.service.curators

import java.util.concurrent.Callable

import com.yahoo.maha.core._
import com.yahoo.maha.parrequest2.future.ParRequest
import com.yahoo.maha.parrequest2.{GeneralError, ParCallable}
import com.yahoo.maha.service.error.{MahaServiceBadRequestException, MahaServiceExecutionException}
import com.yahoo.maha.service.utils.CuratorMahaRequestLogBuilder
import com.yahoo.maha.service.{MahaRequestContext, MahaService}
import grizzled.slf4j.Logging

import scala.util.Try

/**
 * Created by pranavbhole on 09/04/18.
 */
object TotalMetricsCurator {
  val name = "totalmetrics"
}

case class TotalMetricsCurator(override val requestModelValidator: CuratorRequestModelValidator = NoopCuratorRequestModelValidator) extends Curator with Logging {

  override val name: String = TotalMetricsCurator.name
  override val level: Int = 1
  override val priority: Int = 2
  override val isSingleton: Boolean = false
  override val requiresDefaultCurator = true

  override def process(resultMap: Map[String, ParRequest[CuratorResult]]
                       , mahaRequestContext: MahaRequestContext
                       , mahaService: MahaService
                       , mahaRequestLogBuilder: CuratorMahaRequestLogBuilder
                       , curatorConfig: CuratorConfig): ParRequest[CuratorResult] = {

    val registryConfig = mahaService.getMahaServiceConfig.registry.get(mahaRequestContext.registryName).get
    val parallelServiceExecutor = registryConfig.parallelServiceExecutor
    val parRequestLabel = s"process${TotalMetricsCurator.name}"

    parallelServiceExecutor.parRequestBuilder[CuratorResult]()
      .setLabel(parRequestLabel)
      .setParCallable(
        ParCallable.from[Either[GeneralError, CuratorResult]](
        new Callable[Either[GeneralError, CuratorResult]](){
          override def call(): Either[GeneralError, CuratorResult] = {

                  val reportingRequest = mahaRequestContext.reportingRequest
                  val registry = registryConfig.registry
                  val publicFactOption = registry.getFact(reportingRequest.cube)
                  if (publicFactOption.isEmpty) {
                     val message = s"Failed to find the cube ${reportingRequest.cube} in registry"
                     return GeneralError.either[CuratorResult](parRequestLabel, message, new MahaServiceBadRequestException(message))
                  }
                  val publicFact = publicFactOption.get
                  val factColsSet = publicFact.factCols.map(_.alias)

                  val totalMetricsReportingRequest = reportingRequest.copy(selectFields = reportingRequest.selectFields.filter(f=> factColsSet.contains(f.field)))

                  val totalMetricsRequestModelResultTry: Try[RequestModelResult] = mahaService.generateRequestModel(mahaRequestContext.registryName, totalMetricsReportingRequest, mahaRequestContext.bucketParams , mahaRequestLogBuilder)
                  if(totalMetricsRequestModelResultTry.isFailure) {
                    val message = totalMetricsRequestModelResultTry.failed.get.getMessage
                    mahaRequestLogBuilder.logFailed(message)
                    return GeneralError.either[CuratorResult](parRequestLabel, message, new MahaServiceBadRequestException(message, totalMetricsRequestModelResultTry.failed.toOption))
                  } else {
                    requestModelValidator.validate(mahaRequestContext, totalMetricsRequestModelResultTry.get)
                  }

                  val totalMetricsRequestModel: RequestModel = totalMetricsRequestModelResultTry.get.model

                  val totalMetricsRequestResultTry = mahaService.processRequestModel(mahaRequestContext.registryName, totalMetricsRequestModel, mahaRequestLogBuilder)

                  if(totalMetricsRequestResultTry.isFailure) {
                    val message = totalMetricsRequestResultTry.failed.get.getMessage
                    mahaRequestLogBuilder.logFailed(message)
                    return GeneralError.either[CuratorResult](parRequestLabel, message, new MahaServiceExecutionException(message))
                  }

                  new Right[GeneralError, CuratorResult](CuratorResult(TotalMetricsCurator.this,
                  NoConfig,
                  totalMetricsRequestResultTry,
                  totalMetricsRequestModelResultTry.get))
             }
          }
          )
      ).build()
  }
}