package com.yahoo.maha.service.curators

import com.yahoo.maha.core._
import com.yahoo.maha.core.request.Field
import com.yahoo.maha.parrequest2.GeneralError
import com.yahoo.maha.parrequest2.future.{ParFunction, ParRequest}
import com.yahoo.maha.service.error.MahaServiceBadRequestException
import com.yahoo.maha.service.utils.CuratorMahaRequestLogBuilder
import com.yahoo.maha.service.{CuratorInjector, MahaRequestContext, MahaService, ParRequestResult}
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

  override def process(resultMap: Map[String, Either[CuratorError, ParRequest[CuratorResult]]]
                       , mahaRequestContext: MahaRequestContext
                       , mahaService: MahaService
                       , mahaRequestLogBuilder: CuratorMahaRequestLogBuilder
                       , curatorConfig: CuratorConfig
                       , curatorInjector: CuratorInjector
                      ) : Either[CuratorError, ParRequest[CuratorResult]] = {

    val registryConfig = mahaService.getMahaServiceConfig.registry.get(mahaRequestContext.registryName).get
    val parallelServiceExecutor = registryConfig.parallelServiceExecutor
    val parRequestLabel = s"processTotalMetricsCurator"
    val reportingRequest = mahaRequestContext.reportingRequest
    val registry = registryConfig.registry
    val publicFactOption = registry.getFact(reportingRequest.cube)
    if (publicFactOption.isEmpty) {
      val message = s"Failed to find the cube ${reportingRequest.cube} in registry"
      return withError(curatorConfig
        , GeneralError.from(parRequestLabel, message, new MahaServiceBadRequestException(message)))
    }
    val publicFact = publicFactOption.get
    val factColsSet = publicFact.factCols.map(_.alias)

    val lowestLevelDimKeyField: Field = {
      val keyAlias =  publicFact.foreignKeyAliases.map {
        alias =>
          val dimOption = registry.getDimensionByPrimaryKeyAlias(alias, Some(publicFactOption.get.dimRevision))
          if (dimOption.isEmpty) {
            val message = s"Failed to find the dimension for key $alias in registry"
            return withError(curatorConfig, GeneralError.from(parRequestLabel
              , message, MahaServiceBadRequestException(message)))
          }
          alias -> dimOption.get.dimLevel
      }.minBy(_._2.level)._1
      Field(keyAlias, None, None)
    }

    val totalMetricsReportingRequest = reportingRequest
      .copy(selectFields =
        IndexedSeq(lowestLevelDimKeyField)
          ++ reportingRequest.selectFields.filter(f=> factColsSet.contains(f.field))
      )

    val totalMetricsRequestModelResultTry: Try[RequestModelResult] = mahaService.generateRequestModel(
      mahaRequestContext.registryName
      , totalMetricsReportingRequest
      , mahaRequestContext.bucketParams , mahaRequestLogBuilder)
    if(totalMetricsRequestModelResultTry.isFailure) {
      val message = totalMetricsRequestModelResultTry.failed.get.getMessage
      mahaRequestLogBuilder.logFailed(message)
      return withError(curatorConfig, GeneralError.from(parRequestLabel, message
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
                logger.error("error in post processor, returning original result", e)
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
          withError(curatorConfig, GeneralError.from(parRequestLabel
            , e.getMessage, new MahaServiceBadRequestException(e.getMessage, Option(e))))

      }
    }
  }
}