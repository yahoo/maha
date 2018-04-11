package com.yahoo.maha.service.curators

import java.util.concurrent.Callable

import com.yahoo.maha.core._
import com.yahoo.maha.parrequest2.future.{ParFunction, ParRequest}
import com.yahoo.maha.parrequest2.{GeneralError, ParCallable}
import com.yahoo.maha.service.error.{MahaServiceBadRequestException, MahaServiceExecutionException}
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import com.yahoo.maha.service.{MahaRequestContext, MahaService, RequestResult}
import grizzled.slf4j.Logging

import scala.util.Try

/**
 * Created by pranavbhole on 09/04/18.
 */
object TotalMetricsCurator {
  val name = "totalMetrics"
}

case class TotalMetricsCuratorResult(requestResultTry: Try[RequestResult],
                                     requestModelReference: RequestModelResult,
                                     totalMetricsResultTry: Try[RequestResult]) extends CuratorResult

case class TotalMetricsCurator(override val requestModelValidator: CuratorRequestModelValidator = NoopCuratorRequestModelValidator) extends Curator with Logging {

  override val name: String = TotalMetricsCurator.name
  override val level: Int = 1
  override val priority: Int = 0
  override val isSingleton: Boolean = true

  override def process(mahaRequestContext: MahaRequestContext,
                       mahaService: MahaService,
                       mahaRequestLogHelper: MahaRequestLogHelper): ParRequest[CuratorResult] = {

    val registryConfig = mahaService.getMahaServiceConfig.registry.get(mahaRequestContext.registryName).get
    val parallelServiceExecutor = registryConfig.parallelServiceExecutor
    val parRequestLabel = s"process${TotalMetricsCurator.name}"

    val parRequest2Builder = parallelServiceExecutor.parRequest2Builder[CuratorResult,CuratorResult]()
    parRequest2Builder.setLabel(parRequestLabel)

    parRequest2Builder.setFirstParCallable(
      ParCallable.from[Either[GeneralError, CuratorResult]](
        new Callable[Either[GeneralError, CuratorResult]](){
          override def call(): Either[GeneralError, CuratorResult] = {
              val requestModelResultTry: Try[RequestModelResult] = mahaService.generateRequestModel(
                  mahaRequestContext.registryName, mahaRequestContext.reportingRequest, mahaRequestContext.bucketParams
                , mahaRequestLogHelper)

              if(requestModelResultTry.isFailure) {
                val message = requestModelResultTry.failed.get.getMessage
                mahaRequestLogHelper.logFailed(message)
                return GeneralError.either[CuratorResult](parRequestLabel, message, new MahaServiceBadRequestException(message, requestModelResultTry.failed.toOption))
              } else {
                requestModelValidator.validate(mahaRequestContext, requestModelResultTry.get)
                val requestResultTry = mahaService.processRequestModel(mahaRequestContext.registryName
                  , requestModelResultTry.get.model, mahaRequestLogHelper)
                return new Right[GeneralError, CuratorResult](DefaultCuratorResult(requestResultTry, requestModelResultTry.get))
              }

            }
          }
       )
    )

    parRequest2Builder.setSecondParCallable(ParCallable.from[Either[GeneralError, CuratorResult]](
        new Callable[Either[GeneralError, CuratorResult]]() {
          override def call(): Either[GeneralError, CuratorResult] = {
            val reportingRequest = mahaRequestContext.reportingRequest

            val publicFactOption = registryConfig.registry.getFact(reportingRequest.cube)
            if (publicFactOption.isEmpty) {
               val message = s"Failed to find the cube ${reportingRequest.cube} in registry"
               return GeneralError.either[CuratorResult](parRequestLabel, message, new MahaServiceBadRequestException(message))
            }
            val factColsSet = publicFactOption.get.factCols.map(_.alias)

            val totalMetricsReportingRequest = reportingRequest.copy(selectFields = reportingRequest.selectFields.filter(f=> factColsSet.contains(f.field)))

            val totalMetricsRequestModelResultTry: Try[RequestModelResult] = mahaService.generateRequestModel(mahaRequestContext.registryName, totalMetricsReportingRequest, mahaRequestContext.bucketParams , mahaRequestLogHelper)
            if(totalMetricsRequestModelResultTry.isFailure) {
              val message = totalMetricsRequestModelResultTry.failed.get.getMessage
              mahaRequestLogHelper.logFailed(message)
              return GeneralError.either[CuratorResult](parRequestLabel, message, new MahaServiceBadRequestException(message, totalMetricsRequestModelResultTry.failed.toOption))
            } else {
              requestModelValidator.validate(mahaRequestContext, totalMetricsRequestModelResultTry.get)
            }

            val totalMetricsRequestModel: RequestModel = totalMetricsRequestModelResultTry.get.model


            val totalMetricsRequestResultTry = mahaService.processRequestModel(mahaRequestContext.registryName, totalMetricsRequestModel, mahaRequestLogHelper)

            if(totalMetricsRequestResultTry.isFailure) {
              val message = totalMetricsRequestResultTry.failed.get.getMessage
              mahaRequestLogHelper.logFailed(message)
              return GeneralError.either[CuratorResult](parRequestLabel, message, new MahaServiceExecutionException(message))
            }

            new Right[GeneralError, CuratorResult](DefaultCuratorResult(totalMetricsRequestResultTry, totalMetricsRequestModelResultTry.get))
          }
        }
      )
    )

    val sucessFunction: ParFunction[(CuratorResult, CuratorResult), CuratorResult] =
      ParFunction.fromScala(
        (result) => {
          val defaultCuratorResult = result._1
          val totalMetricsCuratorResult = result._2

          TotalMetricsCuratorResult(defaultCuratorResult.requestResultTry
            , defaultCuratorResult.requestModelReference
            , totalMetricsCuratorResult.requestResultTry)
        }
      )

    val errorFunction: ParFunction[GeneralError, CuratorResult] =
      ParFunction.fromScala(
        (result) => {
          val message = s"Failed to execute the totalMetricsCurator, ${result.message}"
          if (result.throwableOption.isDefined) {
            result.throwableOption.get.printStackTrace()
            throw result.throwableOption.get
          } else {
            throw new MahaServiceExecutionException(message)
          }
        }
      )


    parallelServiceExecutor.parRequestBuilder[CuratorResult]()
      .setLabel(parRequestLabel)
      .setParCallable(
        ParCallable.from[Either[GeneralError, CuratorResult]](
        new Callable[Either[GeneralError, CuratorResult]](){
          override def call(): Either[GeneralError, CuratorResult] = {
               val combinedResult =  parRequest2Builder.build().get()
               val curatorResult: CuratorResult = {
                   if (combinedResult.isLeft) {
                      val error = combinedResult.left.get
                      val message = s"Failed to execute combine result of totalMetricsCurator, ${error.message}"
                      if (error.throwableOption.isDefined) {
                         error.throwableOption.get.printStackTrace()
                         throw error.throwableOption.get
                      } else {
                        throw new MahaServiceExecutionException(message)
                      }
                 } else {
                 val curatorResult = combinedResult.right.get
                   TotalMetricsCuratorResult(curatorResult._1.requestResultTry, curatorResult._1.requestModelReference, curatorResult._2.requestResultTry)
                 }
               }
               new Right(curatorResult)
             }
          }
          )
      ).build()
  }

}