// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.curators

import java.util.concurrent.Callable

import com.yahoo.maha.core.RequestModelResult
import com.yahoo.maha.parrequest2.future.ParRequest
import com.yahoo.maha.parrequest2.{GeneralError, ParCallable}
import com.yahoo.maha.service.error.MahaServiceBadRequestException
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import com.yahoo.maha.service.{MahaRequestContext, MahaService, RequestResult}
import grizzled.slf4j.Logging

import scala.util.Try

case class CuratorResult(requestResultTry: Try[RequestResult], requestModelReference: RequestModelResult)

trait Curator extends Ordered[Curator] {
  def name: String
  def level: Int
  def priority: Int
  def process(mahaRequestContext: MahaRequestContext
              , mahaService: MahaService
              , mahaRequestLogHelper: MahaRequestLogHelper) : ParRequest[CuratorResult]
  def compare(that: Curator) = {
    if(this.level == that.level) {
      Integer.compare(this.priority, that.priority)
    } else Integer.compare(this.level, that.level)
  }
  def isSingleton: Boolean
  protected def requestModelValidator: CuratorRequestModelValidator
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

case class DefaultCurator(protected val requestModelValidator: CuratorRequestModelValidator = NoopCuratorRequestModelValidator) extends Curator with Logging {

  override val name: String = DefaultCurator.name
  override val level: Int = 0
  override val priority: Int = 0
  override val isSingleton: Boolean = false

  override def process(mahaRequestContext: MahaRequestContext
                       , mahaService: MahaService
                       , mahaRequestLogHelper: MahaRequestLogHelper): ParRequest[CuratorResult] = {

    val registryConfig = mahaService.getMahaServiceConfig.registry.get(mahaRequestContext.registryName).get
    val parallelServiceExecutor = registryConfig.parallelServiceExecutor
    val parRequestLabel = "processDefaultCurator"

    val parRequest = parallelServiceExecutor.parRequestBuilder[CuratorResult].setLabel(parRequestLabel).
      setParCallable(ParCallable.from[Either[GeneralError, CuratorResult]](
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
              return new Right[GeneralError, CuratorResult](CuratorResult(requestResultTry, requestModelResultTry.get))
            }
          }
        }
      )).build()
    parRequest
  }

}
