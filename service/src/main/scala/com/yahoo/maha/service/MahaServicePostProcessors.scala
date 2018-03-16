// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service

import com.yahoo.maha.core.RequestModelResult
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.parrequest.{Either, GeneralError, Right}
import com.yahoo.maha.service.error.{MahaServiceExecutionException, MahaServiceBadRequestException}
import com.yahoo.maha.service.utils.MahaRequestLogHelper

import scala.util.Try

/**
 * Created by pranavbhole on 15/03/18.
 */
trait RmResultPostProcessor {
  def label : String
  def process(requestModelResultTry: Try[RequestModelResult], mahaRequestLogHelper: MahaRequestLogHelper) : Either[GeneralError, RequestModelResult]
}

trait MahaServiceMonitor {
  def start(reportingRequest: ReportingRequest)
  def stop()
}

trait RequestResultPostProcessor {
  def label : String
  def process(requestResultTry: Try[RequestResult], mahaRequestLogHelper: MahaRequestLogHelper): Either[GeneralError, RequestResult]
}

case class DefaultRmResultPostProcessor(label : String) extends RmResultPostProcessor {

  override def process(requestModelResultTry: Try[RequestModelResult], mahaRequestLogHelper: MahaRequestLogHelper): Either[GeneralError, RequestModelResult] = {
    if (requestModelResultTry.isFailure) {
      val message = "Failed to create Report Model:"
      val error = requestModelResultTry.failed.toOption
      mahaRequestLogHelper.logFailed(message)
      return GeneralError.either[RequestModelResult](label, message, new MahaServiceBadRequestException(message, error))
    } else {
      new Right(requestModelResultTry.get)
    }
  }
}

class DefaultMahaServiceMonitor extends MahaServiceMonitor {
  override def start(reportingRequest: ReportingRequest): Unit = {}

  override def stop(): Unit =  {}
}

case class DefaultRequestResultPostProcessor(label : String) extends RequestResultPostProcessor {
  override def process(requestResultTry: Try[RequestResult], mahaRequestLogHelper: MahaRequestLogHelper): Either[GeneralError, RequestResult] = {
    if (requestResultTry.isFailure) {
      val message = "Failed on executing the query pipeline:"
      val error = requestResultTry.failed.toOption
      mahaRequestLogHelper.logFailed(message)
      return GeneralError.either(label, message, new MahaServiceExecutionException(message, error))
    } else {
      return new Right(requestResultTry.get)
    }
  }
}