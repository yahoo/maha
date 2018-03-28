// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service

import com.google.protobuf.ByteString
import com.yahoo.maha.core.bucketing.BucketParams
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.core.{RequestModel, RequestModelResult}
import com.yahoo.maha.parrequest2.GeneralError
import com.yahoo.maha.parrequest2.future.ParFunction
import com.yahoo.maha.proto.MahaRequestLog.MahaRequestProto
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import grizzled.slf4j.Logging

import scala.util.Try

trait BaseMahaRequestProcessor {
  def registryName: String
  def mahaService: MahaService
  def mahaServiceMonitor : MahaServiceMonitor
  def mahaRequestLogHelperOption: Option[MahaRequestLogHelper]

  def process(bucketParams: BucketParams,
              reportingRequest: ReportingRequest,
              rawJson: Array[Byte]): Unit
  def onSuccess(fn: (RequestModel, RequestResult) => Unit)
  def onFailure(fn: (GeneralError) => Unit)

  //Optional model/result validation Functional Traits
  def withRequestModelValidator(fn: (RequestModelResult) => Unit)
  def withRequestResultValidator(fn: (RequestResult) => Unit)
}

case class MahaRequestProcessor(registryName: String,
                                 mahaService: MahaService,
                                 mahaServiceMonitor : MahaServiceMonitor = DefaultMahaServiceMonitor,
                                 processingLabel : String  = MahaServiceConstants.MahaRequestLabel,
                                 mahaRequestLogHelperOption:Option[MahaRequestLogHelper] = None) extends BaseMahaRequestProcessor with Logging {
  private[this] val mahaRequestLogHelper = if(mahaRequestLogHelperOption.isEmpty) {
     MahaRequestLogHelper(registryName, mahaService)
  } else {
    mahaRequestLogHelperOption.get
  }

  private[this] var onSuccessFn: Option[(RequestModel, RequestResult) => Unit] = None
  private[this] var onFailureFn: Option[GeneralError => Unit] = None

  //Optional Post Operation Functional Traits
  /*
   Defines the validation steps for Request Model Result Success and Failure handling
  */
  private[this] var requestModelValidationFn: Option[(RequestModelResult) => Unit] = None

  /*
   Defines the validation steps for Request Result Success and Failure handling
 */
  private[this] var requestResultValidationFn : Option[(RequestResult) => Unit] = None

  def onSuccess(fn: (RequestModel, RequestResult) => Unit) : Unit = {
    onSuccessFn = Some(fn)
  }

  def onFailure(fn: (GeneralError) => Unit) : Unit = {
    onFailureFn = Some(fn)
  }

  def process(bucketParams: BucketParams,
               reportingRequest: ReportingRequest,
               rawJson: Array[Byte]) : Unit = {

    require(onSuccessFn.isDefined || onFailureFn.isDefined, "Nothing to do after processing!")
    mahaRequestLogHelper.init(reportingRequest, None, MahaRequestProto.RequestType.SYNC, ByteString.copyFrom(rawJson))
    //Starting Service Monitor
    mahaServiceMonitor.start(reportingRequest)

    val requestModelResultTry: Try[RequestModelResult] = mahaService.generateRequestModel(registryName, reportingRequest, bucketParams , mahaRequestLogHelper)
    // Custom validation for RequestModel
    val requestModelValidationTry = for {
      requestModelResult <- requestModelResultTry
    } yield requestModelValidationFn.foreach(_ (requestModelResult))

    if(requestModelValidationTry.isFailure) {
      val err = requestModelValidationTry.failed.get
      callOnFailureFn(mahaRequestLogHelper, reportingRequest)(GeneralError.from(processingLabel, err.getMessage, err))
    } else {
      val requestModelResult = requestModelResultTry.get
      val parRequestResult = mahaService.executeRequestModelResult(registryName, requestModelResult, mahaRequestLogHelper)

      val errParFunction: ParFunction[GeneralError, Unit] = ParFunction.fromScala(callOnFailureFn(mahaRequestLogHelper, reportingRequest))
      val validationParFunction: ParFunction[RequestResult, Either[GeneralError, RequestResult]] =
      ParFunction.fromScala(
        (result: RequestResult) => {
          try {
            requestResultValidationFn.foreach(_ (result))
            new Right(result)
          } catch {
            case e: Exception =>
              GeneralError.either("resultValidation", e.getMessage, e)
          }
        }
      )
      val successParFunction: ParFunction[RequestResult, Unit] =
      ParFunction.fromScala(
        (result: RequestResult) => {
          try {
            onSuccessFn.foreach(_ (requestModelResult.model, result))
            mahaRequestLogHelper.logSuccess()
            mahaServiceMonitor.stop(reportingRequest)
          } catch {
            case e: Exception =>
              logger.error("Failed while calling onSuccessFn", e)
              mahaRequestLogHelper.logFailed(e.getMessage)
              mahaServiceMonitor.stop(reportingRequest)
          }
        }
      )
      parRequestResult.prodRun.map[RequestResult]("resultValidation", validationParFunction)
        .fold[Unit](errParFunction, successParFunction)
    }
  }

  private[this] def callOnFailureFn(mahaRequestLogHelper: MahaRequestLogHelper, reportingRequest: ReportingRequest)(err: GeneralError) : Unit = {
    try {
      onFailureFn.foreach(_ (err))
    } catch {
      case e: Exception =>
        logger.error("Failed while calling onFailureFn", e)
    } finally {
      mahaRequestLogHelper.logFailed(err.message)
      mahaServiceMonitor.stop(reportingRequest)
    }
  }

  override def withRequestModelValidator(fn: (RequestModelResult) => Unit): Unit =  {
    requestModelValidationFn = Some(fn)
  }

  override def withRequestResultValidator(fn: (RequestResult) => Unit): Unit =  {
    requestResultValidationFn = Some(fn)
  }

}