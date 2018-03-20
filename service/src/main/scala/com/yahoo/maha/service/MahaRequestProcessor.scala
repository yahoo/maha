// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service

import com.google.protobuf.ByteString
import com.yahoo.maha.core.bucketing.BucketParams
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.core.{RequestModel, RequestModelResult}
import com.yahoo.maha.parrequest.GeneralError
import com.yahoo.maha.proto.MahaRequestLog.MahaRequestProto
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import grizzled.slf4j.Logging

import scala.util.{Success, Failure, Try}

trait BaseMahaRequestProcessor {
  def process(bucketParams: BucketParams,
              reportingRequest: ReportingRequest,
              rawJson: Array[Byte]): MahaRequestProto.Builder
  def onSuccess(fn: (RequestModel, RequestResult) => Unit)
  def onFailure(fn: (GeneralError) => Unit)

  //Optional model/result validation Functional Traits
  def withRequestModelValidator(fn: (Try[RequestModelResult]) => Unit)
  def withRequestResultValidator(fn: (Try[RequestResult]) => Unit)
  def mahaServiceMonitor : MahaServiceMonitor
}

case class MahaRequestProcessor (registryName: String,
                                 mahaService: MahaService,
                                 mahaServiceMonitor : MahaServiceMonitor = DefaultMahaServiceMonitor,
                                 processingLabel : String  = MahaServiceConstants.MahaRequestLabel) extends BaseMahaRequestProcessor with Logging {

  var onSuccessFn: Option[(RequestModel, RequestResult) => Unit] = None
  var onFailureFn: Option[GeneralError => Unit] = None

  //Optional Post Operation Functional Traits
  /*
   Defines the validation steps for Request Model Result Success and Failure handling
  */
  var requestModelValidationFn: Option[(Try[RequestModelResult]) => Unit] = None

  /*
   Defines the validation steps for Request Result Success and Failure handling
 */
  var requestResultValidationFn : Option[(Try[RequestResult]) => Unit] = None

  def onSuccess(fn: (RequestModel, RequestResult) => Unit) : Unit = {
    onSuccessFn = Some(fn)
  }

  def onFailure(fn: (GeneralError) => Unit) : Unit = {
    onFailureFn = Some(fn)
  }

  def process(bucketParams: BucketParams,
               reportingRequest: ReportingRequest,
               rawJson: Array[Byte]) : MahaRequestProto.Builder = {

    require(onSuccessFn.isDefined || onFailureFn.isDefined, "Nothing to do after processing!")
    val mahaRequestLogHelper = MahaRequestLogHelper(registryName, mahaService)
    mahaRequestLogHelper.init(reportingRequest, None, MahaRequestProto.RequestType.SYNC, ByteString.copyFrom(rawJson))
    //Starting Service Monitor
    mahaServiceMonitor.start(reportingRequest)

    val requestModelResultTry: Try[RequestModelResult] = mahaService.generateRequestModel(registryName, reportingRequest, bucketParams , mahaRequestLogHelper)
    // Custom validation for RequestModel
    val requestModelValidationTry = Try(requestModelValidationFn.foreach(_ (requestModelResultTry)))
    requestModelValidationTry match {
      case Failure(err) =>
        onFailureFn.foreach(_ (GeneralError.from(processingLabel, err.getMessage, err)))
        mahaRequestLogHelper.logFailed(err.getMessage)
      case _ =>
    }

    if(requestModelResultTry.isSuccess && requestModelValidationTry.isSuccess) {
      val requestModelResult = requestModelResultTry.get
      val requestResultTry = mahaService.processRequestModel(registryName, requestModelResult.model, mahaRequestLogHelper)

      // Custom validation for RequestResult
      val requestResultValidationTry = Try(requestResultValidationFn.foreach(_ (requestResultTry)))
      requestResultValidationTry match {
        case Failure(err) =>
          onFailureFn.foreach(_ (GeneralError.from(processingLabel, err.getMessage, err)))
          mahaRequestLogHelper.logFailed(err.getMessage)
        case _ =>
      }

      if (requestResultValidationTry.isSuccess) {
        requestResultTry match {
          case Success(result) =>
            onSuccessFn.foreach(_ (requestModelResult.model, result))
            mahaRequestLogHelper.logSuccess()
          case Failure(err) =>
            onFailureFn.foreach(_ (GeneralError.from(processingLabel, requestModelResultTry.failed.get.getMessage, requestModelResultTry.failed.get)))
            mahaRequestLogHelper.logFailed(err.getMessage)
        }
      }

    } else {
      //construct general error
      onFailureFn.foreach(_ (GeneralError.from(processingLabel, requestModelResultTry.failed.get.getMessage, requestModelResultTry.failed.get)))
      mahaRequestLogHelper.logFailed(requestModelResultTry.failed.get.getMessage)
    }
    info(s"Logging ${mahaRequestLogHelper.protoBuilder} to kafka")
    //Stopping Service Monitor
    mahaServiceMonitor.stop

    mahaService.mahaRequestLogWriter.write(mahaRequestLogHelper.protoBuilder)
    mahaRequestLogHelper.protoBuilder
  }

  override def withRequestModelValidator(fn: (Try[RequestModelResult]) => Unit): Unit =  {
    requestModelValidationFn = Some(fn)
  }

  override def withRequestResultValidator(fn: (Try[RequestResult]) => Unit): Unit =  {
    requestResultValidationFn = Some(fn)
  }

}