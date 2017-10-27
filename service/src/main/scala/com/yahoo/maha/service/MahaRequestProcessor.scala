// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service

import com.google.protobuf.ByteString
import com.yahoo.maha.core.{RequestModel, RequestModelResult}
import com.yahoo.maha.core.bucketing.BucketParams
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.parrequest.GeneralError
import com.yahoo.maha.parrequest.future.ParFunction
import com.yahoo.maha.proto.MahaRequestLog.MahaRequestProto
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import grizzled.slf4j.Logging

import scala.util.Try

case class MahaRequestProcessor (registryName: String,
                                 bucketParams: BucketParams,
                                 reportingRequest: ReportingRequest,
                                 mahaService: MahaService,
                                 rawJson: Array[Byte]) extends Logging {

  var onSuccessFn: Option[(RequestModel, RequestResult) => Unit] = None
  var onFailureFn: Option[GeneralError => Unit] = None

  def onSuccess(fn: (RequestModel, RequestResult) => Unit) : Unit = {
    onSuccessFn = Some(fn)
  }

  def onFailure(fn: (GeneralError) => Unit) : Unit = {
    onFailureFn = Some(fn)
  }

  def process() : MahaRequestProto.Builder = {

    require(onSuccessFn.isDefined || onFailureFn.isDefined, "Nothing to do after processing!")
    val mahaRequestLogHelper = MahaRequestLogHelper(registryName, mahaService)
    mahaRequestLogHelper.init(reportingRequest, None, MahaRequestProto.RequestType.SYNC, ByteString.copyFrom(rawJson))

    val requestModelResultTry: Try[RequestModelResult] = mahaService.generateRequestModel(registryName, reportingRequest, bucketParams , mahaRequestLogHelper)

    if(requestModelResultTry.isSuccess) {
      val requestModelResult = requestModelResultTry.get
      val parRequestResult: ParRequestResult = mahaService.executeRequestModelResult(registryName, requestModelResult, mahaRequestLogHelper)
      val errParFunction: ParFunction[GeneralError, Unit] =
        (err: GeneralError) => {
          onFailureFn.foreach(_ (err))
          mahaRequestLogHelper.logFailed(err.message)
        }
      val successParFunction: ParFunction[RequestResult, Unit] =
        (result: RequestResult) => {
          onSuccessFn.foreach(_ (requestModelResult.model, result))
          mahaRequestLogHelper.logSuccess()
        }
      parRequestResult.prodRun.fold[Unit](errParFunction, successParFunction)
    } else {
      //construct general error
      onFailureFn.foreach(_ (GeneralError.from("", requestModelResultTry.failed.get.getMessage, requestModelResultTry.failed.get)))
      mahaRequestLogHelper.logFailed(requestModelResultTry.failed.get.getMessage)
    }
    info(s"Logging ${mahaRequestLogHelper.protoBuilder} to kafka")
    mahaService.mahaRequestLogWriter.write(mahaRequestLogHelper.protoBuilder)
    mahaRequestLogHelper.protoBuilder
  }

}