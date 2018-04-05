// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service

import com.google.protobuf.ByteString
import com.yahoo.maha.core.RequestModel
import com.yahoo.maha.core.bucketing.BucketParams
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.parrequest2.GeneralError
import com.yahoo.maha.parrequest2.future.{ParFunction, ParRequest}
import com.yahoo.maha.proto.MahaRequestLog.MahaRequestProto
import com.yahoo.maha.service.curators.CuratorResult
import com.yahoo.maha.service.utils.{MahaRequestLogHelper, MahaRequestLogWriter}
import grizzled.slf4j.Logging

trait BaseMahaRequestProcessor {
  def mahaRequestContext: MahaRequestContext
  def requestCoordinator: RequestCoordinator
  def mahaServiceMonitor : MahaServiceMonitor
  def mahaRequestLogHelperOption: Option[MahaRequestLogHelper]

  def process(bucketParams: BucketParams,
              reportingRequest: ReportingRequest,
              rawJson: Array[Byte]): Unit
  def onSuccess(fn: (RequestModel, RequestResult) => Unit)
  def onFailure(fn: (GeneralError) => Unit)

}

case class MahaRequestProcessorFactory(requestCoordinator: RequestCoordinator
                                       , mahaService: MahaService
                                       , mahaRequestLogWriter: MahaRequestLogWriter
                                       , mahaServiceMonitor: MahaServiceMonitor= DefaultMahaServiceMonitor) {
  def create(mahaRequestContext: MahaRequestContext, processingLabel: String, mahaRequestLogHelper: MahaRequestLogHelper) : MahaRequestProcessor = {
    MahaRequestProcessor(mahaRequestContext
      , requestCoordinator, mahaRequestLogWriter, mahaServiceMonitor, processingLabel, Option(mahaRequestLogHelper))
  }
  def create(mahaRequestContext: MahaRequestContext, processingLabel: String): MahaRequestProcessor = {
    MahaRequestProcessor(mahaRequestContext
      , requestCoordinator, mahaRequestLogWriter, mahaServiceMonitor, processingLabel, None)
  }
}

case class MahaRequestProcessor(mahaRequestContext: MahaRequestContext
                                , requestCoordinator: RequestCoordinator
                                , mahaRequestLogWriter: MahaRequestLogWriter
                                , mahaServiceMonitor : MahaServiceMonitor = DefaultMahaServiceMonitor
                                , processingLabel : String  = MahaServiceConstants.MahaRequestLabel
                                , mahaRequestLogHelperOption:Option[MahaRequestLogHelper] = None
                               ) extends BaseMahaRequestProcessor with Logging {
  private[this] val mahaRequestLogHelper = if(mahaRequestLogHelperOption.isEmpty) {
     MahaRequestLogHelper(mahaRequestContext.registryName, mahaRequestLogWriter)
  } else {
    mahaRequestLogHelperOption.get
  }

  private[this] var onSuccessFn: Option[(RequestModel, RequestResult) => Unit] = None
  private[this] var onFailureFn: Option[GeneralError => Unit] = None

  def onSuccess(fn: (RequestModel, RequestResult) => Unit) : Unit = {
    onSuccessFn = Some(fn)
  }

  def onFailure(fn: (GeneralError) => Unit) : Unit = {
    onFailureFn = Some(fn)
  }

  def process() : Unit = {

    require(onSuccessFn.isDefined || onFailureFn.isDefined, "Nothing to do after processing!")
    mahaRequestLogHelper.init(mahaRequestContext.reportingRequest
      , None, MahaRequestProto.RequestType.SYNC, ByteString.copyFrom(mahaRequestContext.rawJson))
    //Starting Service Monitor
    mahaServiceMonitor.start(mahaRequestContext.reportingRequest)

    val parRequest: ParRequest[CuratorResult] = requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper)

    val errParFunction: ParFunction[GeneralError, Unit] = ParFunction.fromScala(callOnFailureFn(mahaRequestLogHelper, mahaRequestContext.reportingRequest))

    val successParFunction: ParFunction[CuratorResult, Unit] =
      ParFunction.fromScala(
        (curatorResult: CuratorResult) => {
          val model = curatorResult.requestModelReference
          if (curatorResult.requestResultTry.isFailure) {
            val message = "Failed to execute the query pipeline"
            val generalError = GeneralError.from(message, processingLabel, curatorResult.requestResultTry.failed.get)
            callOnFailureFn(mahaRequestLogHelper,mahaRequestContext.reportingRequest)(generalError)
          } else {
            try {
              onSuccessFn.foreach(_ (curatorResult.requestModelReference.model, curatorResult.requestResultTry.get))
              mahaRequestLogHelper.logSuccess()
              mahaServiceMonitor.stop(mahaRequestContext.reportingRequest)
            } catch {
              case e: Exception =>
                val message = s"Failed while calling onSuccessFn ${e.getMessage}"
                logger.error(message, e)
                mahaRequestLogHelper.logFailed(message)
                mahaServiceMonitor.stop(mahaRequestContext.reportingRequest)
            }
          }
        }
      )

    parRequest.fold[Unit](errParFunction, successParFunction)

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
}