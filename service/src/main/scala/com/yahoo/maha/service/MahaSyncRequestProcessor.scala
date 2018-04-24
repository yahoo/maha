// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service

import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.parrequest2.GeneralError
import com.yahoo.maha.parrequest2.future.{ParFunction, ParRequest}
import com.yahoo.maha.service.utils.{MahaRequestLogBuilder, MahaRequestLogHelper, MahaRequestLogWriter}
import grizzled.slf4j.Logging

trait BaseMahaRequestProcessor {
  def mahaRequestContext: MahaRequestContext
  def requestCoordinator: RequestCoordinator
  def mahaServiceMonitor : MahaServiceMonitor
  def mahaRequestLogBuilderOption: Option[MahaRequestLogBuilder]
  def process(): Unit
  def onSuccess(fn: (RequestCoordinatorResult) => Unit)
  def onFailure(fn: (GeneralError) => Unit)
}

case class MahaSyncRequestProcessorFactory(requestCoordinator: RequestCoordinator
                                           , mahaService: MahaService
                                           , mahaRequestLogWriter: MahaRequestLogWriter
                                           , mahaServiceMonitor: MahaServiceMonitor= DefaultMahaServiceMonitor) {
  def create(mahaRequestContext: MahaRequestContext, processingLabel: String, mahaRequestLogBuilder: MahaRequestLogBuilder) : MahaSyncRequestProcessor = {
    MahaSyncRequestProcessor(mahaRequestContext
      , requestCoordinator, mahaRequestLogWriter, mahaServiceMonitor, processingLabel, Option(mahaRequestLogBuilder))
  }
  def create(mahaRequestContext: MahaRequestContext, processingLabel: String): MahaSyncRequestProcessor = {
    MahaSyncRequestProcessor(mahaRequestContext
      , requestCoordinator, mahaRequestLogWriter, mahaServiceMonitor, processingLabel, None)
  }
}

case class MahaSyncRequestProcessor(mahaRequestContext: MahaRequestContext
                                    , requestCoordinator: RequestCoordinator
                                    , mahaRequestLogWriter: MahaRequestLogWriter
                                    , mahaServiceMonitor : MahaServiceMonitor = DefaultMahaServiceMonitor
                                    , processingLabel : String  = MahaServiceConstants.MahaRequestLabel
                                    , mahaRequestLogBuilderOption:Option[MahaRequestLogBuilder] = None
                               ) extends BaseMahaRequestProcessor with Logging {
  private[this] val mahaRequestLogBuilder = if(mahaRequestLogBuilderOption.isEmpty) {
     MahaRequestLogHelper(mahaRequestContext, mahaRequestLogWriter)
  } else {
    mahaRequestLogBuilderOption.get
  }

  private[this] var onSuccessFn: Option[RequestCoordinatorResult => Unit] = None
  private[this] var onFailureFn: Option[GeneralError => Unit] = None

  def onSuccess(fn: (RequestCoordinatorResult) => Unit) : Unit = {
    onSuccessFn = Some(fn)
  }

  def onFailure(fn: (GeneralError) => Unit) : Unit = {
    onFailureFn = Some(fn)
  }

  def process() {
    mahaServiceMonitor.start(mahaRequestContext.reportingRequest)
    require(onSuccessFn.isDefined || onFailureFn.isDefined, "Nothing to do after processing!")

    val requestCoordinatorResultEither: Either[RequestCoordinatorError, ParRequest[RequestCoordinatorResult]] =
      requestCoordinator.execute(mahaRequestContext, mahaRequestLogBuilder)

    val errParFunction: ParFunction[GeneralError, Unit] = ParFunction.fromScala(callOnFailureFn(mahaRequestLogBuilder, mahaRequestContext.reportingRequest))

    requestCoordinatorResultEither.fold({
      err: RequestCoordinatorError =>
        callOnFailureFn(mahaRequestLogBuilder, mahaRequestContext.reportingRequest)(err)
    }, {
      (parRequestResult: ParRequest[RequestCoordinatorResult]) =>
          parRequestResult.fold(errParFunction
            , ParFunction.fromScala {
              requestCoordinatorResult =>
                onSuccessFn.foreach(_ (requestCoordinatorResult))
            })
    })
  }

  private[this] def callOnFailureFn[T<:GeneralError](mahaRequestLogBuilder: MahaRequestLogBuilder, reportingRequest: ReportingRequest)(err: T) : Unit = {
    try {
      onFailureFn.foreach(_ (err))
    } catch {
      case e: Exception =>
        logger.error("Failed while calling onFailureFn", e)
    } finally {
      mahaRequestLogBuilder.logFailed(err.message)
      mahaServiceMonitor.stop(reportingRequest)
    }
  }
}