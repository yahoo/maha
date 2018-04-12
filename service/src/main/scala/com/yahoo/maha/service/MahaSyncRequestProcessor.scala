// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service

import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.parrequest2.GeneralError
import com.yahoo.maha.parrequest2.future.ParFunction
import com.yahoo.maha.service.curators.CuratorResult
import com.yahoo.maha.service.utils.{MahaRequestLogBuilder, MahaRequestLogHelper, MahaRequestLogWriter}
import grizzled.slf4j.Logging

trait BaseMahaRequestProcessor {
  def mahaRequestContext: MahaRequestContext
  def requestCoordinator: RequestCoordinator
  def mahaServiceMonitor : MahaServiceMonitor
  def mahaRequestLogBuilderOption: Option[MahaRequestLogBuilder]

  def process(): Unit
  def onSuccess(fn: (IndexedSeq[CuratorResult]) => Unit)
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

  private[this] var onSuccessFn: Option[IndexedSeq[CuratorResult] => Unit] = None
  private[this] var onFailureFn: Option[GeneralError => Unit] = None

  def onSuccess(fn: (IndexedSeq[CuratorResult]) => Unit) : Unit = {
    onSuccessFn = Some(fn)
  }

  def onFailure(fn: (GeneralError) => Unit) : Unit = {
    onFailureFn = Some(fn)
  }

  def process() {
    mahaServiceMonitor.start(mahaRequestContext.reportingRequest)
    require(onSuccessFn.isDefined || onFailureFn.isDefined, "Nothing to do after processing!")

    val requestCoordinatorResultEither: Either[GeneralError, RequestCoordinatorResult] = requestCoordinator.execute(mahaRequestContext, mahaRequestLogBuilder)

    val errParFunction: ParFunction[GeneralError, Unit] = ParFunction.fromScala(callOnFailureFn(mahaRequestLogBuilder, mahaRequestContext.reportingRequest))

    val successParFunction: ParFunction[java.util.List[Either[GeneralError, CuratorResult]], Unit] =
      ParFunction.fromScala(
        (javaResult: java.util.List[Either[GeneralError, CuratorResult]]) => {
          import collection.JavaConverters._
          val seq = javaResult.asScala.toIndexedSeq
          val firstFailure = seq.head
          if(firstFailure.isLeft) {
            val message = "Failed to execute the query pipeline"
            val generalError = {
              if(firstFailure.left.get.throwableOption.isDefined) {
                GeneralError.from(processingLabel, message, firstFailure.left.get.throwableOption.get)
              } else {
                GeneralError.from(processingLabel, message)
              }
            }
            callOnFailureFn(mahaRequestLogBuilder,mahaRequestContext.reportingRequest)(generalError)
          } else {
            try {
              onSuccessFn.foreach(_ (seq.view.filter(_.isRight).map(_.right.get).toIndexedSeq))
              mahaRequestLogBuilder.logSuccess()
              mahaServiceMonitor.stop(mahaRequestContext.reportingRequest)
            } catch {
              case e: Exception =>
                val message = s"Failed while calling onSuccessFn ${e.getMessage}"
                logger.error(message, e)
                mahaRequestLogBuilder.logFailed(message)
                mahaServiceMonitor.stop(mahaRequestContext.reportingRequest)
            }
          }
        }
      )

    requestCoordinatorResultEither.fold({
      err: GeneralError =>
        callOnFailureFn(mahaRequestLogBuilder, mahaRequestContext.reportingRequest)(err)
    }, {
      (requestCoordinatorResult: RequestCoordinatorResult) =>
        requestCoordinatorResult.combinedResultList.fold[Unit](errParFunction, successParFunction)
    })
  }

  private[this] def callOnFailureFn(mahaRequestLogBuilder: MahaRequestLogBuilder, reportingRequest: ReportingRequest)(err: GeneralError) : Unit = {
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