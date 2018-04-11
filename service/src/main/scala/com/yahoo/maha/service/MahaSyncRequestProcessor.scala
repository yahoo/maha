// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service

import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.parrequest2.GeneralError
import com.yahoo.maha.parrequest2.future.ParFunction
import com.yahoo.maha.service.curators.{CuratorResult, DefaultCurator}
import com.yahoo.maha.service.utils.{MahaRequestLogHelper, MahaRequestLogWriter}
import grizzled.slf4j.Logging

trait BaseMahaRequestProcessor {
  def mahaRequestContext: MahaRequestContext
  def requestCoordinator: RequestCoordinator
  def mahaServiceMonitor : MahaServiceMonitor
  def mahaRequestLogHelperOption: Option[MahaRequestLogHelper]

  def process(): Unit
  def onSuccess(fn: (IndexedSeq[CuratorResult]) => Unit)
  def onFailure(fn: (GeneralError) => Unit)

}

case class MahaSyncRequestProcessorFactory(requestCoordinator: RequestCoordinator
                                           , mahaService: MahaService
                                           , mahaRequestLogWriter: MahaRequestLogWriter
                                           , mahaServiceMonitor: MahaServiceMonitor= DefaultMahaServiceMonitor) {
  def create(mahaRequestContext: MahaRequestContext, processingLabel: String, mahaRequestLogHelper: MahaRequestLogHelper) : MahaSyncRequestProcessor = {
    MahaSyncRequestProcessor(mahaRequestContext
      , requestCoordinator, mahaRequestLogWriter, mahaServiceMonitor, processingLabel, Option(mahaRequestLogHelper))
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
                                    , mahaRequestLogHelperOption:Option[MahaRequestLogHelper] = None
                               ) extends BaseMahaRequestProcessor with Logging {
  private[this] val mahaRequestLogHelper = if(mahaRequestLogHelperOption.isEmpty) {
     MahaRequestLogHelper(mahaRequestContext, mahaRequestLogWriter)
  } else {
    mahaRequestLogHelperOption.get
  }

  private[this] var onSuccessFn: Option[IndexedSeq[CuratorResult] => Unit] = None
  private[this] var onFailureFn: Option[GeneralError => Unit] = None

  def onSuccess(fn: (IndexedSeq[CuratorResult]) => Unit) : Unit = {
    onSuccessFn = Some(fn)
  }

  def onFailure(fn: (GeneralError) => Unit) : Unit = {
    onFailureFn = Some(fn)
  }

  def process() : Unit = {

    mahaServiceMonitor.start(mahaRequestContext.reportingRequest)
    require(onSuccessFn.isDefined || onFailureFn.isDefined, "Nothing to do after processing!")

    val requestCoordinatorResultEither: Either[GeneralError, RequestCoordinatorResult] = requestCoordinator.execute(mahaRequestContext, mahaRequestLogHelper)

    val errParFunction: ParFunction[GeneralError, Unit] = ParFunction.fromScala(callOnFailureFn(mahaRequestLogHelper, mahaRequestContext.reportingRequest))

    val successParFunction: ParFunction[java.util.List[Option[CuratorResult]], Unit] =
      ParFunction.fromScala(
        (javaResult: java.util.List[Option[CuratorResult]]) => {
          import collection.JavaConverters._
          val seq = javaResult.asScala.toIndexedSeq.flatten
          val firstFailure = seq.find(_.requestResultTry.isFailure)
          if(firstFailure.isDefined && firstFailure.get.curator.name == DefaultCurator.name) {
            val message = "Failed to execute the query pipeline"
            val generalError = GeneralError.from(message, processingLabel, firstFailure.get.requestResultTry.failed.get)
            callOnFailureFn(mahaRequestLogHelper,mahaRequestContext.reportingRequest)(generalError)
          } else {
            try {
              onSuccessFn.foreach(_ (seq))
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

    requestCoordinatorResultEither.fold({
      err: GeneralError =>
        callOnFailureFn(mahaRequestLogHelper, mahaRequestContext.reportingRequest)(err)
    }, {
      (requestCoordinatorResult: RequestCoordinatorResult) =>
        requestCoordinatorResult.combinedResultList.fold[Unit](errParFunction, successParFunction)
    })
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