// Copyright 2018, Oath Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.

package com.yahoo.maha.worker

import com.yahoo.maha.service.{DefaultMahaService, MahaService, MahaServiceConfig}
import com.yahoo.maha.worker.AckStatus.AckStatus
import com.yahoo.maha.worker.proto.MahaWorkerReportingProto.{MahaCustomReportRequest, OutputFormat, QueryEngine, ReportType}
import com.yahoo.maha.worker.request.{MahaWorkerProtoParser, MahaWorkerRequest}
import com.yahoo.maha.worker.state.WorkerStateReporter
import grizzled.slf4j.Logging

/*
    Created by pranavbhole on 8/13/18
*/

object AckStatus extends Enumeration {
  type AckStatus = Value
  val PROCESSED,    // This means processing is complete (success or failed) and message should be acknowledged

  RETRY,            // This means message should be retries. For Kafka, it means ack and reenqueue, for PULSAR it means don't ack

  DUPLICATE = Value // This means the job is already being processed by another worker.
  // For Kafka, this occurs during rebalance and the message ids are different. So the duplicate message should be acked.
  // For PULSAR, this occurs when unacked messages are redelivered, in which case the message ids are same and the duplicate message shouldn't be acked else the job can't be retried.
}

case class WorkerConfig(mahaServiceConfig: MahaServiceConfig,
                        mahaWorkerProtoParser: MahaWorkerProtoParser,
                        supportedEngines: Set[QueryEngine]
                     // deliveryConfig:
                     // jobMetaConfig:
                     )

trait BaseMahaWorker {
  def workerConfig: WorkerConfig
  def workerStateReporter: WorkerStateReporter
  def initializeWork(inputProtoRequest: MahaCustomReportRequest) : MahaWorkerRequest
}

abstract class Worker(workerConfig: WorkerConfig
                      , workerStateReporter: WorkerStateReporter
                     ) extends Logging {

  val JOB_ID = "JobID"

  val mahaService:MahaService = DefaultMahaService(workerConfig.mahaServiceConfig)

  def work(getData: Array[Byte]): AckStatus

  val mahaWorkerProtoParser = workerConfig.mahaWorkerProtoParser

  def initializeWork(inputProtoRequest: MahaCustomReportRequest) : MahaWorkerRequest = {

    //Input Proto  Request Validation
    //Mandatory fields
    if(!mahaService.isValidRegistry(inputProtoRequest.getRegistryName)) {
      throw new IllegalArgumentException(s"Unknown registry, failed to find the given registry ${inputProtoRequest.getRegistryName} in mahaServiceConfig")
    }
    if(!inputProtoRequest.hasSchema) {
      throw new IllegalArgumentException(s"Failed to find the Schema in MahaCustomReportRequest proto ${inputProtoRequest}")
    }
    if(!inputProtoRequest.hasJobId) {
      throw new IllegalArgumentException(s"Failed to find the jobID in MahaCustomReportRequest proto ${inputProtoRequest}")
    }
    if(!inputProtoRequest.hasRawRequest) {
      throw new IllegalArgumentException(s"Failed to find requestJson/rawRequest in MahaCustomReportRequest proto ${inputProtoRequest}")
    }
    if(!inputProtoRequest.hasDeliveryMethod) {
      throw new IllegalArgumentException(s"Failed to find DeliveryMethod in MahaCustomReportRequest proto ${inputProtoRequest}")
    }
    if(!inputProtoRequest.hasOutputFormat) { // Is it good to default this to CSV ?
      throw new IllegalArgumentException(s"Failed to find OutputFormat in MahaCustomReportRequest proto ${inputProtoRequest}")
    }

    //Optional good to have fields
    if(!inputProtoRequest.hasQueueType) {
      warn(s"QueueType is not populated in the MahaCustomReportRequest ${inputProtoRequest}")
    }
    if(!inputProtoRequest.hasUserId) {
      warn(s"UserId is not populated in the MahaCustomReportRequest ${inputProtoRequest}")
    }
    if(!inputProtoRequest.hasRequestSubmittedTime) {
      warn(s"RequestSubmittedTime is not populated in the MahaCustomReportRequest ${inputProtoRequest}")
    }

    val engine = inputProtoRequest.getQueryEngine
    val schemaOption = mahaWorkerProtoParser.schemaProvider(inputProtoRequest.getSchema)
    require(schemaOption.isDefined, s"CustomReportRequest does not contain schema: $inputProtoRequest")

    if(!workerConfig.supportedEngines(engine)) {
      warn(s"Unsupported worker engine=$engine jobId=${inputProtoRequest.getJobId}")
    }

    mahaWorkerProtoParser.parseProto(inputProtoRequest)
  }

}
