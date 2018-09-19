// Copyright 2018, Yahoo Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.

package com.yahoo.maha.worker

import com.yahoo.maha.core.Engine
import com.yahoo.maha.service.{DefaultMahaService, MahaService, MahaServiceConfig}
import com.yahoo.maha.worker.AckStatus.AckStatus
import com.yahoo.maha.worker.proto.MahaWorkerReportingProto.{MahaCustomReportRequest}
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
                        supportedEngines: Set[Engine]
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
                     ) extends BaseMahaWorker with Logging {

  val mahaService:MahaService = DefaultMahaService(workerConfig.mahaServiceConfig)

  def work(getData: Array[Byte]): AckStatus

  val mahaWorkerProtoParser = workerConfig.mahaWorkerProtoParser

  def initializeWork(inputProtoRequest: MahaCustomReportRequest) : MahaWorkerRequest = {

    //Input Proto  Request Validation
    /*
    Required Fields in proto
    required bytes raw_request = 1;
    required DeliveryMethod delivery_method = 2;
    required QueryEngine query_engine = 3;
    required OutputFormat output_format = 4;
    required int64 job_id = 5;
    required string schema = 6;
    required ReportType report_type = 7;
     */

    require(mahaService.isValidRegistry(inputProtoRequest.getRegistryName), s"Unknown registry, failed to find the given registry ${inputProtoRequest.getRegistryName} in mahaServiceConfig")

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

    val schemaOption = mahaWorkerProtoParser.schemaProvider(inputProtoRequest.getSchema)
    require(schemaOption.isDefined, s"CustomReportRequest does not contain correct schema ${inputProtoRequest.getSchema}: $inputProtoRequest")

    val engineOption = Engine.from(inputProtoRequest.getQueryEngine.toString)

    require(engineOption.isDefined, s"Failed to decode the engine value = ${inputProtoRequest.getQueryEngine.toString} from proto to core Engine")

    val engine = engineOption.get

    require(workerConfig.supportedEngines.contains(engine), s"Unsupported worker engine=$engine jobId=${inputProtoRequest.getJobId}")

    mahaWorkerProtoParser.parseProto(inputProtoRequest)
  }

}
