// Copyright 2018, Oath Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.

package com.yahoo.maha.worker.request

import java.io.File

import com.yahoo.maha.core.Schema
import com.yahoo.maha.core.request.{BaseRequest, Parameter, ReportingRequest}
import com.yahoo.maha.worker.proto.MahaWorkerReportingProto.{MahaCustomReportRequest, OutputFormat, QueryEngine, ReportType}
import grizzled.slf4j.Logging
import org.apache.commons.lang3.StringUtils

/*
    Created by pranavbhole on 8/14/18
*/

/*
   Trait which defines the input ReportingRequest for Maha-Worker
   so that MahaCustomCustomReportRequest can be mapped to any ReportingRequest extension
 */
trait MahaWorkerRequest {
  def requestType: ReportType
  def reportingRequest: ReportingRequest
  def mahaCustomReportRequest: MahaCustomReportRequest
  def reportFile: File
  def registryName: String = mahaCustomReportRequest.getRegistryName // putting variables on top for accessibility
  def jobId: Long = mahaCustomReportRequest.getJobId
}

/*
   Default Adhoc Asynchronous Reporting Request
 */
case class AsyncAdhocMahaWorkerRequest(reportingRequest: ReportingRequest,
                                       mahaCustomReportRequest: MahaCustomReportRequest,
                                       reportFile: File) extends MahaWorkerRequest {
  val requestType:ReportType = ReportType.AD_HOC
}

/*
   Default Scheduled Asynchronous Reporting Request
 */
case class AsyncScheduledMahaWorkerRequest(firstName: String,
                                           lastName: String,
                                           description: String,
                                           userId: String,
                                           email: String,
                                           schema: Schema,
                                           reportingRequest: ReportingRequest,
                                           mahaCustomReportRequest: MahaCustomReportRequest,
                                           reportFile: File) extends MahaWorkerRequest {
  val requestType:ReportType = ReportType.SCHEDULED
}


trait MahaWorkerProtoParser {
  def requestTypeToSerDeMap : Map[ReportType, BaseRequest]
  def schemaProvider : (String => Option[Schema])
  def parseProto(mahaCustomReportRequest: MahaCustomReportRequest) : MahaWorkerRequest

  def validate(): Unit = {
    require(requestTypeToSerDeMap.nonEmpty, "RequestType to deserializer map can not be empty")
    require(requestTypeToSerDeMap.contains(ReportType.AD_HOC), "Failed to find the deserializer for adhoc request")
    require(requestTypeToSerDeMap.contains(ReportType.SCHEDULED), "Failed to find the deserializer for scheduled request")
  }
}

/*
  Default Implementation of MahaWorkerProtoParser
  used to deserialize proto request to Reporting Request
  schemaProvider is function to deserialize string to Schema
 */
case class DefaultMahaWorkerProtoParser(requestTypeToSerDeMap: Map[ReportType, BaseRequest],
                                        schemaProvider : (String => Option[Schema])) extends MahaWorkerProtoParser with Logging {
  validate()

  override def parseProto(mahaCustomReportRequest: MahaCustomReportRequest): MahaWorkerRequest = {

    val schemaOption = schemaProvider(mahaCustomReportRequest.getSchema)
    require(schemaOption.isDefined, s"unrecognized schema in the inputProtoRequest ${mahaCustomReportRequest.getSchema}")
    val schema = schemaOption.get

    val reportFile: File = generateReportLocationOnDisk(mahaCustomReportRequest.getOutputFormat, mahaCustomReportRequest.getJobId)

    mahaCustomReportRequest.getReportType match {
      case scheduled@ReportType.SCHEDULED =>
        val requestDeserializer = requestTypeToSerDeMap.get(scheduled).get
        // to be replaced with the Schedule Request Deserializer
        val scheduledRequestResult = requestDeserializer.deserializeAsync(mahaCustomReportRequest.getRawRequest.toByteArray, schema)
        require(scheduledRequestResult.isSuccess, s"Cannot deserialize Scheduled ReportRequest $scheduledRequestResult")

        val scheduledRequest = enforceEngineAndTimezone(requestDeserializer,
          scheduledRequestResult.toOption.get,
          mahaCustomReportRequest.getQueryEngine,
          mahaCustomReportRequest.getTimezone)

        // TODO fill the hardcoded fields once deserializer is ready
        AsyncScheduledMahaWorkerRequest("first name",
          "last name",
          "description",
          mahaCustomReportRequest.getUserId,
          "email",
          schema,
          scheduledRequest,
          mahaCustomReportRequest,
          reportFile)
      case adhoc@ReportType.AD_HOC =>
        val requestDeserializer = requestTypeToSerDeMap.get(adhoc).get
        val reportingRequestEither = requestDeserializer.deserializeAsync(mahaCustomReportRequest.getRawRequest.toByteArray, schema, None)
        require(reportingRequestEither.isSuccess, s"Cannot deserialize reportingRequest $reportingRequestEither")

        val reportingRequest = enforceEngineAndTimezone(requestDeserializer,
          reportingRequestEither.toOption.get,
          mahaCustomReportRequest.getQueryEngine,
          mahaCustomReportRequest.getTimezone)

        AsyncAdhocMahaWorkerRequest(reportingRequest, mahaCustomReportRequest, reportFile)
      case a => throw new IllegalArgumentException(s"Unknown request type: $a")
    }
  }

  protected def enforceEngineAndTimezone(requestDeserializer: BaseRequest, reportingRequest: ReportingRequest, queryEngine: QueryEngine, timezone: String) : ReportingRequest = {
    val reportingRequestForceEngine = queryEngine match {
      case QueryEngine.ORACLE =>
        ReportingRequest.forceOracle(reportingRequest)
      case QueryEngine.HIVE =>
        ReportingRequest.forceHive(reportingRequest)
      case QueryEngine.DRUID =>
        ReportingRequest.forceDruid(reportingRequest)
      case QueryEngine.PRESTO =>
        ReportingRequest.forcePresto(reportingRequest)
      case a =>
        throw new UnsupportedOperationException(s"Unknown query engine : $a")
    }

    val reportingRequestWithEngineTimeZone: ReportingRequest = {
      if ( StringUtils.isNotBlank(timezone)) {
        val reportingRequestWithTimeZone = requestDeserializer.withTimeZone(reportingRequestForceEngine, timezone)
        info(s"Updated timezone: ${reportingRequestWithTimeZone.additionalParameters.get(Parameter.TimeZone)}")
        reportingRequestWithTimeZone
      } else {
        info(s"Timezone is null or empty")
        reportingRequestForceEngine
      }
    }
    reportingRequestWithEngineTimeZone
  }

  private def generateReportLocationOnDisk(outputFormat: OutputFormat, jobId: Long): File = {
    outputFormat match {
      case OutputFormat.CSV =>
        File.createTempFile(s"report-$jobId-", ".csv")
      case OutputFormat.JSON =>
        File.createTempFile(s"report-$jobId-", ".json")
      case a =>
        throw new UnsupportedOperationException(s"Unknown output format: $outputFormat")
    }
  }
}

object DefaultMahaWorkerProtoParser {

  /*
  Default Implementation of Deserializer of Reporting Request
   */
  val defaultRequestTypeToSerDeMap: Map[ReportType, BaseRequest] = Map(
      ReportType.AD_HOC-> ReportingRequest,
      ReportType.SCHEDULED -> ReportingRequest // TODO Write default Scheduled request deserializer
  )

  /*
  Default Implementation of Schema Provider Function
   */
  val defaultSchemaProvider : (String => Option[Schema]) = (schemaStr) => {
    Schema.withNameInsensitiveOption(schemaStr)
  }
}