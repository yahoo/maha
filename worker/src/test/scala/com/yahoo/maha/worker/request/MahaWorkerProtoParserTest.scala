package com.yahoo.maha.worker.request

import com.google.protobuf.ByteString
import com.yahoo.maha.core.request.Parameter.TimeZone
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.service.BaseMahaServiceTest
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.worker.AckStatus
import com.yahoo.maha.worker.proto.MahaWorkerReportingProto._

/*
    Created by pranavbhole on 8/30/18
*/
class MahaWorkerProtoParserTest extends BaseMahaServiceTest {
  import  DefaultMahaWorkerProtoParser._

  val defaultMahaWorkerProtoParser =  DefaultMahaWorkerProtoParser(defaultRequestTypeToSerDeMap, defaultSchemaProvider)

  val jsonRequest = s"""{
                          "cube": "student_performance",
                          "selectFields": [
                            {"field": "Student ID"},
                            {"field": "Class ID"},
                            {"field": "Section ID"},
                            {"field": "Total Marks"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Student ID", "operator": "=", "value": "213"}
                          ]
                        }"""

  test("test proto parser") {

    val mahaCustomReportRequestProto = MahaCustomReportRequest.newBuilder()
        .setRawRequest(ByteString.copyFrom(jsonRequest.getBytes))
        .setDeliveryMethod(DeliveryMethod.CONTENT_STORE)
        .setQueryEngine(QueryEngine.PRESTO)
        .setOutputFormat(OutputFormat.CSV)
        .setJobId(12345)
        .setSchema(StudentSchema.entryName)
        .setReportType(ReportType.AD_HOC)
        .setTimezone("UTC")
        .setDryrun(false)
        .setRevision(1)
        .setRequestSubmittedTime(System.currentTimeMillis())
        .setQueueType(QueueType.KAFKA)
        .setUserId("maha-worker-user")
        .setRegistryName("er")
        .build()

    try {
      val mahaWorkerRequest = defaultMahaWorkerProtoParser.parseProto(mahaCustomReportRequestProto)
      assert(mahaWorkerRequest.jobId == 12345)
      assert(mahaWorkerRequest.requestType == ReportType.AD_HOC)
      assert(mahaWorkerRequest.reportFile.getName.endsWith(".csv"))
      assert(mahaWorkerRequest.registryName == "er")
    } catch {
      case e:Exception=>
        assert(fail(s"Got Exception in parsing proto ${e.getMessage}"))
    }
  }

  test("Validate the proto parser") {

    val exception = intercept[IllegalArgumentException] {
      DefaultMahaWorkerProtoParser(Map.empty, DefaultMahaWorkerProtoParser.defaultSchemaProvider)
    }
    assert(exception.getMessage.contains("RequestType to deserializer map can not be empty"))

  }

  test("Validate the proto parser for valid serde map") {

    val exception = intercept[IllegalArgumentException] {
      DefaultMahaWorkerProtoParser(Map(ReportType.SCHEDULED -> ReportingRequest), DefaultMahaWorkerProtoParser.defaultSchemaProvider)
    }
    assert(exception.getMessage.contains("Failed to find the deserializer for adhoc request"))

  }

  test("Validate the proto parser for valid serde map SCHEDULED") {

    val exception = intercept[IllegalArgumentException] {
      DefaultMahaWorkerProtoParser(Map(ReportType.AD_HOC -> ReportingRequest), DefaultMahaWorkerProtoParser.defaultSchemaProvider)
    }
    assert(exception.getMessage.contains("Failed to find the deserializer for scheduled request"))

  }

  test("Validate the proto parser for correct schema") {

    val exception = intercept[IllegalArgumentException] {

      val mahaCustomReportRequestProto = MahaCustomReportRequest.newBuilder()
        .setRawRequest(ByteString.copyFrom(jsonRequest.getBytes))
        .setDeliveryMethod(DeliveryMethod.CONTENT_STORE)
        .setQueryEngine(QueryEngine.ORACLE)
        .setOutputFormat(OutputFormat.CSV)
        .setJobId(12345)
        .setSchema("Unknown schema")
        .setReportType(ReportType.AD_HOC)
        .setTimezone("UTC")
        .setDryrun(false)
        .setRevision(1)
        .setRequestSubmittedTime(System.currentTimeMillis())
        .setQueueType(QueueType.KAFKA)
        .setUserId("maha-worker-user")
        .setRegistryName("er")
        .build()

      defaultMahaWorkerProtoParser.parseProto(mahaCustomReportRequestProto)

    }
    assert(exception.getMessage.contains("unrecognized schema in the inputProtoRequest"))

  }

  test("With no timezone") {
    val mahaCustomReportRequestProto = MahaCustomReportRequest.newBuilder()
      .setRawRequest(ByteString.copyFrom(jsonRequest.getBytes))
      .setDeliveryMethod(DeliveryMethod.CONTENT_STORE)
      .setQueryEngine(QueryEngine.HIVE)
      .setOutputFormat(OutputFormat.CSV)
      .setJobId(12345)
      .setSchema(StudentSchema.entryName)
      .setReportType(ReportType.AD_HOC)
      //.setTimezone("UTC")
      .setDryrun(false)
      .setRevision(1)
      .setRequestSubmittedTime(System.currentTimeMillis())
      .setQueueType(QueueType.KAFKA)
      .setUserId("maha-worker-user")
      .setRegistryName("er")
      .build()
    val workerRequest = defaultMahaWorkerProtoParser.parseProto(mahaCustomReportRequestProto)
    assert(!workerRequest.reportingRequest.additionalParameters.contains(TimeZone))
  }

  test("With Json Output Format") {
    val mahaCustomReportRequestProto = MahaCustomReportRequest.newBuilder()
      .setRawRequest(ByteString.copyFrom(jsonRequest.getBytes))
      .setDeliveryMethod(DeliveryMethod.CONTENT_STORE)
      .setQueryEngine(QueryEngine.ORACLE)
      .setOutputFormat(OutputFormat.JSON)
      .setJobId(12345)
      .setSchema(StudentSchema.entryName)
      .setReportType(ReportType.AD_HOC)
      .setTimezone("UTC")
      .setDryrun(false)
      .setRevision(1)
      .setRequestSubmittedTime(System.currentTimeMillis())
      .setQueueType(QueueType.KAFKA)
      .setUserId("maha-worker-user")
      .setRegistryName("er")
      .build()
    val workerRequest = defaultMahaWorkerProtoParser.parseProto(mahaCustomReportRequestProto)
    assert(workerRequest.reportFile.getName.endsWith("json"))
  }

  test("With Json Output Format and druid engine") {
    val mahaCustomReportRequestProto = MahaCustomReportRequest.newBuilder()
      .setRawRequest(ByteString.copyFrom(jsonRequest.getBytes))
      .setDeliveryMethod(DeliveryMethod.CONTENT_STORE)
      .setQueryEngine(QueryEngine.DRUID)
      .setOutputFormat(OutputFormat.JSON)
      .setJobId(12345)
      .setSchema(StudentSchema.entryName)
      .setReportType(ReportType.AD_HOC)
      .setTimezone("UTC")
      .setDryrun(false)
      .setRevision(1)
      .setRequestSubmittedTime(System.currentTimeMillis())
      .setQueueType(QueueType.KAFKA)
      .setUserId("maha-worker-user")
      .setRegistryName("er")
      .build()
    val workerRequest = defaultMahaWorkerProtoParser.parseProto(mahaCustomReportRequestProto)
    assert(workerRequest.reportFile.getName.endsWith("json"))
  }


  test("With Bad ReportingRequest") {
    val mahaCustomReportRequestProto = MahaCustomReportRequest.newBuilder()
      .setRawRequest(ByteString.copyFrom(jsonRequest.replaceFirst("cube", "_cube").getBytes))
      .setDeliveryMethod(DeliveryMethod.CONTENT_STORE)
      .setQueryEngine(QueryEngine.DRUID)
      .setOutputFormat(OutputFormat.JSON)
      .setJobId(12345)
      .setSchema(StudentSchema.entryName)
      .setReportType(ReportType.AD_HOC)
      .setTimezone("UTC")
      .setDryrun(false)
      .setRevision(1)
      .setRequestSubmittedTime(System.currentTimeMillis())
      .setQueueType(QueueType.KAFKA)
      .setUserId("maha-worker-user")
      .setRegistryName("er")
      .build()
    val exception = intercept[IllegalArgumentException] {
      val workerRequest = defaultMahaWorkerProtoParser.parseProto(mahaCustomReportRequestProto)
    }
    assert(exception.getMessage.contains("requirement failed: Cannot deserialize reportingRequest"))
  }

  test("With Bad Scheduled ReportingRequest") {
    val mahaCustomReportRequestProto = MahaCustomReportRequest.newBuilder()
      .setRawRequest(ByteString.copyFrom(jsonRequest.replaceFirst("cube", "_cube").getBytes))
      .setDeliveryMethod(DeliveryMethod.CONTENT_STORE)
      .setQueryEngine(QueryEngine.DRUID)
      .setOutputFormat(OutputFormat.JSON)
      .setJobId(12345)
      .setSchema(StudentSchema.entryName)
      .setReportType(ReportType.SCHEDULED)
      .setTimezone("UTC")
      .setDryrun(false)
      .setRevision(1)
      .setRequestSubmittedTime(System.currentTimeMillis())
      .setQueueType(QueueType.KAFKA)
      .setUserId("maha-worker-user")
      .setRegistryName("er")
      .build()
    val exception = intercept[IllegalArgumentException] {
      val workerRequest = defaultMahaWorkerProtoParser.parseProto(mahaCustomReportRequestProto)
    }
    assert(exception.getMessage.contains("Cannot deserialize Scheduled ReportRequest"))
  }

  test("With scheduled reportType") {
    val mahaCustomReportRequestProto = MahaCustomReportRequest.newBuilder()
      .setRawRequest(ByteString.copyFrom(jsonRequest.getBytes))
      .setDeliveryMethod(DeliveryMethod.CONTENT_STORE)
      .setQueryEngine(QueryEngine.ORACLE)
      .setOutputFormat(OutputFormat.CSV)
      .setJobId(12345)
      .setSchema(StudentSchema.entryName)
      .setReportType(ReportType.SCHEDULED)
      .setTimezone("UTC")
      .setDryrun(false)
      .setRevision(1)
      .setRequestSubmittedTime(System.currentTimeMillis())
      .setQueueType(QueueType.KAFKA)
      .setUserId("maha-worker-user")
      .setRegistryName("er")
      .build()
    val workerRequest = defaultMahaWorkerProtoParser.parseProto(mahaCustomReportRequestProto)
    assert(workerRequest.isInstanceOf[AsyncScheduledMahaWorkerRequest])
  }

  test("ack status") {
    assert(AckStatus.PROCESSED.toString  === "PROCESSED")
  }

}
