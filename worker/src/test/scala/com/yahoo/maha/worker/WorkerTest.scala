package com.yahoo.maha.worker

import com.google.protobuf.ByteString
import com.yahoo.maha.core.{DruidEngine, OracleEngine}
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.worker.AckStatus.AckStatus
import com.yahoo.maha.worker.proto.MahaWorkerReportingProto._
import com.yahoo.maha.worker.request.{DefaultMahaWorkerProtoParser, MahaWorkerRequest}
import com.yahoo.maha.worker.request.DefaultMahaWorkerProtoParser.{defaultRequestTypeToSerDeMap, defaultSchemaProvider}
import com.yahoo.maha.worker.state.WorkerStateReporter

/*
    Created by pranavbhole on 8/31/18
*/
class WorkerTest extends BaseWorkerTest {
  import  DefaultMahaWorkerProtoParser._

  val defaultMahaWorkerProtoParser =  DefaultMahaWorkerProtoParser(defaultRequestTypeToSerDeMap, defaultSchemaProvider)

  val workerConfig = WorkerConfig(mahaServiceConfig, defaultMahaWorkerProtoParser, Set(OracleEngine, DruidEngine))
  val workerStateReporter = WorkerStateReporter("test-config")

  val baseMahaWorker = new Worker(workerConfig, workerStateReporter ) {
    override def work(getData: Array[Byte]): AckStatus = AckStatus.PROCESSED

    override def workerConfig: WorkerConfig = workerConfig

    override def workerStateReporter: WorkerStateReporter = workerStateReporter
  }

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

  test("Validate the proto and init MahaWorkerRequest") {

    val mahaCustomReportRequestProto = MahaCustomReportRequest.newBuilder()
      .setRawRequest(ByteString.copyFrom(jsonRequest.getBytes))
      .setDeliveryMethod(DeliveryMethod.CONTENT_STORE)
      .setQueryEngine(QueryEngine.ORACLE)
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


    val mahaWorkerRequest: MahaWorkerRequest = baseMahaWorker.initializeWork(mahaCustomReportRequestProto)

    //Validate correctly

    assert(mahaWorkerRequest.jobId === 12345)

  }

  test("Missing Registry Name in  proto") {

    val mahaCustomReportRequestProto = MahaCustomReportRequest.newBuilder()
      .setRawRequest(ByteString.copyFrom(jsonRequest.getBytes))
      .setDeliveryMethod(DeliveryMethod.CONTENT_STORE)
      .setQueryEngine(QueryEngine.ORACLE)
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
//      .setRegistryName("er")
      .build()

    val exception  = intercept[IllegalArgumentException] {
      val mahaWorkerRequest: MahaWorkerRequest = baseMahaWorker.initializeWork(mahaCustomReportRequestProto)
    }

    assert(exception.getMessage.contains("Unknown registry, failed to find the given registry"))
  }

  test("Missing Schema in  proto") {

    val exception = intercept[com.google.protobuf.UninitializedMessageException] {
      val mahaCustomReportRequestProto = MahaCustomReportRequest.newBuilder()
        .setRawRequest(ByteString.copyFrom(jsonRequest.getBytes))
        .setDeliveryMethod(DeliveryMethod.CONTENT_STORE)
        .setQueryEngine(QueryEngine.ORACLE)
        .setOutputFormat(OutputFormat.CSV)
        .setJobId(12345)
        //.setSchema(StudentSchema.entryName)
        .setReportType(ReportType.AD_HOC)
        .setTimezone("UTC")
        .setDryrun(false)
        .setRevision(1)
        .setRequestSubmittedTime(System.currentTimeMillis())
        .setQueueType(QueueType.KAFKA)
        .setUserId("maha-worker-user")
        .setRegistryName("er")
        .build()
       baseMahaWorker.initializeWork(mahaCustomReportRequestProto)
    }
    assert(exception.getMessage.contains("Message missing required fields: schema"))
  }

  test("Missing JobID in  proto") {

    val exception = intercept[com.google.protobuf.UninitializedMessageException] {
      val mahaCustomReportRequestProto = MahaCustomReportRequest.newBuilder()
        .setRawRequest(ByteString.copyFrom(jsonRequest.getBytes))
        .setDeliveryMethod(DeliveryMethod.CONTENT_STORE)
        .setQueryEngine(QueryEngine.ORACLE)
        .setOutputFormat(OutputFormat.CSV)
        //.setJobId(12345)
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
      baseMahaWorker.initializeWork(mahaCustomReportRequestProto)
    }
    assert(exception.getMessage.contains("Message missing required fields: job_id"))
  }

  test("Generate the warnings on missing optional fields") {
    val mahaCustomReportRequestProto = MahaCustomReportRequest.newBuilder()
      .setRawRequest(ByteString.copyFrom(jsonRequest.getBytes))
      .setDeliveryMethod(DeliveryMethod.CONTENT_STORE)
      .setQueryEngine(QueryEngine.ORACLE)
      .setOutputFormat(OutputFormat.CSV)
      .setJobId(12345)
      .setSchema(StudentSchema.entryName)
      .setReportType(ReportType.AD_HOC)
      .setTimezone("UTC")
      .setDryrun(false)
      .setRevision(1)
      //.setRequestSubmittedTime(System.currentTimeMillis())
      //.setQueueType(QueueType.KAFKA)
      //.setUserId("maha-worker-user")
      .setRegistryName("er")
      .build()
    val workerRequest = baseMahaWorker.initializeWork(mahaCustomReportRequestProto)
    assert(workerRequest.isInstanceOf[MahaWorkerRequest])

  }

  test("Unsupported Engine in  proto") {

    val mahaCustomReportRequestProto = MahaCustomReportRequest.newBuilder()
      .setRawRequest(ByteString.copyFrom(jsonRequest.getBytes))
      .setDeliveryMethod(DeliveryMethod.CONTENT_STORE)
      .setQueryEngine(QueryEngine.HIVE)
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

    val exception  = intercept[IllegalArgumentException] {
      val mahaWorkerRequest: MahaWorkerRequest = baseMahaWorker.initializeWork(mahaCustomReportRequestProto)
    }

    assert(exception.getMessage.contains("Unsupported worker engine=Hive jobId=12345"))
  }

  test("Invalid Schema in  proto") {

    val mahaCustomReportRequestProto = MahaCustomReportRequest.newBuilder()
      .setRawRequest(ByteString.copyFrom(jsonRequest.getBytes))
      .setDeliveryMethod(DeliveryMethod.CONTENT_STORE)
      .setQueryEngine(QueryEngine.HIVE)
      .setOutputFormat(OutputFormat.CSV)
      .setJobId(12345)
      .setSchema("invalidSchema")
      .setReportType(ReportType.AD_HOC)
      .setTimezone("UTC")
      .setDryrun(false)
      .setRevision(1)
      .setRequestSubmittedTime(System.currentTimeMillis())
      .setQueueType(QueueType.KAFKA)
      .setUserId("maha-worker-user")
      .setRegistryName("er")
      .build()

    val exception  = intercept[IllegalArgumentException] {
      val mahaWorkerRequest: MahaWorkerRequest = baseMahaWorker.initializeWork(mahaCustomReportRequestProto)
    }

    assert(exception.getMessage.contains("CustomReportRequest does not contain correct schema invalidSchema"))
  }

}
