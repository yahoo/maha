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

}
