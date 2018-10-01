package com.yahoo.maha.worker.throttling

import com.google.protobuf.ByteString
import com.yahoo.maha.job.service.{Job, JobMetadata, JobType}
import com.yahoo.maha.job.service.JobStatus.JobStatus
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.worker.BaseWorkerTest
import com.yahoo.maha.worker.proto.MahaWorkerReportingProto._
import com.yahoo.maha.worker.request.DefaultMahaWorkerProtoParser.{defaultRequestTypeToSerDeMap, defaultSchemaProvider}
import com.yahoo.maha.worker.request.{AsyncAdhocMahaWorkerRequest, DefaultMahaWorkerProtoParser, MahaWorkerRequest}
import org.joda.time.DateTime

import scala.concurrent.Future

/*
    Created by pranavbhole on 9/27/18
*/
class ThrotterTest extends BaseWorkerTest {

  import scala.concurrent.ExecutionContext.Implicits.global

  val noopJobMeta = new JobMetadata {
    override def insertJob(job: Job): Future[Boolean] = Future(true)

    override def deleteJob(jobId: Long): Future[Boolean] = Future(true)

    override def findById(jobId: Long): Future[Option[Job]] = Future(None)

    override def updateJobStatus(jobId: Long, jobStatus: JobStatus): Future[Boolean] = Future(true)

    override def updateJobEnded(jobId: Long, endStatus: JobStatus, message: String): Future[Boolean] = Future(true)

    override def countJobsByTypeAndStatus(jobType: JobType, jobStatus: JobStatus, jobCreatedTs: DateTime): Future[Int] = Future(2)
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

  import  DefaultMahaWorkerProtoParser._

  val defaultMahaWorkerProtoParser =  DefaultMahaWorkerProtoParser(defaultRequestTypeToSerDeMap, defaultSchemaProvider)

  test("Test engine based throttling, should not throttle with count = threshold") {
    val throttler = EngineBasedThrottler(EngineThrottlingConfig(true, 0, 2, 100, 2), noopJobMeta, JobMetaExecConfig(100, 2))

    val mahaWorkerRequest:MahaWorkerRequest = defaultMahaWorkerProtoParser.parseProto(mahaCustomReportRequestProto)

    val result = throttler.throttle(mahaWorkerRequest)

    assert(result == false)
  }

  test("Engine Base throttling should throttle with count > threshold") {
    val throttler = EngineBasedThrottler(EngineThrottlingConfig(true, 0, 1, 100, 2), noopJobMeta, JobMetaExecConfig(100, 2))

    val mahaWorkerRequest:MahaWorkerRequest = defaultMahaWorkerProtoParser.parseProto(mahaCustomReportRequestProto)

    val result = throttler.throttle(mahaWorkerRequest)

    assert(result == true)
  }



}
