package com.yahoo.maha.worker.request

import java.util.TimeZone

import com.google.protobuf.ByteString
import com.yahoo.maha.service.BaseMahaServiceTest
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import com.yahoo.maha.worker.BaseJobServiceTest
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

}
