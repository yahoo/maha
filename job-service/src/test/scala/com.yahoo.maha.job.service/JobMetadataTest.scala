package com.yahoo.maha.job.service

import com.yahoo.maha.job.service.JobStatus.JobStatus
import org.joda.time.DateTime
import org.scalatest.{FunSuite, Matchers}
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/*
    Created by pranavbhole on 8/30/18
*/
class JobMetadataTest extends FunSuite with Matchers {


  test("test Job metadata") {
    val passThroughJobMetadata :JobMetadata =   new JobMetadata {
      override def insertJob(job: Job): Future[Boolean] = Future(true)

      override def deleteJob(job: Job): Future[Boolean] = Future(true)

      override def findById(jobId: Long): Future[Option[Job]] = Future(None)

      override def updateJobStatus(jobId: Long, jobStatus: JobStatus): Future[Boolean] = Future(true)

      override def updateJobEnded(jobId: Long, endStatus: JobStatus, message: String): Future[Boolean] = Future(true)

      override def countJobsByTypeAndStatus(jobType: JobType, jobStatus: JobStatus, jobCreatedTs: DateTime): Future[Int] = Future(1)
    }


    val futurePassThroughJobMetadata = passThroughJobMetadata.findById(12345)
    Await.result(futurePassThroughJobMetadata, 100 millis)
    assert(futurePassThroughJobMetadata.isCompleted)

  }

  test("Test JobType") {

    intercept[IllegalArgumentException] { // Result type: IndexOutOfBoundsException
      JobType.fromString("unknown")
    }

    val oracleJobType = JobType.fromString(AsyncOracle.name)
    assert(oracleJobType == AsyncOracle)

    val druidJobType = JobType.fromString(AsyncDruid.name)
    assert(druidJobType == AsyncDruid)

    val hiveJobType = JobType.fromString(AsyncHive.name)
    assert(hiveJobType == AsyncHive)

    val prestoJobType = JobType.fromString(AsyncPresto.name)
    assert(prestoJobType == AsyncPresto)

  }

}
