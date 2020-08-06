// Copyright 2018, Yahoo Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.

package com.yahoo.maha.job.service

import com.yahoo.maha.core._
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

  class JobMetadataTest extends JobMetadata {
    def insertJob(job: Job): Future[Boolean] = {
      Future(true)
    }
    override def deleteJob(jobId: Long): Future[Boolean] = Future(true)

    override def findById(jobId: Long): Future[Option[Job]] = Future(None)

    override def updateJobStatus(jobId: Long, jobStatus: JobStatus): Future[Boolean] = Future(true)

    override def updateJobEnded(jobId: Long, endStatus: JobStatus, message: String): Future[Boolean] = Future(true)

    override def countJobsByTypeAndStatus(jobType: JobType, jobStatus: JobStatus, jobCreatedTs: DateTime): Future[Int] = Future(1)
  }


  test("test Job metadata") {

    val futurePassThroughJobMetadata = new JobMetadataTest().findById(12345)
    val futurePassThough = new JobMetadataTest().insertJob(null)
    futurePassThroughJobMetadata.onComplete {
      result =>

        assert(result.get.isEmpty)
    }
    futurePassThough.onComplete {
      result=>
        assert(result.get == true)
    }
    Await.result(futurePassThroughJobMetadata, 100 millis)
    Await.result(futurePassThough, 100 millis)
    assert(futurePassThroughJobMetadata.isCompleted)
    assert(futurePassThough.isCompleted)
  }

  test("Test JobType decode") {

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

  test("JobMetadata trait testing") {
    new {

    } with JobMetadata {
      override def insertJob(job: Job): Future[Boolean] = Future(true)

      override def deleteJob(jobId: Long): Future[Boolean] = Future(true)

      override def findById(jobId: Long): Future[Option[Job]] = Future(None)

      override def updateJobStatus(jobId: Long, jobStatus: JobStatus): Future[Boolean] = Future(true)

      override def updateJobEnded(jobId: Long, endStatus: JobStatus, message: String): Future[Boolean] = Future(true)

      override def countJobsByTypeAndStatus(jobType: JobType, jobStatus: JobStatus, jobCreatedTs: DateTime): Future[Int] = Future(1)

      val futurePassThroughJobMetadata = findById(12345)
      val futurePassThough = insertJob(null)
      futurePassThroughJobMetadata.onComplete {
        result =>

          assert(result.get.isEmpty)
      }
      futurePassThough.onComplete {
        result=>
          assert(result.get == true)
      }
      Await.result(futurePassThroughJobMetadata, 100 millis)
      Await.result(futurePassThough, 100 millis)
      assert(futurePassThroughJobMetadata.isCompleted)
      assert(futurePassThough.isCompleted)
    }
  }

  test("Test JobType encode") {
    val oracleJobTypeOption = JobType.getJobType(OracleEngine)
    assert(oracleJobTypeOption.get == AsyncOracle)

    val druidJobTypeOption = JobType.getJobType(DruidEngine)
    assert(druidJobTypeOption.get == AsyncDruid)

    val hiveJobTypeOption = JobType.getJobType(HiveEngine)
    assert(hiveJobTypeOption.get == AsyncHive)

    val prestoJobTypeOption = JobType.getJobType(PrestoEngine)
    assert(prestoJobTypeOption.get == AsyncPresto)

    intercept[IllegalArgumentException] { // Result type: IndexOutOfBoundsException
      JobType.getJobType(null)
    }

  }

  test("Job Status") {
    intercept[IllegalArgumentException] { // Result type: IndexOutOfBoundsException
      JobStatus.fromString("randomString")
    }
    assert(JobStatus.fromString("COMPLETED").toString() === "COMPLETED")
  }


}
