package com.yahoo.maha.job.service

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/*
    Created by pranavbhole on 8/29/18
*/
class AsyncJobMetadataTest extends BaseJobServiceTest {

  val jobMetadataDao:JobMetadata = new TestJobMetadataDao(jdbcConnection.get)

  val job = AsyncJob(jobId = 12345
    , jobType = AsyncOracle.name
    , jobStatus = JobStatus.SUBMITTED
    , jobResponse = "{}"
    , numAcquired = 0
    , createdTimestamp = now
    , acquiredTimestamp = now
    , endedTimestamp = now
    , jobParentId = -1
    , jobRequest = "{}"
    , hostname = "localhost"
    , cubeName = "student_performance")

  test("Test Job Creation") {

    val insertFuture = jobMetadataDao.insertJob(job)

    Await.result(insertFuture, 500 millis)
    assert(insertFuture.isCompleted)

    var count = 0
    jdbcConnection.get.queryForObject("select * from maha_worker_job") {
      rs =>
        while (rs.next()) {

          println("JobID= "+rs.getString("jobId"))
          count += 1
        }
    }

    assert(count == 1, "Job Insertion Failed")

    val jobFuture = jobMetadataDao.findById(12345)
    jobFuture.onComplete {
      result =>
        assert(result.isSuccess, s"Future failure $result")
        assert(result.get.isDefined)
        info("Found the job ="+result.get.get.jobId)
    }
    Await.result(jobFuture, 1000 millis)
    assert(jobFuture.isCompleted)
  }

}
