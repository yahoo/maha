package com.yahoo.maha.job.service

import org.joda.time.DateTime

import scala.concurrent.{Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/*
    Created by pranavbhole on 8/29/18
*/
class SqlJobMetadataTest extends BaseJobServiceTest {

  val jobMetadataDao:JobMetadata = new SqlJobMetadata(jdbcConnection.get, "maha_worker_job")

  val jobOracle = AsyncJob(jobId = 12345
    , jobType = AsyncOracle
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
  val hiveJob= jobOracle.copy(jobType = AsyncHive)
  val druidJob= jobOracle.copy(jobType = AsyncDruid)
  val prestoJob= jobOracle.copy(jobType = AsyncPresto)

  test("Test Job Creation") {

    val insertFuture = jobMetadataDao.insertJob(jobOracle)

    Await.result(insertFuture, 500 millis)
    assert(insertFuture.isCompleted)

    var count = 0
    jdbcConnection.get.queryForObject("select * from maha_worker_job") {
      rs =>
        while (rs.next()) {

          println("JobID= " + rs.getString("jobId"))
          count += 1
        }
    }

    assert(count == 1, "Job Insertion Failed")

    val jobFuture = jobMetadataDao.findById(12345)
    jobFuture.onComplete {
      result =>
        assert(result.isSuccess, s"Future failure $result")
        assert(result.get.isDefined)
        info("Found the job =" + result.get.get.jobId)
    }
    Await.result(jobFuture, 500 millis)
    assert(jobFuture.isCompleted)

    val deleteJobFuture = jobMetadataDao.deleteJob(12345)
    deleteJobFuture.onComplete {
      result=>
        assert(result.isSuccess)
        assert(result.get == true)
    }
    Await.result(deleteJobFuture, 500 millis)
    assert(deleteJobFuture.isCompleted)

    val updateJobFuture = jobMetadataDao.updateJobStatus(12345, JobStatus.RUNNING)
    updateJobFuture.onComplete {
      result=>
        assert(result.isSuccess)
        assert(result.get == true)
    }
    Await.result(updateJobFuture, 500 millis)
    assert(updateJobFuture.isCompleted)

    val jobEndedJobFuture = jobMetadataDao.updateJobEnded(12345, JobStatus.COMPLETED, s"""{"status": "COMPLETED", "url": "https://maha.abc.com/filename.csv"}""")
    jobEndedJobFuture.onComplete {
      result=>
        assert(result.isSuccess)
        assert(result.get == true)
    }
    Await.result(jobEndedJobFuture, 500 millis)
    assert(jobEndedJobFuture.isCompleted)

    val currentJobsFuture = jobMetadataDao.countJobsByTypeAndStatus(AsyncOracle, JobStatus.COMPLETED, new DateTime().minusDays(1))
    currentJobsFuture.onComplete {
      result=>
        assert(result.isSuccess)
        assert(result.get == 1)
    }
    Await.result(currentJobsFuture, 500 millis)
    JobStatus.toString
    assert(currentJobsFuture.isCompleted)
  }

  test("test SQL Job metadata failures") {
    jobMetadataDao

  }


}
