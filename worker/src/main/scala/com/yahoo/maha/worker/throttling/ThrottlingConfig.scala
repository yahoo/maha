package com.yahoo.maha.worker.throttling

import java.util.concurrent.Executors

import com.yahoo.maha.core.Engine
import com.yahoo.maha.job.service.{JobMetadata, JobStatus, JobType}
import com.yahoo.maha.worker.request.MahaWorkerRequest
import grizzled.slf4j.Logging
import org.joda.time.{DateTime, DateTimeZone}

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/*
    Created by pranavbhole on 9/26/18
*/

trait ThrottlingConfig {
  def throttlingEnabled: Boolean
  def lookbackMins: Int
  def countThreshold: Int
  def checkDelayMs: Int
  def maxChecks: Int
}

case class EngineThrottlingConfig(throttlingEnabled: Boolean,
                             lookbackMins: Int,
                             countThreshold: Int,
                             checkDelayMs: Int,
                             maxChecks: Int
                            ) extends ThrottlingConfig


/*
   Open/unsealed trait to implement the custom Throttler with custom config
 */
trait Throttler {
  def throttlingConfig : ThrottlingConfig
  def jobMetadata : JobMetadata
  def throttle(mahaWorkerRequest: MahaWorkerRequest): Boolean
}

case class JobMetaExecConfig( maxWaitMills: Long, poolSize: Int)


/*
  Implementing an Engine Baseed Throttler which can throttle the job based on the number of jobs running with given engine,
  with custom EngineThrottlingConfig
 */
case class EngineBasedThrottler(throttlingConfig: EngineThrottlingConfig, jobMetadata : JobMetadata, jobMetaExecConfig: JobMetaExecConfig)  extends Throttler with Logging {

  implicit val executor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(jobMetaExecConfig.poolSize))


  override def throttle(mahaWorkerRequest: MahaWorkerRequest): Boolean = {
    val engine: Engine = mahaWorkerRequest.engine

    val jobType:JobType = {
      val jobTypeOption = JobType.getJobType(engine)
      require(jobTypeOption.isDefined, s"Unable to get the job type for engine $engine")
      jobTypeOption.get
    }

    var timesChecked = 0
    var countOfRunningJobs = getRunningJobs(engine, jobType)
    while (countOfRunningJobs > throttlingConfig.countThreshold && timesChecked < throttlingConfig.maxChecks) {
      warn(s"Throttling: Number of running jobs ($countOfRunningJobs) exceeds threshold (${throttlingConfig.countThreshold}). Checked $timesChecked times.")
      Thread.sleep(throttlingConfig.checkDelayMs)
      countOfRunningJobs = getRunningJobs(engine, jobType)
      timesChecked += 1
    }
    if (timesChecked == throttlingConfig.maxChecks && countOfRunningJobs > throttlingConfig.countThreshold) {
      warn(s"Timeout: Count of running jobs exceeds threshold even after ${throttlingConfig.checkDelayMs * throttlingConfig.maxChecks} ms. Continuing to process to avoid increasing PULSAR/KAFKA backlog.")
      //monManager.incrementMetric(Metrics.ThrottleCheckTimeouts)
    }
    info(s"Number of running jobs ($countOfRunningJobs) below threshold (${throttlingConfig.countThreshold}), proceeding to process message.")

    if(timesChecked > 0) {
      true
    } else false
  }

  def getRunningJobs(engine: Engine, jobType:JobType): Int = {
    val jobCreatedTs = DateTime.now(DateTimeZone.UTC).minusMinutes(throttlingConfig.lookbackMins)
    val countOfRunningJobsFuture = jobMetadata.countJobsByTypeAndStatus(jobType, JobStatus.RUNNING, jobCreatedTs)

    Await.result(countOfRunningJobsFuture, jobMetaExecConfig.maxWaitMills millis)

    val runningJobCount: Int = if(countOfRunningJobsFuture.value.isEmpty) {
      warn(s"Failed to get the runningJobCount in ${jobMetaExecConfig.maxWaitMills}")
      0
    } else {
      countOfRunningJobsFuture.value.get match {
        case Success(count) =>  count
        case Failure(t) =>  {
          error(s"Failed to get the result from jobMeta ${t.getMessage}", t)
          0
        }
      }
    }
    runningJobCount
  }

}