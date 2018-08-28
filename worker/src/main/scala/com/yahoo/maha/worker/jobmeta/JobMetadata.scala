// Copyright 2018, Oath Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.

package com.yahoo.maha.worker.jobmeta

import com.yahoo.maha.core._
import com.yahoo.maha.worker.jobmeta.JobStatus.JobStatus
import org.joda.time.DateTime

/*
    Created by pranavbhole on 8/13/18
    JobMetadata is an interface to define the way job updates are stored,
    it also takes care of the underline storage mechanism for job updates.
*/
trait JobMetadata {
  def insertJob(job:Job):Boolean //not used by maha-worker, it assumes that job is already created by producer
  def deleteJob(job: Job): Boolean
  def findById(jobId:Long): Job
  def updateJobStatus(jobId:Long, jobStatus: JobStatus): Boolean
  def updateJobAcquired(jobId : Long): Boolean = {
    updateJobStatus(jobId, JobStatus.RUNNING)
  }
  def updateJobEnded(jobId: Long, endStatus: JobStatus, message :String) : Boolean

  /*
    Method to find number of jobs with given status, especially used for throttling
   */
  def countJobsByTypeAndStatus(jobType: JobType, jobStatus: JobStatus, jobCreatedTs: DateTime) : Int
}
