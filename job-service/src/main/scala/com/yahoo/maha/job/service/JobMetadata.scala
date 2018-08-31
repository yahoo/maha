// Copyright 2018, Oath Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.

package com.yahoo.maha.job.service

import com.yahoo.maha.job.service.JobStatus.JobStatus
import org.joda.time.DateTime

import scala.concurrent.Future

/*
    Created by pranavbhole on 8/13/18
    JobMetadata is an interface to define the way job updates are stored,
    it also takes care of the underline storage mechanism for job updates.
*/
trait JobMetadata {
  def insertJob(job:Job): Future[Boolean] //not used by maha-worker, it assumes that job is already created by producer
  def deleteJob(jobId:Long): Future[Boolean]
  def findById(jobId:Long): Future[Option[Job]]
  def updateJobStatus(jobId:Long, jobStatus: JobStatus): Future[Boolean]
  def updateJobAcquired(jobId : Long): Future[Boolean] = {
    updateJobStatus(jobId, JobStatus.RUNNING)
  }
  def updateJobEnded(jobId: Long, endStatus: JobStatus, message :String) : Future[Boolean]

  /*
    Method to find number of jobs with given status, especially used for throttling
   */
  def countJobsByTypeAndStatus(jobType: JobType, jobStatus: JobStatus, jobCreatedTs: DateTime) : Future[Int]
}
