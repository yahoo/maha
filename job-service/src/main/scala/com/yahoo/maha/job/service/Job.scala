// Copyright 2018, Yahoo Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.

package com.yahoo.maha.job.service

import com.yahoo.maha.job.service.JobStatus.JobStatus
import org.joda.time.DateTime

/*
    Created by pranavbhole on 8/14/18
*/
trait Job {
  def jobId: Long
  def jobType: JobType
  def jobStatus: JobStatus
  def jobResponse: String
  def numAcquired: Int
  def createdTimestamp: DateTime
  def acquiredTimestamp: DateTime
  def endedTimestamp: DateTime
  def jobParentId: Long
  def jobRequest: String
  def hostname: String
  def cubeName: String
  def isDeleted: Boolean
}

case class AsyncJob(jobId: Long
                , jobType: JobType
                , jobStatus: JobStatus
                , jobResponse: String
                , numAcquired: Int
                , createdTimestamp: DateTime
                , acquiredTimestamp: DateTime
                , endedTimestamp: DateTime
                , jobParentId: Long
                , jobRequest: String
                , hostname: String
                , cubeName: String
                , isDeleted : Boolean = false) extends Job
