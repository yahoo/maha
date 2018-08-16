// Copyright 2018, Oath Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.

package com.yahoo.maha.worker.jobmeta

import com.yahoo.maha.worker.jobmeta.JobStatus.JobStatus
import org.joda.time.DateTime

/*
    Created by pranavbhole on 8/14/18
*/
trait Job {
  def jobId: Long
  def jobType: String
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
}

case class AsyncJob(jobId: Long
                , jobType: String
                , jobStatus: JobStatus
                , jobResponse: String
                , numAcquired: Int
                , createdTimestamp: DateTime
                , acquiredTimestamp: DateTime
                , endedTimestamp: DateTime
                , jobParentId: Long
                , jobRequest: String
                , hostname: String
                , cubeName: String) extends Job
