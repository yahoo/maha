// Copyright 2018, Yahoo Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.

package com.yahoo.maha.job.service

/*
    Created by pranavbhole on 8/13/18
*/
object JobStatus extends Enumeration {
  type JobStatus = Value

  val SUBMITTED = Value("SUBMITTED")
  val RUNNING = Value("RUNNING")
  val WAITING = Value("WAITING")
  val READY = Value("READY")
  val PROCESSED = Value("PROCESSED")
  val COMPLETED = Value("COMPLETED")
  val FAILED = Value("FAILED")
  val KILLED = Value("KILLED")

  override def toString: String = this.Value.toString

  def fromString(status: String): JobStatus = {
    val jobStatusOption = JobStatus.values.find(v=> status.equalsIgnoreCase(v.toString))
    if(jobStatusOption.isDefined) {
      jobStatusOption.get
    } else {
      throw new IllegalArgumentException("Unknown status " + status + " in JobStatus enum.")
    }
  }
}

