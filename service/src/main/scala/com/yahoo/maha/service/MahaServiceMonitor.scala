// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service

import com.yahoo.maha.core.request.ReportingRequest

/**
 * Created by pranavbhole on 15/03/18.
 */

/*
   MahaServiceMonitor is application monitoring interface for logging the api request into monitoring system
 */
trait MahaServiceMonitor {
  def start: (ReportingRequest) => Unit
  def stop : () => Unit
}

object DefaultMahaServiceMonitor extends MahaServiceMonitor {
  override def start: (ReportingRequest) => Unit = (reportingRequest) => {}

  override def stop: () => Unit = () => {}
}