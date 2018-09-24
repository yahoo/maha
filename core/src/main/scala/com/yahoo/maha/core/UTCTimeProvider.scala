// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import com.yahoo.maha.core.request.{Parameter, ReportingRequest, TimeZoneValue}

/**
 * Created by hiral on 1/19/16.
 */
trait UTCTimeProvider {
  def getUTCDayHourMinuteFilter(localTimeDayFilter: Filter, localTimeHourFilter: Option[Filter], localTimeMinuteFilter: Option[Filter], timezone: Option[String], isDebugEnabled: Boolean): (Filter, Option[Filter], Option[Filter])
  def getTimezone(reportingRequest: ReportingRequest) : Option[String] = {
    reportingRequest.additionalParameters.get(Parameter.TimeZone) collect {
      case TimeZoneValue(value) => value
    }
  }
}

object PassThroughUTCTimeProvider extends UTCTimeProvider {
  override def getUTCDayHourMinuteFilter(localTimeDayFilter: Filter, localTimeHourFilter: Option[Filter], localTimeMinuteFilter: Option[Filter], timezone: Option[String], isDebugEnabled: Boolean): (Filter, Option[Filter], Option[Filter]) = {
    (localTimeDayFilter, localTimeHourFilter, localTimeMinuteFilter)
  }
}

