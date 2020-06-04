// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

/**
 * Created by hiral on 1/19/16.
 */
trait UTCTimeProvider {
  def getUTCDayHourMinuteFilter(localTimeDayFilter: Filter, localTimeHourFilter: Option[Filter], localTimeMinuteFilter: Option[Filter], timezone: Option[String], isDebugEnabled: Boolean): (Filter, Option[Filter], Option[Filter])
}

object PassThroughUTCTimeProvider extends UTCTimeProvider {
  override def getUTCDayHourMinuteFilter(localTimeDayFilter: Filter, localTimeHourFilter: Option[Filter], localTimeMinuteFilter: Option[Filter], timezone: Option[String], isDebugEnabled: Boolean): (Filter, Option[Filter], Option[Filter]) = {
    (localTimeDayFilter, localTimeHourFilter, localTimeMinuteFilter)
  }
}

