package com.yahoo.maha.core

import com.yahoo.maha.core.request.ReportingRequest

trait UserTimeZoneProvider {
  def getTimeZone(request: ReportingRequest): Option[String]
}

object NoopUserTimeZoneProvider extends UserTimeZoneProvider {
  def getTimeZone(request: ReportingRequest): Option[String] = None
}
