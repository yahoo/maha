package com.yahoo.maha.core

import com.yahoo.maha.core.request.ReportingRequest
import grizzled.slf4j.Logging

trait UserTimeZoneProvider {
  def getTimeZone(request: ReportingRequest): Option[String]
}

object NoopUserTimeZoneProvider extends UserTimeZoneProvider with Logging{
  def getTimeZone(request: ReportingRequest): Option[String] = {
    info(s"In NoopUserTimeZoneProvider which sends timezone as None")
    None
  }
}
