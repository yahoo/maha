// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.curators

import com.yahoo.maha.core.RequestModelResult
import com.yahoo.maha.core.bucketing.BucketParams
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import com.yahoo.maha.service.{MahaService, RequestResult}
import grizzled.slf4j.Logging

import scala.util.Try

trait Curator extends Ordered[Curator] {
  val name: String
  val level: Int
  val priority: Int
  def process(registryName: String,
              bucketParams: BucketParams,
              reportingRequest: ReportingRequest,
              mahaService: MahaService,
              mahaRequestLogHelper: MahaRequestLogHelper,
              curatorResultMapAtCurrentLevel: Map[String, (Try[RequestModelResult], Try[RequestResult])],
              curatorResultMapAcrossAllLevel: Map[Int, (Try[RequestModelResult], Try[RequestResult])]) : (Try[RequestModelResult], Try[RequestResult])
  def compare(that: Curator) = {
    if(this.level == that.level) {
      Integer.compare(this.priority, that.priority)
    } else Integer.compare(this.level, that.level)
  }
}

object DefaultCurator {
  val name: String = "default"
}

class DefaultCurator extends Curator with Logging {

  override val name: String = DefaultCurator.name
  override val level: Int = 0
  override val priority: Int = 0

  override def process(registryName: String,
                       bucketParams: BucketParams,
                       reportingRequest: ReportingRequest,
                       mahaService: MahaService,
                       mahaRequestLogHelper: MahaRequestLogHelper,
                       curatorResultMapAtCurrentLevel: Map[String, (Try[RequestModelResult], Try[RequestResult])],
                       curatorResultMapAcrossAllLevel: Map[Int, (Try[RequestModelResult], Try[RequestResult])]): (Try[RequestModelResult], Try[RequestResult]) = {
    val requestModelResultTry: Try[RequestModelResult] = mahaService.generateRequestModel(registryName, reportingRequest, bucketParams , mahaRequestLogHelper)
    require(requestModelResultTry.isSuccess)
    val requestResultTry = mahaService.processRequestModel(registryName, requestModelResultTry.get.model, mahaRequestLogHelper)
    (requestModelResultTry, requestResultTry)
  }

}
