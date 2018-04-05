package com.yahoo.maha.service

import com.yahoo.maha.core.bucketing.BucketParams
import com.yahoo.maha.core.request.{CuratorJsonConfig, ReportingRequest}
import com.yahoo.maha.parrequest2.future.ParRequest
import com.yahoo.maha.service.curators.{Curator, CuratorResult}
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import grizzled.slf4j.Logging

trait RequestCoordinator {
  protected def mahaService: MahaService

  def execute(mahaRequestContext: MahaRequestContext
              , mahaRequestLogHelper: MahaRequestLogHelper): ParRequest[CuratorResult]
}

case class DefaultRequestCoordinator(protected val mahaService: MahaService) extends RequestCoordinator with Logging {

  override def execute(mahaRequestContext: MahaRequestContext
              , mahaRequestLogHelper: MahaRequestLogHelper): ParRequest[CuratorResult] = {
    val curatorJsonConfigMapFromRequest: Map[String, CuratorJsonConfig] = mahaRequestContext.reportingRequest.curatorJsonConfigMap
    // for now supporting only one curator
    val curator: Curator = mahaService.getMahaServiceConfig.curatorMap(curatorJsonConfigMapFromRequest.head._1)
    curator.process(mahaRequestContext, mahaRequestLogHelper)

  }

}
