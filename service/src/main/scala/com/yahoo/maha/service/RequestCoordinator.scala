package com.yahoo.maha.service

import com.yahoo.maha.core.request.CuratorJsonConfig
import com.yahoo.maha.parrequest2.future.ParRequest
import com.yahoo.maha.service.curators.{Curator, CuratorResult}
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import grizzled.slf4j.Logging

import scala.collection.SortedSet

trait RequestCoordinator {
  protected def mahaService: MahaService

  def execute(mahaRequestContext: MahaRequestContext
              , mahaRequestLogHelper: MahaRequestLogHelper): ParRequest[CuratorResult]
}

case class DefaultRequestCoordinator(protected val mahaService: MahaService) extends RequestCoordinator with Logging {

  override def execute(mahaRequestContext: MahaRequestContext
              , mahaRequestLogHelper: MahaRequestLogHelper): ParRequest[CuratorResult] = {
    val curatorJsonConfigMapFromRequest: Map[String, CuratorJsonConfig] = mahaRequestContext.reportingRequest.curatorJsonConfigMap
    val curatorsOrdered: SortedSet[Curator] = curatorJsonConfigMapFromRequest
      .keys
      .flatMap(mahaService.getMahaServiceConfig.curatorMap.get)
      .to[SortedSet]

    // for now supporting only one curator
    val curator = curatorsOrdered.head
    curator.process(mahaRequestContext,mahaService, mahaRequestLogHelper)

  }

}
