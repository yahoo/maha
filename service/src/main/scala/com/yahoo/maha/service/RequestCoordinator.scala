package com.yahoo.maha.service

import com.yahoo.maha.core.bucketing.BucketParams
import com.yahoo.maha.core.request.{CuratorJsonConfig, ReportingRequest}
import com.yahoo.maha.parrequest2.future.ParRequest
import com.yahoo.maha.service.curators.{Curator, CuratorResult}
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import grizzled.slf4j.Logging

trait RequestCoordinator {
  def execute(registryName: String,
              bucketParams: BucketParams,
              reportingRequest: ReportingRequest,
              mahaService: MahaService,
              mahaRequestLogHelper: MahaRequestLogHelper): ParRequest[CuratorResult]
}

class DefaultRequestCoordinator extends RequestCoordinator with Logging {

  override def execute(registryName: String,
                       bucketParams: BucketParams,
                       reportingRequest: ReportingRequest,
                       mahaService: MahaService,
                       mahaRequestLogHelper: MahaRequestLogHelper): ParRequest[CuratorResult] = {

    val curatorJsonConfigMapFromRequest: Map[String, CuratorJsonConfig] = reportingRequest.curatorJsonConfigMap

    // for now supporting only one curator
    val curator: Curator = mahaService.getMahaServiceConfig.curatorMap(curatorJsonConfigMapFromRequest.head._1)
    curator.process(registryName, bucketParams, reportingRequest, mahaService, mahaRequestLogHelper)

  }

}
