package com.yahoo.maha.service.curators

import com.yahoo.maha.parrequest2.GeneralError
import com.yahoo.maha.parrequest2.future.ParRequest
import com.yahoo.maha.service.{MahaRequestContext, MahaService}
import com.yahoo.maha.service.utils.CuratorMahaRequestLogBuilder

/**
  * Created by hiral on 4/11/18.
  */
class FailingCurator extends Curator {
  override def name: String = "fail"

  override def level: Int = 100

  override def priority: Int = 0

  override def process(resultMap: Map[String, ParRequest[CuratorResult]], mahaRequestContext: MahaRequestContext, mahaService: MahaService, mahaRequestLogBuilder: CuratorMahaRequestLogBuilder, curatorConfig: CuratorConfig): ParRequest[CuratorResult] = {
    ParRequest.immediateResult("fail", mahaService.getParallelServiceExecutor(mahaRequestContext), GeneralError.either("fail", "failed"))
  }

  override def isSingleton: Boolean = false

  override def requiresDefaultCurator: Boolean = true

  override protected def requestModelValidator: CuratorRequestModelValidator = NoopCuratorRequestModelValidator
}
