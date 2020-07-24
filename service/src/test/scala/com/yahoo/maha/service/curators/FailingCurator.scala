package com.yahoo.maha.service.curators

import com.yahoo.maha.core.query.QueryPipeline
import com.yahoo.maha.parrequest2.GeneralError
import com.yahoo.maha.parrequest2.future.ParRequest
import com.yahoo.maha.service.{CuratorInjector, MahaRequestContext, MahaService, ParRequestResult, RequestResult}
import com.yahoo.maha.service.utils.CuratorMahaRequestLogBuilder

import scala.util.Try

/**
  * Created by hiral on 4/11/18.
  */
object FailingCurator {
  val name = "fail"
}
class FailingCurator extends Curator {
  override def name: String = FailingCurator.name

  override def level: Int = 100

  override def priority: Int = 0

  override def process(resultMap: Map[String, Either[CuratorError, IndexedSeq[ParRequest[CuratorResult]]]]
                       , mahaRequestContext: MahaRequestContext
                       , mahaService: MahaService
                       , mahaRequestLogBuilder: CuratorMahaRequestLogBuilder
                       , curatorConfig: CuratorConfig
                       , curatorInjector: CuratorInjector
                      ) : Either[CuratorError, IndexedSeq[ParRequest[CuratorResult]]] = {
    val pse = mahaService.getParallelServiceExecutor(mahaRequestContext)
    mahaRequestContext.context.get("faillevel") match {
      case Some("requestresult") =>
        val qp: Try[QueryPipeline] = scala.util.Failure(new IllegalArgumentException("blah"))
        val pr: ParRequest[RequestResult] = pse.immediateResult("fail", GeneralError.either("fail", "failed"))
        val parRequestResult: ParRequestResult = ParRequestResult(qp, pr, None)
        val curatorResult = CuratorResult(this, curatorConfig, Option(parRequestResult), null)
        val curatorResultEither: Either[GeneralError, CuratorResult] = new Right(curatorResult)
        val parRequest: ParRequest[CuratorResult] =
          pse.immediateResult("fail", curatorResultEither)
        new Right(IndexedSeq(parRequest))
      case Some("curatorresult") =>
        val parRequest: ParRequest[CuratorResult] =
          pse.immediateResult("fail", withParRequestError(curatorConfig, GeneralError.from("fail", "failed")))
        new Right(IndexedSeq(parRequest))
      case _ =>
        withError(curatorConfig, GeneralError.from("fail", "failed"))
    }
  }

  override def isSingleton: Boolean = false

  override def requiresDefaultCurator: Boolean = true

  override protected def requestModelValidator: CuratorRequestModelValidator = NoopCuratorRequestModelValidator
}
