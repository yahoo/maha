package com.yahoo.maha.service

import com.yahoo.maha.parrequest2.GeneralError
import com.yahoo.maha.parrequest2.future.{CombinableRequest, ParRequest, ParRequestListEither, ParallelServiceExecutor}
import com.yahoo.maha.service.curators._
import com.yahoo.maha.service.utils.MahaRequestLogBuilder
import grizzled.slf4j.Logging

import scala.collection.SortedSet
import scala.collection.mutable.ArrayBuffer

case class RequestCoordinatorResult(orderedList: IndexedSeq[ParRequest[CuratorResult]]
                                    , resultMap: Map[String, ParRequest[CuratorResult]]
                                    , private val pse: ParallelServiceExecutor
                                   ) {
  lazy val combinedResultList: ParRequestListEither[CuratorResult] = {
    import collection.JavaConverters._
    val futures : java.util.List[CombinableRequest[CuratorResult]] = resultMap
      .values.view.map(_.asInstanceOf[CombinableRequest[CuratorResult]]).toList.asJava
    pse.combineListEither(futures)
  }
}

trait RequestCoordinator {
  protected def mahaService: MahaService

  def execute(mahaRequestContext: MahaRequestContext
              , mahaRequestLogBuilder: MahaRequestLogBuilder): Either[GeneralError, RequestCoordinatorResult]
}

case class DefaultRequestCoordinator(protected val mahaService: MahaService) extends RequestCoordinator with Logging {

  def curatorMap: Map[String, Curator] = mahaService.getMahaServiceConfig.curatorMap

  override def execute(mahaRequestContext: MahaRequestContext
              , mahaRequestLogBuilder: MahaRequestLogBuilder): Either[GeneralError, RequestCoordinatorResult] = {
    val curatorConfigMapFromRequest: Map[String, CuratorConfig] = {
      mahaRequestContext.reportingRequest.curatorJsonConfigMap.collect {
        case (name, json) if curatorMap.contains(name) =>
          val result = curatorMap(name).parseConfig(mahaRequestContext.reportingRequest)
          if(result.isFailure) {
            return GeneralError.either("curatorJsonParse", result.toString)
          }
          name -> result.toOption.get
      }
    }
    val curatorsOrdered: SortedSet[Curator] = curatorConfigMapFromRequest
      .keys
      .flatMap(mahaService.getMahaServiceConfig.curatorMap.get)
      .to[SortedSet]

    if(curatorsOrdered.size != mahaRequestContext.reportingRequest.curatorJsonConfigMap.size
      && mahaRequestContext.reportingRequest.isDebugEnabled) {
      info(s"Curators not found : ${mahaRequestContext.reportingRequest.curatorJsonConfigMap.keySet -- curatorConfigMapFromRequest.keySet}")
    }

    if(curatorsOrdered.size > 1 && curatorsOrdered.exists(_.isSingleton)) {
      return GeneralError.either("singletonCheck"
        , s"Singleton curators can only be requested by themselves but found ${curatorsOrdered.map(c => (c.name, c.isSingleton))}")
    }

    val orderedResultList: ArrayBuffer[ParRequest[CuratorResult]] =
      new ArrayBuffer[ParRequest[CuratorResult]](initialSize = curatorsOrdered.size + 1)

    //inject default if required
    val initialResultMap: Map[String, ParRequest[CuratorResult]] = {
      if (curatorsOrdered.exists(_.requiresDefaultCurator)) {
        val curator = curatorMap(DefaultCurator.name)
        val logHelper = mahaRequestLogBuilder.curatorLogBuilder(curator)
        val result = curator
          .process(Map.empty, mahaRequestContext, mahaService, logHelper, NoConfig)
        orderedResultList += result
        Map(DefaultCurator.name -> result)
      } else Map.empty
    }

    val finalResultMap: Map[String, ParRequest[CuratorResult]] = curatorsOrdered.foldLeft(initialResultMap) {
      (prevResult, curator) =>
        val config = curatorConfigMapFromRequest(curator.name)
        val logHelper = mahaRequestLogBuilder.curatorLogBuilder(curator)
        val result = curator.process(prevResult, mahaRequestContext, mahaService, logHelper, config)
        orderedResultList += result
        prevResult.+(curator.name -> result)
    }

    val pse = mahaService.getParallelServiceExecutor(mahaRequestContext)
    new Right(RequestCoordinatorResult(orderedResultList, finalResultMap, pse))
  }

}
