package com.yahoo.maha.service

import com.yahoo.maha.core.request.CuratorJsonConfig
import com.yahoo.maha.parrequest2.future.{CombinableRequest, ParRequest, ParRequestListOption, ParallelServiceExecutor}
import com.yahoo.maha.service.curators._
import com.yahoo.maha.service.utils.MahaRequestLogHelper
import grizzled.slf4j.Logging
import org.json4s.scalaz.JsonScalaz

import scala.collection.SortedSet
import scala.collection.mutable.ArrayBuffer
import scalaz.{NonEmptyList, Validation}

case class RequestCoordinatorResult(orderedList: IndexedSeq[ParRequest[CuratorResult]]
                                    , resultMap: Map[String, ParRequest[CuratorResult]]
                                    , private val pse: ParallelServiceExecutor
                                   ) {
  lazy val combinedResultList: ParRequestListOption[CuratorResult] = {
    import collection.JavaConverters._
    val futures : java.util.List[CombinableRequest[CuratorResult]] = resultMap
      .values.view.map(_.asInstanceOf[CombinableRequest[CuratorResult]]).toList.asJava
    pse.combineList(futures)
  }
}

trait RequestCoordinator {
  protected def mahaService: MahaService

  def execute(mahaRequestContext: MahaRequestContext
              , mahaRequestLogHelper: MahaRequestLogHelper): RequestCoordinatorResult
}



object DefaultRequestCoordinator {
  import scalaz.syntax.validation._
  val noConfig: Validation[NonEmptyList[JsonScalaz.Error], CuratorConfig] = NoConfig.successNel
}
case class DefaultRequestCoordinator(protected val mahaService: MahaService) extends RequestCoordinator with Logging {

  override def execute(mahaRequestContext: MahaRequestContext
              , mahaRequestLogHelper: MahaRequestLogHelper): RequestCoordinatorResult = {
    val curatorJsonConfigMapFromRequest: Map[String, CuratorJsonConfig] = mahaRequestContext.reportingRequest.curatorJsonConfigMap
    val curatorsOrdered: SortedSet[Curator] = curatorJsonConfigMapFromRequest
      .keys
      .flatMap(mahaService.getMahaServiceConfig.curatorMap.get)
      .to[SortedSet]

    require(curatorsOrdered.size == 1 || curatorsOrdered.forall(!_.isSingleton)
      , s"Singleton curators can only be requested by themselves but found ${curatorsOrdered.map(c => (c.name, c.isSingleton))}")

    val orderedResultList: ArrayBuffer[ParRequest[CuratorResult]] =
      new ArrayBuffer[ParRequest[CuratorResult]](initialSize = curatorsOrdered.size + 1)

    //inject default if required
    val initialResultMap: Map[String, ParRequest[CuratorResult]] = {
      if (curatorsOrdered.exists(_.requiresDefaultCurator)) {
        val curator = mahaService.getMahaServiceConfig.curatorMap(DefaultCurator.name)
        val result = curator
          .process(Map.empty, mahaRequestContext, mahaService, mahaRequestLogHelper, DefaultRequestCoordinator.noConfig)
        orderedResultList += result
        Map(DefaultCurator.name -> result)
      } else Map.empty
    }

    val finalResultMap: Map[String, ParRequest[CuratorResult]] = curatorsOrdered.foldLeft(initialResultMap) {
      (prevResult, curator) =>
        val config = curator.parseConfig(curatorJsonConfigMapFromRequest(curator.name))
        val result = curator.process(prevResult, mahaRequestContext, mahaService, mahaRequestLogHelper, config)
        orderedResultList += result
        prevResult.+(curator.name -> result)
    }

    val pse = mahaService.getParallelServiceExecutor(mahaRequestContext)
    new RequestCoordinatorResult(orderedResultList, finalResultMap, pse)
  }

}
