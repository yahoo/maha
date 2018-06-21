package com.yahoo.maha.service

import com.yahoo.maha.parrequest2.GeneralError
import com.yahoo.maha.parrequest2.future._
import com.yahoo.maha.service.curators.{CuratorError, _}
import com.yahoo.maha.service.error.{MahaServiceBadRequestException, MahaServiceExecutionException}
import com.yahoo.maha.service.utils.MahaRequestLogBuilder
import grizzled.slf4j.Logging
import org.json4s.scalaz.JsonScalaz
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.{SortedSet, mutable}
import scala.collection.mutable.ArrayBuffer
import scalaz.{NonEmptyList, Validation}

case class RequestCoordinatorError(generalError: GeneralError, curatorError: Option[CuratorError] = None)
  extends GeneralError(generalError.stage, generalError.message, generalError.throwableOption)
object RequestCoordinatorError {
  implicit def apply(generalError: GeneralError) : RequestCoordinatorError = {
    new RequestCoordinatorError(generalError)
  }
  implicit def apply(curatorError: CuratorError) : RequestCoordinatorError = {
    new RequestCoordinatorError(curatorError.error, Option(curatorError))
  }
}

case class RequestCoordinatorResult(orderedList: IndexedSeq[Curator]
                                    , curatorResult: Map[String, CuratorResult]
                                    , failureResults: Map[String, CuratorError]
                                    , successResults: Map[String, RequestResult]
                                    , mahaRequestContext: MahaRequestContext
                                   )


object CuratorInjector {
  val logger = LoggerFactory.getLogger(classOf[CuratorInjector])
}
/**
  * Stores the results from curator - not thread safe
  * @param initialSize
  */
class CuratorInjector(initialSize: Int
                      , mahaService: MahaService
                      , mahaRequestLogBuilder: MahaRequestLogBuilder
                      , requestedCurators: Set[String]
                     ) {
  val curatorList: ArrayBuffer[Curator] = new ArrayBuffer[Curator](initialSize)
  val orderedResultList: ArrayBuffer[Either[CuratorError, ParRequest[CuratorResult]]] =
    new ArrayBuffer(initialSize = initialSize)
  var resultMap: Map[String, Either[GeneralError, ParRequest[CuratorResult]]] = Map.empty

  def injectCurator(curatorName: String
                    , inputResultMap: Map[String, Either[CuratorError, ParRequest[CuratorResult]]]
                    , mahaRequestContext: MahaRequestContext
                    , curatorConfig: CuratorConfig): Unit = {
    try {
      require(!requestedCurators(curatorName), s"Cannot inject curator already requested : $curatorName")
      mahaService.getMahaServiceConfig.curatorMap.get(curatorName).foreach {
        curatorToInject =>
          val result: Either[CuratorError, ParRequest[CuratorResult]] = curatorToInject.process(inputResultMap
            , mahaRequestContext
            , mahaService
            , mahaRequestLogBuilder.curatorLogBuilder(curatorToInject)
            , curatorConfig
            , this
          )
          curatorList += curatorToInject
          orderedResultList += result
          resultMap += curatorName -> result
      }
    }
    catch {
      case e: Exception =>
        CuratorInjector.logger.error(s"Failed to inject curator : $curatorName", e)
    }
  }
}

trait RequestCoordinator {
  protected def mahaService: MahaService

  def execute(mahaRequestContext: MahaRequestContext
              , mahaRequestLogBuilder: MahaRequestLogBuilder): Either[RequestCoordinatorError, ParRequest[RequestCoordinatorResult]]

  protected def withError(generalError: GeneralError) : Either[RequestCoordinatorError, ParRequest[RequestCoordinatorResult]] = {
    generalError match  {
      case ce: CuratorError => withError(ce)
      case ge: GeneralError => new Left(generalError)
    }
  }
  protected def withError(curatorError: CuratorError) : Either[RequestCoordinatorError, ParRequest[RequestCoordinatorResult]] = {
    new Left(curatorError)
  }
}

case class DefaultRequestCoordinator(protected val mahaService: MahaService) extends RequestCoordinator with Logging {

  def curatorMap: Map[String, Curator] = mahaService.getMahaServiceConfig.curatorMap

  override def execute(mahaRequestContext: MahaRequestContext
              , mahaRequestLogBuilder: MahaRequestLogBuilder): Either[RequestCoordinatorError, ParRequest[RequestCoordinatorResult]] = {
    try {
      val curatorConfigErrorList: ArrayBuffer[Validation[NonEmptyList[JsonScalaz.Error], CuratorConfig]] = ArrayBuffer.empty
      val curatorConfigMapFromRequest: Map[String, CuratorConfig] = {
        val mmap: mutable.Map[String, CuratorConfig] = new mutable.HashMap()
        mahaRequestContext.reportingRequest.curatorJsonConfigMap.foreach {
          case (name, json) if curatorMap.contains(name) =>
            val result = curatorMap(name).parseConfig(json)
            if (result.isFailure) {
              curatorConfigErrorList += result
            } else {
              mmap += name -> result.toOption.get
            }
          case _ =>
            //ignore unknown
        }
        mmap.toMap
      }

      if(curatorConfigErrorList.nonEmpty) {
        return withError(GeneralError.from("curatorJsonParse", curatorConfigErrorList.mkString("\n")))
      }

      val curatorsOrdered: SortedSet[Curator] = curatorConfigMapFromRequest
        .keys
        .flatMap(mahaService.getMahaServiceConfig.curatorMap.get)
        .to[SortedSet]

      if (mahaRequestContext.reportingRequest.isDebugEnabled) {
        info(s"curators from request : ${curatorsOrdered.map(_.name)}")
        if (curatorsOrdered.size != mahaRequestContext.reportingRequest.curatorJsonConfigMap.size) {
          info(s"Curators not found : ${mahaRequestContext.reportingRequest.curatorJsonConfigMap.keySet -- curatorConfigMapFromRequest.keySet}")
        }
      }

      if (curatorsOrdered.size > 1 && curatorsOrdered.exists(_.isSingleton)) {
        withError(GeneralError.from("singletonCheck"
          , s"Singleton curators can only be requested by themselves but found ${curatorsOrdered.map(c => (c.name, c.isSingleton))}"))
      } else {

        val curatorInjector = new CuratorInjector(curatorsOrdered.size
          , mahaService, mahaRequestLogBuilder, curatorConfigMapFromRequest.keySet)
        val orderedResultList: ArrayBuffer[Either[CuratorError, ParRequest[CuratorResult]]] =
          new ArrayBuffer(initialSize = curatorsOrdered.size + 1)

        //inject default if required
        val initialResultMap: Map[String, Either[CuratorError, ParRequest[CuratorResult]]] = {
          if (curatorsOrdered.exists(_.requiresDefaultCurator)) {
            debugInfo(s"injecting default curator", mahaRequestContext)
            val curator = curatorMap(DefaultCurator.name)
            val logHelper = mahaRequestLogBuilder.curatorLogBuilder(curator)
            val result: Either[CuratorError, ParRequest[CuratorResult]] = curator
              .process(Map.empty, mahaRequestContext, mahaService, logHelper, NoConfig, curatorInjector)
            //short circuit on default failure
            if (result.isLeft) {
              return withError(result.left.get)
            }
            orderedResultList += result
            Map(DefaultCurator.name -> result)
          } else Map.empty
        }

        val resultMap: Map[String, Either[CuratorError, ParRequest[CuratorResult]]] = curatorsOrdered.foldLeft(initialResultMap) {
          (prevResult, curator) =>
            val config = curatorConfigMapFromRequest(curator.name)
            val logHelper = mahaRequestLogBuilder.curatorLogBuilder(curator)
            val result = curator.process(prevResult, mahaRequestContext, mahaService, logHelper, config, curatorInjector)
            orderedResultList += result
            prevResult.+(curator.name -> result)
        }

        val pse = mahaService.getParallelServiceExecutor(mahaRequestContext)
        orderedResultList ++= curatorInjector.orderedResultList
        val finalResultMap = resultMap ++ curatorInjector.resultMap

        val combinedCuratorResultList: ParRequestListEither[CuratorResult] = {
          val futures: java.util.List[CombinableRequest[CuratorResult]] = orderedResultList
            .view.map {
            case Right(parRequest) =>
              parRequest
            case error: Left[CuratorError, ParRequest[CuratorResult]] =>
              pse.immediateResult[ParRequest[CuratorResult]]("combinedResultList", error)
          }.map(_.asInstanceOf[CombinableRequest[CuratorResult]]).toList.asJava
          pse.combineListEither(futures)
        }

        val result: ParRequest[RequestCoordinatorResult] = combinedCuratorResultList.flatMap("combinedResultListFlatMap", ParFunction.fromScala {
          javaCuratorResult =>
            val seq = javaCuratorResult.asScala.toIndexedSeq
            val firstErrorOrResult = seq.head
            //fail fast on first failure in curator execution
            if (firstErrorOrResult.isLeft) {
              val ge = firstErrorOrResult.left.get
              throw ge.throwableOption.getOrElse(new MahaServiceBadRequestException(ge.message))
            } else {
              //fail fast on first failure in request execution
              val firstResult = firstErrorOrResult.right.get
              val firstResultParRequestResultOption = firstResult.parRequestResultOption
              if (firstResultParRequestResultOption.isEmpty) {
                throw new MahaServiceExecutionException("firstResultFailFast: No result defined")
              } else {
                firstResultParRequestResultOption.get.prodRun.flatMap("firstResultMap",
                  ParFunction.fromScala {
                    firstRequestResult =>
                      var i = 0
                      val failureResults = new scala.collection.mutable.HashMap[String, CuratorError]
                      val inProgressResults = new scala.collection.mutable.ArrayBuffer[CuratorResult]
                      while (i < seq.size) {
                        val errorOrResult = seq(i)
                        if (errorOrResult.isLeft) {
                          errorOrResult.left.get match {
                            case ce: CuratorError =>
                              failureResults.put(ce.curator.name, ce)
                            case ge: GeneralError =>
                              if (ge.throwableOption.isDefined) {
                                logger.error(s"Unknown curator error : ${ge.stage} : ${ge.message}", ge.throwableOption.get)
                              } else {
                                logger.error(s"Unknown curator error : ${ge.toString}")
                              }
                          }
                        } else {
                          inProgressResults += errorOrResult.right.get
                        }
                        i += 1
                      }
                      val combinedRequestResultList: ParRequestListEither[RequestResult] = {
                        val futures: java.util.List[CombinableRequest[RequestResult]] = inProgressResults
                          .flatMap(_.parRequestResultOption.map(_.prodRun))
                          .map(_.asInstanceOf[CombinableRequest[RequestResult]]).toList.asJava
                        pse.combineListEither(futures)
                      }
                      combinedRequestResultList.map("combinedRequestResultListMap",
                        ParFunction.fromScala {
                          javaRequestResult =>
                            val requestResultSeq = javaRequestResult.asScala.toIndexedSeq
                            var j = 0
                            val successResults = new scala.collection.mutable.HashMap[String, RequestResult]
                            while (j < requestResultSeq.size) {
                              val curatorResult = inProgressResults(j)
                              val errorOrResult = requestResultSeq(j)
                              if (errorOrResult.isLeft) {
                                errorOrResult.left.get match {
                                  case ce: CuratorError =>
                                    failureResults.put(ce.curator.name, ce)
                                  case ge: GeneralError =>
                                    failureResults.put(curatorResult.curator.name, CuratorError(curatorResult.curator, curatorResult.curatorConfig, ge))
                                }
                              } else {
                                successResults.put(curatorResult.curator.name, errorOrResult.right.get)
                              }
                              j += 1
                            }
                            val finalOrderedList: IndexedSeq[Curator] = curatorsOrdered.toIndexedSeq ++ curatorInjector.curatorList
                            val curatorResultMap: Map[String, CuratorResult] = inProgressResults.map(cr => cr.curator.name -> cr).toMap
                            new Right(RequestCoordinatorResult(finalOrderedList, curatorResultMap, failureResults.toMap, successResults.toMap, mahaRequestContext))
                        }
                      )
                  })
              }
            }
        })
        new Right(result)
      }
    } catch {
      case t: Throwable => new Left(RequestCoordinatorError(GeneralError.from("executeDefaultRequestCoordinator", "unknown error", t)))
    }
  }

  def debugInfo(msg: String, mahaRequestContext: MahaRequestContext): Unit = {
    if(mahaRequestContext.reportingRequest.isDebugEnabled) {
      info(msg)
    }
  }
}
