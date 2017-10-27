// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service


import java.nio.charset.StandardCharsets
import java.util.concurrent.Callable

import com.google.common.io.Closer
import com.yahoo.maha.core._
import com.yahoo.maha.core.bucketing.{BucketParams, BucketSelector, BucketingConfig}
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.registry.{DimensionRegistrationFactory, FactRegistrationFactory, Registry, RegistryBuilder}
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.parrequest.future.{NoopRequest, ParRequest, ParallelServiceExecutor}
import com.yahoo.maha.parrequest.{Either, GeneralError, ParCallable, Right}
import com.yahoo.maha.service.config._
import com.yahoo.maha.service.error._
import com.yahoo.maha.service.factory._
import com.yahoo.maha.service.utils.{MahaRequestLogHelper, MahaRequestLogWriter}
import grizzled.slf4j.Logging
import org.json4s.jackson.JsonMethods.parse
import org.json4s.scalaz.JsonScalaz._

import scala.collection.mutable
import scala.util.Try
import scalaz.Validation.FlatMap._
import scalaz.syntax.validation._
import scalaz.{Failure, ValidationNel, _}


/**
 * Created by hiral on 5/26/17.
 */
case class RegistryConfig(name: String, registry: Registry, queryPipelineFactory: QueryPipelineFactory, queryExecutorContext: QueryExecutorContext, bucketSelector: BucketSelector, utcTimeProvider: UTCTimeProvider, parallelServiceExecutor: ParallelServiceExecutor)

case class MahaServiceConfig(registry: Map[String, RegistryConfig], mahaRequestLogWriter: MahaRequestLogWriter)

case class RequestResult(rowList: RowList, queryAttributes: QueryAttributes)

case class ParRequestResult(prodRun: ParRequest[RequestResult], dryRunOption: Option[ParRequest[RequestResult]])

trait MahaService {
  def mahaRequestLogWriter: MahaRequestLogWriter
  /**
   * Generates own model and create ParRequestResult and invoke the execution
   */
  def processRequest(registryName: String
                     , reportingRequest: ReportingRequest
                     , bucketParams: BucketParams
                     , mahaRequestLogHelper: MahaRequestLogHelper): Try[RequestResult]

  /**
   * Generates own model, create ParRequestResult. Invocation is left on the user's implementation details.
   */
  def executeRequest(registryName: String
                     , reportingRequest: ReportingRequest
                     , bucketParams: BucketParams, mahaRequestLogHelper: MahaRequestLogHelper): ParRequestResult

  def generateRequestModel(registryName: String, reportingRequest: ReportingRequest, bucketParams: BucketParams, mahaRequestLogHelper: MahaRequestLogHelper): Try[RequestModelResult]

  /**
   * Provide model
   */
  def processRequestModel(registryName: String, requestModel: RequestModel, mahaRequestLogHelper: MahaRequestLogHelper): Try[RequestResult]

  /**
   * Provide model result
   */
  def executeRequestModelResult(registryName: String, requestModelResult: RequestModelResult, mahaRequestLogHelper: MahaRequestLogHelper): ParRequestResult

  def getDomain(registryName: String) : Option[String]
  def getDomainForCube(registryName: String, cube : String) : Option[String]
  def getFlattenDomain(registryName: String) : Option[String]
  def getFlattenDomainForCube(registryName: String, cube : String, revision: Option[Int]= None) : Option[String]
}


class DefaultMahaService(config: MahaServiceConfig) extends MahaService with Logging {
  val mahaRequestLogWriter: MahaRequestLogWriter = config.mahaRequestLogWriter

  /**
   * Generates own model
   */
  override def processRequest(registryName: String
                              , reportingRequest: ReportingRequest
                              , bucketParams: BucketParams
                              , mahaRequestLogHelper: MahaRequestLogHelper): Try[RequestResult] = {
    val requestModelResultTry = generateRequestModel(registryName, reportingRequest, bucketParams, mahaRequestLogHelper)
    if (requestModelResultTry.isFailure) {
      val message = "Failed to create Report Model:"
      val error = requestModelResultTry.failed.toOption
      throw new MahaServiceBadRequestException(message, error)
      mahaRequestLogHelper.logFailed(message)
    }
    val requestModelResult = requestModelResultTry.get

    val parRequestResult = executeRequestModelResult(registryName, requestModelResult, mahaRequestLogHelper)
    val finalResult = syncParRequestExecutor(parRequestResult.prodRun, mahaRequestLogHelper)

    parRequestResult.dryRunOption.foreach(asyncParRequestExecutor(_,mahaRequestLogHelper))
    finalResult
  }

  /**
   * Generates own model
   */
  override def executeRequest(registryName: String
                              , reportingRequest: ReportingRequest
                              , bucketParams: BucketParams
                              , mahaRequestLogHelper: MahaRequestLogHelper): ParRequestResult = {
    val parLabel = "executeRequest"
    val requestModelResultTry = generateRequestModel(registryName, reportingRequest, bucketParams, mahaRequestLogHelper)
    if (requestModelResultTry.isFailure) {
      val message = "Failed to create Report Model:"
      val error = requestModelResultTry.failed.toOption
      throw new MahaServiceBadRequestException(message, error)
    }
    val requestModelResult = requestModelResultTry.get
    val reportingModel = requestModelResult.model
    val finalResult = createParRequest(registryName, reportingModel, parLabel, mahaRequestLogHelper)
    val dryRunResult = {
      if (requestModelResult.dryRunModelTry.isDefined &&
        requestModelResult.dryRunModelTry.get.isSuccess) {
        Some(createParRequest(registryName, requestModelResult.dryRunModelTry.get.get, parLabel, mahaRequestLogHelper))
      } else None
    }
    ParRequestResult(finalResult, dryRunResult)
  }

  override def generateRequestModel(registryName: String, reportingRequest: ReportingRequest, bucketParams: BucketParams, mahaRequestLogHelper: MahaRequestLogHelper): Try[RequestModelResult] = {
    val registryConfig = config.registry.get(registryName).get
    return RequestModelFactory.fromBucketSelector(reportingRequest, bucketParams, registryConfig.registry, registryConfig.bucketSelector, utcTimeProvider = registryConfig.utcTimeProvider)
  }

  /**
   * Provide model
   *
   */
  override def processRequestModel(registryName: String, requestModel: RequestModel, mahaRequestLogHelper: MahaRequestLogHelper): Try[RequestResult] = {
    val parLabel = "processRequestModel"
    val parRequest = createParRequest(registryName, requestModel, parLabel, mahaRequestLogHelper)
    syncParRequestExecutor(parRequest, mahaRequestLogHelper)
  }

  /**
   * Provide model
   */
  override def executeRequestModelResult(registryName: String, requestModelResult: RequestModelResult, mahaRequestLogHelper: MahaRequestLogHelper): ParRequestResult = {
    val parLabel = "executeRequestModelResult"
    val reportingModel = requestModelResult.model
    val finalResult = createParRequest(registryName, reportingModel, parLabel, mahaRequestLogHelper)
    val dryRunResult = {
      if (requestModelResult.dryRunModelTry.isDefined &&
        requestModelResult.dryRunModelTry.get.isSuccess) {
        Some(createParRequest(registryName, requestModelResult.dryRunModelTry.get.get, parLabel, mahaRequestLogHelper))
      } else None
    }
    ParRequestResult(finalResult, dryRunResult)
  }

  private def createParRequest(registryName: String, requestModel: RequestModel, parRequestLabel: String, mahaRequestLogHelper: MahaRequestLogHelper): ParRequest[RequestResult] = {
    val registryConfig = config.registry.get(registryName).get
    val queryPipelineFactory = registryConfig.queryPipelineFactory
    val parallelServiceExecutor = registryConfig.parallelServiceExecutor

    val parRequest = parallelServiceExecutor.parRequestBuilder[RequestResult].setLabel(parRequestLabel).
      setParCallable(ParCallable.from[Either[GeneralError, RequestResult]](
      new Callable[Either[GeneralError, RequestResult]](){
      override def call(): Either[GeneralError, RequestResult] = {
          val queryPipelineTry = queryPipelineFactory.from(requestModel, QueryAttributes.empty)
          if(queryPipelineTry.isFailure) {
            val error = queryPipelineTry.failed.get
            val message = error.getMessage
            mahaRequestLogHelper.logFailed(message)

            return GeneralError.either[RequestResult](parRequestLabel, message, new MahaServiceBadRequestException(message))
          }
          val queryPipeline = queryPipelineTry.get
          mahaRequestLogHelper.logQueryPipeline(queryPipeline)

          val rowListTry = queryPipeline.execute(registryConfig.queryExecutorContext, QueryAttributes.empty)
          if(rowListTry.isFailure) {
            val error = rowListTry.failed.get
            val message = error.getMessage
            mahaRequestLogHelper.logFailed(message)

            return GeneralError.either[RequestResult](parRequestLabel, message, new MahaServiceBadRequestException(message))
          }
          val rowList = rowListTry.get
          mahaRequestLogHelper.logQueryStats(rowList._2)
          mahaRequestLogHelper.logSuccess()

          return new Right[GeneralError, RequestResult](RequestResult(rowList._1, rowList._2))
      }
    }
    )).build()
    parRequest
  }

  /*
  Blocking call for ParRequest Execution as it using resultMap
   */
  private def syncParRequestExecutor(parRequest: ParRequest[RequestResult], mahaRequestLogHelper: MahaRequestLogHelper): Try[RequestResult] = {
    val requestResultEither = {
      parRequest.resultMap((t: RequestResult) => t)
    }

    requestResultEither.fold(
      (t: GeneralError) => {
        val error = t.throwableOption
        val message = s"Failed to execute the Request Model: ${t.message} "
        val exception = if (error.isDefined) {
          new MahaServiceExecutionException(message, Option(error.get()))
        } else {
          new MahaServiceExecutionException(message)
        }
        mahaRequestLogHelper.logFailed(message)
        scala.util.Failure(exception)
      },
      (requestResult: RequestResult) => {
        Try(requestResult)
      }
    )
  }

  /*
  Non Blocking call for ParRequest Execution as it is using fold/map
  */
  private def asyncParRequestExecutor(parRequest: ParRequest[RequestResult], mahaRequestLogHelper: MahaRequestLogHelper): Try[NoopRequest[Unit]] = {

    Try(parRequest.fold[Unit]((ge: GeneralError) => {
          val warnMessage= s"Failed to execute the dryRun Model ${ge.message} ${ge.throwableOption.get().getStackTrace}"
          mahaRequestLogHelper.logFailed(warnMessage)
          warn(warnMessage)
      }, (t: RequestResult) => {}
    ))
  }

  override def getDomain(registryName: String): Option[String] = {
    if (config.registry.contains(registryName)) {
      Some(config.registry.get(registryName).get.registry.domainJsonAsString)
    } else None
  }

  override def getFlattenDomain(registryName: String): Option[String] = {
    if (config.registry.contains(registryName)) {
      Some(config.registry.get(registryName).get.registry.flattenDomainJsonAsString)
    } else None
  }

  override def getDomainForCube(registryName: String, cube: String): Option[String] = {
    if (config.registry.contains(registryName)) {
      Some(config.registry.get(registryName).get.registry.getCubeJsonAsStringForCube(cube))
    } else None
  }

  override def getFlattenDomainForCube(registryName: String, cube: String, revision: Option[Int]): Option[String] = {
    if (config.registry.contains(registryName)) {
      if(revision.isDefined) {
        Some(config.registry.get(registryName).get.registry.getFlattenCubeJsonAsStringForCube(cube, revision.get))
      } else {
        Some(config.registry.get(registryName).get.registry.getFlattenCubeJsonAsStringForCube(cube))
      }
    } else None
  }
}

object MahaServiceConfig {
  type MahaConfigResult[+A] = scalaz.ValidationNel[MahaServiceError, A]

  private[this] val closer: Closer = Closer.create()

  def fromJson(ba: Array[Byte]): MahaServiceConfig.MahaConfigResult[MahaServiceConfig] = {
    val json = {
      Try(parse(new String(ba, StandardCharsets.UTF_8))) match {
        case t if t.isSuccess => t.get
        case t if t.isFailure => {
          return Failure(JsonParseError(s"invalidInputJson : ${t.failed.get.getMessage}", Option(t.failed.toOption.get))).toValidationNel
        }
      }
    }

    val jsonMahaServiceConfigResult: ValidationNel[MahaServiceError, JsonMahaServiceConfig] = fromJSON[JsonMahaServiceConfig](json).leftMap {
      nel => nel.map(err => JsonParseError(err.toString))
    }

    val mahaServiceConfig = for {
      jsonMahaServiceConfig <- jsonMahaServiceConfigResult
      validationResult <- validateReferenceByName(jsonMahaServiceConfig)
      bucketConfigMap <- initBucketingConfig(jsonMahaServiceConfig.bucketingConfigMap)
      utcTimeProviderMap <- initUTCTimeProvider(jsonMahaServiceConfig.utcTimeProviderMap)
      generatorMap <- initGenerators(jsonMahaServiceConfig.generatorMap)
      executorMap <- initExecutors(jsonMahaServiceConfig.executorMap)
      registryMap <- initRegistry(jsonMahaServiceConfig.registryMap)
      parallelServiceExecutorConfigMap <- initParallelServiceExecutors(jsonMahaServiceConfig.parallelServiceExecutorConfigMap)
      mahaRequestLogWriter <- initKafkaLogWriter(jsonMahaServiceConfig.jsonMahaRequestLogConfig)
    } yield {
        val resultMap: Map[String, RegistryConfig] = registryMap.map {
          case (regName, registry) => {
            val registryConfig = jsonMahaServiceConfig.registryMap.get(regName.toLowerCase).get

            implicit val queryGeneratorRegistry = new QueryGeneratorRegistry
            generatorMap.filter(g => registryConfig.generators.contains(g._1)).foreach {
              case (name, generator) =>
                queryGeneratorRegistry.register(generator.engine, generator)
            }
            val queryExecutorContext = new QueryExecutorContext
            executorMap.filter(e => registryConfig.executors.contains(e._1)).foreach {
              case (_, executor) =>
                queryExecutorContext.register(executor)
            }
            (regName -> RegistryConfig(regName,
              registry,
              new DefaultQueryPipelineFactory(),
              queryExecutorContext,
              new BucketSelector(registry, bucketConfigMap.get(registryConfig.bucketConfigName).get),
              utcTimeProviderMap.get(registryConfig.utcTimeProviderName).get,
              parallelServiceExecutorConfigMap.get(registryConfig.parallelServiceExecutorName).get))
          }
        }
        MahaServiceConfig(resultMap, mahaRequestLogWriter)
      }
    mahaServiceConfig
  }

  def validateReferenceByName(config: JsonMahaServiceConfig): MahaConfigResult[Boolean] = {
    //do some validation
    val registryJsonConfigMap = config.registryMap
    val errorList = new mutable.HashSet[MahaServiceError]

    registryJsonConfigMap.foreach {
      case (name, regConfig) =>
        if (!config.bucketingConfigMap.contains(regConfig.bucketConfigName)) {
          errorList.add(ServiceConfigurationError(s"Unable to find bucket config name ${regConfig.bucketConfigName} in map"))
        }
        if (!config.parallelServiceExecutorConfigMap.contains(regConfig.parallelServiceExecutorName)) {
          errorList.add(ServiceConfigurationError(s"Unable to find parallelServiceExecutor name ${regConfig.parallelServiceExecutorName} in map"))
        }
        if (!config.utcTimeProviderMap.contains(regConfig.utcTimeProviderName)) {
          errorList.add(ServiceConfigurationError(s"Unable to find utcTimeProvider name ${regConfig.utcTimeProviderName} in map"))
        }
        regConfig.generators.foreach {
          generator => if (!config.generatorMap.contains(generator)) {
            errorList.add(ServiceConfigurationError(s"Unable to find generator config name $generator in map"))
          }
        }
        regConfig.executors.foreach {
          executor => if (!config.executorMap.contains(executor)) {
            errorList.add(ServiceConfigurationError(s"Unable to find executor config name $executor in map"))
          }
        }
    }
    if (errorList.nonEmpty) {
      return Failure(errorList).toValidationNel.asInstanceOf[MahaConfigResult[Boolean]]
    }
    return true.successNel
  }

  def initBucketingConfig(bucketConfigMap: Map[String, JsonBucketingConfig]): MahaServiceConfig.MahaConfigResult[Map[String, BucketingConfig]] = {
    import Scalaz._
    val result: MahaServiceConfig.MahaConfigResult[Map[String, BucketingConfig]] = {
      val constructBucketingConfig: Iterable[MahaServiceConfig.MahaConfigResult[(String, BucketingConfig)]] = {
        bucketConfigMap.map {
          case (name, jsonConfig) =>
            for {
              factory <- getFactory[BucketingConfigFactory](jsonConfig.className, closer)
              built <- factory.fromJson(jsonConfig.json)
            } yield (name, built)
        }
      }
      val resultList: MahaServiceConfig.MahaConfigResult[List[(String, BucketingConfig)]] =
        constructBucketingConfig.toList.sequence[MahaServiceConfig.MahaConfigResult, (String, BucketingConfig)]

      resultList.map(_.toMap)
    }
    result
  }

  def initUTCTimeProvider(utcTimeProviderMap: Map[String, JsonUTCTimeProviderConfig]): MahaServiceConfig.MahaConfigResult[Map[String, UTCTimeProvider]] = {
    import Scalaz._
    val result: MahaServiceConfig.MahaConfigResult[Map[String, UTCTimeProvider]] = {
      val constructUTCTimeProvider: Iterable[MahaServiceConfig.MahaConfigResult[(String, UTCTimeProvider)]] = {
        utcTimeProviderMap.map {
          case (name, jsonConfig) =>
            for {
              factory <- getFactory[UTCTimeProvideryFactory](jsonConfig.className, closer)
              built <- factory.fromJson(jsonConfig.json)
            } yield (name, built)
        }
      }
      val resultList: MahaServiceConfig.MahaConfigResult[List[(String, UTCTimeProvider)]] =
        constructUTCTimeProvider.toList.sequence[MahaServiceConfig.MahaConfigResult, (String, UTCTimeProvider)]

      resultList.map(_.toMap)
    }
    result
  }

  def initExecutors(executorMap: Map[String, JsonQueryExecutorConfig]): MahaServiceConfig.MahaConfigResult[Map[String, QueryExecutor]] = {
    import Scalaz._
    val result: MahaServiceConfig.MahaConfigResult[Map[String, QueryExecutor]] = {
      val constructExecutor: Iterable[MahaServiceConfig.MahaConfigResult[(String, QueryExecutor)]] = {
        executorMap.map {
          case (name, jsonConfig) =>
            for {
              factory <- getFactory[QueryExecutoryFactory](jsonConfig.className, closer)
              built <- factory.fromJson(jsonConfig.json)
            } yield (name, built)
        }
      }
      val resultList: MahaServiceConfig.MahaConfigResult[List[(String, QueryExecutor)]] =
        constructExecutor.toList.sequence[MahaServiceConfig.MahaConfigResult, (String, QueryExecutor)]

      resultList.map(_.toMap)
    }
    result
  }

  def initGenerators(generatorMap: Map[String, JsonQueryGeneratorConfig]): MahaServiceConfig.MahaConfigResult[Map[String, QueryGenerator[_ <: EngineRequirement]]] = {
    import Scalaz._
    val result: MahaServiceConfig.MahaConfigResult[Map[String, QueryGenerator[_ <: EngineRequirement]]] = {
      val constructGenerator: Iterable[MahaServiceConfig.MahaConfigResult[(String, QueryGenerator[_ <: EngineRequirement])]] = {
        generatorMap.map {
          case (name, jsonConfig) =>
            val factoryResult: MahaConfigResult[QueryGeneratorFactory] = getFactory[QueryGeneratorFactory](jsonConfig.className, closer)
            val built: MahaConfigResult[QueryGenerator[_ <: EngineRequirement]] = factoryResult.flatMap(_.fromJson(jsonConfig.json))
            built.map(g => (name, g))
        }
      }
      val resultList: MahaServiceConfig.MahaConfigResult[List[(String, QueryGenerator[_ <: EngineRequirement])]] =
        constructGenerator.toList.sequence[MahaServiceConfig.MahaConfigResult, (String, QueryGenerator[_ <: EngineRequirement])]

      resultList.map(_.toMap)
    }
    result
  }

  def initRegistry(registryMap: Map[String, JsonRegistryConfig]): MahaServiceConfig.MahaConfigResult[Map[String, Registry]] = {
    import Scalaz._
    val result: MahaServiceConfig.MahaConfigResult[Map[String, Registry]] = {
      val constructRegistry: Iterable[MahaServiceConfig.MahaConfigResult[(String, Registry)]] = {
        registryMap.map {
          case (name, jsonConfig) =>
            for {
              factRegistrationFactory <- getFactory[FactRegistrationFactory](jsonConfig.factRegistrationdFactoryClass, closer)
              dimRegistrationFactory <- getFactory[DimensionRegistrationFactory](jsonConfig.dimensionRegistrationFactoryClass, closer)
              dimEstimatorFactory <- getFactory[DimCostEstimatorFactory](jsonConfig.dimEstimatorFactoryClass, closer)
              factEstimatorFactory <- getFactory[FactCostEstimatorFactory](jsonConfig.factEstimatorFactoryClass, closer)
              dimEstimator <- dimEstimatorFactory.fromJson(jsonConfig.dimEstimatorFactoryConfig)
              factEstimator <- factEstimatorFactory.fromJson(jsonConfig.factEstimatorFactoryConfig)
            } yield {
              val registryBuilder = new RegistryBuilder
              factRegistrationFactory.register(registryBuilder)
              dimRegistrationFactory.register(registryBuilder)
              (name, registryBuilder.build(dimEstimator, factEstimator, jsonConfig.defaultPublicFactRevisionMap, jsonConfig.defaultPublicDimRevisionMap))
            }
        }
      }
      val resultList: MahaServiceConfig.MahaConfigResult[List[(String, Registry)]] =
        constructRegistry.toList.sequence[MahaServiceConfig.MahaConfigResult, (String, Registry)]

      resultList.map(_.toMap)
    }
    result
  }

  def initParallelServiceExecutors(parallelServiceExecutorConfigMap: Map[String, JsonParallelServiceExecutorConfig]): MahaServiceConfig.MahaConfigResult[Map[String, ParallelServiceExecutor]] = {
    import Scalaz._
    val result: MahaServiceConfig.MahaConfigResult[Map[String, ParallelServiceExecutor]] = {
      val constructParallelServiceExecutor: Iterable[MahaServiceConfig.MahaConfigResult[(String, ParallelServiceExecutor)]] = {
        parallelServiceExecutorConfigMap.map {
          case (name, jsonConfig) =>
            for {
              factoryResult <- getFactory[ParallelServiceExecutoryFactory](jsonConfig.className, closer)
              built <- factoryResult.fromJson(jsonConfig.json)
            } yield (name, built)
        }
      }
      val resultList: MahaServiceConfig.MahaConfigResult[List[(String, ParallelServiceExecutor)]] =
        constructParallelServiceExecutor.toList.sequence[MahaServiceConfig.MahaConfigResult, (String, ParallelServiceExecutor)]

      resultList.map(_.toMap)
    }
    result
  }

  def initKafkaLogWriter(jsonMahaRequestLogConfig: JsonMahaRequestLogConfig): MahaServiceConfig.MahaConfigResult[MahaRequestLogWriter] = {
    val result: MahaServiceConfig.MahaConfigResult[MahaRequestLogWriter] = {
      val requestLogWriter = {
        for {
          factoryResult <- getFactory[MahaRequestLogWriterFactory](jsonMahaRequestLogConfig.className, closer)
          requestLogWriter <- factoryResult.fromJson(jsonMahaRequestLogConfig.kafkaConfig, jsonMahaRequestLogConfig.isLoggingEnabled)
        } yield (requestLogWriter)
      }
      requestLogWriter
    }
    result
  }

}