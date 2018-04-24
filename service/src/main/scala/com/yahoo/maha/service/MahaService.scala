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
import com.yahoo.maha.parrequest2.future.{ParRequest, ParallelServiceExecutor}
import com.yahoo.maha.parrequest2.{GeneralError, ParCallable}
import com.yahoo.maha.service.config._
import com.yahoo.maha.service.curators.Curator
import com.yahoo.maha.service.error._
import com.yahoo.maha.service.factory._
import com.yahoo.maha.service.utils.{BaseMahaRequestLogBuilder, MahaRequestLogWriter}
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

case class MahaServiceConfig(registry: Map[String, RegistryConfig], mahaRequestLogWriter: MahaRequestLogWriter, curatorMap: Map[String, Curator])

case class RequestResult(queryPipelineResult: QueryPipelineResult)

case class ParRequestResult(queryPipeline: Try[QueryPipeline], prodRun: ParRequest[RequestResult], dryRunOption: Option[ParRequest[RequestResult]])

case class QueryChainResult(queryChain:QueryChain, dryRunQueryChainOption:Option[QueryChain])

trait MahaService {

  /**
    * validate a registry name
    * @param name
    * @return
    */
  def isValidRegistry(name: String): Boolean

  /*
   Kafka logger for every Maha Reporting Request
 */
  def mahaRequestLogWriter: MahaRequestLogWriter

  /**
   * Generates own model and create ParRequestResult and invoke the execution
   */
  def processRequest(registryName: String
                     , reportingRequest: ReportingRequest
                     , bucketParams: BucketParams
                     , mahaRequestLogBuilder: BaseMahaRequestLogBuilder): Either[GeneralError, RequestResult]

  /**
   * Generates own model, create ParRequestResult. Invocation is left on the user's implementation details.
   */
  def executeRequest(registryName: String
                     , reportingRequest: ReportingRequest
                     , bucketParams: BucketParams
                     , mahaRequestLogBuilder: BaseMahaRequestLogBuilder): ParRequestResult

  /*
   Generates the RequestModelResult for given ReportingRequest
   */
  def generateRequestModel(registryName: String,
                           reportingRequest: ReportingRequest,
                           bucketParams: BucketParams,
                           mahaRequestLogBuilder: BaseMahaRequestLogBuilder): Try[RequestModelResult]

  /*
   Generate the query execution metadata for the given RequestModelResult
   */
  def generateQueryMetadata(mahaRequestContext: MahaRequestContext,
                                    requestModelResult: RequestModelResult,
                                    mahaRequestLogHelper: BaseMahaRequestLogBuilder): QueryChainResult


  /**
    * generate query pipeline from model
    * @param registryName
    * @param requestModel
    * @param mahaRequestLogBuilder
    * @return
    */
  def generateQueryPipeline(registryName: String,
                                    requestModel: RequestModel,
                                    mahaRequestLogBuilder: BaseMahaRequestLogBuilder): Try[QueryPipeline]

  /**
   * Executes the RequestModel and provide the RequestResult
   */
  def processRequestModel(registryName: String,
                          requestModel: RequestModel,
                          mahaRequestLogBuilder: BaseMahaRequestLogBuilder): Either[GeneralError, RequestResult]

  /**
   * Prepare the ParRequests for given RequestModel. Invocation is left on the user's implementation details.
   */
  def executeRequestModelResult(registryName: String,
                                requestModelResult: RequestModelResult,
                                mahaRequestLogBuilder: BaseMahaRequestLogBuilder): ParRequestResult

  def getDomain(registryName: String) : Option[String]
  def getDomainForCube(registryName: String, cube : String) : Option[String]
  def getFlattenDomain(registryName: String) : Option[String]
  def getFlattenDomainForCube(registryName: String, cube : String, revision: Option[Int]= None) : Option[String]

  /*
    Defines list of engines which are not capable calculating the totalRowCount in one run
   */
  def rowCountIncomputableEngineSet: Set[Engine]

  def getMahaServiceConfig: MahaServiceConfig

  /**
    * Note, it is assumed the context has been validated already, e.g. registry name is valid
    * @param mahaRequestContext
    * @return
    */
  def getParallelServiceExecutor(mahaRequestContext: MahaRequestContext) : ParallelServiceExecutor


  protected def validateRegistry(mahaRequestContext: MahaRequestContext): Unit = {
    validateRegistry(mahaRequestContext.registryName)
  }
  protected def validateRegistry(name: String): Unit = {
    require(isValidRegistry(name), s"Unknown registry : $name")
  }
}

case class DefaultMahaService(config: MahaServiceConfig) extends MahaService with Logging {

  override def isValidRegistry(name: String): Boolean = config.registry.contains(name)
  override val mahaRequestLogWriter: MahaRequestLogWriter = config.mahaRequestLogWriter
  val rowCountIncomputableEngineSet: Set[Engine] = Set(DruidEngine)

  override def getMahaServiceConfig: MahaServiceConfig = config

  /**
   * Generates own model and create ParRequestResult and invoke the execution
   */
  override def processRequest(registryName: String
                              , reportingRequest: ReportingRequest
                              , bucketParams: BucketParams
                              , mahaRequestLogBuilder: BaseMahaRequestLogBuilder): Either[GeneralError, RequestResult] = {
    val requestModelResultTry = generateRequestModel(registryName, reportingRequest, bucketParams, mahaRequestLogBuilder)

    if (requestModelResultTry.isFailure) {
      val message = "Failed to create Report Model:"
      val error = requestModelResultTry.failed.toOption
      mahaRequestLogBuilder.logFailed(message)
      throw new MahaServiceBadRequestException(message, error)
    }
    val requestModelResult = requestModelResultTry.get

    val result = processRequestModel(registryName, requestModelResult.model, mahaRequestLogBuilder)
    Try(
      requestModelResult
        .dryRunModelTry
        .foreach(
          _.toOption
            .foreach( model => asParRequest(registryName, model, "dryRunProcessRequest", mahaRequestLogBuilder))
        )
    )
    result
  }

  /**
   * Generates own model, create ParRequestResult. Invocation is left on the user's implementation details.
   */
  override def executeRequest(registryName: String
                              , reportingRequest: ReportingRequest
                              , bucketParams: BucketParams
                              , mahaRequestLogBuilder: BaseMahaRequestLogBuilder): ParRequestResult = {
    val parLabel = "executeRequest"
    val requestModelResultTry = generateRequestModel(registryName, reportingRequest, bucketParams, mahaRequestLogBuilder)
    if (requestModelResultTry.isFailure) {
      val message = "Failed to create Report Model:"
      val error = requestModelResultTry.failed.toOption
      mahaRequestLogBuilder.logFailed(message)
      logger.error(message, error.get)
      throw new MahaServiceBadRequestException(message, error)
    }
    val requestModelResult = requestModelResultTry.get
    val reportingModel = requestModelResult.model
    val (queryPipelineTry, finalResult) = asParRequest(registryName, reportingModel, parLabel, mahaRequestLogBuilder)
    val dryRunResult = {
      if (requestModelResult.dryRunModelTry.isDefined &&
        requestModelResult.dryRunModelTry.get.isSuccess) {
        val dryRunResult = asParRequest(registryName, requestModelResult.dryRunModelTry.get.get, parLabel, mahaRequestLogBuilder)
        Option(dryRunResult._2)
      } else None
    }
    ParRequestResult(queryPipelineTry, finalResult, dryRunResult)
  }

  override def generateRequestModel(registryName: String, reportingRequest: ReportingRequest, bucketParams: BucketParams, mahaRequestLogBuilder: BaseMahaRequestLogBuilder): Try[RequestModelResult] = {
    validateRegistry(registryName)
    val registryConfig = config.registry(registryName)
    return RequestModelFactory.fromBucketSelector(reportingRequest, bucketParams, registryConfig.registry, registryConfig.bucketSelector, utcTimeProvider = registryConfig.utcTimeProvider)
  }

  /**
   * Executes the RequestModel and provide the RequestResult
   */
  override def processRequestModel(registryName: String, requestModel: RequestModel, mahaRequestLogBuilder: BaseMahaRequestLogBuilder):Either[GeneralError, RequestResult] = {
    val parLabel = "processRequestModel"
    asRequest(registryName, requestModel, parLabel, mahaRequestLogBuilder)
  }

  /**
   * Prepare the ParRequests for given RequestModel. Invocation is left on the user's implementation details.
   */
  override def executeRequestModelResult(registryName: String, requestModelResult: RequestModelResult, mahaRequestLogBuilder: BaseMahaRequestLogBuilder): ParRequestResult = {
    val parLabel = "executeRequestModelResult"
    val reportingModel = requestModelResult.model
    val (queryPipelineTry, finalResult) = asParRequest(registryName, reportingModel, parLabel, mahaRequestLogBuilder)
    val dryRunResult = {
      if (requestModelResult.dryRunModelTry.isDefined &&
        requestModelResult.dryRunModelTry.get.isSuccess) {
        val dryRunResult = asParRequest(registryName, requestModelResult.dryRunModelTry.get.get, parLabel, mahaRequestLogBuilder)
        Option(dryRunResult._2)
      } else None
    }
    ParRequestResult(queryPipelineTry, finalResult, dryRunResult)
  }

  def generateQueryPipeline(registryName: String,
                                     requestModel: RequestModel,
                                     mahaRequestLogBuilder: BaseMahaRequestLogBuilder): Try[QueryPipeline] = {

    val registryConfig = config.registry(registryName)
    val queryPipelineFactory = registryConfig.queryPipelineFactory
    val parallelServiceExecutor = registryConfig.parallelServiceExecutor

    val queryPipelineTry = queryPipelineFactory.from(requestModel, QueryAttributes.empty)
    queryPipelineTry
  }

  private def processQueryPipeline(registryConfig: RegistryConfig,
                           queryPipeline: QueryPipeline,
                           mahaRequestLogBuilder: BaseMahaRequestLogBuilder): Try[QueryPipelineResult] = {
    mahaRequestLogBuilder.logQueryPipeline(queryPipeline)
    val result = queryPipeline.execute(registryConfig.queryExecutorContext, QueryAttributes.empty)
    if(result.isSuccess) {
      val queryPipelineResult = result.get
      mahaRequestLogBuilder.logQueryStats(queryPipelineResult.queryAttributes)
    }
    result
  }

  private def asRequest(registryName: String, requestModel: RequestModel, parRequestLabel: String, mahaRequestLogBuilder: BaseMahaRequestLogBuilder): Either[GeneralError, RequestResult] = {
    validateRegistry(registryName)
    val registryConfig = config.registry(registryName)
    val parallelServiceExecutor = registryConfig.parallelServiceExecutor

    val queryPipelineTry = generateQueryPipeline(registryName, requestModel, mahaRequestLogBuilder)
    if(queryPipelineTry.isFailure) {
      val error = queryPipelineTry.failed.get
      val message = s"Failed to compile the query pipeline ${error.getMessage}"
      logger.error(message, error)
      GeneralError.either("createQueryPipeline", message, error)
    } else {
      val triedQueryPipelineResult = processQueryPipeline(registryConfig, queryPipelineTry.get, mahaRequestLogBuilder)
      if (triedQueryPipelineResult.isFailure) {
        val error = triedQueryPipelineResult.failed.get
        val message = s"Failed to execute the query pipeline"
        logger.error(message, error)
        GeneralError.either[RequestResult](parRequestLabel, message, new MahaServiceExecutionException(message, Some(error)))
      } else {
        new Right[GeneralError, RequestResult](RequestResult(triedQueryPipelineResult.get))
      }
    }
  }

  private def asParRequest(registryName: String, requestModel: RequestModel, parRequestLabel: String, mahaRequestLogBuilder: BaseMahaRequestLogBuilder): (Try[QueryPipeline], ParRequest[RequestResult])= {
    validateRegistry(registryName)
    val registryConfig = config.registry(registryName)
    val parallelServiceExecutor = registryConfig.parallelServiceExecutor

    val queryPipelineTry = generateQueryPipeline(registryName, requestModel, mahaRequestLogBuilder)
    if(queryPipelineTry.isFailure) {
      val error = queryPipelineTry.failed.get
      val message = s"Failed to compile the query pipeline ${error.getMessage}"
      logger.error(message, error)
      mahaRequestLogBuilder.logFailed(message)
      return (queryPipelineTry, parallelServiceExecutor.immediateResult("createParRequest", GeneralError.either("createQueryPipeline", message, error)))
    } else {
      val queryPipeline = queryPipelineTry.get
      val parRequest = parallelServiceExecutor.parRequestBuilder[RequestResult].setLabel(parRequestLabel).
        setParCallable(ParCallable.from[Either[GeneralError, RequestResult]](
          new Callable[Either[GeneralError, RequestResult]]() {
            override def call(): Either[GeneralError, RequestResult] = {
              val triedQueryPipelineResult = processQueryPipeline(registryConfig, queryPipelineTry.get, mahaRequestLogBuilder)
              if (triedQueryPipelineResult.isFailure) {
                val error = triedQueryPipelineResult.failed.get
                val message = s"Failed to execute the query pipeline"
                logger.error(message, error)
                GeneralError.either[RequestResult](parRequestLabel, message, new MahaServiceExecutionException(message, Some(error)))
              } else {
                new Right[GeneralError, RequestResult](RequestResult(triedQueryPipelineResult.get))
              }
            }
          }
        )).build()
      (queryPipelineTry, parRequest)
    }
  }

  override def getDomain(registryName: String): Option[String] = {
    if (config.registry.contains(registryName)) {
      Some(config.registry(registryName).registry.domainJsonAsString)
    } else None
  }

  override def getFlattenDomain(registryName: String): Option[String] = {
    if (config.registry.contains(registryName)) {
      Some(config.registry(registryName).registry.flattenDomainJsonAsString)
    } else None
  }

  override def getDomainForCube(registryName: String, cube: String): Option[String] = {
    if (config.registry.contains(registryName)) {
      Some(config.registry(registryName).registry.getCubeJsonAsStringForCube(cube))
    } else None
  }

  override def getFlattenDomainForCube(registryName: String, cube: String, revision: Option[Int]): Option[String] = {
    if (config.registry.contains(registryName)) {
      if(revision.isDefined) {
        Some(config.registry(registryName).registry.getFlattenCubeJsonAsStringForCube(cube, revision.get))
      } else {
        Some(config.registry(registryName).registry.getFlattenCubeJsonAsStringForCube(cube))
      }
    } else None
  }

  /**
    * Note, it is assumed the context has been validated already, e.g. registry name is valid
    * @param mahaRequestContext
    * @return
    */
  override def getParallelServiceExecutor(mahaRequestContext: MahaRequestContext) : ParallelServiceExecutor = {
    validateRegistry(mahaRequestContext)
    config.registry(mahaRequestContext.registryName).parallelServiceExecutor
  }

  /*
   Generate the query execution metadata for the given RequestModelResult
   */
  override def generateQueryMetadata(mahaRequestContext: MahaRequestContext,
                                             requestModelResult: RequestModelResult,
                                             mahaRequestLogHelper: BaseMahaRequestLogBuilder): QueryChainResult = {
    validateRegistry(mahaRequestContext)

    val queryPipeline = generateQueryPipeline(requestModelResult.model, mahaRequestContext.registryName, mahaRequestLogHelper)
    mahaRequestLogHelper.logQueryPipeline(queryPipeline)

    val dryRunQueryChainOption:Option[QueryChain]  = {
      if (requestModelResult.dryRunModelTry.isDefined) {
        val requestModelTry = requestModelResult.dryRunModelTry.get
        if(requestModelTry.isFailure) {
          val error = requestModelTry.failed.get
          val message = s"DryRun, Failed to create the request Model ${error.getMessage}"
          logger.error(message, error)
          error.printStackTrace()
          mahaRequestLogHelper.logFailed(message)
          throw new MahaServiceBadRequestException(message, Some(error))
        }
        Some(generateQueryPipeline(requestModelTry.get, mahaRequestContext.registryName, mahaRequestLogHelper, true).queryChain)
      } else None
    }
    QueryChainResult(queryPipeline.queryChain, dryRunQueryChainOption)
  }

  private[this] def generateQueryPipeline(requestModel : RequestModel,
                                          registryName:String,
                                          mahaRequestLogHelper: BaseMahaRequestLogBuilder,
                                          isDryRun: Boolean = false): QueryPipeline = {
    val registry = config.registry.get(registryName).get
    val queryPipelineFactory = registry.queryPipelineFactory
    val queryPipelineTry = queryPipelineFactory.from(requestModel, QueryAttributes.empty)
    if(queryPipelineTry.isFailure) {
      val error = queryPipelineTry.failed.get
      val message = if (isDryRun) {
        s"DryRun, Failed to compile the query pipeline ${error.getMessage}"
      } else s"Failed to compile the query pipeline ${error.getMessage}"
      logger.error(message, error)
      error.printStackTrace()
      mahaRequestLogHelper.logFailed(message)
      throw new MahaServiceBadRequestException(message, Some(error))
    }
    queryPipelineTry.get
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
      curatorMap <- initCurators(jsonMahaServiceConfig.curatorMap)
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
        MahaServiceConfig(resultMap, mahaRequestLogWriter, curatorMap)
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

  def initCurators(curatorConfigMap: Map[String, JsonCuratorConfig]): MahaServiceConfig.MahaConfigResult[Map[String, Curator]] = {
    import Scalaz._
    val result: MahaServiceConfig.MahaConfigResult[Map[String, Curator]] = {
      val constructCurator: Iterable[MahaServiceConfig.MahaConfigResult[(String, Curator)]] = {
        curatorConfigMap.map {
          case (name, jsonConfig) =>
            for {
              factoryResult <- getFactory[CuratorFactory](jsonConfig.className, closer)
              built <- factoryResult.fromJson(jsonConfig.json)
            } yield (name, built)
        }
      }
      val resultList: MahaServiceConfig.MahaConfigResult[List[(String, Curator)]] =
        constructCurator.toList.sequence[MahaServiceConfig.MahaConfigResult, (String, Curator)]

      resultList.map(_.toMap)
    }
    result
  }

}