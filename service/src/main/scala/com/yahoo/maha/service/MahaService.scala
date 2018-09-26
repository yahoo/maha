// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service


import java.nio.charset.StandardCharsets
import java.util.concurrent.Callable
import java.util.regex.Pattern

import com.yahoo.maha.utils.DynamicConfigurationUtils._
import com.google.common.io.Closer
import com.yahoo.maha.core._
import com.yahoo.maha.core.bucketing.{BucketParams, BucketSelector, BucketingConfig}
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.registry.{DimensionRegistrationFactory, FactRegistrationFactory, Registry, RegistryBuilder}
import com.yahoo.maha.core.request.{ReportingRequest, fieldExtended}
import com.yahoo.maha.log.MahaRequestLogWriter
import com.yahoo.maha.parrequest2.future.{ParRequest, ParallelServiceExecutor}
import com.yahoo.maha.parrequest2.{GeneralError, ParCallable}
import com.yahoo.maha.service.config._
import com.yahoo.maha.service.curators.Curator
import com.yahoo.maha.service.error._
import com.yahoo.maha.service.factory._
import com.yahoo.maha.service.utils.BaseMahaRequestLogBuilder
import grizzled.slf4j.Logging
import javax.sql.DataSource
import org.json4s
import org.json4s.JsonAST.JObject
import org.json4s.{JString, JValue}
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

case class MahaServiceConfig(context: MahaServiceConfigContext, registry: Map[String, RegistryConfig], mahaRequestLogWriter: MahaRequestLogWriter, curatorMap: Map[String, Curator])

case class RequestResult(queryPipelineResult: QueryPipelineResult)

case class ParRequestResult(queryPipeline: Try[QueryPipeline], prodRun: ParRequest[RequestResult], dryRunOption: Option[ParRequest[RequestResult]])

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
   * Generates own model, executes request, logs failures with request log builder
   */
  def processRequest(registryName: String
                     , reportingRequest: ReportingRequest
                     , bucketParams: BucketParams
                     , mahaRequestLogBuilder: BaseMahaRequestLogBuilder): Either[GeneralError, RequestResult]

  /**
   * Generates own model, create ParRequestResult, logs failures with request log builder
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
                           bucketParams: BucketParams): Try[RequestModelResult]

  /**
    * generate query pipeline from model
    * @param registryName
    * @param requestModel
    * @return
    */
  def generateQueryPipeline(registryName: String
                            , requestModel: RequestModel): Try[QueryPipeline]

  /**
   * Executes the RequestModel and provide the RequestResult, logs failures with request log builder
   */
  def processRequestModel(registryName: String,
                          requestModel: RequestModel,
                          mahaRequestLogBuilder: BaseMahaRequestLogBuilder): Either[GeneralError, RequestResult]

  /**
   * Async execution, returns ParRequests for given RequestModel, logs failures with request log builder
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
    val requestModelResultTry = generateRequestModel(registryName, reportingRequest, bucketParams)

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
    val requestModelResultTry = generateRequestModel(registryName, reportingRequest, bucketParams)
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
        val dryRunResult = asParRequest(registryName, requestModelResult.dryRunModelTry.get.get, parLabel, mahaRequestLogBuilder.dryRun())
        Option(dryRunResult._2)
      } else None
    }
    ParRequestResult(queryPipelineTry, finalResult, dryRunResult)
  }

  override def generateRequestModel(registryName: String, reportingRequest: ReportingRequest, bucketParams: BucketParams): Try[RequestModelResult] = {
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
        val dryRunResult = asParRequest(registryName, requestModelResult.dryRunModelTry.get.get, parLabel, mahaRequestLogBuilder.dryRun())
        Option(dryRunResult._2)
      } else None
    }
    ParRequestResult(queryPipelineTry, finalResult, dryRunResult)
  }

  def generateQueryPipeline(registryName: String,
                                     requestModel: RequestModel): Try[QueryPipeline] = {

    val registryConfig = config.registry(registryName)
    val queryPipelineFactory = registryConfig.queryPipelineFactory

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

    val queryPipelineTry = generateQueryPipeline(registryName, requestModel)
    if(queryPipelineTry.isFailure) {
      val error = queryPipelineTry.failed.get
      val message = s"Failed to compile the query pipeline ${error.getMessage}"
      logger.error(message, error)
      mahaRequestLogBuilder.logFailed(message)
      GeneralError.either("createQueryPipeline", message, error)
    } else {
      val triedQueryPipelineResult = processQueryPipeline(registryConfig, queryPipelineTry.get, mahaRequestLogBuilder)
      if (triedQueryPipelineResult.isFailure) {
        val error = triedQueryPipelineResult.failed.get
        val message = s"Failed to execute the query pipeline"
        logger.error(message, error)
        mahaRequestLogBuilder.logFailed(message)
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

    val queryPipelineTry = generateQueryPipeline(registryName, requestModel)
    if(queryPipelineTry.isFailure) {
      val error = queryPipelineTry.failed.get
      val message = s"Failed to compile the query pipeline ${error.getMessage}"
      logger.error(message, error)
      mahaRequestLogBuilder.logFailed(message)
      return (queryPipelineTry, parallelServiceExecutor.immediateResult("createParRequest", GeneralError.either("createQueryPipeline", message, error)))
    } else {
      val parRequest = parallelServiceExecutor.parRequestBuilder[RequestResult].setLabel(parRequestLabel).
        setParCallable(ParCallable.from[Either[GeneralError, RequestResult]](
          new Callable[Either[GeneralError, RequestResult]]() {
            override def call(): Either[GeneralError, RequestResult] = {
              val triedQueryPipelineResult = processQueryPipeline(registryConfig, queryPipelineTry.get, mahaRequestLogBuilder)
              if (triedQueryPipelineResult.isFailure) {
                val error = triedQueryPipelineResult.failed.get
                val message = s"Failed to execute the query pipeline"
                logger.error(message, error)
                mahaRequestLogBuilder.logFailed(message)
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

}

trait MahaServiceConfigContext {
  def bucketConfigMap: Map[String, BucketingConfig]
  def dataSourceMap: Map[String, DataSource]
  def utcTimeProviderMap: Map[String, UTCTimeProvider]
  def generatorMap: Map[String, QueryGenerator[_ <: EngineRequirement]]
  def executorMap: Map[String, QueryExecutor]
  def registryMap: Map[String, Registry]
  def parallelServiceExecutorMap: Map[String, ParallelServiceExecutor]
  def curatorMap: Map[String, Curator]
}
case class DefaultMahaServiceConfigContext(bucketConfigMap: Map[String, BucketingConfig] = Map.empty
                                          , dataSourceMap: Map[String, DataSource] = Map.empty
                                          , utcTimeProviderMap: Map[String, UTCTimeProvider] = Map.empty
                                          , generatorMap: Map[String, QueryGenerator[_ <: EngineRequirement]] = Map.empty
                                          , executorMap: Map[String, QueryExecutor] = Map.empty
                                          , registryMap: Map[String, Registry] = Map.empty
                                          , parallelServiceExecutorMap: Map[String, ParallelServiceExecutor] = Map.empty
                                          , curatorMap: Map[String, Curator] = Map.empty
                                          ) extends MahaServiceConfigContext
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

    var defaultContext = DefaultMahaServiceConfigContext()
    val mahaServiceConfig = for {
      jsonMahaServiceConfig <- jsonMahaServiceConfigResult
      validationResult <- validateReferenceByName(jsonMahaServiceConfig)
      bucketConfigMap <- initBucketingConfig(jsonMahaServiceConfig.bucketingConfigMap)(defaultContext)
      postBucketContext = defaultContext.copy(bucketConfigMap = bucketConfigMap)
      dataSourceMap <- initDataSources(jsonMahaServiceConfig.datasourceMap)(postBucketContext)
      postDataSourceContext = postBucketContext.copy(dataSourceMap = dataSourceMap)
      utcTimeProviderMap <- initUTCTimeProvider(jsonMahaServiceConfig.utcTimeProviderMap)(postDataSourceContext)
      postTimeProviderContext = postDataSourceContext.copy(utcTimeProviderMap = utcTimeProviderMap)
      generatorMap <- initGenerators(jsonMahaServiceConfig.generatorMap)(postTimeProviderContext)
      postGeneratorContext = postTimeProviderContext.copy(generatorMap = generatorMap)
      executorMap <- initExecutors(jsonMahaServiceConfig.executorMap)(postGeneratorContext)
      postExecutorContext = postGeneratorContext.copy(executorMap = executorMap)
      registryMap <- initRegistry(jsonMahaServiceConfig.registryMap)(postExecutorContext)
      postRegistryContext = postExecutorContext.copy(registryMap = registryMap)
      parallelServiceExecutorConfig <- initParallelServiceExecutors(jsonMahaServiceConfig.parallelServiceExecutorConfigMap)(postRegistryContext)
      postParallelServiceExecutorContext = postRegistryContext.copy(parallelServiceExecutorMap = parallelServiceExecutorConfig)
      curatorMap <- initCurators(jsonMahaServiceConfig.curatorMap)(postParallelServiceExecutorContext)
      postCuratorContext = postParallelServiceExecutorContext.copy(curatorMap = curatorMap)
      mahaRequestLogWriter <- initKafkaLogWriter(jsonMahaServiceConfig.jsonMahaRequestLogConfig)(postCuratorContext)
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
              parallelServiceExecutorConfig.get(registryConfig.parallelServiceExecutorName).get))
          }
        }
        MahaServiceConfig(postCuratorContext, resultMap, mahaRequestLogWriter, curatorMap)
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

  def initBucketingConfig(bucketConfigMap: Map[String, JsonBucketingConfig])(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[Map[String, BucketingConfig]] = {
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

  def initUTCTimeProvider(utcTimeProviderMap: Map[String, JsonUTCTimeProviderConfig])(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[Map[String, UTCTimeProvider]] = {
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

  def initExecutors(executorMap: Map[String, JsonQueryExecutorConfig])(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[Map[String, QueryExecutor]] = {
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

  def initGenerators(generatorMap: Map[String, JsonQueryGeneratorConfig])(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[Map[String, QueryGenerator[_ <: EngineRequirement]]] = {
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

  def initRegistry(registryMap: Map[String, JsonRegistryConfig])(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[Map[String, Registry]] = {
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

  def initParallelServiceExecutors(parallelServiceExecutorConfigMap: Map[String, JsonParallelServiceExecutorConfig])(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[Map[String, ParallelServiceExecutor]] = {
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

  def initKafkaLogWriter(jsonMahaRequestLogConfig: JsonMahaRequestLogConfig)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[MahaRequestLogWriter] = {
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

  def initCurators(curatorConfigMap: Map[String, JsonCuratorConfig])(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[Map[String, Curator]] = {
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

  def initDataSources(curatorConfigMap: Map[String, JsonDataSourceConfig])(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[Map[String, DataSource]] = {
    import Scalaz._
    val result: MahaServiceConfig.MahaConfigResult[Map[String, DataSource]] = {
      val constructDataSource: Iterable[MahaServiceConfig.MahaConfigResult[(String, DataSource)]] = {
        curatorConfigMap.map {
          case (name, jsonConfig) =>
            for {
              factoryResult <- getFactory[DataSourceFactory](jsonConfig.className, closer)
              built <- factoryResult.fromJson(jsonConfig.json)
            } yield (name, built)
        }
      }
      val resultList: MahaServiceConfig.MahaConfigResult[List[(String, DataSource)]] =
        constructDataSource.toList.sequence[MahaServiceConfig.MahaConfigResult, (String, DataSource)]

      resultList.map(_.toMap)
    }
    result
  }

}


case class DynamicMahaServiceConfig(dynamicProperties: Map[String, DynamicPropertyInfo], context: MahaServiceConfigContext, registry: Map[String, RegistryConfig], mahaRequestLogWriter: MahaRequestLogWriter, curatorMap: Map[String, Curator])

object DynamicMahaServiceConfig {

  import MahaServiceConfig._
  def getDynamicObject(obj: Object): Object = {
    val dynamicClass = DynamicWrapper.getDynamicClassFor(obj)
    val dynamicInstance = dynamicClass.newInstance()
    dynamicClass.getField(DynamicWrapper.CURRENT_OBJECT).set(dynamicInstance, obj)
    dynamicInstance
  }

  def getDynamicObjects(namedObjectMap: MahaServiceConfig.MahaConfigResult[Map[String, Object]]): MahaServiceConfig.MahaConfigResult[Map[String, Object]] = {
    val dynamicObjectMap = new mutable.HashMap[String, Object]()
    namedObjectMap.foreach(map => {
      for ((name, obj) <- map) {
        val dynamicInstance = getDynamicObject(obj)
        dynamicObjectMap.+=((name, dynamicInstance))
      }
    })
    dynamicObjectMap.toMap.successNel[MahaServiceError]
  }

  def createObject(ba: Array[Byte], objectName: String): Option[Object] = {
    None
  }


  def fromJson(ba: Array[Byte]): MahaServiceConfig.MahaConfigResult[DynamicMahaServiceConfig] = {
    val json = {
      Try(parse(new String(ba, StandardCharsets.UTF_8))) match {
        case t if t.isSuccess => t.get
        case t if t.isFailure => {
          return Failure(JsonParseError(s"invalidInputJson : ${t.failed.get.getMessage}", Option(t.failed.toOption.get))).toValidationNel
        }
      }
    }

    val jsonMahaServiceConfigResult: ValidationNel[MahaServiceError, JsonMahaServiceConfig] = fromJSON[JsonMahaServiceConfig](json).leftMap {
      nel => nel.map(err => {
        JsonParseError(err.toString)
      })
    }

    var defaultContext = DefaultMahaServiceConfigContext()
    val dynamicMahaServiceConfig = for {
      jsonMahaServiceConfig <- jsonMahaServiceConfigResult
      validationResult <- validateReferenceByName(jsonMahaServiceConfig)
      bucketConfigMap <- initBucketingConfig(jsonMahaServiceConfig.bucketingConfigMap)(defaultContext)
      postBucketContext = defaultContext.copy(bucketConfigMap = bucketConfigMap)
      dataSourceMap <- initDataSources(jsonMahaServiceConfig.datasourceMap)(postBucketContext)
      postDataSourceContext = postBucketContext.copy(dataSourceMap = dataSourceMap)
      utcTimeProviderMap <- initUTCTimeProvider(jsonMahaServiceConfig.utcTimeProviderMap)(postDataSourceContext)
      postTimeProviderContext = postDataSourceContext.copy(utcTimeProviderMap = utcTimeProviderMap)
      generatorMap <- initGenerators(jsonMahaServiceConfig.generatorMap)(postTimeProviderContext)
      postGeneratorContext = postTimeProviderContext.copy(generatorMap = generatorMap)
      executorMap <- initExecutors(jsonMahaServiceConfig.executorMap)(postGeneratorContext)
      postExecutorContext = postGeneratorContext.copy(executorMap = executorMap)
      registryMap <- initRegistry(jsonMahaServiceConfig.registryMap)(postExecutorContext)
      postRegistryContext = postExecutorContext.copy(registryMap = registryMap)
      parallelServiceExecutorConfig <- initParallelServiceExecutors(jsonMahaServiceConfig.parallelServiceExecutorConfigMap)(postRegistryContext)
      postParallelServiceExecutorContext = postRegistryContext.copy(parallelServiceExecutorMap = parallelServiceExecutorConfig)
      curatorMap <- initCurators(jsonMahaServiceConfig.curatorMap)(postParallelServiceExecutorContext)
      postCuratorContext = postParallelServiceExecutorContext.copy(curatorMap = curatorMap)
      mahaRequestLogWriter <- initKafkaLogWriter(jsonMahaServiceConfig.jsonMahaRequestLogConfig)(postCuratorContext)
    } yield {
      val objectNameMap = mergeMaps(bucketConfigMap, utcTimeProviderMap, generatorMap, executorMap, registryMap, parallelServiceExecutorConfig, curatorMap)
      val dynamicProperties = findDynamicProperties(json, objectNameMap.toMap)
      for ((_, dynamicProperty) <- dynamicProperties) {
        dynamicProperty.objects.keys.foreach(objName => {
          val dynamicObject = getDynamicObject(objectNameMap(objName))
          objectNameMap.put(objName, dynamicObject)
        })
      }

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
            parallelServiceExecutorConfig.get(registryConfig.parallelServiceExecutorName).get))
        }
      }
      DynamicMahaServiceConfig(dynamicProperties, postCuratorContext, resultMap, mahaRequestLogWriter, curatorMap)
    }
    dynamicMahaServiceConfig
  }

  def mergeMaps(maps: Map[String, Object]*): mutable.Map[String, Object] = {
    val mergedMap = new scala.collection.mutable.HashMap[String, Object]()
    maps.foreach(map => {
      map.foreach(f => mergedMap.put(f._1, f._2))
    })
    mergedMap
  }

  def findDynamicProperties(json: JValue, objectNameMap: Map[String, Object]): Map[String, DynamicPropertyInfo] = {
    val dynamicProperties = new mutable.HashMap[String, DynamicPropertyInfo]()
    implicit val formats = org.json4s.DefaultFormats
    json.children.foreach(c => {
      c.asInstanceOf[JObject].obj.foreach(map => {
      val dynamicFields = extractDynamicFields(map._2)
        for ((_, (propertyKey, defaultValue)) <- dynamicFields) {
          val objectName = map._1.toLowerCase
          require(objectNameMap.contains(objectName), s"Dynamic object with name $objectName not present in objectMap: $objectNameMap")
          if (dynamicProperties.contains(propertyKey)) {
            dynamicProperties(propertyKey).objects.put(objectName, objectNameMap(objectName))
          } else {
            val dynamicObjects = new mutable.HashMap[String, Object]()
            dynamicObjects.put(objectName, objectNameMap(objectName))
            dynamicProperties.put(propertyKey, new DynamicPropertyInfo(propertyKey, defaultValue, dynamicObjects))
          }
        }
      })
    })
    dynamicProperties.toMap
  }
}
