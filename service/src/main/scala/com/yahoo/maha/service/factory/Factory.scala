// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import java.io.Closeable
import java.util.concurrent.RejectedExecutionHandler
import com.google.common.io.Closer
import com.yahoo.maha.core._
import com.yahoo.maha.core.bucketing._
import com.yahoo.maha.core.query.bigquery.BigqueryPartitionColumnRenderer
import com.yahoo.maha.core.query.druid.DruidQueryOptimizer
import com.yahoo.maha.core.query.{ResultSetTransformer, _}
import com.yahoo.maha.executor.bigquery.BigqueryQueryExecutorConfig
import com.yahoo.maha.executor.druid.{AuthHeaderProvider, DruidQueryExecutorConfig}
import com.yahoo.maha.executor.presto.PrestoQueryTemplate
import com.yahoo.maha.log.MahaRequestLogWriter
import com.yahoo.maha.parrequest2.CustomRejectPolicy
import com.yahoo.maha.parrequest2.future.ParallelServiceExecutor
import com.yahoo.maha.service.MahaServiceConfig.MahaConfigResult
import com.yahoo.maha.service.config.{PassThroughPasswordProvider, PasswordProvider}
import com.yahoo.maha.service.curators.Curator
import com.yahoo.maha.service.error.{FailedToConstructFactory, MahaServiceError}
import com.yahoo.maha.service.request._
import com.yahoo.maha.service.{MahaServiceConfig, MahaServiceConfigContext}
import javax.sql.DataSource
import org.json4s.JValue
import org.json4s.JsonAST.JString

/**
  * Created by hiral on 5/25/17.
  */

trait BaseFactory extends Closeable {
  protected[this] val closer: Closer = Closer.create()
  override def close(): Unit = closer.close()
}

trait QueryGeneratorFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[QueryGenerator[_ <: EngineRequirement]]
  def supportedProperties: List[(String, Boolean)]
}

trait QueryExecutoryFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[QueryExecutor]
  def supportedProperties: List[(String, Boolean)]
}
trait ParallelServiceExecutoryFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[ParallelServiceExecutor]
  def supportedProperties: List[(String, Boolean)]
}
trait RejectedExecutionHandlerFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[RejectedExecutionHandler]
  def supportedProperties: List[(String, Boolean)]
}

trait UserTimeZoneProviderFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[UserTimeZoneProvider]
  def supportedProperties: List[(String, Boolean)]
}

trait UTCTimeProviderFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[UTCTimeProvider]
  def supportedProperties: List[(String, Boolean)]
}

trait DataSourceFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[DataSource]
  def supportedProperties: List[(String, Boolean)]
}

trait BucketingConfigFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[BucketingConfig]
  def supportedProperties: List[(String, Boolean)]
}

trait PartitionColumnRendererFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[PartitionColumnRenderer]
  def supportedProperties: List[(String, Boolean)]
}

trait OracleLiteralMapperFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[OracleLiteralMapper]
  def supportedProperties: List[(String, Boolean)]
}

trait PostgresLiteralMapperFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[PostgresLiteralMapper]
  def supportedProperties: List[(String, Boolean)]
}

trait BigqueryLiteralMapperFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[BigqueryLiteralMapper]
  def supportedProperties: List[(String, Boolean)]
}

trait DruidLiteralMapperFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[DruidLiteralMapper]
  def supportedProperties: List[(String, Boolean)]
}

trait DruidQueryOptimizerFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[DruidQueryOptimizer]
  def supportedProperties: List[(String, Boolean)]
}

trait DruidQueryExecutorConfigFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[DruidQueryExecutorConfig]
  def supportedProperties: List[(String, Boolean)]
}

trait BigqueryQueryExecutorConfigFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[BigqueryQueryExecutorConfig]
  def supportedProperties: List[(String, Boolean)]
}

trait ResultSetTransformersFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[List[ResultSetTransformer]]
  def supportedProperties: List[(String, Boolean)]
}

trait PasswordProviderFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[PasswordProvider]
  def supportedProperties: List[(String, Boolean)]
}

trait ExecutionLifecycleListenerFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[ExecutionLifecycleListener]
  def supportedProperties: List[(String, Boolean)]
}

trait DimCostEstimatorFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[DimCostEstimator]
  def supportedProperties: List[(String, Boolean)]
}

trait FactCostEstimatorFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[FactCostEstimator]
  def supportedProperties: List[(String, Boolean)]
}

trait MahaRequestLogWriterFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue, isLoggingEnabled: Boolean) : MahaServiceConfig.MahaConfigResult[MahaRequestLogWriter]
  def supportedProperties: List[(String, Boolean)]
}

trait CuratorFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[Curator]
  def supportedProperties: List[(String, Boolean)]
}

trait MahaUDFRegistrationFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[Set[UDFRegistration]]
  def supportedProperties: List[(String, Boolean)]
}

trait PrestoQueryTemplateFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[PrestoQueryTemplate]
  def supportedProperties: List[(String, Boolean)]
}

trait AuthHeaderProviderFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[AuthHeaderProvider]
  def supportedProperties: List[(String, Boolean)]
}

import scalaz.syntax.validation._
class NoopUserTimeZoneProviderFactory extends UserTimeZoneProviderFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[UserTimeZoneProvider] = NoopUserTimeZoneProvider.successNel
  def supportedProperties: List[(String, Boolean)] = List.empty
}
class PassThroughUTCTimeProviderFactory extends UTCTimeProviderFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[UTCTimeProvider] = PassThroughUTCTimeProvider.successNel
  def supportedProperties: List[(String, Boolean)] = List.empty
}
class BaseUTCTimeProviderFactory extends UTCTimeProviderFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[UTCTimeProvider] = new BaseUTCTimeProvider().successNel
  def supportedProperties: List[(String, Boolean)] = List.empty
}
class PassThroughPasswordProviderFactory  extends  PasswordProviderFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[PasswordProvider] = PassThroughPasswordProvider.successNel
  def supportedProperties: List[(String, Boolean)] = List.empty
}

class DefaultMahaUDFRegistrationFactory extends MahaUDFRegistrationFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[Set[UDFRegistration]] = DefaultUDFRegistrationFactory.apply().successNel
  def supportedProperties: List[(String, Boolean)] = List.empty
}

class DefaultPrestoQueryTemplateFactory extends PrestoQueryTemplateFactory {
  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[PrestoQueryTemplate] = new PrestoQueryTemplate {
    override def buildFinalQuery(query: String, queryContext: QueryContext, queryAttributes: QueryAttributes): String = query
  }.successNel
  def supportedProperties: List[(String, Boolean)] = List.empty
}

object DefaultBucketingConfigFactory {
  import _root_.scalaz._
  import Scalaz._
  import Validation.FlatMap._
  import org.json4s.scalaz.JsonScalaz.{Error, _}

  case class RevisionPercentConfig(revision: Int, percent: Int)
  case class RevisionPercentEngineConfig(revision: Int, percent: Int, engine: Option[Engine])
  case class UserRevisionConfig(user: String, revision: Int)
  case class CubeConfig(cube: String
                        , internal: List[RevisionPercentConfig]
                        , external: List[RevisionPercentConfig]
                        , dryRun: List[RevisionPercentEngineConfig]
                        , userWhiteList: List[UserRevisionConfig])
  case class QueryGenConfig(engine: String
                        , internal: List[RevisionPercentConfig]
                        , external: List[RevisionPercentConfig]
                        , dryRun: List[RevisionPercentConfig]
                        , userWhiteList: List[UserRevisionConfig])
  case class CombinedBucketingConfig(cubeBucketingConfigMapList: List[CubeConfig],
                                     queryGenBucketingConfigList: List[QueryGenConfig])


  implicit def engineJSON: JSONR[Engine] = new JSONR[Engine] {
    override def read(json: JValue): Result[Engine] = {
      json match {
        case JString(s) =>
          Engine.from(s).fold(
            UncategorizedError("engine", s"unrecognized engine : $s", List.empty).asInstanceOf[Error].failureNel[Engine]
          )(e => e.successNel[Error])
        case other =>
            UnexpectedJSONError(other, classOf[JString]).asInstanceOf[Error].failureNel[Engine]
      }
    }
  }

  implicit def rpcJSON: JSONR[RevisionPercentConfig] = RevisionPercentConfig.applyJSON(fieldExtended[Int]("revision")
    , fieldExtended[Int]("percent"))
  implicit def rpecJSON: JSONR[RevisionPercentEngineConfig] = RevisionPercentEngineConfig.applyJSON(fieldExtended[Int]("revision")
    , fieldExtended[Int]("percent")
    , fieldExtended[Option[Engine]]("engine"))
  implicit def urcJSON: JSONR[UserRevisionConfig] = UserRevisionConfig.applyJSON(fieldExtended[String]("user")
    , fieldExtended[Int]("revision"))
  implicit def cubeConfigJSON: JSONR[CubeConfig] = CubeConfig.applyJSON(fieldExtended[String]("cube")
    , fieldExtended[List[RevisionPercentConfig]]("internal")
    , fieldExtended[List[RevisionPercentConfig]]("external")
    , fieldExtended[List[RevisionPercentEngineConfig]]("dryRun")
    , fieldExtended[List[UserRevisionConfig]]("userWhiteList")
  )
  implicit def queryGenConfigJSON: JSONR[QueryGenConfig] = QueryGenConfig.applyJSON(fieldExtended[String]("engine")
    , fieldExtended[List[RevisionPercentConfig]]("internal")
    , fieldExtended[List[RevisionPercentConfig]]("external")
    , fieldExtended[List[RevisionPercentConfig]]("dryRun")
    , fieldExtended[List[UserRevisionConfig]]("userWhiteList")
  )
  implicit def bucketingConfigJSON: JSONR[CombinedBucketingConfig] = CombinedBucketingConfig.applyJSON(
    fieldExtended[List[CubeConfig]]("cube"),
    fieldExtended[List[QueryGenConfig]]("queryGenerator"))

  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[BucketingConfig] = {
    import _root_.scalaz.Scalaz._
    val bucketingConfigResult: MahaServiceConfig.MahaConfigResult[CombinedBucketingConfig] = fromJSON[CombinedBucketingConfig](config)

    bucketingConfigResult.flatMap {
      bucketingConfig =>
        val builtCubeConfig: MahaServiceConfig.MahaConfigResult[List[(String, CubeBucketingConfig)]] =
          bucketingConfig.cubeBucketingConfigMapList.map {
          cubeConfig =>
            Validation.fromTryCatchNonFatal {
              val bldr = new CubeBucketingConfigBuilder
              val internal = cubeConfig.internal.map {
                case RevisionPercentConfig(r, p) => (r, p)
              }.toMap
              bldr.internalBucketPercentage(internal)
              val external = cubeConfig.external.map {
                case RevisionPercentConfig(r, p) => (r, p)
              }.toMap
              bldr.externalBucketPercentage(external)
              val dryRun = cubeConfig.dryRun.map {
                case RevisionPercentEngineConfig(r, p, e) => (r, (p, e))
              }.toMap
              bldr.dryRunPercentage(dryRun)
              val userWhiteList = cubeConfig.userWhiteList.map {
                case UserRevisionConfig(u, r) => (u, r)
              }.toMap
              bldr.userWhiteList(userWhiteList)
              (cubeConfig.cube, bldr.build())
            }.leftMap(t => FailedToConstructFactory(t.getMessage, Option(t)).asInstanceOf[MahaServiceError]).toValidationNel
        }.sequence[MahaServiceConfig.MahaConfigResult, (String, CubeBucketingConfig)]

        val builtQgenConfig: MahaServiceConfig.MahaConfigResult[List[(Engine, QueryGenBucketingConfig)]] =
          bucketingConfig.queryGenBucketingConfigList.map {
          queryGenConfig =>
            Validation.fromTryCatchNonFatal {
              val bldr = new QueryGenBucketingConfigBuilder
              val internal = queryGenConfig.internal.map {
                case RevisionPercentConfig(r, p) => (Version.from(r).get, p)
              }.toMap
              bldr.internalBucketPercentage(internal)
              val external = queryGenConfig.external.map {
                case RevisionPercentConfig(r, p) => (Version.from(r).get, p)
              }.toMap
              bldr.externalBucketPercentage(external)
              val dryRun = queryGenConfig.dryRun.map {
                case RevisionPercentConfig(r, p) => (Version.from(r).get, p)
              }.toMap
              bldr.dryRunPercentage(dryRun)
              val userWhiteList = queryGenConfig.userWhiteList.map {
                case UserRevisionConfig(u, r) => (u, Version.from(r).get)
              }.toMap
              bldr.userWhiteList(userWhiteList)
              (Engine.from(queryGenConfig.engine).get, bldr.build())
            }.leftMap(t => FailedToConstructFactory(t.getMessage, Option(t)).asInstanceOf[MahaServiceError]).toValidationNel
        }.sequence[MahaServiceConfig.MahaConfigResult, (Engine, QueryGenBucketingConfig)]

        (builtCubeConfig |@| builtQgenConfig)((cubeConfigList, qgenConfigList) =>
          new DefaultBucketingConfig(cubeConfigList.toMap, qgenConfigList.toMap))
    }
  }
}

class DefaultBucketingConfigFactory extends BucketingConfigFactory {

  def fromJson(config: org.json4s.JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[BucketingConfig] = {
    DefaultBucketingConfigFactory.fromJson(config)
  }
  def supportedProperties: List[(String, Boolean)] = {
    //TODO: fix me
    List.empty
  }
}

class DefaultPartitionColumnRendererFactory extends PartitionColumnRendererFactory {
  override def fromJson(config: JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[PartitionColumnRenderer] =  DefaultPartitionColumnRenderer.successNel

  override def supportedProperties: List[(String, Boolean)] = {
    List.empty
  }
}

class BigqueryPartitionColumnRendererFactory extends PartitionColumnRendererFactory {
  override def fromJson(config: JValue)(implicit context: MahaServiceConfigContext) : MahaServiceConfig.MahaConfigResult[PartitionColumnRenderer] =  BigqueryPartitionColumnRenderer.successNel

  override def supportedProperties: List[(String, Boolean)] = {
    List.empty
  }
}

class NoopExecutionLifecycleListenerFactory extends  ExecutionLifecycleListenerFactory {
  override def fromJson(config: JValue)(implicit context: MahaServiceConfigContext): MahaConfigResult[ExecutionLifecycleListener] = new NoopExecutionLifecycleListener().successNel

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class DefaultResultSetTransformersFactory extends ResultSetTransformersFactory {
  override def fromJson(config: JValue)(implicit context: MahaServiceConfigContext): MahaConfigResult[List[ResultSetTransformer]] =  ResultSetTransformer.DEFAULT_TRANSFORMS.successNel

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class DefaultDimCostEstimatorFactory extends DimCostEstimatorFactory {
  override def fromJson(config: JValue)(implicit context: MahaServiceConfigContext) : MahaConfigResult[DimCostEstimator] = new DefaultDimEstimator().successNel

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class DefaultFactCostEstimatorFactory extends FactCostEstimatorFactory {
  override def fromJson(config: JValue)(implicit context: MahaServiceConfigContext) : MahaConfigResult[FactCostEstimator] = new DefaultFactEstimator().successNel

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class DefaultRejectedExecutionHandlerFactory extends RejectedExecutionHandlerFactory {
  override def fromJson(config: JValue)(implicit context: MahaServiceConfigContext): MahaConfigResult[RejectedExecutionHandler] = new CustomRejectPolicy().successNel

  override def supportedProperties: List[(String, Boolean)] = List.empty
}
