// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import java.io.Closeable
import java.util.concurrent.RejectedExecutionHandler
import javax.sql.DataSource

import com.google.common.io.Closer
import com.yahoo.maha.core._
import com.yahoo.maha.core.bucketing.{BucketingConfig, CubeBucketingConfig, CubeBucketingConfigBuilder, DefaultBucketingConfig}
import com.yahoo.maha.core.query.druid.DruidQueryOptimizer
import com.yahoo.maha.core.query.{ExecutionLifecycleListener, NoopExecutionLifecycleListener, QueryExecutor, QueryGenerator}
import com.yahoo.maha.core.request._
import com.yahoo.maha.executor.druid.{DruidQueryExecutorConfig, ResultSetTransformers}
import com.yahoo.maha.parrequest2.CustomRejectPolicy
import com.yahoo.maha.parrequest2.future.ParallelServiceExecutor
import com.yahoo.maha.service.MahaServiceConfig
import com.yahoo.maha.service.MahaServiceConfig.MahaConfigResult
import com.yahoo.maha.service.config.{PassThroughPasswordProvider, PasswordProvider}
import com.yahoo.maha.service.curators.Curator
import com.yahoo.maha.service.error.{FailedToConstructFactory, MahaServiceError}
import com.yahoo.maha.service.utils.MahaRequestLogWriter
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
  def fromJson(config: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[QueryGenerator[_ <: EngineRequirement]]
  def supportedProperties: List[(String, Boolean)]
}

trait QueryExecutoryFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[QueryExecutor]
  def supportedProperties: List[(String, Boolean)]
}
trait ParallelServiceExecutoryFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[ParallelServiceExecutor]
  def supportedProperties: List[(String, Boolean)]
}
trait RejectedExecutionHandlerFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[RejectedExecutionHandler]
  def supportedProperties: List[(String, Boolean)]
}

trait UTCTimeProvideryFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[UTCTimeProvider]
  def supportedProperties: List[(String, Boolean)]
}

trait DataSourceFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[DataSource]
  def supportedProperties: List[(String, Boolean)]
}

trait BucketingConfigFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[BucketingConfig]
  def supportedProperties: List[(String, Boolean)]
}

trait PartitionColumnRendererFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[PartitionColumnRenderer]
  def supportedProperties: List[(String, Boolean)]
}

trait MahaUDFRegistrationFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[Set[UDFRegistration]]
  def supportedProperties: List[(String, Boolean)]
}

trait OracleLiteralMapperFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[OracleLiteralMapper]
  def supportedProperties: List[(String, Boolean)]
}

trait DruidLiteralMapperFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[DruidLiteralMapper]
  def supportedProperties: List[(String, Boolean)]
}

trait DruidQueryOptimizerFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[DruidQueryOptimizer]
  def supportedProperties: List[(String, Boolean)]
}

trait DruidQueryExecutorConfigFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[DruidQueryExecutorConfig]
  def supportedProperties: List[(String, Boolean)]
}

trait ResultSetTransformersFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[List[ResultSetTransformers]]
  def supportedProperties: List[(String, Boolean)]
}

trait PasswordProviderFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[PasswordProvider]
  def supportedProperties: List[(String, Boolean)]
}

trait ExecutionLifecycleListenerFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[ExecutionLifecycleListener]
  def supportedProperties: List[(String, Boolean)]
}

trait DimCostEstimatorFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[DimCostEstimator]
  def supportedProperties: List[(String, Boolean)]
}

trait FactCostEstimatorFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[FactCostEstimator]
  def supportedProperties: List[(String, Boolean)]
}

trait MahaRequestLogWriterFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue, isLoggingEnabled: Boolean) : MahaServiceConfig.MahaConfigResult[MahaRequestLogWriter]
  def supportedProperties: List[(String, Boolean)]
}

trait CuratorFactory extends BaseFactory {
  def fromJson(config: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[Curator]
  def supportedProperties: List[(String, Boolean)]
}

import scalaz.syntax.validation._
class PassThroughUTCTimeProviderFactory extends UTCTimeProvideryFactory {
  def fromJson(config: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[UTCTimeProvider] = PassThroughUTCTimeProvider.successNel
  def supportedProperties: List[(String, Boolean)] = List.empty
}
class BaseUTCTimeProviderFactory extends UTCTimeProvideryFactory {
  def fromJson(config: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[UTCTimeProvider] = new BaseUTCTimeProvider().successNel
  def supportedProperties: List[(String, Boolean)] = List.empty
}
class PassThroughPasswordProviderFactory  extends  PasswordProviderFactory {
  def fromJson(config: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[PasswordProvider] = PassThroughPasswordProvider.successNel
  def supportedProperties: List[(String, Boolean)] = List.empty
}

object DefaultBucketingConfigFactory {
  import org.json4s.scalaz.JsonScalaz.{Error, _}

  import _root_.scalaz._
  import Scalaz._
  import Validation.FlatMap._

  case class RevisionPercentConfig(revision: Int, percent: Int)
  case class RevisionPercentEngineConfig(revision: Int, percent: Int, engine: Option[Engine])
  case class UserRevisionConfig(user: String, revision: Int)
  case class CubeConfig(cube: String
                        , internal: List[RevisionPercentConfig]
                        , external: List[RevisionPercentConfig]
                        , dryRun: List[RevisionPercentEngineConfig]
                        , userWhiteList: List[UserRevisionConfig])
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

  def fromJson(config: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[BucketingConfig] = {
    import _root_.scalaz.Scalaz._
    val cubeConfigResult: MahaServiceConfig.MahaConfigResult[List[CubeConfig]] = fromJSON[List[CubeConfig]](config)

    cubeConfigResult.flatMap {
      listCubeConfig =>
        val builtConfig: MahaServiceConfig.MahaConfigResult[List[(String, CubeBucketingConfig)]] = listCubeConfig.map {
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
        builtConfig.map {
          builtConfigList =>
            new DefaultBucketingConfig(builtConfigList.toMap)
        }
    }
  }
}
class DefaultBucketingConfigFactory extends BucketingConfigFactory {

  def fromJson(config: org.json4s.JValue) : MahaServiceConfig.MahaConfigResult[BucketingConfig] = {
    DefaultBucketingConfigFactory.fromJson(config)
  }
  def supportedProperties: List[(String, Boolean)] = {
    //TODO: fix me
    List.empty
  }
}

class DefaultPartitionColumnRendererFactory extends PartitionColumnRendererFactory {
  override def fromJson(config: JValue): MahaServiceConfig.MahaConfigResult[PartitionColumnRenderer] =  DefaultPartitionColumnRenderer.successNel

  override def supportedProperties: List[(String, Boolean)] = {
    List.empty
  }
}

class NoopExecutionLifecycleListenerFactory extends  ExecutionLifecycleListenerFactory {
  override def fromJson(config: JValue): MahaConfigResult[ExecutionLifecycleListener] = new NoopExecutionLifecycleListener().successNel

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class DefaultResultSetTransformersFactory extends ResultSetTransformersFactory {
  override def fromJson(config: JValue): MahaConfigResult[List[ResultSetTransformers]] =  ResultSetTransformers.DEFAULT_TRANSFORMS.successNel

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class DefaultDimCostEstimatorFactory extends DimCostEstimatorFactory {
  override def fromJson(config: JValue): MahaConfigResult[DimCostEstimator] = new DefaultDimEstimator().successNel

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class DefaultFactCostEstimatorFactory extends FactCostEstimatorFactory {
  override def fromJson(config: JValue): MahaConfigResult[FactCostEstimator] = new DefaultFactEstimator().successNel

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class DefaultRejectedExecutionHandlerFactory extends RejectedExecutionHandlerFactory {
  override def fromJson(config: JValue): MahaConfigResult[RejectedExecutionHandler] = new CustomRejectPolicy().successNel

  override def supportedProperties: List[(String, Boolean)] = List.empty
}
