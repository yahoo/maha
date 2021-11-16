// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.query

import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors

import com.yahoo.maha.core.CoreSchema._
import com.yahoo.maha.core._
import com.yahoo.maha.core.bucketing._
import com.yahoo.maha.core.registry.{Registry, RegistryBuilder}
import com.yahoo.maha.core.request._
import org.joda.time.{DateTime, DateTimeZone}

import scala.concurrent.ExecutionContext
import scala.util.Try

/**
 * Created by hiral on 1/15/16.
 */
trait BaseQueryGeneratorTest {

  CoreSchema.register()

  protected[this] def defaultFactEngine: Engine = OracleEngine

  protected[this] val baseDate = DateTime.now(DateTimeZone.UTC)
  protected[this] val iso8601Format = DateTimeBetweenFilterHelper.iso8601FormatString
  protected[this] val druidMultiQueryEngineList = DefaultQueryPipelineFactory.druidMultiQueryEngineList
  protected[this] val fromDate = DailyGrain.toFormattedString(baseDate.minusDays(7))
  protected[this] val fromDateMinusOne = DailyGrain.toFormattedString(baseDate.minusDays(8))
  protected[this] val fromDateMinus10 = DailyGrain.toFormattedString(baseDate.minusDays(7).minusDays(10))
  protected[this] val fromDateTime = DateTimeBetweenFilterHelper.iso8601FormattedString(baseDate.minusDays(7))
  protected[this] val toDate = DailyGrain.toFormattedString(baseDate)
  protected[this] val toDateMinus10 = DailyGrain.toFormattedString(baseDate.minusDays(10))
  protected[this] val toDateMinusOne = DailyGrain.toFormattedString(baseDate.minusDays(1))
  protected[this] val toDateTime = DateTimeBetweenFilterHelper.iso8601FormattedString(baseDate)
  protected[this] val toDateTimeMinusTwoHours = DateTimeBetweenFilterHelper.iso8601FormattedString(baseDate.minusHours(2))
  protected[this] val toDateTimeMinusTenMinutes = DateTimeBetweenFilterHelper.iso8601FormattedString(baseDate.minusMinutes(10))

  protected[this] val fromDateHive = fromDate.replaceAll("-","")
  protected[this] val toDateHive = toDate.replaceAll("-","")
  protected[this] val toDateMinusOneHive = toDateMinusOne.replaceAll("-","")

  protected[this] implicit val queryGeneratorRegistry = new QueryGeneratorRegistry
  protected[this] implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))
  protected[this] lazy val queryPipelineFactory = new DefaultQueryPipelineFactory(defaultFactEngine = defaultFactEngine, druidMultiQueryEngineList = Seq(defaultFactEngine))
  
  protected[this] def registerFacts(forcedFilters: Set[ForcedFilter], registryBuilder : RegistryBuilder) : Unit
  protected[this] def registerDims(registryBuilder : RegistryBuilder) : Unit

  protected[this] def getDefaultRegistry(forcedFilters: Set[ForcedFilter] = Set.empty): Registry = {
    val registryBuilder = new RegistryBuilder
    registerFacts(forcedFilters, registryBuilder)
    registerDims(registryBuilder)
    registryBuilder.build()
  }

  protected[this] def getRequestModel(request: ReportingRequest
                      , registry: Registry
                      , userTimeZoneProvider: UserTimeZoneProvider = NoopUserTimeZoneProvider
                      , utcTimeProvider: UTCTimeProvider = PassThroughUTCTimeProvider
                      , revision: Option[Int] = None): Try[RequestModel] = {
    RequestModel.from(request, registry, userTimeZoneProvider, utcTimeProvider, revision)
  }

  protected[this] def getReportingRequestAsync(jsonString: String, schema: Schema = AdvertiserSchema) = {
    val reportingRequestOption = ReportingRequest.deserializeAsync(jsonString.getBytes(StandardCharsets.UTF_8), schema)
    require(reportingRequestOption.isSuccess)
    reportingRequestOption.toOption.get
  }

  protected[this] def getReportingRequestSync(jsonString: String, schema: Schema = AdvertiserSchema) = {
    val result = ReportingRequest.deserializeSync(jsonString.getBytes(StandardCharsets.UTF_8), schema)
    require(result.isSuccess, result)
    result.toOption.get
  }

  protected[this] def getReportingRequestSyncWithFactBias(jsonString: String, schema: Schema = AdvertiserSchema) = {
    ReportingRequest.deserializeSyncWithFactBias(jsonString.getBytes(StandardCharsets.UTF_8), schema).toOption.get
  }

  protected[this] def getReportingRequestSyncWithAdditionalParameters(jsonString: String, requestContext: RequestContext) = {
    val request = ReportingRequest.deserializeSync(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema).toOption.get
    ReportingRequest.addRequestContext(request, requestContext)
  }

  protected[this] def getReportingRequestSyncWithHostName(jsonString: String, hostName: String) = {
    val request = ReportingRequest.deserializeSync(jsonString.getBytes(StandardCharsets.UTF_8), AdvertiserSchema).toOption.get
    ReportingRequest.withHostname(request, hostName)
  }

  protected[this] def generatePipeline(requestModel: RequestModel) : Try[QueryPipeline] = {
    queryPipelineFactory.from(requestModel, QueryAttributes.empty)
  }

  protected[this] def generatePipeline(requestModel: RequestModel, queryAttributes: QueryAttributes) : Try[QueryPipeline] = {
    queryPipelineFactory.from(requestModel, queryAttributes)
  }

  protected[this] def generatePipeline(requestModel: RequestModel, version:Version) : Try[QueryPipeline] = {
    queryPipelineFactory.fromQueryGenVersion(requestModel, QueryAttributes.empty, version)
  }

  protected[this] def generatePipelineForQgenVersion(registry: Registry, requestModel: RequestModel, queryGenVersion: Version) : Try[QueryPipeline] = {
    val qgenBucketingConfig = new QueryGenBucketingConfigBuilder()
      .externalBucketPercentage(Map(queryGenVersion -> 100))
      .internalBucketPercentage(Map(queryGenVersion -> 100)).build()
    val bucketingConfig = new DefaultBucketingConfig(Map.empty,
      Map(HiveEngine -> qgenBucketingConfig,
        BigqueryEngine -> qgenBucketingConfig,
        OracleEngine -> qgenBucketingConfig,
        DruidEngine -> qgenBucketingConfig,
        PrestoEngine -> qgenBucketingConfig
      ))
    val bucketSelector = new BucketSelector(registry, bucketingConfig)
    queryPipelineFactory.fromBucketSelector((requestModel, None), QueryAttributes.empty, bucketSelector, new BucketParams())._1
  }

  protected[this] def getBaseDir : String = {
    val userDir = System.getProperty("user.dir")
    if(userDir.endsWith("core")) {
      s"$userDir/src/test/resources/"
    } else if (userDir.endsWith("presto")) {
      s"$userDir/../core/src/test/resources/"
    } else {
      s"$userDir/core/src/test/resources/"
    }
  }

  protected[this] def getMaxDaysWindow: Map[(RequestType, Grain), Int] = {
    val interval = DailyGrain.getDaysBetween(fromDate, toDate)
    val result = interval + 1
    Map(
      (SyncRequest, DailyGrain) -> result, (AsyncRequest, DailyGrain) -> result,
      (SyncRequest, HourlyGrain) -> result, (AsyncRequest, HourlyGrain) -> result,
      (SyncRequest, MinuteGrain) -> result, (AsyncRequest, MinuteGrain) -> result
    )
  }

  protected[this] def getMaxDaysLookBack: Map[(RequestType, Grain), Int] = {
    val daysBack = DailyGrain.getDaysFromNow(fromDate)
    val result = daysBack + 10
    Map(
      (SyncRequest, DailyGrain) -> result, (AsyncRequest, DailyGrain) -> result,
      (SyncRequest, HourlyGrain) -> result, (AsyncRequest, HourlyGrain) -> result,
      (SyncRequest, MinuteGrain) -> result, (AsyncRequest, MinuteGrain) -> result
    )
  }

  protected[this] def getPlusDays(date: String, plus: Int) : String = {
    DailyGrain.toFormattedString(DailyGrain.fromFormattedString(date).plusDays(plus))
  }
}
