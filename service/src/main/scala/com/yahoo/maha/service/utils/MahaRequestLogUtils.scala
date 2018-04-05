// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.utils

import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.core.{DimensionCandidate, RequestModel, SortByColumnInfo}
import com.yahoo.maha.proto.MahaRequestLog.MahaRequestProto
import com.yahoo.maha.service.MahaService
import org.apache.commons.lang.StringUtils
import org.apache.log4j.MDC
import org.slf4j.LoggerFactory

/**
 * Created by pranavbhole on 11/08/17.
 */

object MahaConstants {
  val REQUEST_ID: String = "requestId"
  val USER_ID: String = "user"
  val IS_INTERNAL: String = "isInternal"
}

trait MahaRequestLogBuilder {

  protected[this] val protoBuilder: MahaRequestProto.Builder = MahaRequestProto.newBuilder()

  def registryName: String

  def init(reportingRequest: ReportingRequest, jobIdOption: Option[Long], requestType: MahaRequestProto.RequestType, rawJson: ByteString)

  def logQueryPipeline(queryPipeline: QueryPipeline)

  def logQueryStats(queryAttributes: QueryAttributes)

  def logFailed(errorMessage: String, httpStatusOption: Option[Int])

  def logSuccess()
}

object MahaRequestLogHelper {
  val logger: org.slf4j.Logger = LoggerFactory.getLogger(classOf[MahaRequestLogHelper])
}
case class MahaRequestLogHelper(registryName: String, mahaRequestLogWriter: MahaRequestLogWriter) extends MahaRequestLogBuilder {

  private[this] val complete: AtomicBoolean = new AtomicBoolean(false)
  import MahaRequestLogHelper._

  override def init(reportingRequest: ReportingRequest,
                    jobIdOption: Option[Long],
                    requestType: MahaRequestProto.RequestType,
                    rawJson: ByteString): Unit = {
    protoBuilder.setMahaServiceRegistryName(registryName)
    protoBuilder.setRequestStartTime(System.currentTimeMillis())

    val requestId = MDC.get(MahaConstants.REQUEST_ID)
    val userId = MDC.get(MahaConstants.USER_ID)

    if (requestId != null && StringUtils.isNotBlank(requestId.toString)) {
      protoBuilder.setRequestId(requestId.toString)
    }
    if (userId != null && StringUtils.isNotBlank(userId.toString)) {
      protoBuilder.setUserId(userId.toString)
    }
    protoBuilder.setRequestType(requestType)
    protoBuilder.setCube(reportingRequest.cube)
    protoBuilder.setSchema(reportingRequest.schema.toString)
    protoBuilder.setJson(rawJson)
    logger.info(s"init MahaRequestLog requestId = $requestId , userId = $userId")
  }

  def setDryRun(): Unit = {
    protoBuilder.setIsDryRun(true)
  }

  def setAsyncQueueParams(): Unit = {

  }

  def getRevision(requestModel: RequestModel): Option[Int] = {
    try {
      Some(requestModel.bestCandidates.get.publicFact.revision)
    } catch {
      case e:Exception =>
        logger.warn(s"Something went wrong in extracting cube revision for logging purpose $e")
        None
    }
  }

  override def logQueryPipeline(queryPipeline: QueryPipeline): Unit = {
    val drivingQuery = queryPipeline.queryChain.drivingQuery
    val model = queryPipeline.queryChain.drivingQuery.queryContext.requestModel
    protoBuilder.setDrivingQueryEngine(drivingQuery.engine.toString)
    protoBuilder.setDrivingTable(drivingQuery.tableName)
    protoBuilder.setQueryChainType(queryPipeline.queryChain.getClass.getSimpleName)
    protoBuilder.setHasFactFilters(model.hasFactFilters)
    protoBuilder.setHasNonFKFactFilters(model.hasNonFKFactFilters)
    protoBuilder.setHasDimFilters(model.hasDimFilters)
    protoBuilder.setHasNonFKDimFilters(model.hasNonFKDimFilters)
    protoBuilder.setHasFactSortBy(model.hasFactSortBy)
    protoBuilder.setHasDimSortBy(model.hasDimSortBy)
    protoBuilder.setIsFactDriven(model.isFactDriven)
    protoBuilder.setForceDimDriven(model.forceDimDriven)
    protoBuilder.setForceFactDriven(model.forceFactDriven)
    protoBuilder.setHasNonDrivingDimSortOrFilter(model.hasNonDrivingDimSortOrFilter)
    protoBuilder.setHasDimAndFactOperations(model.hasDimAndFactOperations)
    if (model.queryGrain.isDefined) {
      protoBuilder.setTimeGrain(model.queryGrain.toString)
    }
    if (model.dimCardinalityEstimate.isDefined) {
      protoBuilder.setDimCardinalityEstimate(model.dimCardinalityEstimate.get.asInstanceOf[Long])
    }
    val cubeRevision = getRevision(model)

    if (cubeRevision.isDefined) {
      protoBuilder.setCubeRevision(cubeRevision.get)
    }

    val columnInfoIterator: Iterator[SortByColumnInfo] = model.requestSortByCols.iterator
    while (columnInfoIterator != null && columnInfoIterator.hasNext) {
      val sortByColumnInfo: SortByColumnInfo = columnInfoIterator.next
      val sortByColumnInfoProtoBuilder: MahaRequestProto.SortByColumnInfo.Builder = MahaRequestProto.SortByColumnInfo.newBuilder.setAlias(sortByColumnInfo.alias)
      if (MahaRequestProto.Order.ASC.toString.equalsIgnoreCase(sortByColumnInfo.order.toString)) sortByColumnInfoProtoBuilder.setOrder(MahaRequestProto.Order.ASC)
      else sortByColumnInfoProtoBuilder.setOrder(MahaRequestProto.Order.DESC)
      protoBuilder.addRequestSortByCols(sortByColumnInfoProtoBuilder.build)
    }
    val dimensionsCandidates: Iterator[DimensionCandidate] = model.dimensionsCandidates.iterator
    while (dimensionsCandidates != null && dimensionsCandidates.hasNext) {
      val dimensionCandidate: DimensionCandidate = dimensionsCandidates.next
      protoBuilder.addDimensionsCandidates(dimensionCandidate.dim.name)
    }

  }

  override def logQueryStats(queryAttributes: QueryAttributes): Unit = {

    val queryAttributeOption: Option[QueryAttribute] = queryAttributes.getAttributeOption(QueryAttributes.QueryStats)
    if (queryAttributeOption.isDefined) {
      val queryStatsAttribute: QueryStatsAttribute = queryAttributeOption.get.asInstanceOf[QueryStatsAttribute]
      val engineQueryStatsIterator: Iterator[EngineQueryStat] = queryStatsAttribute.stats.getStats.iterator
      if (engineQueryStatsIterator != null && engineQueryStatsIterator.hasNext) {
        val drivingEngineQueryStat: EngineQueryStat = engineQueryStatsIterator.next
        protoBuilder.setDrivingQueryEngineLatency(drivingEngineQueryStat.endTime - drivingEngineQueryStat.startTime)
        if (engineQueryStatsIterator.hasNext) {
          val firsEngineQueryStat: EngineQueryStat = engineQueryStatsIterator.next
          protoBuilder.setFirstSubsequentQueryEngineLatency(firsEngineQueryStat.endTime - firsEngineQueryStat.startTime)
        }
        if (engineQueryStatsIterator.hasNext) {
          val reRunEngineQueryStats: EngineQueryStat = engineQueryStatsIterator.next
          protoBuilder.setReRunEngineQueryLatency(reRunEngineQueryStats.endTime - reRunEngineQueryStats.startTime)
          if (MahaRequestProto.Engine.Druid.toString.equalsIgnoreCase(reRunEngineQueryStats.engine.toString)) protoBuilder.setReRunEngine(MahaRequestProto.Engine.Druid)
          else if (MahaRequestProto.Engine.Oracle.toString.equalsIgnoreCase(reRunEngineQueryStats.engine.toString)) protoBuilder.setReRunEngine(MahaRequestProto.Engine.Oracle)
          else if (MahaRequestProto.Engine.Hive.toString.equalsIgnoreCase(reRunEngineQueryStats.engine.toString)) protoBuilder.setReRunEngine(MahaRequestProto.Engine.Hive)
        }
      }
    }
  }

  override def logFailed(errorMessage: String, httpStatusOption: Option[Int] = None): Unit = {
    if(complete.compareAndSet(false, true)) {
      if(httpStatusOption.isDefined) {
        protoBuilder.setStatus(httpStatusOption.get)
      } else protoBuilder.setStatus(500)
      protoBuilder.setErrorMessage(errorMessage)
      protoBuilder.setRequestEndTime(System.currentTimeMillis())
      writeLog()
    } else {
      logger.warn("logFailed called more than once!")
    }
  }

  override def logSuccess(): Unit =  {
    if(complete.compareAndSet(false, true)) {
      protoBuilder.setStatus(200)
      protoBuilder.setRequestEndTime(System.currentTimeMillis())
      writeLog()
    } else {
      logger.warn("logSuccess called more than once!")
    }
  }

  private[this] def writeLog(): Unit = {
    try {
      mahaRequestLogWriter.write(protoBuilder.build())
    } catch {
      case e=>
        logger.warn(s"Failed to log the event to kafka ${e.getMessage} $e")
    }
  }

  @VisibleForTesting
  def getbuilder(): MahaRequestProto.Builder = {
    protoBuilder
  }

}
