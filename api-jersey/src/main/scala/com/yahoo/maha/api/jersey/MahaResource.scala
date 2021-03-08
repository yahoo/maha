// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.api.jersey

import java.util.UUID

import javax.servlet.http.HttpServletRequest
import javax.ws.rs.container.{AsyncResponse, ContainerRequestContext, Suspended}
import javax.ws.rs.core.{Context, MediaType}
import javax.ws.rs.{Path, Produces, _}
import com.yahoo.maha.core.bucketing.{BucketParams, UserInfo}
import com.yahoo.maha.core.request.{BaseRequest, ReportingRequest, RequestContext}
import com.yahoo.maha.core.{Schema, _}
import com.yahoo.maha.parrequest2.GeneralError
import com.yahoo.maha.service._
import com.yahoo.maha.service.output.{DebugRenderer, NoopDebugRenderer}
import com.yahoo.maha.service.utils.MahaConstants
import grizzled.slf4j.Logging
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.MDC
import org.springframework.stereotype.Component

import scala.util.Try

@Path("/registry")
@Component
class MahaResource(mahaService: MahaService
                   , baseRequest: BaseRequest
                   , requestValidator: RequestValidator
                   , mahaRequestContextBuilder: MahaRequestContextBuilder
                   , debugRenderer: DebugRenderer = new NoopDebugRenderer
                   , debugUserListCSV: String = ""
                  ) extends Logging {

  private[this] val debugUsers: Set[String] = debugUserListCSV.split(",").toSet
  private[this] val defaultRequestCoordinator = DefaultRequestCoordinator(mahaService)
  private[this] val mahaRequestProcessorFactory = MahaSyncRequestProcessorFactory(defaultRequestCoordinator
    , mahaService
    , mahaService.mahaRequestLogWriter
    , mahaServiceMonitor = DefaultMahaServiceMonitor)
  private[this] val defaultDebugRenderer = Option(debugRenderer)


  @GET
  @Path("/{registryName}/cubes")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getCubes(@PathParam("registryName") registryName: String): String = {
    val domainjson: Option[String] = mahaService.getCubes(registryName)
    if(domainjson.isDefined) {
      domainjson.get
    } else {
      throw NotFoundException(Error(s"registry $registryName not found"))
    }
  }

  @GET
  @Path("/{registryName}/domain")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getDomain(@PathParam("registryName") registryName: String): String = {
    val domainjson: Option[String] = mahaService.getDomain(registryName)
    if(domainjson.isDefined) {
      domainjson.get
    } else {
      throw NotFoundException(Error(s"registry $registryName not found"))
    }
  }

  @GET
  @Path("/{registryName}/domain/cubes/{cube}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getDomainForCube(@PathParam("registryName") registryName: String, @PathParam("cube") cube: String): String = {
    val domainjson: Option[String] = mahaService.getDomainForCube(registryName, cube)
    if(domainjson.isDefined) {
      domainjson.get
    } else {
      throw NotFoundException(Error(s"registry $registryName and cube $cube not found"))
    }
  }

  @GET
  @Path("/{registryName}/flattenDomain")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getFlattenDomain(@PathParam("registryName") registryName: String): String = {
    val domainjson: Option[String] = mahaService.getFlattenDomain(registryName)
    if(domainjson.isDefined) {
      domainjson.get
    } else {
      throw NotFoundException(Error(s"registry $registryName not found"))
    }
  }

  @GET
  @Path("/{registryName}/flattenDomain/cubes/{cube}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getFlattenDomainForCube(@PathParam("registryName") registryName: String, @PathParam("cube") cube: String): String = {
    val domainjson: Option[String] = mahaService.getFlattenDomainForCube(registryName, cube)
    if(domainjson.isDefined) {
      domainjson.get
    } else {
      throw NotFoundException(Error(s"registry $registryName and cube $cube not found"))
    }
  }

  @GET
  @Path("/{registryName}/flattenDomain/cubes/{cube}/{revision}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getFlattenDomainForCube(@PathParam("registryName") registryName: String, @PathParam("cube") cube: String, @PathParam("revision") revision: Int): String = {
    val domainjson: Option[String] = mahaService.getFlattenDomainForCube(registryName, cube, Option(revision))
    if(domainjson.isDefined) {
      domainjson.get
    } else {
      throw NotFoundException(Error(s"registry $registryName and cube $cube with revision $revision not found"))
    }
  }

  @POST
  @Path("/{registryName}/schemas/{schema}/query")
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def query(@PathParam("registryName") registryName: String,
            @PathParam("schema") schema: String,
            @QueryParam("debug") @DefaultValue("false") requestDebug: Boolean,
            @QueryParam("forceEngine") forceEngine: String,
            @QueryParam("forceRevision") forceRevision: Int,
            @QueryParam("testName") testName: String,
            @QueryParam("labels") labels: java.util.List[String],
            @Context httpServletRequest: HttpServletRequest,
            @Context containerRequestContext: ContainerRequestContext,
            @Suspended response: AsyncResponse) : Unit = {

    val requestStartTime = System.currentTimeMillis()
    info(s"registryName: $registryName, schema: $schema, forceEngine: $forceEngine, forceRevision: $forceRevision")
    val schemaOption: Option[Schema] = Schema.withNameInsensitiveOption(schema)

    if(schemaOption.isEmpty) {
      throw NotFoundException(Error(s"schema $schema not found"))
    }

    val userId: String = Option(MDC.get(MahaConstants.USER_ID)).getOrElse("unknown")
    val requestId: String = Option(MDC.get(MahaConstants.REQUEST_ID)).getOrElse(UUID.randomUUID().toString)
    val debug: Boolean = if(requestDebug) requestDebug else debugUsers(userId)
    val (reportingRequest: ReportingRequest, rawJson: Array[Byte]) = createReportingRequest(
      requestId
      , userId
      , httpServletRequest
      , schemaOption.get
      , debug
      , forceEngine
      , testName
      , labels
    )

    val bucketParams: BucketParams = BucketParams(UserInfo(userId, Try(MDC.get(MahaConstants.IS_INTERNAL).toBoolean).getOrElse(false)), forceRevision = Option(forceRevision))

    val mahaRequestContext: MahaRequestContext = mahaRequestContextBuilder.build(registryName
      , bucketParams, reportingRequest, rawJson, requestId, userId, requestStartTime = requestStartTime, containerRequestContext)
    requestValidator.validate(mahaRequestContext, containerRequestContext)
    val mahaRequestProcessor: MahaSyncRequestProcessor = mahaRequestProcessorFactory
      .create(mahaRequestContext, MahaServiceConstants.MahaRequestLabel)

    mahaRequestProcessor.onSuccess((requestCoordinatorResult: RequestCoordinatorResult) => {
      response.resume(new JsonStreamingOutput(requestCoordinatorResult, debugRenderer = defaultDebugRenderer))
    })

    mahaRequestProcessor.onFailure((ge: GeneralError) => {
      if(ge.throwableOption.isDefined) {
        val error = ge.throwableOption.get
        this.error(ge.message, error)
        if(error.getCause != null) {
          response.resume(error.getCause)
        } else {
          response.resume(error)
        }
      } else {
        response.resume(new Exception(ge.message))
      }
    })

    mahaRequestProcessor.process()

  }

  private def createReportingRequest(requestId:String
                                     , userId:String
                                     , httpServletRequest: HttpServletRequest
                                     , schema: Schema
                                     , debug: Boolean
                                     , forceEngine: String
                                     , testName: String
                                     , labels: java.util.List[String]) : (ReportingRequest, Array[Byte]) = {
    val rawJson = IOUtils.toByteArray(httpServletRequest.getInputStream)
    val reportingRequestResult = baseRequest.deserializeSyncWithFactBias(rawJson, schema)
    require(reportingRequestResult.isSuccess, reportingRequestResult.toString)
    val originalRequest = reportingRequestResult.toOption.get
    val request = {
      if(!debug && StringUtils.isBlank(forceEngine)) {
        originalRequest
      } else {
        val withDebug = if(debug) {
          ReportingRequest.enableDebug(originalRequest)
        } else {
          originalRequest
        }
        val withEngine = if(StringUtils.isNotBlank(forceEngine)) {
          Engine.from(forceEngine).fold(withDebug) {
            case OracleEngine => ReportingRequest.forceOracle(withDebug)
            case DruidEngine => ReportingRequest.forceDruid(withDebug)
            case HiveEngine => ReportingRequest.forceHive(withDebug)
            case PrestoEngine => ReportingRequest.forcePresto(withDebug)
            case PostgresEngine => ReportingRequest.forcePostgres(withDebug)
            case BigqueryEngine => ReportingRequest.forceBigquery(withDebug)
            case _ => withDebug
          }
        } else {
          withDebug
        }
        val withTestName = if(StringUtils.isNotBlank(testName)) {
          ReportingRequest.withTestName(withEngine, testName)
        } else {
          withEngine
        }

        val withLabels = if(labels != null && !labels.isEmpty) {
          import scala.collection.JavaConverters._
          ReportingRequest.withLabels(withTestName, labels.asScala.toList)
        } else {
          withTestName
        }

        withLabels
      }
    }
    (ReportingRequest.addRequestContext(request, RequestContext(requestId, userId)), rawJson)
  }

}
