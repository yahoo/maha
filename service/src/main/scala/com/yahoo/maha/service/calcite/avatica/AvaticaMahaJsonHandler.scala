package com.yahoo.maha.service.calcite.avatica

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.Lists
import com.yahoo.maha.service.error.{MahaServiceBadRequestException, MahaServiceExecutionException}
import grizzled.slf4j.Logging
import org.apache.calcite.avatica.{AvaticaSeverity, SqlState}
import org.apache.calcite.avatica.metrics.MetricsSystem
import org.apache.calcite.avatica.remote.Handler.HandlerResponse
import org.apache.calcite.avatica.remote.Service.RpcMetadataResponse
import org.apache.calcite.avatica.remote.{Handler, JsonHandler, Service}
import org.apache.calcite.sql.parser.SqlParseException

case class AvaticaMahaJsonHandler(service1: Service, metrics1: MetricsSystem) extends JsonHandler(service1, metrics1) with Logging {
  val objectMapper = new ObjectMapper()

  def getBadRequest(message:String, source: Option[Throwable]) = {
    (s"Bad SQL Request,${message},${getError(source)} ", 400, SqlState.INVALID_SQL_STATEMENT, AvaticaSeverity.ERROR)
  }
  override def convertToErrorResponse(e: Exception): Handler.HandlerResponse[String] = {
    logger.error(s"sql-avatica Exception: ${e.getMessage}", e)
    val (errorMsg, errorCode, sqlState, avaticaSeverity) =  e match {
      case bad@MahaServiceBadRequestException(message, source)=>
        getBadRequest(message, source)
      case exe@MahaServiceExecutionException(message, source)=>
        (s"Failed to execute the Request,${message} ${getError(source)} ", 500, SqlState.DATA_EXCEPTION_NO_SUBCLASS, AvaticaSeverity.FATAL)
      case nae: java.util.NoSuchElementException=>
        getBadRequest(e.getMessage, Some(e.getCause))
      case iae: IllegalArgumentException=>
        getBadRequest(e.getMessage, Some(e.getCause))
      case calcite: SqlParseException=>
        getBadRequest(e.getMessage, Some(e.getCause))
      case e=>
        (s"Exception at executing query ${e.getMessage}, ${getError(Some(e.getCause))}", 500, SqlState.DATA_EXCEPTION_NO_SUBCLASS, AvaticaSeverity.UNKNOWN)
    }
    val errorResponse = new Service.ErrorResponse(Lists.newArrayList(), errorMsg, errorCode, sqlState.toString ,avaticaSeverity, new RpcMetadataResponse(""))
    new HandlerResponse(objectMapper.writeValueAsString(errorResponse), errorCode)
  }

  def getError(source: Option[Throwable]): String = {
    source.filter(t=> t!=null).map(t=> t.getMessage).headOption.getOrElse("")
  }

}
