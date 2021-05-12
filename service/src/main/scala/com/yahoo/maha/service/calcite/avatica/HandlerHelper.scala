package com.yahoo.maha.service.calcite.avatica

import com.google.common.collect.Lists
import com.yahoo.maha.service.error.{MahaServiceBadRequestException, MahaServiceExecutionException}
import org.apache.calcite.avatica.{AvaticaSeverity, SqlState}
import org.apache.calcite.avatica.remote.Service
import org.apache.calcite.sql.parser.SqlParseException

case object HandlerHelper {

  def getBadRequest(message:String, source: Option[Throwable]) = {
    (s"Bad SQL Request,${message},${getError(source)} ", 400, SqlState.INVALID_SQL_STATEMENT, AvaticaSeverity.ERROR)
  }

  def convertToErrorResponse(e: Exception): (Service.ErrorResponse, Int) = {
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
    val errorResponse = new Service.ErrorResponse(Lists.newArrayList(), errorMsg, errorCode, sqlState.toString ,avaticaSeverity, null)
    (errorResponse, errorCode)
  }

  def getError(source: Option[Throwable]): String = {
    source.filter(t=> t!=null).map(t=> t.getMessage).headOption.getOrElse("")
  }
}
