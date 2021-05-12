package com.yahoo.maha.service.calcite.avatica

import com.fasterxml.jackson.databind.ObjectMapper
import grizzled.slf4j.Logging
import org.apache.calcite.avatica.metrics.MetricsSystem
import org.apache.calcite.avatica.remote.Handler.HandlerResponse
import org.apache.calcite.avatica.remote.{Handler, JsonHandler, Service}

case class AvaticaMahaJsonHandler(service1: Service, metrics1: MetricsSystem) extends JsonHandler(service1, metrics1) with Logging {
  val objectMapper = new ObjectMapper()

  override def convertToErrorResponse(e: Exception): Handler.HandlerResponse[String] = {
    logger.error(s"sql-avatica Exception: ${e.getMessage}", e)
    val (errorResponse, errorCode) = HandlerHelper.convertToErrorResponse(e)
    new HandlerResponse(objectMapper.writeValueAsString(errorResponse), errorCode)
  }

}
