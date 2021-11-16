package com.yahoo.maha.service.calcite.avatica

import grizzled.slf4j.Logging
import org.apache.calcite.avatica.metrics.MetricsSystem
import org.apache.calcite.avatica.remote.Handler.HandlerResponse
import org.apache.calcite.avatica.remote.{Handler, ProtobufHandler, ProtobufTranslation, Service}

case class AvaticaMahaProtobufHandler(service1: Service, translation1: ProtobufTranslation, metrics1: MetricsSystem) extends ProtobufHandler(service1, translation1, metrics1) with Logging {

  override def convertToErrorResponse(e: Exception): Handler.HandlerResponse[Array[Byte]] = {
    logger.error(s"sql-avatica Exception: ${e.getMessage}", e)
    val (errorResponse, errorCode) = HandlerHelper.convertToErrorResponse(e)
    logger.info(s"errorResponse:" + errorResponse.toString)
    new HandlerResponse(translation1.serializeResponse(errorResponse), errorCode)
  }

}
