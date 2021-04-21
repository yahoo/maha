// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.api.jersey

import javax.ws.rs.core.{MediaType, Response}
import javax.ws.rs.ext.{ExceptionMapper, Provider}

import com.yahoo.maha.service.error.{MahaServiceExecutionException, MahaServiceBadRequestException}
import grizzled.slf4j.Logging

import scala.beans.BeanProperty

@Provider
class GenericExceptionMapper extends ExceptionMapper[Throwable] with Logging {

  override def toResponse(e: Throwable): Response = {

    val response: Response = {
      e match {
        case iae: IllegalArgumentException => Response.status(Response.Status.BAD_REQUEST).entity(Error(iae.getMessage)).`type`(MediaType.APPLICATION_JSON).build()
        case NotFoundException(error) => Response.status(Response.Status.BAD_REQUEST).entity(error).`type`(MediaType.APPLICATION_JSON).build()
        case MahaServiceBadRequestException(message, source) => Response.status(Response.Status.BAD_REQUEST).entity(Error(message)).`type`(MediaType.APPLICATION_JSON).build()
        case MahaServiceExecutionException(message, source) =>
          source match {
            case Some(e) if e.isInstanceOf[IllegalArgumentException] =>
              Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage).`type`(MediaType.APPLICATION_JSON).build()
            case _ =>
              Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(Error(message)).`type`(MediaType.APPLICATION_JSON).build()
          }
        case _ => {
          e.printStackTrace()
          Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(Error(s"$e")).`type`(MediaType.APPLICATION_JSON).build()
        }
      }
    }

    error(s"response status: ${response.getStatus} , response entity: ${response.getEntity}")
    response
  }

}

case class Error(@BeanProperty errorMsg: String)

case class NotFoundException(error: Error) extends Exception
