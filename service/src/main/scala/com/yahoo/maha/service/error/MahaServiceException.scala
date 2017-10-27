// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.error

/**
 * Created by pranavbhole on 14/06/17.
 */
trait MahaServiceException extends Exception {
  def errorCode: Int
  def message : String
  def source: Option[Throwable]
}

case class MahaServiceBadRequestException(message : String, source: Option[Throwable] = None) extends MahaServiceException {
  override val errorCode = 400
}

case class MahaServiceExecutionException(message : String, source: Option[Throwable] = None) extends MahaServiceException {
  override val errorCode = 500
}
