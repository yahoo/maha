// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.error

/**
  * Created by hiral on 5/25/17.
  */
sealed abstract class MahaServiceError extends Product with Serializable {
  def message: String
  def source: Option[Throwable]
}

case class JsonParseError(message: String, source: Option[Throwable] = None) extends MahaServiceError
case class ServiceConfigurationError(message: String, source: Option[Throwable] = None) extends MahaServiceError
case class FailedToConstructFactory(message: String, source: Option[Throwable] = None) extends MahaServiceError

case class MahaCalciteSqlParserError(errorStr: String, sql:String, source: Option[Throwable] = None) extends MahaServiceError {
  override val message: String = s"MAHA-SQL-${sql}: ${errorStr}"
}