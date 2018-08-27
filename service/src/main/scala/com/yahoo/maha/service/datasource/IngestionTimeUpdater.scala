// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.datasource

import com.yahoo.maha.core.Engine

/**
 * Created by pranavbhole on 06/04/18.
 */
trait IngestionTimeUpdater {
  def engine : Engine
  def source: String
  def getIngestionTime(dataSource: String): Option[String]
  def getIngestionTimeLong(dataSource: String): Option[Long]
}

case class NoopIngestionTimeUpdater(engine: Engine, source: String) extends IngestionTimeUpdater {
  override def getIngestionTime(dataSource: String): Option[String] = None
  override def getIngestionTimeLong(dataSource: String): Option[Long] = None
}