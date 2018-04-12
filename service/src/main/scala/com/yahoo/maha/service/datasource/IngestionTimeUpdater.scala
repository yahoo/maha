package com.yahoo.maha.service.datasource

import com.yahoo.maha.core.Engine

/**
 * Created by pranavbhole on 06/04/18.
 */
trait IngestionTimeUpdater {
  def engine : Engine
  def source: String
  def getIngestionTime(dataSource: String): Option[String]
}

case class NoopIngestionTimeUpdater(engine: Engine, source: String) extends IngestionTimeUpdater {
  override def getIngestionTime(dataSource: String): Option[String] = None
}