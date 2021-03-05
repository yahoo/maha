// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

/**
 * Created by hiral on 10/5/15.
 */
sealed trait Engine

case object HiveEngine extends Engine {
  override def toString = "Hive"
}
case object OracleEngine extends Engine {
  override def toString = "Oracle"
  val MAX_SIZE_IN_FILTER = 999
}
case object DruidEngine extends Engine {
  override def toString = "Druid"
}
case object PrestoEngine extends Engine {
  override def toString = "Presto"
}
case object PostgresEngine extends Engine {
  override def toString = "Postgres"
  val MAX_SIZE_IN_FILTER = 32766
}
case object BigqueryEngine extends Engine {
  override def toString = "Bigquery"
}

object Engine {
  //keep list in one place and derive from here so we only need to update in one place
  val engines: IndexedSeq[Engine] = IndexedSeq(DruidEngine, OracleEngine, PrestoEngine, HiveEngine, PostgresEngine, BigqueryEngine)
  require(engines.size == engines.toSet.size, "Engines list must be unique!")
  val enginesMap: Map[String, Engine] = engines.map(e => e.toString.toLowerCase -> e).toMap
  def from(s: String): Option[Engine] = enginesMap.get(s.toLowerCase)
}

sealed trait EngineRequirement {
  def engine: Engine
  def acceptEngine(engine: Engine) = engine == this.engine
}

trait WithHiveEngine extends EngineRequirement {
  final val engine: Engine = HiveEngine
}

trait WithOracleEngine extends EngineRequirement {
  final val engine: Engine = OracleEngine
}

trait WithDruidEngine extends EngineRequirement {
  final val engine: Engine = DruidEngine
}

trait WithPrestoEngine extends EngineRequirement {
  final val engine: Engine = PrestoEngine
}

trait WithPostgresEngine extends EngineRequirement {
  final val engine: Engine = PostgresEngine
}

trait WithBigqueryEngine extends EngineRequirement {
  final val engine: Engine = BigqueryEngine
}

