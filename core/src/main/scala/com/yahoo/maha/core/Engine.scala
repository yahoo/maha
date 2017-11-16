// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import scala.collection.SortedSet

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
  val MAX_ALLOWED_ROWS: Int = 5000
}

object Engine {
  def from(s: String): Option[Engine] = {
    s.toLowerCase match {
      case "hive" => Option(HiveEngine)
      case "oracle" => Option(OracleEngine)
      case "druid" => Option(DruidEngine)
      case _ => None
    }
  }
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

object EngineConstraint {

  def canSupportFilter(engine: Engine, filters: SortedSet[Filter]) : Boolean = {
    val result: Boolean = engine match {
      case OracleEngine => {
        !filters.exists(f => {
          f match {
            case pdf@PushDownFilter(filter) => filter match {
              case inf@InFilter(field,values,_,_) => if(values.size > OracleEngine.MAX_SIZE_IN_FILTER) true else false
              case _ => false
            }
            case inf@InFilter(field,values,_,_) => if(values.size > OracleEngine.MAX_SIZE_IN_FILTER) true else false
            case _ => false
          }
        })
      }
      case _ => true
    }
    result
  }

}
