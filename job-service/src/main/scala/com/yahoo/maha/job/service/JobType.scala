// Copyright 2018, Oath Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.

package com.yahoo.maha.job.service

import com.yahoo.maha.core._

/*
    Created by pranavbhole on 8/14/18
*/
trait JobType {
  def engine : Engine
  def name: String
}
object JobType {
  def getJobType(engine: Engine): JobType = {
    engine match  {
      case OracleEngine => AsyncOracle
      case DruidEngine => AsyncDruid
      case HiveEngine=> AsyncHive
      case PrestoEngine=> AsyncPresto
      case _=> throw new IllegalArgumentException(s"Failed to find the JobType for given engine $engine")
    }
  }
  def fromString(name:String) : JobType = {
    name match  {
      case AsyncOracle.name=> AsyncOracle
      case AsyncDruid.name=> AsyncDruid
      case AsyncHive.name=> AsyncHive
      case AsyncPresto.name=> AsyncPresto
      case a=> throw new IllegalArgumentException(s"Unknown jobType $a")
    }

  }
}

case object AsyncOracle extends JobType {
  val engine: Engine = OracleEngine
  val name: String = "maha-async-oracle"
}
case object AsyncDruid extends JobType {
  val engine: Engine = DruidEngine
  val name: String = "maha-async-druid"
}
case object AsyncPresto extends JobType {
  val engine: Engine = PrestoEngine
  val name: String = "maha-async-presto"
}

/*
   Please note that async-hive is currently experimental in maha-workers
 */
case object AsyncHive extends JobType {
  val engine: Engine = HiveEngine
  val name: String = "maha-async-hive"
}

