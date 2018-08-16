// Copyright 2018, Oath Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.

package com.yahoo.maha.worker.jobmeta

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
}

case object AsyncOracle extends JobType {
  override def engine: Engine = OracleEngine
  override def name: String = "maha-async-oracle"
}
case object AsyncDruid extends JobType {
  override def engine: Engine = DruidEngine
  override def name: String = "maha-async-druid"
}
case object AsyncPresto extends JobType {
  override def engine: Engine = PrestoEngine
  override def name: String = "maha-async-presto"
}

/*
   Please note that async-hive is currently experimental in maha-workers
 */
case object AsyncHive extends JobType {
  override def engine: Engine = HiveEngine
  override def name: String = "maha-async-hive"
}

