// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import com.yahoo.maha.service.MahaServiceConfig
import com.yahoo.maha.service.MahaServiceConfig.MahaConfigResult
import com.yahoo.maha.core.query.druid.{AsyncDruidQueryOptimizer, SyncDruidQueryOptimizer, DruidQueryOptimizer}
import com.yahoo.maha.core.request._
import org.json4s.JValue
import _root_.scalaz._
import syntax.applicative._
import syntax.validation._


/**
 * Created by pranavbhole on 31/05/17.
 */
class SyncDruidQueryOptimizerFactory extends DruidQueryOptimizerFactory {
  """
    | {
    | "maxSingleThreadedDimCardinality" : 40000,
    | "maxNoChunkCost": 280000,
    | "maxChunks": 3,
    | "timeout": 300000
    | }
  """.stripMargin

  override def fromJson(configJson: JValue): MahaConfigResult[DruidQueryOptimizer] =  {
    import org.json4s.scalaz.JsonScalaz._
    val maxSingleThreadedDimCardinalityResult: MahaServiceConfig.MahaConfigResult[Long] = fieldExtended[Long]("maxSingleThreadedDimCardinality")(configJson)
    val maxNoChunkCostResult: MahaServiceConfig.MahaConfigResult[Long] = fieldExtended[Long]("maxNoChunkCost")(configJson)
    val maxChunksResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("maxChunks")(configJson)
    val timeoutResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("timeout")(configJson)

    (maxSingleThreadedDimCardinalityResult |@| maxNoChunkCostResult |@| maxChunksResult |@| timeoutResult) {
      (a, b, c ,d) => new SyncDruidQueryOptimizer(a, b, c, d)
    }
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class AsyncDruidQueryOptimizerFactory extends DruidQueryOptimizerFactory {
  override def fromJson(configJson: JValue): MahaConfigResult[DruidQueryOptimizer] =  {
    import org.json4s.scalaz.JsonScalaz._
    val maxSingleThreadedDimCardinalityResult: MahaServiceConfig.MahaConfigResult[Long] = fieldExtended[Long]("maxSingleThreadedDimCardinality")(configJson)
    val maxNoChunkCostResult: MahaServiceConfig.MahaConfigResult[Long] = fieldExtended[Long]("maxNoChunkCost")(configJson)
    val maxChunksResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("maxChunks")(configJson)
    val timeoutResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("timeout")(configJson)

    (maxSingleThreadedDimCardinalityResult |@| maxNoChunkCostResult |@| maxChunksResult |@| timeoutResult) {
      (a, b, c ,d) => new AsyncDruidQueryOptimizer(a, b, c, d)
    }
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}


class DefaultDruidQueryOptimizerFactory extends DruidQueryOptimizerFactory {
  override def fromJson(config: JValue): MahaConfigResult[DruidQueryOptimizer] = new SyncDruidQueryOptimizer().successNel

  override def supportedProperties: List[(String, Boolean)] = List.empty
}