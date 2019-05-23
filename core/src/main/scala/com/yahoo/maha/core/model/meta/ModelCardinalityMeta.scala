package com.yahoo.maha.core.model.meta

import com.yahoo.maha.core.Engine
import com.yahoo.maha.core.registry.FactRowsCostEstimate

/**
  *
  * - factCost
  * - dimCardinalityEstimate
  */
case class ModelCardinalityMeta (
                                  factCost: Map[(String, Engine), FactRowsCostEstimate]
                                , dimCardinalityEstimateOption: Option[Long]
                                ) {
  override def toString: String =
    s"""${this.getClass.getSimpleName}:
       factCost: $factCost
       dimCardinalityEstimateOption: $dimCardinalityEstimateOption""".stripMargin
}
