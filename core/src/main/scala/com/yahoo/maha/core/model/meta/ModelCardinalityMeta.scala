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

}
