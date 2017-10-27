// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import com.yahoo.maha.core.fact.CostMultiplier
import com.yahoo.maha.core.request.ReportingRequest

/**
 * Created by jians on 10/23/15.
 */

case class DimCostMetrics(averageCardinality7Day: Int, cardinality1Day: Int)

trait FactCostEstimator {
  def getRowsEstimate(grainKey:String, request: ReportingRequest,filters: scala.collection.mutable.Map[String, Filter], defaultRowCount:Long): Long
  def getCostEstimate(rowsEstimate: Long, rowCostMultiplierOption: Option[CostMultiplier]) : Long = {
    val cost = for {
      rowCostMultiplier <- rowCostMultiplierOption
      costMultiplier <- rowCostMultiplier.rows.find(rowsEstimate)
    } yield (costMultiplier * rowsEstimate).longValue()
    cost.getOrElse(Long.MaxValue)
  }
}

trait DimCostEstimator {
  def getCardinalityEstimate(grainKey: String, request: ReportingRequest,filters: scala.collection.mutable.Map[String, Filter]): Option[Long]
}

class DefaultDimEstimator extends DimCostEstimator {
  def getCardinalityEstimate(grainKey: String, request: ReportingRequest,filters: scala.collection.mutable.Map[String, Filter]): Option[Long] = None
}

class DefaultFactEstimator extends FactCostEstimator {
  def getRowsEstimate(grainKey:String, request: ReportingRequest,filters: scala.collection.mutable.Map[String, Filter], defaultRowCount:Long): Long = {
    (defaultRowCount * (request.numDays + 1)).longValue()
  }
}
