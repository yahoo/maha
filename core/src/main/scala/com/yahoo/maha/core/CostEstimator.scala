// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import com.yahoo.maha.core.fact.CostMultiplier
import com.yahoo.maha.core.request.ReportingRequest

import scala.collection.SortedSet

/**
 * Created by jians on 10/23/15.
 */

case class DimCostMetrics(averageCardinality7Day: Int, cardinality1Day: Int)
case class RowsEstimate(rows: Long, isGrainOptimized: Boolean, scanRows:Long, isScanOptimized: Boolean)

/**
  * For a request, we want to estimate cost for following scenarios
  * 1. Grain based request where a slice of the table is to be scanned and rows returned
  * 2. Non-grian based request where all rows are to be scanned and rows returned
  * 3. Non-grain based request where all rows are to be scanned and rolled up rows returned
  * 4. Non-grain based request where slice of rows are to be scanned and rows returned
  */
trait FactCostEstimator {
  def isGrainKey(grainKey: String): Boolean
  def grainPrefix(schemaRequiredEntity: String, entity:String):  String = s"$schemaRequiredEntity-$entity"
  def allPrefix(entity: String): String
  def getRowsEstimate(schemaRequiredEntitySet:Set[(String, Filter)]
                      , dimensionsCandidates: SortedSet[DimensionCandidate]
                      , factDimList: List[String]
                      , request: ReportingRequest
                      , filters: scala.collection.mutable.Map[String, Filter]
                      , defaultRowCount:Long): RowsEstimate = {
    val schemaBasedGrainKeys = schemaRequiredEntitySet.map {
      case (requiredEntity, filter) =>
        /*
        val grainPrefix = s"$requiredEntity-"
        highestLevelDim.fold {
          s"$grainPrefix${factDimList.headOption.getOrElse("")}"
        } { dc =>
          s"$grainPrefix${dc.dim.grainKey}"
        }
        */
        dimensionsCandidates.headOption.map(entity => grainPrefix(requiredEntity, entity.dim.name)).getOrElse(requiredEntity)
    }
    val schemaBasedResult = schemaBasedGrainKeys.find(factEstimator.isGrainKey)
    val grainKey = schemaBasedResult.fold {
      //all based grain key
      factDimList.map(dimName => s"*-$dimName").find(factEstimator.isGrainKey).getOrElse("")
    }(schemaBasedGrainKey => schemaBasedGrainKey)

  }
  def getCostEstimate(rowsEstimate: RowsEstimate, rowCostMultiplierOption: Option[CostMultiplier]) : Long = {
    val cost = for {
      rowCostMultiplier <- rowCostMultiplierOption
      costMultiplier <- rowCostMultiplier.rows.find(rowsEstimate.rows)
    } yield (costMultiplier * rowsEstimate.rows).longValue()
    cost.getOrElse(Long.MaxValue)
  }
  def getGrainRows(grainKey: String, request:ReportingRequest, filters: scala.collection.mutable.Map[String, Filter]): Option[Long]
}

trait DimCostEstimator {
  def getCardinalityEstimate(grainKey: String, request: ReportingRequest,filters: scala.collection.mutable.Map[String, Filter]): Option[Long]
}

class DefaultDimEstimator extends DimCostEstimator {
  def getCardinalityEstimate(grainKey: String, request: ReportingRequest,filters: scala.collection.mutable.Map[String, Filter]): Option[Long] = None
}

class DefaultFactEstimator(grainKeySet: Set[String] = Set.empty
                           , scanKeySet: Set[String] = Set.empty
                           , allPrefix: String = "*-"
                          ) extends FactCostEstimator {
  def isGrainKey(grainKey: String): Boolean = grainKeySet(grainKey)
  def getRowsEstimate(grainKey:String, request: ReportingRequest,filters: scala.collection.mutable.Map[String, Filter], defaultRowCount:Long): RowsEstimate = {
    val estimateRows = (defaultRowCount * (request.numDays + 1)).longValue()
    val isGrainOptimized = isGrainKey(grainKey)
    val isScanOptimized = isGrainOptimized || scanKeySet("")
    RowsEstimate(estimateRows, isGrainOptimized, isGrainOptimized)
  }
}
