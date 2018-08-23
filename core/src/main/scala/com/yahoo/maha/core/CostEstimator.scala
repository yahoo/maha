// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import com.yahoo.maha.core.fact.CostMultiplier
import com.yahoo.maha.core.request.ReportingRequest
import grizzled.slf4j.Logging

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
trait FactCostEstimator extends Logging {
  def isGrainKey(grainKey: String): Boolean
  def grainPrefix(schemaRequiredEntity: String, entity:String):  String = if(schemaRequiredEntity == entity) {
    schemaRequiredEntity
  } else {
    s"$schemaRequiredEntity-$entity"
  }
  def allPrefix(entity: String): String = s"*-$entity"
  def getSchemaBasedGrainRows(grainKey: String, request:ReportingRequest, filters: scala.collection.mutable.Map[String, Filter]): Option[Long]
  def getAllBasedGrainRows(grainKey: String, request:ReportingRequest, filters: scala.collection.mutable.Map[String, Filter]): Option[Long]
  def getDefaultRows(defaultRowCount: Long, request:ReportingRequest, filters: scala.collection.mutable.Map[String, Filter]) = {
    defaultRowCount * (request.numDays + 1)
  }
  def getRowsEstimate(schemaRequiredEntitySet:Set[(String, Filter)]
                      , dimensionsCandidates: SortedSet[DimensionCandidate]
                      , factDimList: List[String]
                      , request: ReportingRequest
                      , filters: scala.collection.mutable.Map[String, Filter]
                      , defaultRowCount:Long): RowsEstimate = {
    val schemaBasedGrainKeys = schemaRequiredEntitySet.map {
      case (requiredEntity, filter) =>
        dimensionsCandidates.headOption.map(entity => grainPrefix(requiredEntity, entity.dim.name)).getOrElse(requiredEntity)
    }
    val schemaBasedResult = schemaBasedGrainKeys.filter(isGrainKey).flatMap {
      grainKey =>
        val grainRows = getSchemaBasedGrainRows(grainKey, request, filters)
        if(request.isDebugEnabled) {
          info(s"schemaBasedResult grainKey=$grainKey grainRows=$grainRows")
        }
        grainRows
    }
    val (isGrainOptimized, rows) = if(schemaBasedResult.nonEmpty) {
      (true, schemaBasedResult.min)
    } else (false, getDefaultRows(defaultRowCount, request, filters))
    //all based grain key, take highest grain for fact table
    val  allBasedResult = factDimList.headOption.map(allPrefix).filter(isGrainKey).flatMap{
      grainKey =>
        val grainRows = getAllBasedGrainRows(grainKey, request, filters)
        if(request.isDebugEnabled) {
          info(s"allBasedResult grainKey=$grainKey grainRows=$grainRows")
        }
        grainRows
    }
    val (isScanOptimized, scanRows) = if(allBasedResult.nonEmpty) {
      (true, allBasedResult.max)
    } else (false, Long.MaxValue)

    val estimate = RowsEstimate(rows, isGrainOptimized, scanRows, isScanOptimized)
    if(request.isDebugEnabled) {
      info(s"$estimate")
    }
    estimate
  }
  def getCostEstimate(rowsEstimate: RowsEstimate, rowCostMultiplierOption: Option[CostMultiplier]) : Long = {
    val cost = for {
      rowCostMultiplier <- rowCostMultiplierOption
      costMultiplier <- rowCostMultiplier.rows.find(rowsEstimate.rows)
    } yield (costMultiplier * rowsEstimate.rows).longValue()
    cost.getOrElse(Long.MaxValue)
  }
}

trait DimCostEstimator {
  def getCardinalityEstimate(grainKey: String, request: ReportingRequest,filters: scala.collection.mutable.Map[String, Filter]): Option[Long]
}

class DefaultDimEstimator extends DimCostEstimator {
  def getCardinalityEstimate(grainKey: String, request: ReportingRequest,filters: scala.collection.mutable.Map[String, Filter]): Option[Long] = None
}

class DefaultFactEstimator(grainKeySet: Set[String] = Set.empty
                           , defaultRowCount: Long = 100
                          ) extends FactCostEstimator {
  def isGrainKey(grainKey: String): Boolean = grainKeySet(grainKey)
  def getGrainRows(grainKey: String, request:ReportingRequest, filters: scala.collection.mutable.Map[String, Filter]): Option[Long] = {
    Option((defaultRowCount * (request.numDays + 1)).longValue())
  }
  def getSchemaBasedGrainRows(grainKey: String, request:ReportingRequest, filters: scala.collection.mutable.Map[String, Filter]): Option[Long] = {
    getGrainRows(grainKey, request, filters)
  }
  def getAllBasedGrainRows(grainKey: String, request:ReportingRequest, filters: scala.collection.mutable.Map[String, Filter]): Option[Long] = {
    getGrainRows(grainKey, request, filters)
  }
}
