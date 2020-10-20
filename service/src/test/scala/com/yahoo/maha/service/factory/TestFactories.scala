package com.yahoo.maha.service.factory

import com.yahoo.maha.core.{DimCostEstimator, DimensionCandidate, FactCostEstimator, Filter, RowsEstimate}
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.service.MahaServiceConfig.MahaConfigResult
import com.yahoo.maha.service.MahaServiceConfigContext

import org.json4s.JValue

import scala.collection.immutable.SortedSet
import scala.collection.mutable

/**
 * Created by pranavbhole on 25/04/18.
 */
class TestFactEstimator extends FactCostEstimator {
  override def isGrainKey(grainKey: String): Boolean = true

  def getRowsEstimate(schemaRequiredEntitySet:Set[(String, Filter)]
                      , dimensionsCandidates: SortedSet[DimensionCandidate]
                      , factDimList: List[String]
                      , request: ReportingRequest
                      , filters: scala.collection.mutable.Map[String, Filter]
                      , defaultRowCount:Long): RowsEstimate = {
    if (request.isDebugEnabled) {
      RowsEstimate(10000, true, Long.MaxValue, false)
    } else RowsEstimate(1000, true, Long.MaxValue, false)
  }

  override def getSchemaBasedGrainRows(grainKey: String, request: ReportingRequest, filters: mutable.Map[String, mutable.TreeSet[Filter]], defaultRowCount: Long): Option[Long] = Option(1000)
  override def getAllBasedGrainRows(grainKey: String, request: ReportingRequest, filters: mutable.Map[String, mutable.TreeSet[Filter]]): Option[Long] = Option(1000)
}

class TestDimEstimator extends DimCostEstimator {
  override def getCardinalityEstimate(grainKey: String, request: ReportingRequest, filters: mutable.Map[String, mutable.TreeSet[Filter]]): Option[Long] = {
    if(request.isDebugEnabled) {
      Some(10000)
    } else Some(1000)
  }
}

import _root_.scalaz._
import Scalaz._


class TestFactCostEstimatoryFactory extends FactCostEstimatorFactory {
  override def fromJson(config: JValue)(implicit context: MahaServiceConfigContext): MahaConfigResult[FactCostEstimator] = new TestFactEstimator().successNel

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class TestDimCostEstimatoryFactory extends DimCostEstimatorFactory {
  override def fromJson(config: JValue)(implicit context: MahaServiceConfigContext): MahaConfigResult[DimCostEstimator] = new TestDimEstimator().successNel

  override def supportedProperties: List[(String, Boolean)] = List.empty
}