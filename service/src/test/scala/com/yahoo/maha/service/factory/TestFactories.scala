package com.yahoo.maha.service.factory

import com.yahoo.maha.core.{DimCostEstimator, RowsEstimate, Filter, FactCostEstimator}
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.service.MahaServiceConfig.MahaConfigResult
import org.json4s.JValue

import scala.collection.mutable

/**
 * Created by pranavbhole on 25/04/18.
 */
class TestFactEstimator extends FactCostEstimator {
  override protected def isGrainKey(grainKey: String): Boolean = true

  override def getRowsEstimate(grainKey: String, request: ReportingRequest, filters: mutable.Map[String, Filter], defaultRowCount: Long): RowsEstimate =
    RowsEstimate(1000, true)
}

class TestDimEstimator extends DimCostEstimator {
  override def getCardinalityEstimate(grainKey: String, request: ReportingRequest, filters: mutable.Map[String, Filter]): Option[Long] = Some(1000)
}

import _root_.scalaz._
import Scalaz._


class TestFactCostEstimatoryFactory extends FactCostEstimatorFactory {
  override def fromJson(config: JValue): MahaConfigResult[FactCostEstimator] = new TestFactEstimator().successNel

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

class TestDimCostEstimatoryFactory extends DimCostEstimatorFactory {
  override def fromJson(config: JValue): MahaConfigResult[DimCostEstimator] = new TestDimEstimator().successNel

  override def supportedProperties: List[(String, Boolean)] = List.empty
}