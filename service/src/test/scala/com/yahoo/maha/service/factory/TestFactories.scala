package com.yahoo.maha.service.factory

import com.yahoo.maha.core.{DimCostEstimator, FactCostEstimator, Filter, RowsEstimate}
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.service.MahaServiceConfig.MahaConfigResult
import com.yahoo.maha.service.MahaServiceConfigContext
import com.yahoo.maha.service.config.JsonDataSourceConfig
import javax.sql.DataSource
import org.json4s.JValue

import scala.collection.mutable

/**
 * Created by pranavbhole on 25/04/18.
 */
class TestFactEstimator extends FactCostEstimator {
  override def isGrainKey(grainKey: String): Boolean = true

  override def getRowsEstimate(grainKey: String, request: ReportingRequest, filters: mutable.Map[String, Filter], defaultRowCount: Long): RowsEstimate =
    if(request.isDebugEnabled) {
      RowsEstimate(10000, true)
    } else RowsEstimate(1000, true)
}

class TestDimEstimator extends DimCostEstimator {
  override def getCardinalityEstimate(grainKey: String, request: ReportingRequest, filters: mutable.Map[String, Filter]): Option[Long] = {
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