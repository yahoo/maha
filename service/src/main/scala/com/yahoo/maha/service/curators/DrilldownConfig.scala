package com.yahoo.maha.service.curators

import com.yahoo.maha.core.request._
import grizzled.slf4j.Logging
import org.json4s.DefaultFormats

import scala.util.Try

/**
  * Parse an input JSON and convert it to a DrilldownConfig object.
  **/
object DrilldownConfig extends Logging{
  implicit val formats: DefaultFormats.type = DefaultFormats

  def validateCuratorConfig(curatorConfigMap: Map[String, CuratorJsonConfig],
                            reportingRequest: ReportingRequest,
                            drilldownConfig: DrilldownConfig) : Unit = {
    require(curatorConfigMap.contains("drillDown"), "DrillDown may not be created without a declaration!")
    val curatorConfig = curatorConfigMap("drillDown")
    val drillDownMap : Map[String, Any] = curatorConfig.json.extract[Map[String, Any]]

    assignDim(drillDownMap, drilldownConfig)

    assignMaxRows(drillDownMap, drilldownConfig)

    assignEnforceFilters(drillDownMap, drilldownConfig)

    assignOrdering(drillDownMap, drilldownConfig, reportingRequest)

    drilldownConfig.cube = reportingRequest.cube
  }

  private def assignDim(drillDownMap: Map[String, Any], drilldownConfig: DrilldownConfig): Unit = {
    require(drillDownMap.contains("dimension"), "CuratorConfig for a DrillDown should have a dimension declared!")
    val drillDim = drillDownMap("dimension").toString
    drilldownConfig.dimension = Field(drillDim, None, None)
  }

  private def assignMaxRows(drillDownMap: Map[String, Any], drilldownConfig: DrilldownConfig): Unit = {
    if(drillDownMap.contains("mr") && Try(drillDownMap("mr").asInstanceOf[BigInt]).isSuccess) {
      require(drillDownMap("mr").asInstanceOf[BigInt] <= 1000, "Max Rows limit of 1000 exceeded.")
      drilldownConfig.maxRows = Try(drillDownMap("mr").asInstanceOf[BigInt]).get
    }
  }

  private def assignEnforceFilters(drillDownMap: Map[String, Any], drilldownConfig: DrilldownConfig): Unit = {
    if(drillDownMap.contains("enforceFilters") && Try(drillDownMap("enforceFilters").asInstanceOf[Boolean]).isSuccess)
      drilldownConfig.enforceFilters = Try(drillDownMap("enforceFilters").asInstanceOf[Boolean]).getOrElse(false)
  }

  private def assignOrdering(drillDownMap: Map[String, Any], drilldownConfig: DrilldownConfig, reportingRequest: ReportingRequest): Unit = {
    if(drillDownMap.contains("ordering") && Try(drillDownMap("ordering").asInstanceOf[List[Map[String, String]]]).isSuccess){
      val orderList = drillDownMap("ordering").asInstanceOf[List[Map[String, String]]]
      val sortByMap = orderList.head
      val curatedSortBy = new SortBy(sortByMap("field"),
        sortByMap("order").toLowerCase match {
            case "asc" => ASC
            case "desc" => DESC
            case others => throw new IllegalArgumentException("Expected either asc or desc, not " + others)
        })
      drilldownConfig.ordering = IndexedSeq(curatedSortBy).union(reportingRequest.sortBy)
    }else
      drilldownConfig.ordering = reportingRequest.sortBy
  }
}

case class DrilldownConfig(var enforceFilters: Boolean,
                           var dimension: Field,
                           var cube: String,
                           var ordering: IndexedSeq[SortBy],
                           var maxRows: BigInt)