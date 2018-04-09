package com.yahoo.maha.service.curators

import com.yahoo.maha.core.request._
import org.json4s.DefaultFormats

import scala.util.Try

/**
  * Parse an input JSON and convert it to a DrilldownConfig object.
  **/
object DrilldownConfig {
  val MAXIMUM_ROWS : BigInt = 1000
  val DEFAULT_ENFORCE_FILTERS : Boolean = false

  implicit val formats: DefaultFormats.type = DefaultFormats

  def parse(curatorConfigMap: Map[String, CuratorJsonConfig],
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
      require(drillDownMap("mr").asInstanceOf[BigInt] <= MAXIMUM_ROWS, s"Max Rows limit of $MAXIMUM_ROWS exceeded.")
      drilldownConfig.maxRows = Try(drillDownMap("mr").asInstanceOf[BigInt]).get
    }
    else{
      drilldownConfig.maxRows = MAXIMUM_ROWS
    }
  }

  private def assignEnforceFilters(drillDownMap: Map[String, Any], drilldownConfig: DrilldownConfig): Unit = {
    if(drillDownMap.contains("enforceFilters") && Try(drillDownMap("enforceFilters").asInstanceOf[Boolean]).isSuccess)
      drilldownConfig.enforceFilters = Try(drillDownMap("enforceFilters").asInstanceOf[Boolean]).getOrElse(DEFAULT_ENFORCE_FILTERS)
    else{
      drilldownConfig.enforceFilters = DEFAULT_ENFORCE_FILTERS
    }
  }

  private def assignOrdering(drillDownMap: Map[String, Any], drilldownConfig: DrilldownConfig, reportingRequest: ReportingRequest): Unit = {
    if(drillDownMap.contains("ordering") && Try(drillDownMap("ordering").asInstanceOf[List[Map[String, String]]]).isSuccess){
      val orderList = drillDownMap("ordering").asInstanceOf[List[Map[String, String]]]
      orderList.foreach { sortByMap =>
        val curatedSortBy = new SortBy(sortByMap("field"),
          sortByMap("order").toLowerCase match {
            case "asc" => ASC
            case "desc" => DESC
            case others => throw new IllegalArgumentException("Expected either asc or desc, not " + others)
          })
        drilldownConfig.ordering = drilldownConfig.ordering ++ IndexedSeq(curatedSortBy)
      }
    }else {
      drilldownConfig.ordering = reportingRequest.sortBy
    }
  }
}

case class DrilldownConfig(var enforceFilters: Boolean,
                           var dimension: Field,
                           var cube: String,
                           var ordering: IndexedSeq[SortBy],
                           var maxRows: BigInt)