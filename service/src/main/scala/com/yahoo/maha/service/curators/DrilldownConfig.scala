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
                            reportingRequest: ReportingRequest) : DrilldownConfig = {

    require(curatorConfigMap.contains("drillDown"), "DrillDown may not be created without a declaration!")

    val drillDownConfigJson = curatorConfigMap("drillDown")
    val drillDownMap : Map[String, Any] = drillDownConfigJson.json.extract[Map[String, Any]]

    val dimension = assignDim(drillDownMap)

    val maxRows = assignMaxRows(drillDownMap)

    val enforceFilters = assignEnforceFilters(drillDownMap)

    val ordering = assignOrdering(drillDownMap, reportingRequest)

    DrilldownConfig(enforceFilters, dimension, reportingRequest.cube, ordering, maxRows)
  }

  private def assignDim(drillDownMap: Map[String, Any]): Field = {
    require(drillDownMap.contains("dimension"), "CuratorConfig for a DrillDown should have a dimension declared!")
    val drillDim = drillDownMap("dimension").toString
    Field(drillDim, None, None)
  }

  private def assignMaxRows(drillDownMap: Map[String, Any]): BigInt = {
    if(drillDownMap.contains("mr") && Try(drillDownMap("mr").asInstanceOf[BigInt]).isSuccess) {
      require(drillDownMap("mr").asInstanceOf[BigInt] <= MAXIMUM_ROWS, s"Max Rows limit of $MAXIMUM_ROWS exceeded.")
      Try(drillDownMap("mr").asInstanceOf[BigInt]).get
    }
    else{
      MAXIMUM_ROWS
    }
  }

  private def assignEnforceFilters(drillDownMap: Map[String, Any]): Boolean = {
    if(drillDownMap.contains("enforceFilters") && Try(drillDownMap("enforceFilters").asInstanceOf[Boolean]).isSuccess)
      Try(drillDownMap("enforceFilters").asInstanceOf[Boolean]).get
    else{
      DEFAULT_ENFORCE_FILTERS
    }
  }

  private def assignOrdering(drillDownMap: Map[String, Any],
                             reportingRequest: ReportingRequest): IndexedSeq[SortBy] = {
    if(drillDownMap.contains("ordering") && Try(drillDownMap("ordering").asInstanceOf[List[Map[String, String]]]).isSuccess){
      val orderList = drillDownMap("ordering").asInstanceOf[List[Map[String, String]]]
      var sortByOrdering : IndexedSeq[SortBy] = IndexedSeq.empty
      orderList.foreach { sortByMap =>
        val curatedSortBy = new SortBy(sortByMap("field"),
          sortByMap("order").toLowerCase match {
            case "asc" => ASC
            case "desc" => DESC
            case others => throw new IllegalArgumentException("Expected either asc or desc, not " + others)
          })
        sortByOrdering = sortByOrdering ++ IndexedSeq(curatedSortBy)
      }
      sortByOrdering
    }else {
      reportingRequest.sortBy
    }
  }
}

case class DrilldownConfig(enforceFilters: Boolean,
                            dimension: Field,
                            cube: String,
                            ordering: IndexedSeq[SortBy],
                            maxRows: BigInt)