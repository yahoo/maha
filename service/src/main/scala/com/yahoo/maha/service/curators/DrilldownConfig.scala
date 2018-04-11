package com.yahoo.maha.service.curators

import com.oracle.javafx.jmx.json.JSONException
import com.yahoo.maha.core.request._
import com.yahoo.maha.service.MahaServiceConfig
import org.json4s.{DefaultFormats, JValue}
import com.yahoo.maha.service.MahaServiceConfig.MahaConfigResult
import org.json4s.scalaz.JsonScalaz
import org.json4s.scalaz.JsonScalaz._

import scala.util.Try
import com.yahoo.maha.core.request._
import com.yahoo.maha.service.MahaServiceConfig
import com.yahoo.maha.service.MahaServiceConfig.MahaConfigResult
import com.yahoo.maha.service.curators._
import com.yahoo.maha.service.factory._
import org.json4s.JValue
import org.json4s.JsonAST.{JArray, JObject}

/**
  * Parse an input JSON and convert it to a DrilldownConfig object.
  **/
object DrilldownConfig {
  val MAXIMUM_ROWS : BigInt = 1000
  val DEFAULT_ENFORCE_FILTERS : Boolean = false
  val validCubes : List[String] = List("performance_stats", "user_stats", "student_performance")

  implicit val formats: DefaultFormats.type = DefaultFormats

  def parse(reportingRequest: ReportingRequest) : DrilldownConfig = {

    require(reportingRequest.curatorJsonConfigMap.contains("drilldown"), "DrillDown may not be created without a declaration!")

    val config: JValue = reportingRequest.curatorJsonConfigMap("drilldown").json

    val dimension : Field = assignDim(config)

    val maxRows : BigInt = assignMaxRows(config)

    val enforceFilters : Boolean = assignEnforceFilters(config)

    val ordering : IndexedSeq[SortBy] = assignOrdering(config, reportingRequest)

    val cube : String = assignCube(config, reportingRequest.cube)

    DrilldownConfig(enforceFilters, dimension, cube, ordering, maxRows)
  }

  private def assignCube(config: JValue, default: String) : String = {
    val cubeResult : MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("cube")(config)
    if (cubeResult.isSuccess && validCubes.contains(cubeResult.toOption.get)) {
      cubeResult.toOption.get
    }
    else if(cubeResult.isSuccess){
      throw new IllegalArgumentException("Declared cube is not a valid drillDown Cube!")
    }
    else{
      default
    }
  }

  private def assignDim(config: JValue): Field = {
    val drillDim : MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("dimension")(config)
    require(drillDim.isSuccess, "CuratorConfig for a DrillDown should have a dimension declared!")
    Field(drillDim.toOption.get, None, None)
  }

  private def assignMaxRows(config: JValue): BigInt = {
    val maxRowsLimitResult : MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("mr")(config)
    if(maxRowsLimitResult.isSuccess) {
      require(maxRowsLimitResult.toOption.get <= MAXIMUM_ROWS, s"Max Rows limit of $MAXIMUM_ROWS exceeded.  Saw ${maxRowsLimitResult.toOption.get}")
      maxRowsLimitResult.toOption.get
    }
    else{
      MAXIMUM_ROWS
    }
  }

  private def assignEnforceFilters(config: JValue): Boolean = {
    val enforceFiltersResult : MahaServiceConfig.MahaConfigResult[Boolean] = fieldExtended[Boolean]("enforceFilters")(config)
    if(enforceFiltersResult.isSuccess)
      enforceFiltersResult.toOption.get
    else{
      DEFAULT_ENFORCE_FILTERS
    }
  }

  private def assignOrdering(config: JValue,
                             reportingRequest: ReportingRequest): IndexedSeq[SortBy] = {
    val orderingResult : MahaServiceConfig.MahaConfigResult[List[SortBy]] = fieldExtended[List[SortBy]]("ordering")(config)
    if(orderingResult.isSuccess){
      orderingResult.toOption.get.toIndexedSeq
    }else {
      if(orderingResult.toEither.left.get.toString().contains("order must be asc|desc not")){
        throw new IllegalArgumentException (orderingResult.toEither.left.get.head.message)
      }
      else{
        reportingRequest.sortBy
      }
    }
  }
}

case class DrilldownConfig(enforceFilters: Boolean,
                            dimension: Field,
                            cube: String,
                            ordering: IndexedSeq[SortBy],
                            maxRows: BigInt)