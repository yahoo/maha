package com.yahoo.maha.service.curators

import org.json4s.DefaultFormats
import org.json4s.scalaz.JsonScalaz._
import com.yahoo.maha.core.request._
import com.yahoo.maha.service.MahaServiceConfig
import com.yahoo.maha.service.factory._
import org.json4s.JValue
import org.json4s.scalaz.JsonScalaz

/**
  * Parse an input JSON and convert it to a DrilldownConfig object.
  **/
object DrilldownConfig extends CuratorConfig {
  protected val MAXIMUM_ROWS : BigInt = 1000
  protected val MAX_DATE_SELECTED : Int = 7
  protected val MAX_DAYS_MONTH_SELECTED : Int = 366
  protected val DEFAULT_ENFORCE_FILTERS : Boolean = false
  protected val validCubes : List[String] = List("performance_stats", "user_stats", "student_performance")
  protected val validDims : List[String] = List("Device Type", "Age", "Date", "Day", "Month", "Gender", "Location", "Section ID")


  implicit val formats: DefaultFormats.type = DefaultFormats

  def parse(reportingRequest: ReportingRequest) : JsonScalaz.Result[DrilldownConfig] = {
    import _root_.scalaz.syntax.validation._

    require(validCubes.contains(reportingRequest.cube), "Cannot drillDown using given source cube " + reportingRequest.cube)

    require(reportingRequest.curatorJsonConfigMap.contains("drilldown"), "DrillDown may not be created without a declaration!")

    val config: JValue = reportingRequest.curatorJsonConfigMap("drilldown").json

    val dimension : Field = assignDim(config)
    checkDim(dimension, reportingRequest)

    val maxRows : BigInt = assignMaxRows(config)

    val enforceFilters : Boolean = assignEnforceFilters(config)

    val ordering : IndexedSeq[SortBy] = assignOrdering(config, reportingRequest)

    val cube : String = assignCube(config, reportingRequest.cube)

    DrilldownConfig(enforceFilters, dimension, cube, ordering, maxRows).successNel
  }

  private def expandDate(reportingRequest: ReportingRequest): Unit = {
    require(reportingRequest.numDays < MAX_DATE_SELECTED, s"Only $MAX_DATE_SELECTED day range may be queried on Date DrillDown.")
  }

  private def expandMonth(reportingRequest: ReportingRequest): Unit = {
    require(reportingRequest.numDays < MAX_DAYS_MONTH_SELECTED, s"Only $MAX_DAYS_MONTH_SELECTED day range may be queried on Month DrillDown.")
  }

  private def checkDim(field: Field, reportingRequest: ReportingRequest): Unit = {
    field.field match{
      case "Date" | "Day" => expandDate(reportingRequest)
      case "Month" => expandMonth(reportingRequest)
      case other : String =>
        if (!validDims.contains(other))
          throw new IllegalArgumentException(s"DrillDown Dimension not within validDims, found: $other but required one of: $validDims")
    }
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
                            maxRows: BigInt) extends CuratorConfig