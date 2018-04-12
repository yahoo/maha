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
object DrilldownConfig {
  val MAXIMUM_ROWS : BigInt = 1000
  val MAX_DATE_SELECTED : Int = 7
  val MAX_DAYS_MONTH_SELECTED : Int = 366
  val DEFAULT_ENFORCE_FILTERS : Boolean = false
  val validCubes : List[String] = List("performance_stats", "user_stats", "student_performance")
  val validDims : List[String] = List("Device Type", "Age", "Date", "Day", "Month", "Gender", "Location", "Section ID")


  implicit val formats: DefaultFormats.type = DefaultFormats

  def parse(curatorJsonConfig: CuratorJsonConfig) : JsonScalaz.Result[DrilldownConfig] = {
    import _root_.scalaz.syntax.validation._

    val config: JValue = curatorJsonConfig.json

    val dimension : Field = assignDim(config)

    val maxRows : BigInt = assignMaxRows(config)

    val enforceFilters : Boolean = assignEnforceFilters(config)

    val ordering : IndexedSeq[SortBy] = assignOrdering(config)

    val cube : String = assignCube(config, "")

    DrilldownConfig(enforceFilters, dimension, cube, ordering, maxRows).successNel
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

  private def assignOrdering(config: JValue): IndexedSeq[SortBy] = {
    val orderingResult : MahaServiceConfig.MahaConfigResult[List[SortBy]] = fieldExtended[List[SortBy]]("ordering")(config)
    if(orderingResult.isSuccess){
      orderingResult.toOption.get.toIndexedSeq
    }else {
      if(orderingResult.toEither.left.get.toString().contains("order must be asc|desc not")){
        throw new IllegalArgumentException (orderingResult.toEither.left.get.head.message)
      }
      else{
        IndexedSeq.empty
      }
    }
  }
}

case class DrilldownConfig(enforceFilters: Boolean,
                            dimension: Field,
                            cube: String,
                            ordering: IndexedSeq[SortBy],
                            maxRows: BigInt) extends CuratorConfig