package com.yahoo.maha.service.curators

import org.json4s.DefaultFormats
import org.json4s.scalaz.JsonScalaz._
import com.yahoo.maha.core.request._
import com.yahoo.maha.service.MahaServiceConfig
import com.yahoo.maha.service.factory._
import org.json4s.JValue
import org.json4s.JsonAST.{JArray, JObject}
import org.json4s.scalaz.JsonScalaz

/**
 * Parse an input JSON and convert it to a DrilldownConfig object.
 **/
object DrilldownConfig {

  implicit val formats: DefaultFormats.type = DefaultFormats

  def parse(curatorJsonConfig: CuratorJsonConfig): JsonScalaz.Result[DrilldownConfig] = {
    curatorJsonConfig.json match {
      case obj: JObject =>
        DrilldownRequest.parse(obj).map(r => DrilldownConfig(IndexedSeq(r)))
      case JArray(arr) =>
        import scalaz.Scalaz._
        arr.map(DrilldownRequest.parse).sequence[JsonScalaz.Result, DrilldownRequest].map(l => DrilldownConfig(l.toIndexedSeq))
    }
  }

}

case class DrilldownConfig(requests: IndexedSeq[DrilldownRequest]) extends CuratorConfig

object DrilldownRequest {
  val MAXIMUM_ROWS: BigInt = 1000
  val DEFAULT_ENFORCE_FILTERS: Boolean = true

  def parse(config: JValue): JsonScalaz.Result[DrilldownRequest] = {
    import _root_.scalaz.syntax.validation._

    val dimensions: List[Field] = assignDim(config)

    val maxRows: BigInt = assignMaxRows(config)

    val enforceFilters: Boolean = assignEnforceFilters(config)

    val ordering: IndexedSeq[SortBy] = assignOrdering(config)

    val cube: String = assignCube(config, "")

    DrilldownRequest(enforceFilters, dimensions.toIndexedSeq, cube, ordering, maxRows).successNel
  }


  private def assignCube(config: JValue, default: String): String = {
    val cubeResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("cube")(config)
    if (cubeResult.isSuccess) {
      cubeResult.toOption.get
    }
    else {
      default
    }
  }

  private def assignDim(config: JValue): List[Field] = {
    val drillDim: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("dimension")(config)
    val drillDims: MahaServiceConfig.MahaConfigResult[List[String]] = fieldExtended[List[String]]("dimensions")(config)
    val dims: MahaServiceConfig.MahaConfigResult[List[String]] = drillDims orElse drillDim.map(s => List(s))
    require(dims.isSuccess, "CuratorConfig for a DrillDown should have a dimension or dimensions declared!")
    dims.toOption.get.map {
      s => Field(s, None, None)
    }
  }

  private def assignMaxRows(config: JValue): BigInt = {
    val maxRowsLimitResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("mr")(config)
    if (maxRowsLimitResult.isSuccess) {
      maxRowsLimitResult.toOption.get
    }
    else {
      MAXIMUM_ROWS
    }
  }

  private def assignEnforceFilters(config: JValue): Boolean = {
    val enforceFiltersResult: MahaServiceConfig.MahaConfigResult[Boolean] = fieldExtended[Boolean]("enforceFilters")(config)
    if (enforceFiltersResult.isSuccess)
      enforceFiltersResult.toOption.get
    else {
      DEFAULT_ENFORCE_FILTERS
    }
  }

  private def assignOrdering(config: JValue): IndexedSeq[SortBy] = {
    val orderingResult: MahaServiceConfig.MahaConfigResult[List[SortBy]] = fieldExtended[List[SortBy]]("ordering")(config)
    if (orderingResult.isSuccess) {
      orderingResult.toOption.get.toIndexedSeq
    } else {
      if (orderingResult.toEither.left.get.toString().contains("order must be asc|desc not")) {
        throw new IllegalArgumentException(orderingResult.toEither.left.get.head.message)
      }
      else {
        IndexedSeq.empty
      }
    }
  }
}

case class DrilldownRequest(enforceFilters: Boolean,
                            dimensions: IndexedSeq[Field],
                            cube: String,
                            ordering: IndexedSeq[SortBy],
                            maxRows: BigInt) extends CuratorConfig