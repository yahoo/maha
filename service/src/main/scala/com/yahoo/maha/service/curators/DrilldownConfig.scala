package com.yahoo.maha.service.curators

import org.json4s.DefaultFormats
import org.json4s.scalaz.JsonScalaz._
import com.yahoo.maha.core.request._
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

import _root_.scalaz.syntax.validation._

object DrilldownRequest {
  val DEFAULT_CUBE: JsonScalaz.Result[String] = "".successNel
  val DEFAULT_MAXIMUM_ROWS: JsonScalaz.Result[Int] = 1000.successNel
  val DEFAULT_ENFORCE_FILTERS: JsonScalaz.Result[Boolean] = true.successNel
  val DEFAULT_ADDITIVE_METRICS: JsonScalaz.Result[Boolean] = true.successNel
  val DEFAULT_FACTS: JsonScalaz.Result[List[Field]] = List.empty.successNel
  val DEFAULT_ORDERING: JsonScalaz.Result[List[SortBy]] = List.empty.successNel
  val ENFORCE_FILTERS_FIELD = "enforceFilters"
  val ADDITIVE_METRICS_FIELD = "additiveFacts"

  def parse(config: JValue): JsonScalaz.Result[DrilldownRequest] = {
    import _root_.scalaz.syntax.applicative._

    val dimensions: JsonScalaz.Result[List[Field]] = assignDim(config)

    val maxRows: JsonScalaz.Result[Int] = assignMaxRows(config)

    //we are doing orElse here since the old code ignored parse errors instead of reporting them upstream so keeping
    //backwards compatibility
    val enforceFilters: JsonScalaz.Result[Boolean] = assignBooleanField(config, ENFORCE_FILTERS_FIELD, DEFAULT_ENFORCE_FILTERS) orElse DEFAULT_ENFORCE_FILTERS

    val ordering: JsonScalaz.Result[List[SortBy]] = assignOrdering(config)

    val cube: JsonScalaz.Result[String] = assignCube(config)

    val additiveFacts: JsonScalaz.Result[Boolean] = assignBooleanField(config, ADDITIVE_METRICS_FIELD, DEFAULT_ADDITIVE_METRICS)

    val facts: JsonScalaz.Result[List[Field]] = assignFacts(config)

    (enforceFilters |@| dimensions |@| cube |@| ordering |@| maxRows |@| facts |@| additiveFacts) ((a, b, c, d, e, f, g) => {
      DrilldownRequest(a, b.toIndexedSeq, c, d.toIndexedSeq, e, f.toIndexedSeq, g)
    })
  }


  private def assignCube(config: JValue): JsonScalaz.Result[String] = {
    val cubeField = config.findField(tpl => tpl._1 == "cube")
    if (cubeField.isDefined) {
      fieldExtended[String]("cube")(config)
    } else DEFAULT_CUBE
  }

  private def assignFacts(config: JValue): JsonScalaz.Result[List[Field]] = {
    val factsField = config.findField(tpl => tpl._1 == "facts")
    if (factsField.isDefined) {
      fieldExtended[List[Field]]("facts")(config)
    } else DEFAULT_FACTS
  }

  private def assignDim(config: JValue): JsonScalaz.Result[List[Field]] = {
    val drillDim: JsonScalaz.Result[String] = fieldExtended[String]("dimension")(config)
    val drillDims: JsonScalaz.Result[List[String]] = fieldExtended[List[String]]("dimensions")(config)
    val dims: JsonScalaz.Result[List[String]] = drillDims orElse drillDim.map(s => List(s))
    dims.map(l => l.map(s => Field(s, None, None)))
  }

  private def assignMaxRows(config: JValue): JsonScalaz.Result[Int] = {
    val mrField = config.findField(tpl => tpl._1 == "mr")
    if (mrField.isDefined) {
      fieldExtended[Int]("mr")(config)
    } else DEFAULT_MAXIMUM_ROWS
  }

  private def assignBooleanField(config: JValue, fieldName: String, defaultValue: JsonScalaz.Result[Boolean]): JsonScalaz.Result[Boolean] = {
    val booleanField = config.findField(tpl => tpl._1 == fieldName)
    if (booleanField.isDefined) {
      fieldExtended[Boolean](fieldName)(config)
    } else defaultValue
  }

  private def assignOrdering(config: JValue): JsonScalaz.Result[List[SortBy]] = {
    val orderingField = config.findField(tpl => tpl._1 == "ordering")
    if (orderingField.isDefined) {
      fieldExtended[List[SortBy]]("ordering")(config)
    } else DEFAULT_ORDERING
  }
}

case class DrilldownRequest(enforceFilters: Boolean,
                            dimensions: IndexedSeq[Field],
                            cube: String,
                            ordering: IndexedSeq[SortBy],
                            maxRows: BigInt,
                            facts: IndexedSeq[Field],
                            additiveFacts: Boolean
                           ) extends CuratorConfig