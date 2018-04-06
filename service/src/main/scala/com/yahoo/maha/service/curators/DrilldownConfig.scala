package com.yahoo.maha.service.curators

import com.yahoo.maha.core.request._
import org.json4s.DefaultFormats
import org.json4s.JsonAST._

import scala.collection.immutable.Map.Map4
import scala.util.Try

/**
  * Parse an input JSON and convert it to a DrilldownConfig object.
  */
trait JsonCurator {
  implicit val formats = DefaultFormats
  var enforceFilters : Boolean
  var dimension : Field
  var cube : String
  var ordering : List[SortBy]
  var maxRows : Int
}

object DrilldownConfig extends JsonCurator {
  override var enforceFilters: Boolean = false
  override var dimension: Field = null
  override var cube: String = ""
  override var ordering: List[SortBy] = List.empty
  override var maxRows: Int = 1000

  def extractField[T<:JValue](field:T):Any ={
    field match{
      case JBool(boolean) => boolean
      case JString(_) => field.values
      case JDecimal(_) => field.values
      case JInt(_) => field.values
      case JDouble(_) => field.values
      case JLong(_) => field.values
      case JNull|JNothing => null
      case _ => throw new  UnsupportedOperationException("unsupported field type")
    }
  }

  def validateCuratorConfig(curatorConfigMap: Map[String, CuratorJsonConfig],
                            reportingRequest: ReportingRequest) : Unit = {
    require(curatorConfigMap.contains("drillDown"), "DrillDown may not be created without a declaration!")
    val curatorConfig = curatorConfigMap("drillDown")
    val drillDownMap : Map[String, Any] = curatorConfig.json.extract[Map[String, Any]]

    require(drillDownMap.contains("dimension") && Try(drillDownMap("dimension").toString).isSuccess, "CuratorConfig for a DrillDown should have a dimension declared!")
    val drillDim = drillDownMap("dimension").toString
    dimension = Field(drillDim, None, None)

    if(drillDownMap.contains("mr") && Try(drillDownMap("mr").asInstanceOf[Int]).isSuccess) {
      require(drillDownMap("mr").asInstanceOf[Int] <= 1000, "Max Rows limit of 1000 exceeded.")
      maxRows = Try(drillDownMap("mr").asInstanceOf[Int]).getOrElse(1000)
    }

    if(drillDownMap.contains("enforceFilters") && Try(drillDownMap("enforceFilters").asInstanceOf[Boolean]).isSuccess) enforceFilters = Try(drillDownMap("enforceFilters").asInstanceOf[Boolean]).getOrElse(false)

    if(drillDownMap.contains("ordering") && Try(drillDownMap("ordering").asInstanceOf[List[Map[String, String]]]).isSuccess){
      val orderList = drillDownMap("ordering").asInstanceOf[List[Map[String, String]]]
      val sortByMap = orderList.head
      val curatedSortBy = new SortBy(sortByMap("field"), if (sortByMap("order").compareToIgnoreCase("asc") == 0) ASC else DESC)
      ordering = List(curatedSortBy).union(reportingRequest.sortBy)
    }else
      ordering = reportingRequest.sortBy.toList

    cube = reportingRequest.cube
  }
}
