// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.request

import com.yahoo.maha.core.Engine
import enumeratum.EnumEntry
import enumeratum.Enum
import enumeratum.EnumEntry.{Snakecase, Uppercase}
import org.json4s.scalaz.JsonScalaz
import scalaz.syntax.applicative._
import _root_.scalaz.{Success, Validation, syntax}
import org.json4s.{JValue, _}
import org.json4s.scalaz.JsonScalaz._
import Validation.FlatMap._
import grizzled.slf4j.Logging
import org.json4s.JsonAST.JObject

/**
 * Created by jians on 10/5/15.
 */
case class Field(field: String, alias: Option[String], value: Option[String])
case class CuratorJsonConfig(json: JValue)

object CuratorJsonConfig {
  import org.json4s.scalaz.JsonScalaz._

  implicit def parse: JSONR[CuratorJsonConfig] = new JSONR[CuratorJsonConfig] {
    override def read(json: JValue): Result[CuratorJsonConfig] = {
      for {
        config <- fieldExtended[JValue]("config")(json)
      } yield {
        CuratorJsonConfig(config)
      }
    }
  }
}

case class PaginationConfig(config: Map[Engine, JValue])
object PaginationConfig {
  import org.json4s.scalaz.JsonScalaz._
  import syntax.validation._

  implicit def parse: JSONR[PaginationConfig] = new JSONR[PaginationConfig] {
    override def read(json: JValue): Result[PaginationConfig] = {
      if (json.isInstanceOf[JObject]) {
        val parsed: Map[Engine, JValue] = Engine.enginesMap.map {
          case (engineString, engine) => engine -> fieldExtended[JValue](engineString)(json)
        }.collect {
          case (engine, jvalueOrError) if jvalueOrError.isSuccess => engine -> jvalueOrError.toOption.get
        }
        PaginationConfig(parsed).successNel
      } else {
        UnexpectedJSONError(json, classOf[JObject]).asInstanceOf[JsonScalaz.Error].failureNel
      }
    }
  }
}

sealed trait Order
case object ASC extends Order { override def toString = "ASC" }
case object DESC extends Order { override def toString = "DESC" }

case class SortBy(field: String, order: Order)

object Field {
  implicit def fieldJSONW : JSONW[Field] = new JSONW[Field] {
    override def write(field: Field): JValue =
        makeObj(
          ("field" -> toJSON(field.field))
          :: ("alias" -> toJSON(field.alias))
          :: ("value" -> toJSON(field.value))
          :: Nil)
  }

  implicit def fieldJSONR: JSONR[Field] = new JSONR[Field] {
    override def read(json: JValue): JsonScalaz.Result[Field] = {
      val fieldResult = fieldExtended[String]("field")(json)
      val aliasOption = if(fieldExtended[String]("alias")(json).isSuccess) fieldExtended[String]("alias")(json).map(_.replaceAll("\"", "'")) else fieldExtended[String]("alias")(json)
      (fieldResult orElse aliasOption) flatMap { fieldName =>
          val valueOption = fieldExtended[String]("value")(json).toOption
          Success(new Field(fieldName, aliasOption.toOption, valueOption))
      }
    }
  }
}

object SortBy {
  import syntax.validation._

  implicit def sortByJSONW : JSONW[SortBy] = new JSONW[SortBy] {
    override def write(sortBy: SortBy): JValue =
        makeObj(
          ("field" -> toJSON(sortBy.field))
          :: ("order" -> toJSON(sortBy.order.toString))
          :: Nil)
  }

  implicit def sortByJSONR: JSONR[SortBy] = new JSONR[SortBy] {
    override def read(json: JValue): JsonScalaz.Result[SortBy] = {
      val orderResult: JsonScalaz.Result[Order] = fieldExtended[String]("order")(json).flatMap { order =>
        order.toLowerCase match {
          case "asc" => ASC.successNel
          case "desc" => DESC.successNel
          case unknown => Fail.apply("order", s"order must be asc|desc not $unknown")
        }
      }
      (fieldExtended[String]("field")(json) |@| orderResult) {
        (fieldName: String, order: Order) => {
          new SortBy(fieldName, order)
        }
      }
    }
  }
}


sealed trait ParameterValue[T] {
  def value: T
}

sealed abstract class ReportFormatType(override val entryName: String) extends EnumEntry with Snakecase with Uppercase
object ReportFormatType extends Enum[ReportFormatType] {
  val values = findValues
  case object CSVFormat extends ReportFormatType("csv")
  case object JsonFormat extends ReportFormatType("json")
  case object ExcelFormat extends ReportFormatType("excel")
}

case class ReportFormatValue(value: ReportFormatType) extends ParameterValue[ReportFormatType]
case class DryRunValue(value: Boolean) extends ParameterValue[Boolean]
case class GeneratedQueryValue(value: String) extends ParameterValue[String]
case class QueryEngineValue(value: Engine) extends ParameterValue[Engine]
case class DebugValue(value: Boolean) extends ParameterValue[Boolean]
case class TestNameValue(value: String) extends ParameterValue[String]
case class LabelsValue(value: List[String]) extends ParameterValue[List[String]]
case class RequestIdValue(value: String) extends ParameterValue[String]
case class UserIdValue(value: String) extends ParameterValue[String]
case class TimeZoneValue(value: String) extends ParameterValue[String]
case class SchemaValue(value: String) extends ParameterValue[String]
case class DistinctValue(value: Boolean) extends ParameterValue[Boolean]
case class JobNameValue(value: String) extends ParameterValue[String]
case class RegistryNameValue(value: String) extends ParameterValue[String]
case class HostNameValue(value: String) extends ParameterValue[String]
case class AllowPushDownNameValue(value: String) extends ParameterValue[String]
case class AdditionalColumnInfoValue(value: List[Field]) extends ParameterValue[List[Field]]

sealed abstract class Parameter(override val entryName: String) extends EnumEntry with Snakecase with Uppercase

object Parameter extends Enum[Parameter] with Logging {
  val values = findValues

  case object ReportFormat extends Parameter("Report-Format")
  case object DryRun extends Parameter("Dry-Run")
  case object GeneratedQuery extends Parameter("Generated-Query")
  case object QueryEngine extends Parameter("Query-Engine")
  case object Debug extends Parameter("debug")
  case object TestName extends Parameter("TestName")
  case object Labels extends Parameter("Labels")
  case object RequestId extends Parameter("Request-Id")
  case object UserId extends Parameter("User-Id")
  case object TimeZone extends Parameter("TimeZone")
  case object Schema extends Parameter("Schema")
  case object Distinct extends Parameter("Distinct")
  case object JobName extends Parameter("Job-Name")
  case object RegistryName extends Parameter("RegistryName")
  case object HostName extends Parameter("HostName")
  case object AllowPushDownName extends Parameter("AllowPushDown")
  case object AdditionalColumnInfo extends Parameter("AdditionalColumnInfo")

  import syntax.validation._
  def deserializeParameters(json: JValue) : JsonScalaz.Result[Map[Parameter, ParameterValue[_]]] = {
    import _root_.scalaz.Scalaz._
    json match {
      case JNothing => Map.empty[Parameter, ParameterValue[_]].successNel
      case JObject(paramsList) =>
        paramsList.map {
          case (name, _) => deserializeParameter(name, json)
        }.flatten.sequence.map(_.toMap)
      case _ => UnexpectedJSONError(json, classOf[JObject]).asInstanceOf[JsonScalaz.Error].failureNel
    }
  }
  
  def deserializeParameter(name: String, json: JValue) : Option[JsonScalaz.Result[(Parameter, ParameterValue[_])]] = {
    withNameInsensitiveOption(name) match {
      case None =>
        warn(s"Found unrecognized param $name")
        None
      case Some(p) =>
        val result = p match {
          case ReportFormat => fieldExtended[String](name)(json).map(ct => p -> ReportFormatValue(ReportFormatType.withNameInsensitive(ct)))
          case DryRun => fieldExtended[Boolean](name)(json).map(ct => p -> DryRunValue(ct))
          case GeneratedQuery => fieldExtended[String](name)(json).map(ct => p -> GeneratedQueryValue(ct))
          case QueryEngine => fieldExtended[String](name)(json).flatMap {
            engine =>
              Engine.from(engine).fold(
                JsonScalaz.UncategorizedError(name, s"Unknown engine : $engine", List.empty).asInstanceOf[JsonScalaz.Error].failureNel[(Parameter, ParameterValue[_])])(
                e => (p, QueryEngineValue(e)).successNel
              )
          }
          case Debug => fieldExtended[Boolean](name)(json).map(d => p -> DebugValue(d))
          case TestName => fieldExtended[String](name)(json).map(d => p -> TestNameValue(d))
          case Labels => fieldExtended[List[String]](name)(json).map(ct => p -> LabelsValue(ct))
          case RequestId => fieldExtended[String](name)(json).map(ct => p -> RequestIdValue(ct))
          case UserId => fieldExtended[String](name)(json).map(ct => p -> UserIdValue(ct))
          case TimeZone => fieldExtended[String](name)(json).map(ct => p -> TimeZoneValue(ct))
          case Schema => fieldExtended[String](name)(json).map(ct => p -> SchemaValue(ct))
          case Distinct => fieldExtended[Boolean](name)(json).map(d => p -> DistinctValue(d))
          case JobName => fieldExtended[String](name)(json).map(ct => p -> JobNameValue(ct))
          case RegistryName => fieldExtended[String](name)(json).map(ct => p -> RegistryNameValue(ct))
          case HostName => fieldExtended[String](name)(json).map(ct => p -> HostNameValue(ct))
          case AllowPushDownName => fieldExtended[String](name)(json).map(ct => p -> AllowPushDownNameValue(ct))
          case AdditionalColumnInfo => fieldExtended[List[Field]](name)(json).map(ct => p -> AdditionalColumnInfoValue(ct))
        }
        Option(result)
    }
  }
  
  def serializeParameters(params: Map[Parameter, ParameterValue[_]]) : List[(String, JValue)] = {
    params.toList.map { 
      case (p, v) => serializeParameter(p, v)
    }
  }
  
  def serializeParameter(p: Parameter, v: ParameterValue[_]) : (String, JValue) = {
    p match {
      case ReportFormat => p.entryName -> JString(v.asInstanceOf[ReportFormatValue].value.entryName)
      case DryRun => p.entryName -> JBool(v.asInstanceOf[DryRunValue].value)
      case GeneratedQuery => p.entryName -> JString(v.asInstanceOf[GeneratedQueryValue].value)
      case QueryEngine => p.entryName -> JString(v.asInstanceOf[QueryEngineValue].value.toString)
      case Debug => p.entryName -> JBool(v.asInstanceOf[DebugValue].value)
      case TestName => p.entryName -> JString(v.asInstanceOf[TestNameValue].value)
      case Labels => p.entryName -> JArray(v.asInstanceOf[LabelsValue].value.map(JString(_)))
      case RequestId => p.entryName -> JString(v.asInstanceOf[RequestIdValue].value.toString)
      case UserId => p.entryName -> JString(v.asInstanceOf[UserIdValue].value.toString)
      case TimeZone => p.entryName -> JString(v.asInstanceOf[TimeZoneValue].value.toString)
      case Schema => p.entryName -> JString(v.asInstanceOf[SchemaValue].value.toString)
      case Distinct => p.entryName -> JBool(v.asInstanceOf[DistinctValue].value)
      case JobName => p.entryName -> JString(v.asInstanceOf[JobNameValue].value)
      case RegistryName => p.entryName -> JString(v.asInstanceOf[RegistryNameValue].value)
      case HostName => p.entryName -> JString(v.asInstanceOf[HostNameValue].value)
      case AllowPushDownName => p.entryName -> JString(v.asInstanceOf[AllowPushDownNameValue].value)
      case AdditionalColumnInfo => p.entryName -> {
        import org.json4s.JsonAST.JObject
        import org.json4s.JsonDSL._

        val jval = v.asInstanceOf[AdditionalColumnInfoValue].value.map(item => Field.fieldJSONW.write(item))
        val jResult: JObject = {("AdditionalColumnInfo" -> jval)}
        jResult
      }
    }
    
  }
  
}
