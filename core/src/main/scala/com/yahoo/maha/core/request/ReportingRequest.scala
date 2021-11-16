// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.request

import java.nio.charset.StandardCharsets

import com.yahoo.maha.core._
import com.yahoo.maha.core.request.ReportFormatType.{CSVFormat, ExcelFormat, JsonFormat}
import org.joda.time.DateTime
import org.json4s.jackson.JsonMethods._
import org.json4s.scalaz.JsonScalaz
import _root_.scalaz._

import scala.collection.mutable.ArrayBuffer
import org.json4s._
import org.json4s.scalaz.JsonScalaz._

import scala.util.Try

/**
 * Created by jians on 10/5/15.
 */

sealed trait Bias
case object FactBias extends Bias
case object DimBias extends Bias

sealed trait RequestType
case object SyncRequest extends RequestType
case object AsyncRequest extends RequestType

sealed trait QueryType {
  def stringValue: String
}
case object GroupByQuery extends QueryType {
  val stringValue: String = "groupby"
}

case object RowCountQuery extends QueryType {
  val stringValue: String = "rowcount"
}

@Deprecated
case object SelectQuery extends QueryType {
  val stringValue: String = "select"
}

case object ScanQuery extends QueryType {
  val stringValue: String = "scan"
}

case class RequestContext(requestId: String, userId: String)

case class ReportingRequest(cube: String
                            , reportDisplayName: Option[String]
                            , schema: Schema
                            , requestType: RequestType
                            , forceDimensionDriven: Boolean
                            , forceFactDriven: Boolean
                            , includeRowCount: Boolean
                            , dayFilter: Filter
                            , hourFilter: Option[Filter]
                            , minuteFilter: Option[Filter]
                            , numDays: Int
                            , selectFields: IndexedSeq[Field] = IndexedSeq.empty
                            , filterExpressions: IndexedSeq[Filter] = IndexedSeq.empty
                            , sortBy: IndexedSeq[SortBy] = IndexedSeq.empty
                            , paginationStartIndex: Int = 0
                            , rowsPerPage: Int = -1
                            , additionalParameters: Map[Parameter, ParameterValue[_]]
                            , curatorJsonConfigMap: Map[String, CuratorJsonConfig]
                            , queryType: QueryType
                            , pagination: PaginationConfig
                           ) {
  def isDebugEnabled : Boolean = {
    additionalParameters.contains(Parameter.Debug) && additionalParameters(Parameter.Debug).asInstanceOf[DebugValue].value
  }
  def isTestEnabled : Boolean = {
    additionalParameters.contains(Parameter.TestName)
  }
  def hasLabels: Boolean = {
    additionalParameters.contains(Parameter.Labels)
  }
  def getTestName: Option[String] = {
    if(isTestEnabled) {
      Option(additionalParameters(Parameter.TestName).asInstanceOf[TestNameValue].value)
    } else None
  }
  def getLabels: List[String] = {
    if(hasLabels) {
      additionalParameters(Parameter.Labels).asInstanceOf[LabelsValue].value
    } else List.empty
  }
  def getTimezone : Option[String] = {
    additionalParameters.get(Parameter.TimeZone) collect {
      case TimeZoneValue(value) => value
    }
  }
}

trait BaseRequest {
  import syntax.validation._

  val EMPTY_FILTER : JsonScalaz.Result[List[Filter]] = List.empty.successNel
  val EMPTY_SORTBY: JsonScalaz.Result[List[SortBy]] = List.empty.successNel
  val EMPTY_ADDITIONAL_PARAMETERS: JsonScalaz.Result[Map[Parameter, ParameterValue[_]]] = Map.empty[Parameter, ParameterValue[_]].successNel
  val DEFAULT_SI: JsonScalaz.Result[Int] = 0.successNel
  val DEFAULT_MR: JsonScalaz.Result[Int] = 200.successNel
  val DEFAULT_FORCE_DIM_DRIVEN: JsonScalaz.Result[Boolean] = false.successNel
  val DEFAULT_FORCE_FACT_DRIVEN: JsonScalaz.Result[Boolean] = false.successNel
  val DEFAULT_INCLUDE_ROW_COUNT: JsonScalaz.Result[Boolean] = false.successNel
  val SYNC_REQUEST: JsonScalaz.Result[RequestType] = SyncRequest.successNel
  val ASYNC_REQUEST: JsonScalaz.Result[RequestType] = AsyncRequest.successNel
  val DEFAULT_DAY_FILTER : Filter = EqualityFilter("Day", "2000-01-01")
  val NOOP_DAY_FILTER : JsonScalaz.Result[Filter] = DEFAULT_DAY_FILTER.successNel
  val NOOP_HOUR_FILTER : JsonScalaz.Result[Option[Filter]] = Option(EqualityFilter("Hour", "00")).successNel
  val NOOP_MINUTE_FILTER : JsonScalaz.Result[Option[Filter]] = Option(EqualityFilter("Minute", "00")).successNel
  val NOOP_NUM_DAYS : JsonScalaz.Result[Int] = 1.successNel
  val DEFAULT_DISPLAY_NAME : JsonScalaz.Result[Option[String]] = None.successNel
  val DEFAULT_CURATOR_JSON_CONFIG_MAP: JsonScalaz.Result[Map[String, CuratorJsonConfig]] = Map("default" -> CuratorJsonConfig(parse("""{}"""))).successNel
  val DEFAULT_PAGINATION_CONFIG: JsonScalaz.Result[PaginationConfig] = PaginationConfig(Map.empty).successNel
  val GROUPBY_QUERY: JsonScalaz.Result[QueryType] = GroupByQuery.successNel
  val SCAN_QUERY: JsonScalaz.Result[QueryType] = ScanQuery.successNel

  protected[this] val factBiasOption : Option[Bias] = Option(FactBias)

  def deserializeWithAdditionalParameters(ba: Array[Byte]
                                          , schema: Schema
                                          , bias: Option[Bias] = None): Validation[NonEmptyList[JsonScalaz.Error], ReportingRequest]
  def deserializeWithAdditionalParameters(ba: Array[Byte]
                                          , bias: Option[Bias]): Validation[NonEmptyList[JsonScalaz.Error], ReportingRequest]
  def deserializeWithAdditionalParamsDefaultDayFilter(ba: Array[Byte]
                                                      , schema: Schema
                                                      , bias: Option[Bias]
                                                      , defaultDayIfNeeded: Boolean): Validation[NonEmptyList[JsonScalaz.Error], ReportingRequest]
  def deserialize(ba: Array[Byte]
                  , requestTypeResult: JsonScalaz.Result[RequestType]
                  , schemaOption: Option[Schema]
                  , deserializeAdditionalParameters: Boolean
                  , bias: Option[Bias]
                  , defaultDayIfNeeded: Boolean = false): Validation[NonEmptyList[JsonScalaz.Error], ReportingRequest]

  protected[this] def validate(filtersResult: JsonScalaz.Result[List[Filter]], defaultDayIfNeeded: Boolean = false):
  (JsonScalaz.Result[IndexedSeq[Filter]], JsonScalaz.Result[Filter], JsonScalaz.Result[Option[Filter]], JsonScalaz.Result[Option[Filter]], JsonScalaz.Result[Int]) = {
    //validate grain
    val attributeAndMetricFilters = ArrayBuffer.empty[Filter]
    var dayFilter: Option[Filter] = None
    var hourFilter: Option[Filter] = None
    var minuteFilter: Option[Filter] = None
    var numDays = 1
    filtersResult.foreach { filters =>
      filters.foreach {
        filter =>
          if (filter.field == DailyGrain.DAY_FILTER_FIELD) {
            dayFilter = Option(filter)
          } else if (filter.field == HourlyGrain.HOUR_FILTER_FIELD) {
            hourFilter = Option(filter)
          } else if (filter.field == MinuteGrain.MINUTE_FILTER_FIELD) {
            minuteFilter = Option(filter)
          } else {
            attributeAndMetricFilters += filter
          }
      }
    }

    if (dayFilter.isEmpty && defaultDayIfNeeded)
      dayFilter = Option(DEFAULT_DAY_FILTER)

    if(filtersResult.isSuccess) {
      //validate day filter
      val dayFilterResult = Validation.fromTryCatchNonFatal {
        require(dayFilter.isDefined, "Day filter not found in list of filters!")
        dayFilter.map(DailyGrain.validateFilterAndGetNumDays).foreach(nd => numDays = nd)
        dayFilter.get
      }.leftMap[JsonScalaz.Error](t => UncategorizedError("Day", t.getMessage, List.empty)).toValidationNel

      //validate hour filter
      val hourFilterResult = Validation.fromTryCatchNonFatal {
        if (dayFilter.isDefined && hourFilter.isDefined) {
          hourFilter.foreach(HourlyGrain.validateFilter)
          val dayFilterOperator = dayFilter.get.operator
          val hourFilterOperator = hourFilter.get.operator
          require(dayFilterOperator == hourFilterOperator,
            s"Day and Hour filters operator mismatch, operator not the same : dayFilterOperator=$dayFilterOperator, hourFilterOperator=$hourFilterOperator")
        }
        hourFilter
      }.leftMap[JsonScalaz.Error](t => UncategorizedError("Hour", t.getMessage, List.empty)).toValidationNel

      //validate minute filter
      val minuteFilterResult = Validation.fromTryCatchNonFatal {
        if (dayFilter.isDefined && hourFilter.isDefined && minuteFilter.isDefined) {
          minuteFilter.foreach(MinuteGrain.validateFilter)
          val dayFilterOperator = dayFilter.get.operator
          val minuteFilterOperator = minuteFilter.get.operator
          require(dayFilterOperator == minuteFilterOperator,
            s"Day and Minute filters operator mismatch, operator not the same : dayFilterOperator=$dayFilterOperator, minuteFilterOperator=$minuteFilterOperator")
        }
        minuteFilter
      }.leftMap[JsonScalaz.Error](t => UncategorizedError("Minute", t.getMessage, List.empty)).toValidationNel


      //carry forward any errors otherwise use new list without day or hour filters
      val newFiltersResult = filtersResult.map { _ => attributeAndMetricFilters.toIndexedSeq }
      (newFiltersResult, dayFilterResult, hourFilterResult, minuteFilterResult, numDays.successNel)
    } else {
      (filtersResult.map(_.toIndexedSeq), NOOP_DAY_FILTER, NOOP_HOUR_FILTER, NOOP_MINUTE_FILTER, NOOP_NUM_DAYS)
    }
  }

  def serialize(request: ReportingRequest): Array[Byte]


  def enableDebug(reportingRequest: ReportingRequest) : ReportingRequest = {
    reportingRequest.copy(additionalParameters = reportingRequest.additionalParameters ++ Map(Parameter.Debug -> DebugValue(value = true)))
  }

  def withNewCubeVersion(reportingRequest: ReportingRequest, newCube: String) : ReportingRequest = {
    reportingRequest.copy(cube = newCube)
  }

  def forceOracle(reportingRequest: ReportingRequest) : ReportingRequest = {
    reportingRequest.copy(additionalParameters = reportingRequest.additionalParameters ++ Map(Parameter.QueryEngine -> QueryEngineValue(OracleEngine)))
  }

  def forcePostgres(reportingRequest: ReportingRequest) : ReportingRequest = {
    reportingRequest.copy(additionalParameters = reportingRequest.additionalParameters ++ Map(Parameter.QueryEngine -> QueryEngineValue(PostgresEngine)))
  }

  def forceDruid(reportingRequest: ReportingRequest) : ReportingRequest = {
    reportingRequest.copy(additionalParameters = reportingRequest.additionalParameters ++ Map(Parameter.QueryEngine -> QueryEngineValue(DruidEngine)))
  }

  def forceHive(reportingRequest: ReportingRequest) : ReportingRequest = {
    reportingRequest.copy(additionalParameters = reportingRequest.additionalParameters ++ Map(Parameter.QueryEngine -> QueryEngineValue(HiveEngine)))
  }

  def forcePresto(reportingRequest: ReportingRequest) : ReportingRequest = {
    reportingRequest.copy(additionalParameters = reportingRequest.additionalParameters ++ Map(Parameter.QueryEngine -> QueryEngineValue(PrestoEngine)))
  }

  def forceBigquery(reportingRequest: ReportingRequest): ReportingRequest = {
    reportingRequest.copy(additionalParameters = reportingRequest.additionalParameters ++ Map(Parameter.QueryEngine -> QueryEngineValue(BigqueryEngine)))
  }

  def withHostname(reportingRequest: ReportingRequest, hostName: String) : ReportingRequest = {
    reportingRequest.copy(additionalParameters = reportingRequest.additionalParameters ++ Map(Parameter.HostName -> HostNameValue(hostName)))
  }

  def addRequestContext(reportingRequest: ReportingRequest, requestContext: RequestContext) : ReportingRequest = {
    reportingRequest.copy(additionalParameters = reportingRequest.additionalParameters ++
      Map(
        Parameter.RequestId -> RequestIdValue(requestContext.requestId),
        Parameter.UserId -> UserIdValue(requestContext.userId)
      )
    )
  }

  def withTimeZone(reportingRequest: ReportingRequest, timeZone: String) : ReportingRequest = {
    reportingRequest.copy(additionalParameters = reportingRequest.additionalParameters ++ Map(Parameter.TimeZone -> TimeZoneValue(timeZone)))
  }

  def withJsonReportFormat(reportingRequest: ReportingRequest) : ReportingRequest = {
    reportingRequest.copy(additionalParameters = reportingRequest.additionalParameters ++ Map(Parameter.ReportFormat -> ReportFormatValue(JsonFormat)))
  }

  def withCSVReportFormat(reportingRequest: ReportingRequest) : ReportingRequest = {
    reportingRequest.copy(additionalParameters = reportingRequest.additionalParameters ++ Map(Parameter.ReportFormat -> ReportFormatValue(CSVFormat)))
  }

  def withExcelReportFormat(reportingRequest: ReportingRequest) : ReportingRequest = {
    reportingRequest.copy(additionalParameters = reportingRequest.additionalParameters ++ Map(Parameter.ReportFormat -> ReportFormatValue(ExcelFormat)))
  }

  def withLabels(reportingRequest: ReportingRequest, labels: List[String]) : ReportingRequest = {
    reportingRequest.copy(additionalParameters = reportingRequest.additionalParameters ++ Map(Parameter.Labels -> LabelsValue(labels)))
  }

  def withTestName(reportingRequest: ReportingRequest, testName: String) : ReportingRequest = {
    reportingRequest.copy(additionalParameters = reportingRequest.additionalParameters ++ Map(Parameter.TestName -> TestNameValue(testName)))
  }

  def deserializeSyncWithFactBias(ba: Array[Byte]): Validation[NonEmptyList[JsonScalaz.Error], ReportingRequest] = {
    deserialize(ba, SYNC_REQUEST, None, deserializeAdditionalParameters =  false, factBiasOption)
  }

  def deserializeSyncWithFactBias(ba: Array[Byte]
                                  , schema: Schema): Validation[NonEmptyList[JsonScalaz.Error], ReportingRequest] = {
    deserialize(ba, SYNC_REQUEST, Option(schema), deserializeAdditionalParameters =  false, factBiasOption)
  }

  def deserializeSync(ba: Array[Byte]
                      , schema: Schema
                      , bias: Option[Bias] = None): Validation[NonEmptyList[JsonScalaz.Error], ReportingRequest] = {
    deserialize(ba, SYNC_REQUEST, Option(schema), deserializeAdditionalParameters =  false, bias)
  }

  def deserializeSync(ba: Array[Byte]
                      , bias: Option[Bias]): Validation[NonEmptyList[JsonScalaz.Error], ReportingRequest] = {
    deserialize(ba, SYNC_REQUEST, None, deserializeAdditionalParameters =  false, bias)
  }

  def deserializeAsync(ba: Array[Byte]
                       , bias: Option[Bias]): Validation[NonEmptyList[JsonScalaz.Error], ReportingRequest] = {
    deserialize(ba, ASYNC_REQUEST, None, deserializeAdditionalParameters = false, bias)
  }

  def deserializeAsync(ba: Array[Byte]
                       , schema: Schema
                       , bias: Option[Bias] = None): Validation[NonEmptyList[JsonScalaz.Error], ReportingRequest] = {
    deserialize(ba, ASYNC_REQUEST, Option(schema), deserializeAdditionalParameters = false, bias)
  }

  def getDefaultDayFilter(): Filter = {
    DEFAULT_DAY_FILTER
  }

  // ******** Need to Move this to ReportingRequest : Pranav
  // Adding this method to be used by schedular and reporting workers
  // need to deprecate Utils class from reporting workers
  def getDayRange(reportingRequest: ReportingRequest): (DateTime, DateTime) = {
    val dayFilter = reportingRequest.dayFilter
    dayFilter match {
      case EqualityFilter(_,day, _, _) =>
        val date = DailyGrain.fromFormattedString(day)
        (date, date)
      case BetweenFilter(_,from,to) =>
        (DailyGrain.fromFormattedString(from), DailyGrain.fromFormattedString(to))
      case dtf: DateTimeBetweenFilter =>
        (DailyGrain.fromFormattedString(DailyGrain.toFormattedString(dtf.fromDateTime))
          , DailyGrain.fromFormattedString(DailyGrain.toFormattedString(dtf.toDateTime)))
      case a =>
        throw new IllegalArgumentException(s"Cannot handle $dayFilter while sending scheduled email")
    }
  }
}

object ReportingRequest extends BaseRequest {
  import syntax.validation._
  import syntax.applicative._
  import Validation.FlatMap._

  def deserializeWithAdditionalParameters(ba: Array[Byte]
                                          , schema: Schema
                                          , bias: Option[Bias] = None): Validation[NonEmptyList[JsonScalaz.Error], ReportingRequest] = {
    deserialize(ba, ASYNC_REQUEST, Option(schema), deserializeAdditionalParameters = true, bias)
  }

  def deserializeWithAdditionalParameters(ba: Array[Byte]
                                          , bias: Option[Bias]): Validation[NonEmptyList[JsonScalaz.Error], ReportingRequest] = {
    deserialize(ba, ASYNC_REQUEST, None, deserializeAdditionalParameters = true, bias)
  }

  def deserializeWithAdditionalParamsDefaultDayFilter(ba: Array[Byte]
                                                    , schema: Schema
                                                    , bias: Option[Bias]
                                                    , defaultDayIfNeeded: Boolean): Validation[NonEmptyList[JsonScalaz.Error], ReportingRequest] = {
    deserialize(ba, ASYNC_REQUEST, Option(schema), deserializeAdditionalParameters = true, bias, defaultDayIfNeeded)
  }

  def deserialize(ba: Array[Byte]
                  , requestTypeResult: JsonScalaz.Result[RequestType]
                  , schemaOption: Option[Schema]
                  , deserializeAdditionalParameters: Boolean
                  , bias: Option[Bias]
                  , defaultDayIfNeeded: Boolean = false): Validation[NonEmptyList[JsonScalaz.Error], ReportingRequest] = {

    val json : JValue = {
        Try(parse(new String(ba, StandardCharsets.UTF_8))) match {
        case t if t.isSuccess => t.get
        case t if t.isFailure => {
          return Failure(JsonScalaz.UncategorizedError("invalidInputJson", t.failed.get.getMessage, List.empty)).toValidationNel
        }
      }
    }

    val queryTypeResult: JsonScalaz.Result[QueryType] = {
      val qtResult = fieldExtended[Option[String]]("queryType")(json)
      qtResult.flatMap {
        optionalQueryType =>
          optionalQueryType.fold(GROUPBY_QUERY) {
            case GroupByQuery.stringValue =>
              GROUPBY_QUERY
            case ScanQuery.stringValue =>
              SCAN_QUERY
            case any =>
              UncategorizedError("queryType", s"unknown query type : $any", List.empty).asInstanceOf[JsonScalaz.Error].failureNel[QueryType]
          }
      }
    }

    val cubeResult: JsonScalaz.Result[String] = fieldExtended[String]("cube")(json)
    val schemaResult: JsonScalaz.Result[Schema] = schemaOption.fold[JsonScalaz.Result[Schema]]{
      val jsonResult = fieldExtended[String]("schema")(json)
      jsonResult.flatMap {
        schemaString =>
          val schemaOption = Schema.withNameInsensitiveOption(schemaString)
          schemaOption.fold[JsonScalaz.Result[Schema]](
            UncategorizedError("schema", s"Unrecognized schema : $schemaString", List.empty).asInstanceOf[JsonScalaz.Error].failureNel[Schema])(
              s => s.successNel[JsonScalaz.Error])
      }
    }{
      s =>
        s.successNel[JsonScalaz.Error]
    }
    val fieldsResult: JsonScalaz.Result[IndexedSeq[Field]] = fieldExtended[List[Field]]("selectFields")(json).map(_.toIndexedSeq)
    val (filtersResult, dayFilterResult, hourFilterResult, minuteFilterResult, numDaysResult) =
      validate(json.findField(_._1  == "filterExpressions").map(_ => fieldExtended[List[Filter]]("filterExpressions")(json)).fold(EMPTY_FILTER)(r => r), defaultDayIfNeeded)
    val orderingResult: JsonScalaz.Result[IndexedSeq[SortBy]] = json.findField(_._1  == "sortBy").map(_ => fieldExtended[List[SortBy]]("sortBy")(json)).fold(EMPTY_SORTBY)(r => r).map(_.toIndexedSeq)
    val siResult: JsonScalaz.Result[Int] = json.findField(_._1 == "paginationStartIndex").map(_ => fieldExtended[Int]("paginationStartIndex")(json)).fold(DEFAULT_SI)(r => r)
    val mrResult: JsonScalaz.Result[Int] = json.findField(_._1 == "rowsPerPage").map(_ => fieldExtended[Int]("rowsPerPage")(json)).fold(DEFAULT_MR)(r => r)
    val forceDimensionDrivenResult: JsonScalaz.Result[Boolean] =
      json
        .findField(_._1 == "forceDimensionDriven")
        .map(_ => fieldExtended[Boolean]("forceDimensionDriven")(json))
        .fold(DEFAULT_FORCE_DIM_DRIVEN)(r => r)
    val forceFactDrivenResult: JsonScalaz.Result[Boolean] =
      json.findField(_._1 == "forceFactDriven").map(_ => fieldExtended[Boolean]("forceFactDriven")(json)).fold {
        bias match {
          case Some(FactBias) =>
            forceDimensionDrivenResult.map(!_)
          case Some(DimBias) | None =>
            DEFAULT_FORCE_FACT_DRIVEN
        }
      }(r => r)
    val includeRowCountResult: JsonScalaz.Result[Boolean] =
      json.findField(_._1 == "includeRowCount").map(_ => fieldExtended[Boolean]("includeRowCount")(json)).fold(DEFAULT_INCLUDE_ROW_COUNT)(r => r)
    val displayNameResult: JsonScalaz.Result[Option[String]] =
      json.findField(_._1 == "reportDisplayName").map(_ => fieldExtended[Option[String]]("reportDisplayName")(json)).fold(DEFAULT_DISPLAY_NAME)(r => r)
    val additonalParamtersResult: JsonScalaz.Result[Map[Parameter, ParameterValue[_]]] = {
      if(deserializeAdditionalParameters) {
        Parameter.deserializeParameters(json \ "additionalParameters")
      } else {
        EMPTY_ADDITIONAL_PARAMETERS
      }
    }

    val curatorJsonConfigMap: Result[Map[String, CuratorJsonConfig]] =
      json
        .findField(_._1 == "curators")
        .map {
          _ =>
            val result = fieldExtended[Map[String, CuratorJsonConfig]]("curators")(json)
            result
        }
        .fold(DEFAULT_CURATOR_JSON_CONFIG_MAP)(r => r)


    val paginationResult: Result[PaginationConfig] =
      json
        .findField(_._1 == "pagination")
        .map {
          _ =>
            val result = fieldExtended[PaginationConfig]("pagination")(json)
            result
        }
        .fold(DEFAULT_PAGINATION_CONFIG)(r => r)

    for {
      queryType <- queryTypeResult
      cube <- cubeResult
      displayName <- displayNameResult
      schema <- schemaResult
      requestType <- requestTypeResult
      forceDimensionDriven <-  forceDimensionDrivenResult
      forceFactDriven <-  forceFactDrivenResult
      includeRowCount <- includeRowCountResult
      dayFilter <- dayFilterResult
      hourFilter <- hourFilterResult
      minuteFilter <- minuteFilterResult
      numDays <-  numDaysResult
      fields <- fieldsResult
      filters <- filtersResult
      ordering <- orderingResult
      si <- siResult
      mr <-  mrResult
      additionalParameters <- additonalParamtersResult
      bothForceDimAndFactCannotBeTrue <- checkBothForceDimensionAndFactCase(forceDimensionDriven = forceDimensionDriven, forceFactDriven = forceFactDriven)
      curatorJsonConfigMap <- curatorJsonConfigMap
      pagination <- paginationResult
    } yield {
      ReportingRequest(
        cube=cube
        , reportDisplayName = displayName
        , schema = schema
        , requestType = requestType
        , forceDimensionDriven = forceDimensionDriven
        , forceFactDriven = forceFactDriven
        , includeRowCount = includeRowCount
        , dayFilter = dayFilter
        , hourFilter = hourFilter
        , minuteFilter = minuteFilter
        , numDays = numDays
        , selectFields = fields
        , filterExpressions = filters
        , sortBy = ordering
        , paginationStartIndex = si
        , rowsPerPage = mr
        , additionalParameters = additionalParameters
        , curatorJsonConfigMap = curatorJsonConfigMap
        , queryType = queryType
        , pagination = pagination
      )
    }
  }

  private[this] def checkBothForceDimensionAndFactCase(forceFactDriven: Boolean, forceDimensionDriven: Boolean) : JsonScalaz.Result[Boolean] = {
    val result: JsonScalaz.Result[Boolean] =
    Validation.fromTryCatchNonFatal[Boolean] {
      require(!(forceFactDriven && forceDimensionDriven), "both cannot be true : forceDimensionDriven, forceFactDriven")
      true
    }.leftMap[JsonScalaz.Error](t => UncategorizedError("forceFactDriven", t.getMessage, List.empty)).toValidationNel
    result
  }

  def serialize(request: ReportingRequest): Array[Byte] = {
    val additionalParams : List[(String, JValue)] = {
      if(request.additionalParameters.nonEmpty) {
        ("additionalParameters" -> makeObj(Parameter.serializeParameters(request.additionalParameters))) :: Nil
      } else {
        Nil
      }
    }
    val json = makeObj(
      ("queryType" -> toJSON(request.queryType.stringValue))
      :: ("cube" -> toJSON(request.cube))
      :: ("reportDisplayName" -> toJSON(request.reportDisplayName))
      :: ("schema" -> toJSON(request.schema.entryName))
      :: ("requestType" -> toJSON(request.requestType.toString))
      :: ("forceDimensionDriven" -> toJSON(request.forceDimensionDriven))
      :: ("selectFields" -> toJSON(request.selectFields.toList))
      :: ("filterExpressions" -> toJSON((request.filterExpressions ++ IndexedSeq(request.dayFilter) ++ request.hourFilter.map(f => IndexedSeq(f)).getOrElse(IndexedSeq.empty[Filter]) ).toList))
      :: ("sortBy" -> toJSON(request.sortBy.toList))
      :: ("paginationStartIndex" -> toJSON(request.paginationStartIndex))
      :: ("rowsPerPage" -> toJSON(request.rowsPerPage))
      :: ("includeRowCount" -> toJSON(request.includeRowCount))
      :: additionalParams
    )
    compact(render(json)).getBytes(StandardCharsets.UTF_8)
  }
}
