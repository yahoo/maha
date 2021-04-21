package com.yahoo.maha.service.calcite

import com.yahoo.maha.core._
import com.yahoo.maha.core.fact.PublicFact
import com.yahoo.maha.core.registry.Registry
import com.yahoo.maha.core.request.ReportingRequest.DEFAULT_DAY_FILTER
import com.yahoo.maha.core.request.{Field, GroupByQuery, PaginationConfig, QueryType, ReportingRequest, SelectQuery, SyncRequest}
import com.yahoo.maha.service.MahaServiceConfig
import com.yahoo.maha.service.error.MahaCalciteSqlParserError
import grizzled.slf4j.Logging
import org.apache.calcite.sql._
import org.apache.calcite.sql.parser.{SqlParseException, SqlParser}
import org.apache.commons.lang.StringUtils
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.mutable.ArrayBuffer

trait MahaCalciteSqlParser {
  def parse(sql:String, schema: Schema, registryName:String) : ReportingRequest
}

case class DefaultMahaCalciteSqlParser(mahaServiceConfig: MahaServiceConfig) extends MahaCalciteSqlParser with Logging {

  lazy protected[this] val fromDate : String = DailyGrain.toFormattedString(DateTime.now(DateTimeZone.UTC).minusDays(7))
  lazy protected[this] val toDate : String = DailyGrain.toFormattedString(DateTime.now(DateTimeZone.UTC))

  val DEFAULT_DAY_FILTER : Filter = BetweenFilter("Day", fromDate, toDate)
  val DAY = "Day"

  override def parse(sql: String, schema: Schema, registryName:String): ReportingRequest = {
    require(!StringUtils.isEmpty(sql), MahaCalciteSqlParserError("Sql can not be empty", sql))
    require(mahaServiceConfig.registry.contains(registryName), s"failed to find the registry ${registryName} in the mahaService Config")
    val registry:Registry  = mahaServiceConfig.registry.get(registryName).get.registry
    val parser: SqlParser = SqlParser.create(sql)
    try {
      val sqlNode: SqlNode = parser.parseQuery
      //validate validate AST
      //optimize convert AST to RequestModel and/or ReportingRequest
      //execute X
      sqlNode match {
        case sqlSelect: SqlSelect =>
          val publicFact = getCube(sqlSelect.getFrom, registry)
          require(publicFact.isDefined,MahaCalciteSqlParserError(s"Failed to find the cube ${sqlSelect.getFrom}", sql))
          val selectFields = getSelectList(sqlSelect.getSelectList, publicFact.get)
          val (filterExpression, dayFilterOption, hourFilterOption, minuteFilterOption, numDays) = getFilterList(sqlSelect.getWhere, publicFact.get)
          //determine if groupby query or select query
          val queryType: QueryType = sqlSelect.getGroup match {
            case null => SelectQuery
            case _ => GroupByQuery
          }
//          val filterExpression = getFilterList(sqlSelect.getWhere, publicFact.get)
//          val nonDayFilterExpressions = filterExpression.filter(f=> !DAY.equalsIgnoreCase(f.field))
//          val dayFilter = filterExpression.filter(f=> DAY.equalsIgnoreCase(f.field)).headOption.getOrElse(DEFAULT_DAY_FILTER)
          return new ReportingRequest(
            cube=publicFact.get.name,
            selectFields = selectFields,
            filterExpressions = filterExpression,
            requestType = SyncRequest,
            schema = schema,
            reportDisplayName = None,
            forceDimensionDriven = false,
            forceFactDriven = false,
            includeRowCount = false,
            dayFilter = dayFilterOption.getOrElse(DEFAULT_DAY_FILTER),
            //dayFilter = dayFilter,
            hourFilter = hourFilterOption,
            minuteFilter = minuteFilterOption,
            numDays = numDays,
            curatorJsonConfigMap = Map.empty,
            additionalParameters = Map.empty,
            queryType = queryType,
            pagination = PaginationConfig(Map.empty)
          )
        case e=>
      }
    }
    catch {
      case e: SqlParseException =>
        val error = s"Failed to parse SQL ${sql} ${e.getMessage}"
        logger.error(error, e)
        Left(MahaCalciteSqlParserError(error, sql))
    }
    null
  }

  def getCube(sqlNode: SqlNode, registry:Registry) : Option[PublicFact] = {
    sqlNode match {
      case sqlIdentifier: SqlIdentifier =>
        registry.getFact(sqlIdentifier.getSimple.toLowerCase)
      case e=>
        throw new IllegalArgumentException(s"missing case ${e} in get cube method")
    }
  }

  def getSelectList(sqlNode: SqlNode, publicFact: PublicFact) : IndexedSeq[Field] = {
    sqlNode match {
      case sqlNodeList: SqlNodeList =>
        if (sqlNodeList.size() > 0) {
          var arrayBuffer: ArrayBuffer[Field] = ArrayBuffer.empty
          val iter = sqlNodeList.iterator()
          while (iter.hasNext) {
            val subNode = iter.next();
            subNode match {
              case sqlIdentifier: SqlIdentifier =>
                if (sqlIdentifier.isStar) {
                  val indexedSeq: IndexedSeq[Field]=
                    publicFact.factCols.map(publicFactCol => Field(publicFactCol.alias, None, None)).toIndexedSeq ++
                      publicFact.dimCols.map(publicDimCol => Field(publicDimCol.alias, None, None)).toIndexedSeq
                  return indexedSeq
                } else {
                  val errMsg = s"SqlIdentifier type ${sqlIdentifier.getKind.toString} in getSelectList is not yet supported"
                  logger.error(errMsg)
                }
              case sqlCharStringLiteral: SqlCharStringLiteral =>
                val publicCol: PublicColumn = publicFact.columnsByAliasMap(toLiteral(sqlCharStringLiteral))
                arrayBuffer += Field(publicCol.name, Option(publicCol.alias), None)
              case sqlBasicCall: SqlBasicCall =>
                val publicCol: PublicColumn = publicFact.columnsByAliasMap(toLiteral(sqlBasicCall.operands(0)))
                arrayBuffer += Field(publicCol.name, Option(publicCol.alias), None)
              case other: AnyRef =>
                val errMsg = s"sqlNode type${other.getClass.toString} in getSelectList is not yet supported"
                logger.error(errMsg);
            }
          }
          arrayBuffer.toIndexedSeq
        } else {
          IndexedSeq.empty
        }
      case e=>
        IndexedSeq.empty
    }
  }

  def getFilterList(sqlNode: SqlNode, publicFact: PublicFact) : (IndexedSeq[Filter], Option[Filter], Option[Filter], Option[Filter], Int) = {
    sqlNode match {
      case sqlBasicCall: SqlBasicCall =>
        val filterListOption = recursiveGetFilterList(sqlBasicCall, publicFact)
        return validate(filterListOption.get)
      case others: AnyRef =>
        logger.error(s"sqlNode type ${sqlNode.getKind} in getSelectList is not yet supported")
        (IndexedSeq.empty, None, None, None, 1)
      case null =>
        logger.error("sqlNode is null in getFilterList")
        (IndexedSeq.empty, None, None, None, 1)
    }
  }

  def recursiveGetFilterList(sqlNode: SqlNode, publicFact: PublicFact): Option[ArrayBuffer[Filter]] = {
    sqlNode match {
      case sqlBasicCall: SqlBasicCall =>
        sqlBasicCall.getOperator.kind match {
          case SqlKind.AND =>
            val filterOption: Option[ArrayBuffer[Filter]] = recursiveGetFilterList(sqlBasicCall.operands(0), publicFact)
            if(filterOption.isDefined) {
              Some(filterOption.get ++ constructFilterList(sqlBasicCall.operands(1)))
            } else {
              Some(constructFilterList(sqlBasicCall.operands(1)))
            }
          case SqlKind.BETWEEN =>
            None
          case SqlKind.OR =>
            None

          case _ => //only one filter left
            Some(constructFilterList(sqlBasicCall))
        }
      case other: AnyRef =>
        None
    }
  }

  def constructFilterList(sqlNode: SqlNode): ArrayBuffer[Filter] = {
    require(sqlNode.isInstanceOf[SqlBasicCall], s"type ${sqlNode.getKind} not supported in construct current filter")
    val sqlBasicCall: SqlBasicCall = sqlNode.asInstanceOf[SqlBasicCall]
    val operands = sqlBasicCall.getOperands
    sqlBasicCall.getOperator.kind match {
      case SqlKind.EQUALS =>
        ArrayBuffer.empty += EqualityFilter(toLiteral(operands(0)), toLiteral(operands(1))).asInstanceOf[Filter]
      case SqlKind.GREATER_THAN =>
        ArrayBuffer.empty += GreaterThanFilter(toLiteral(operands(0)), toLiteral(operands(1))).asInstanceOf[Filter]
      case SqlKind.IN =>
        val inList: List[String] = operands(1).asInstanceOf[SqlNodeList].toArray.toList.map(sqlNode => toLiteral(sqlNode))
        ArrayBuffer.empty += InFilter(toLiteral(operands(0)), inList).asInstanceOf[Filter]
    }
  }

//  toLiteral
  def toLiteral(sqlNode: SqlNode): String = {
    if(sqlNode != null)
    sqlNode.toString.replaceAll("^[\"']+|[\"']+$", "")
    else ""
  }

  def validate(arrayBuffer: ArrayBuffer[Filter]): (IndexedSeq[Filter], Option[Filter], Option[Filter], Option[Filter], Int)= {
    val attributeAndMetricFilters = ArrayBuffer.empty[Filter]
    var dayFilter: Option[Filter] = None
    var hourFilter: Option[Filter] = None
    var minuteFilter: Option[Filter] = None
    var numDays = 1

    arrayBuffer.foreach {
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

    if (dayFilter.isEmpty)
      dayFilter = Option(DEFAULT_DAY_FILTER)

    //validate day filter
    require(dayFilter.isDefined, "Day filter not found in list of filters!")
    dayFilter.map(DailyGrain.validateFilterAndGetNumDays).foreach(nd => numDays = nd)

    (attributeAndMetricFilters.toIndexedSeq, dayFilter, hourFilter, minuteFilter, numDays)
  }
}
