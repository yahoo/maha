package com.yahoo.maha.service.calcite

import com.yahoo.maha.core._
import com.yahoo.maha.core.fact.PublicFact
import com.yahoo.maha.core.registry.Registry
import com.yahoo.maha.core.request.{ASC, DESC, Field, GroupByQuery, Order, PaginationConfig, ReportingRequest, SortBy, SyncRequest}
import com.yahoo.maha.service.MahaServiceConfig
import com.yahoo.maha.service.error.MahaCalciteSqlParserError
import grizzled.slf4j.Logging
import org.apache.calcite.sql._
import org.apache.calcite.sql.fun.SqlLikeOperator
import org.apache.calcite.sql.parser.{SqlParseException, SqlParser}
import org.apache.calcite.sql.validate.{SqlConformance, SqlConformanceEnum}
import org.apache.commons.lang.StringUtils
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat,DateTimeFormatter,DateTimeFormatterBuilder,DateTimeParser}

import scala.collection.mutable.ArrayBuffer

trait MahaCalciteSqlParser {
  def parse(sql:String, schema: Schema, registryName:String) : MahaSqlNode
}

case class DefaultMahaCalciteSqlParser(mahaServiceConfig: MahaServiceConfig) extends MahaCalciteSqlParser with Logging {

  lazy protected[this] val defaultFromDate : String = DailyGrain.toFormattedString(DateTime.now(DateTimeZone.UTC).minusDays(7))
  lazy protected[this] val defaultToDate : String = DailyGrain.toFormattedString(DateTime.now(DateTimeZone.UTC))
  val parsers:Array[DateTimeParser] = Array(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").getParser(),
    DateTimeFormat.forPattern("yyyy-MM-dd HH").getParser(),
    DateTimeFormat.forPattern("yyyy-MM-dd").getParser()
  )
  val dateTimeFormatter = new DateTimeFormatterBuilder().append(null,parsers).toFormatter()

  val DEFAULT_DAY_FILTER : Filter = BetweenFilter("Day", defaultFromDate, defaultToDate)
  val DAY = "Day"
  val config = SqlParser.config().withConformance(SqlConformanceEnum.LENIENT)
  import scala.collection.JavaConverters._

  override def parse(sql: String, schema: Schema, registryName:String): MahaSqlNode = {
    val mahaSqlRequestContext = MahaSqlRequestContext()
    require(!StringUtils.isEmpty(sql), MahaCalciteSqlParserError("Sql can not be empty", sql))
    require(mahaServiceConfig.registry.contains(registryName), s"failed to find the registry ${registryName} in the mahaService Config")
    val registry:Registry  = mahaServiceConfig.registry.get(registryName).get.registry
    val parser: SqlParser = SqlParser.create(sql, config)
    try {
      val topSqlNode: SqlNode = parser.parseQuery
      val sqlNode:SqlNode = topSqlNode match {
         case sqlSelect: SqlSelect=> sqlSelect
         case sqlOrderBy: SqlOrderBy=> sqlOrderBy.query
         case sqlDescribeTable: SqlDescribeTable => sqlDescribeTable
         case e=>
           throw new IllegalArgumentException(s"Query type ${e.getKind} is not supported by Maha-Calcite")
      }

      val (startIndex, maxRow) = topSqlNode match {
        case sqlOrderBy: SqlOrderBy =>
          val si =  sqlOrderBy.offset match {
            case sqlNumericLiteral: SqlNumericLiteral => sqlNumericLiteral.toValue.toInt
            case null => 0
            case e =>
            warn(s"Offset type ${e.getKind} is not supported for si by Maha-Calcite, setting to 0")
            0
          }
          val mr = sqlOrderBy.fetch match {
            case sqlNumericLiteral: SqlNumericLiteral => sqlNumericLiteral.toValue.toInt
            case null => -1
            case e =>
            warn(s"Offset type ${e.getKind} is not supported for mr by Maha-Calcite, setting to -1")
            -1
          }
          (si, mr)
        case e=>
          warn(s"Query type ${e.getKind} is not supported for si/mr by Maha-Calcite, setting to 0/-1")
          (0, -1)
      }

      sqlNode match {
        case sqlSelect: SqlSelect =>
          val (fromTable, tableSchema): (String, Option[String]) = getFromTableAndTableSchema(sqlSelect.getFrom)
          val publicFact = getCube(fromTable, registry)
          require(publicFact.isDefined,MahaCalciteSqlParserError(s"Failed to find the cube $fromTable", sql))
          val columnAliasToColumnMap:Map[String, PublicColumn] = publicFact.get.getAllDomainColumnAliasToPublicColumnMap(registry)
          val selectFields = getSelectList(sqlSelect.getSelectList, publicFact.get, columnAliasToColumnMap,mahaSqlRequestContext)
          val (filterExpression, dayFilterOption, hourFilterOption, minuteFilterOption, numDays) = getFilterList(sqlSelect, publicFact.get,mahaSqlRequestContext)
          val orderByList: IndexedSeq[SortBy] = {
            if (topSqlNode.isInstanceOf[SqlOrderBy]) {
              val orderBySql = topSqlNode.asInstanceOf[SqlOrderBy]
              orderBySql.orderList.asScala.map {
                case sqlOrderByBasic:SqlBasicCall=>
                  require(sqlOrderByBasic.getOperandList.size()>0, s"Missing field and Order by Clause ${sqlOrderByBasic}")
                  val order:Order = {
                    if (sqlOrderByBasic.getOperator!=null && sqlOrderByBasic.getKind == SqlKind.DESCENDING) {
                      DESC
                    } else ASC
                  }
                  SortBy(mahaSqlRequestContext.queryAliasToColumnNameMap.getOrElse(toLiteral(sqlOrderByBasic.getOperandList.get(0)),toLiteral(sqlOrderByBasic.getOperandList.get(0))), order)
                case sqlString:SqlCharStringLiteral=>
                  SortBy(mahaSqlRequestContext.queryAliasToColumnNameMap.getOrElse(toLiteral(sqlString),toLiteral(sqlString)), ASC)
                case sqlString:SqlIdentifier=>
                  SortBy(mahaSqlRequestContext.queryAliasToColumnNameMap.getOrElse(toLiteral(sqlString),toLiteral(sqlString)), ASC)
                case other =>
                  throw new IllegalArgumentException(s"Maha Calcite: order by case ${other} not supported")
              }.toIndexedSeq
            } else IndexedSeq.empty
          }
          SelectSqlNode(
            new ReportingRequest(
              cube=publicFact.get.name,
              selectFields = selectFields,
              filterExpressions = filterExpression,
              requestType = SyncRequest,
              sortBy = orderByList,
              schema = schema,
              reportDisplayName = None,
              forceDimensionDriven = false,
              forceFactDriven = false,
              includeRowCount = false,
              dayFilter = dayFilterOption.getOrElse(DEFAULT_DAY_FILTER),
              hourFilter = hourFilterOption,
              minuteFilter = minuteFilterOption,
              numDays = numDays,
              curatorJsonConfigMap = Map.empty,
              additionalParameters = Map.empty,
              queryType = GroupByQuery,
              pagination = PaginationConfig(Map.empty),
              paginationStartIndex = startIndex,
              rowsPerPage = maxRow
            )
          )
        case sqlDescribeTable: SqlDescribeTable =>
          val (fromTable, tableSchema): (String, Option[String]) = getFromTableAndTableSchema(sqlDescribeTable.getTable)
          val cubeOption = getCube(fromTable, registry)
          require (cubeOption.isDefined, s"Failed to find the table $fromTable in the registry")
          DescribeSqlNode(cubeOption.get.name)
        case e=>
          throw new IllegalArgumentException(s"Query type ${e.getKind} is not supported by Maha-Calcite")
      }
    }
    catch {
      case e: SqlParseException =>
        val error = s"Calcite Error: Failed to parse SQL ${sql} ${e.getMessage}"
        logger.error(error, e)
        throw e;
    }
  }

  def getCube(fromTable: String, registry:Registry) : Option[PublicFact] = {
    registry.getFact(fromTable)
  }

  def getFromTableAndTableSchema(sqlNode: SqlNode): (String, Option[String]) = {
    val sqlFromNode = sqlNode match {
      case sqlBasicCall: SqlBasicCall =>
        sqlBasicCall.getOperator.kind match {
          case SqlKind.AS => sqlBasicCall.getOperandList().get(0)
          case _ => sqlNode
        }
      case _ => sqlNode
    }
    sqlFromNode match {
      case sqlIdentifier: SqlIdentifier =>
        val fromTableAndTableSchema = sqlIdentifier.names
        if (fromTableAndTableSchema.size == 1)
          (fromTableAndTableSchema.get(0).toLowerCase, None)
        else if (fromTableAndTableSchema.size == 2)
          (fromTableAndTableSchema.get(1).toLowerCase, Some(fromTableAndTableSchema.get(0).toLowerCase))
        else
          throw new IllegalArgumentException(s"Incorrect FROM clause. Expected FROM table or FROM schema.table")
      case _ =>
        throw new IllegalArgumentException(s"Incorrect FROM clause. Expected FROM table or FROM schema.table")
    }
  }

  def getSelectList(sqlNode: SqlNode, publicFact: PublicFact, columnAliasToColumnMap:Map[String, PublicColumn], mahaSqlRequestContext:MahaSqlRequestContext) : IndexedSeq[Field] = {
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
                }
                else {
                  sqlIdentifier.names.asScala.foreach(
                    c => {
                      val publicCol: PublicColumn = getColumnFromPublicFact(publicFact, c, columnAliasToColumnMap)
                      mahaSqlRequestContext.queryAliasToColumnNameMap += (publicCol.alias -> publicCol.alias)
                      arrayBuffer += Field(publicCol.alias, None, None)
                    }
                  )
                }
              case sqlCharStringLiteral: SqlCharStringLiteral =>
                val publicCol: PublicColumn = getColumnFromPublicFact(publicFact, toLiteral(sqlCharStringLiteral), columnAliasToColumnMap)
                arrayBuffer += Field(publicCol.alias, None, None)
              case sqlBasicCall: SqlBasicCall =>
                sqlBasicCall.getOperandList.get(0) match {
                  case innerSqlBasicCall: SqlBasicCall =>
                    val publicCol: PublicColumn = getColumnFromPublicFact(publicFact, toLiteral(innerSqlBasicCall.getOperandList.get(0)), columnAliasToColumnMap)
                    if(sqlBasicCall.getOperandList.size()>1) {
                      val alias = toLiteral(sqlBasicCall.getOperandList.get(1))
                      mahaSqlRequestContext.queryAliasToColumnNameMap += (alias -> publicCol.alias)
                      arrayBuffer += Field(publicCol.alias, Option(alias), None)
                    }
                    else arrayBuffer += Field(publicCol.alias, None, None)
                  case sqlIdentifier: SqlIdentifier =>
                    val publicCol: PublicColumn = getColumnFromPublicFact(publicFact, toLiteral(sqlIdentifier), columnAliasToColumnMap)
                    if(sqlBasicCall.getOperandList.size>1) {
                      val alias = toLiteral(sqlBasicCall.getOperandList.get(1))
                      mahaSqlRequestContext.queryAliasToColumnNameMap += (alias -> publicCol.alias)
                      arrayBuffer += Field(publicCol.alias, Option(alias), None)
                    }
                    else arrayBuffer += Field(publicCol.alias, None, None)
                  case sqlCharStringLiteral: SqlCharStringLiteral =>
                    val publicCol: PublicColumn = getColumnFromPublicFact(publicFact, toLiteral(sqlCharStringLiteral), columnAliasToColumnMap)
                    if(sqlBasicCall.getOperandList.size>1) {
                      val alias = toLiteral(sqlBasicCall.getOperandList.get(1))
                      mahaSqlRequestContext.queryAliasToColumnNameMap += (alias -> publicCol.alias)
                      arrayBuffer += Field(publicCol.alias, Option(alias), None)
                    }
                    else arrayBuffer += Field(publicCol.alias, None, None)
                }
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

  def getColumnFromPublicFact(publicFact: PublicFact, alias: String, columnAliasToColumnMap:Map[String, PublicColumn]): PublicColumn = {
    require(columnAliasToColumnMap.contains(alias),  s"Failed to find the column ${alias} in cube ${publicFact.name}")
    columnAliasToColumnMap.get(alias).get
  }

  def getFilterList(sqlNode: SqlNode, publicFact: PublicFact, mahaSqlRequestContext: MahaSqlRequestContext) : (IndexedSeq[Filter], Option[Filter], Option[Filter], Option[Filter], Int) = {
    val sqlWhere = sqlNode.asInstanceOf[SqlSelect].getWhere
    val sqlHaving = sqlNode.asInstanceOf[SqlSelect].getHaving
    var filterList = ArrayBuffer.empty[Filter]

    sqlWhere match {
      case sqlBasicCall: SqlBasicCall =>
        filterList = filterList ++ constructFilters(sqlBasicCall, mahaSqlRequestContext)
      case others: AnyRef =>
        logger.error(s"sqlNode type ${sqlNode.getKind} in getSelectList is not yet supported")
        return (IndexedSeq.empty, None, None, None, 1)
      case null =>
        logger.error("Empty WHERE clause.")
        return (IndexedSeq.empty, None, None, None, 1)
    }

    sqlHaving match {
      case sqlBasicCall: SqlBasicCall =>
        filterList = filterList ++ constructFilters(sqlBasicCall, mahaSqlRequestContext)
      case others: AnyRef =>
        logger.error(s"sqlNode type ${sqlNode.getKind} in getSelectList is not yet supported")
        (IndexedSeq.empty, None, None, None, 1)
      case null =>
        logger.debug("Empty HAVING clause.")
    }
    validate(filterList, mahaSqlRequestContext)
  }

  def addDateFilter(operandNode: SqlNode, dateNode: SqlNode, sqlKind: SqlKind, mahaSqlRequestContext: MahaSqlRequestContext, dateNode2: SqlNode=null) = {

    val date = DailyGrain.toFormattedString(dateTimeFormatter.parseDateTime(toLiteral(dateNode)))
    val hour = HourlyGrain.toFormattedString(dateTimeFormatter.parseDateTime(toLiteral(dateNode)))
    if(sqlKind==SqlKind.GREATER_THAN_OR_EQUAL || sqlKind==SqlKind.GREATER_THAN) {
      mahaSqlRequestContext.fromDate = date
      mahaSqlRequestContext.fromHour = hour
    }
    else if(sqlKind==SqlKind.LESS_THAN_OR_EQUAL || sqlKind==SqlKind.LESS_THAN) {
      mahaSqlRequestContext.toDate = date
      mahaSqlRequestContext.toHour = hour
    }
    else if(sqlKind==SqlKind.EQUALS) {
      mahaSqlRequestContext.fromDate = date
      mahaSqlRequestContext.fromHour = hour
      mahaSqlRequestContext.toDate = date
      mahaSqlRequestContext.toHour = hour
    }
    else if(sqlKind==SqlKind.BETWEEN) {
      mahaSqlRequestContext.fromDate = date
      mahaSqlRequestContext.fromHour = hour
      mahaSqlRequestContext.toDate = DailyGrain.toFormattedString(dateTimeFormatter.parseDateTime(toLiteral(dateNode2)))
      mahaSqlRequestContext.toHour = HourlyGrain.toFormattedString(dateTimeFormatter.parseDateTime(toLiteral(dateNode2)))
    }
  }

  def constructFilters(sqlNode: SqlNode, mahaSqlRequestContext: MahaSqlRequestContext): ArrayBuffer[Filter] = {
    require(sqlNode.isInstanceOf[SqlBasicCall], s"type ${sqlNode.getKind} not supported in construct current filter")
    val sqlBasicCall: SqlBasicCall = sqlNode.asInstanceOf[SqlBasicCall]
    val operands = sqlBasicCall.getOperandList
    if(toLiteral(operands.get(0)).equals(DailyGrain.DAY_FILTER_FIELD)){
      if(sqlBasicCall.getOperator.kind equals SqlKind.BETWEEN)
        addDateFilter(operands.get(0), operands.get(1), sqlBasicCall.getOperator.kind, mahaSqlRequestContext, operands.get(2))
      else addDateFilter(operands.get(0), operands.get(1), sqlBasicCall.getOperator.kind, mahaSqlRequestContext)
      ArrayBuffer.empty
    }
    else sqlBasicCall.getOperator.kind match {
      case SqlKind.AND =>
        constructFilters(operands.get(0), mahaSqlRequestContext) ++ constructFilters(operands.get(1), mahaSqlRequestContext)
      case SqlKind.NOT_IN =>
        val notInList: List[String] = operands.get(1).asInstanceOf[SqlNodeList].getList.asScala.map(sqlNode=> toLiteral(sqlNode)).toList
        ArrayBuffer.empty += NotInFilter(toLiteral(operands.get(0)), notInList).asInstanceOf[Filter]
      case SqlKind.OR =>
        val mergeBuffer: ArrayBuffer[Filter] = constructFilters(operands.get(0), mahaSqlRequestContext) ++ constructFilters(operands.get(1), mahaSqlRequestContext)
        ArrayBuffer.empty += OrFilter(mergeBuffer.toList).asInstanceOf[Filter]
      case SqlKind.EQUALS =>
        ArrayBuffer.empty += EqualityFilter(toLiteral(operands.get(0)), toLiteral(operands.get(1))).asInstanceOf[Filter]
      case SqlKind.NOT_EQUALS =>
        ArrayBuffer.empty += NotEqualToFilter(toLiteral(operands.get(0)), toLiteral(operands.get(1))).asInstanceOf[Filter]
      case SqlKind.GREATER_THAN =>
        ArrayBuffer.empty += GreaterThanFilter(toLiteral(operands.get(0)), toLiteral(operands.get(1))).asInstanceOf[Filter]
      case SqlKind.IN =>
        val inList: List[String] = operands.get(1).asInstanceOf[SqlNodeList].getList.asScala.map(sqlNode => toLiteral(sqlNode)).toList
        ArrayBuffer.empty += InFilter(toLiteral(operands.get(0)), inList).asInstanceOf[Filter]
      case SqlKind.BETWEEN =>
        ArrayBuffer.empty += BetweenFilter(toLiteral(operands.get(0)), toLiteral(operands.get(1)), toLiteral(operands.get(2))).asInstanceOf[Filter]
      case SqlKind.LIKE =>
        if (sqlBasicCall.getOperator.asInstanceOf[SqlLikeOperator].isNegated)
          ArrayBuffer.empty += NotLikeFilter(toLiteral(operands.get(0)), toLiteral(operands.get(1))).asInstanceOf[Filter]
        else
          ArrayBuffer.empty += LikeFilter(toLiteral(operands.get(0)), toLiteral(operands.get(1))).asInstanceOf[Filter]
      case SqlKind.IS_NULL =>
        ArrayBuffer.empty += IsNullFilter(toLiteral(operands.get(0))).asInstanceOf[Filter]
      case SqlKind.LESS_THAN=>
        ArrayBuffer.empty += LessThanFilter(toLiteral(operands.get(0)), toLiteral(operands.get(1))).asInstanceOf[Filter]
      case SqlKind.GREATER_THAN_OR_EQUAL =>
        logger.error(s"GREATER_THAN_OR_EQUAL filter is supported only for Day column")
        ArrayBuffer.empty
      case SqlKind.LESS_THAN_OR_EQUAL=>
        logger.error(s"LESS_THAN_OR_EQUAL filter is supported only for Day column")
        ArrayBuffer.empty
    }
  }

  def toLiteral(sqlNode: SqlNode): String = {
    if(sqlNode != null)
    sqlNode.toString.replaceAll("^[\"']+|[\"']+$", "")
    else ""
  }

  def validate(arrayBuffer: ArrayBuffer[Filter], mahaSqlRequestContext: MahaSqlRequestContext): (IndexedSeq[Filter], Option[Filter], Option[Filter], Option[Filter], Int)= {
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

    if (dayFilter.isEmpty) {
      if (mahaSqlRequestContext.fromDate != null)
        dayFilter = Option(BetweenFilter("Day", mahaSqlRequestContext.fromDate, mahaSqlRequestContext.toDate))
      else dayFilter = Option(DEFAULT_DAY_FILTER)
    }

    if (hourFilter.isEmpty && mahaSqlRequestContext.queryAliasToColumnNameMap.exists(_._2 == "Hour") && mahaSqlRequestContext.fromHour != null)
      hourFilter = Option(BetweenFilter("Hour", mahaSqlRequestContext.fromHour, mahaSqlRequestContext.toHour))

    //validate day filter
    require(dayFilter.isDefined, "Day filter not found in list of filters!")
    dayFilter.map(DailyGrain.validateFilterAndGetNumDays).foreach(nd => numDays = nd)

    (attributeAndMetricFilters.toIndexedSeq, dayFilter, hourFilter, minuteFilter, numDays)
  }
}
