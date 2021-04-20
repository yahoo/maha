package com.yahoo.maha.service.calcite

import com.yahoo.maha.core._
import com.yahoo.maha.core.error.MahaCalciteSqlParserError
import com.yahoo.maha.core.fact.PublicFact
import com.yahoo.maha.core.registry.Registry
import com.yahoo.maha.core.request.ReportingRequest.DEFAULT_DAY_FILTER
import com.yahoo.maha.core.request.{Field, PaginationConfig, ReportingRequest, SelectQuery, SyncRequest}
import com.yahoo.maha.service.MahaServiceConfig
import grizzled.slf4j.Logging
import org.apache.calcite.sql._
import org.apache.calcite.sql.parser.{SqlParseException, SqlParser}
import org.apache.commons.lang.StringUtils

trait MahaCalciteSqlParser {
  def parse(sql:String, schema: Schema, registryName:String) : ReportingRequest
}

case class DefaultMahaCalciteSqlParser(mahaServiceConfig: MahaServiceConfig) extends MahaCalciteSqlParser with Logging {

  override def parse(sql: String, schema: Schema, registryName:String): ReportingRequest = {
    require(!StringUtils.isEmpty(sql), MahaCalciteSqlParserError("Sql can not be empty", sql))
    require(mahaServiceConfig.registry.contains(registryName), s"failed to find the registry ${registryName} in the mahaService Config")
    val registry:Registry  = mahaServiceConfig.registry.get(registryName).get.registry

    val parser: SqlParser = SqlParser.create(sql)
    try {
      var sqlNode: SqlNode = null
      sqlNode = parser.parseQuery
      //validate validate AST
      //optimize convert AST to RequestModel and/or ReportingRequest
      //execute X
      sqlNode match {
        case sqlSelect: SqlSelect =>
          val publicFact = getCube(sqlSelect.getFrom, registry)
          require(publicFact.isDefined,MahaCalciteSqlParserError(s"Failed to find the cube ${sqlSelect.getFrom}", sql))
          val selectFields = getSelectList(sqlSelect.getSelectList, publicFact.get)
          val filterExpression = getFilterList(sqlSelect.getWhere, publicFact.get)
          val dayFilter = filterExpression.filter(f=> "Day".equalsIgnoreCase(f.field)).headOption.getOrElse(DEFAULT_DAY_FILTER)
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
            dayFilter = dayFilter,
            hourFilter = None,
            minuteFilter = None,
            numDays = 1,
            curatorJsonConfigMap = Map.empty,
            additionalParameters = Map.empty,
            queryType = SelectQuery,
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
          val iter = sqlNodeList.iterator()
          while (iter.hasNext) {
            val subNode = iter.next();
            subNode match {
              case sqlIdentifier: SqlIdentifier =>
                if (sqlIdentifier.isStar) {
                  val indexedSeq: IndexedSeq[Field]=
                    publicFact.factCols.map(publicFactCol => Field(publicFactCol.name, Option(publicFactCol.alias), None)).toIndexedSeq ++
                      publicFact.dimCols.map(publicDimCol => Field(publicDimCol.name, Option(publicDimCol.alias), None)).toIndexedSeq
                  return indexedSeq
                }
              //TODO: other non-star cases
              //else {
              //
              //}
              case other: AnyRef =>
                val errMsg = String.format("sqlNode type[%s] in getSelectList is not yet supported", other.getClass.toString)
                logger.error(errMsg);
            }
          }
        } else {
          IndexedSeq.empty
        }
      case e=>
    }
    IndexedSeq(null)
  }

  def getFilterList(sqlNode: SqlNode, publicFact: PublicFact) : IndexedSeq[Filter] = {
    if (sqlNode!=null) {
      sqlNode match {
        case sqlBasicCall: SqlBasicCall =>
          val filterListOption = recursiveGetFilterList(sqlBasicCall, publicFact)
          filterListOption.getOrElse(List.empty).toIndexedSeq
        case other: AnyRef =>
          val errMsg = String.format("sqlNode type[%s] in getSelectList is not yet supported", other.getClass.toString)
          logger.error(errMsg);
          IndexedSeq.empty
      }
    } else IndexedSeq.empty
  }

  def recursiveGetFilterList(sqlNode: SqlNode, publicFact: PublicFact): Option[List[Filter]] = {
    sqlNode match {
      case sqlBasicCall: SqlBasicCall =>
        sqlBasicCall.getOperator.kind match {
          case SqlKind.EQUALS => //only one filter left
            Some(constructFilterList(sqlBasicCall))
          case SqlKind.AND =>
            val filterOption: Option[List[Filter]] = recursiveGetFilterList(sqlBasicCall.operands(0), publicFact)
            println(constructFilterList(sqlBasicCall.operands(1)))
            if(filterOption.isDefined) {
              Some(filterOption.get ++ constructFilterList(sqlBasicCall.operands(1)))
            } else {
              Some(constructFilterList(sqlBasicCall.operands(1)))
            }
          case SqlKind.OR =>
            None

          case _ =>
            None
        }
      case other: AnyRef =>
        None
    }
  }

  def constructFilterList(sqlNode: SqlNode): List[Filter] = {
    require(sqlNode.isInstanceOf[SqlBasicCall], "type not supported in construct current filter")
    val sqlBasicCall: SqlBasicCall = sqlNode.asInstanceOf[SqlBasicCall]
    val operands = sqlBasicCall.getOperands
    sqlBasicCall.getOperator.kind match {
      case SqlKind.EQUALS =>
        List.empty ++ List(EqualityFilter(operands(0).toString, operands(1).toString))
      case SqlKind.GREATER_THAN =>
        List.empty ++ List(GreaterThanFilter(operands(0).toString, operands(1).toString))
      case SqlKind.IN =>
        val inList: List[String] = operands(1).asInstanceOf[SqlNodeList].toArray.toList.map(sqlNode => sqlNode.toString)
        List.empty ++ List(InFilter(operands(0).toString, inList))
    }
  }
}
