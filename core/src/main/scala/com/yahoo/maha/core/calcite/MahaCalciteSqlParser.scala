package com.yahoo.maha.core.calcite

import com.yahoo.maha.core.error.{Error, MahaCalciteSqlParserError}
import com.yahoo.maha.core.fact.PublicFact
import com.yahoo.maha.core.registry.Registry
import com.yahoo.maha.core.request.{Field, ReportingRequest}
import grizzled.slf4j.Logging
import org.apache.calcite.sql.{SqlIdentifier, SqlNode, SqlNodeList, SqlSelect}
import org.apache.calcite.sql.parser.{SqlParseException, SqlParser}
import org.apache.commons.lang.StringUtils

trait MahaCalciteSqlParser {
  def parse(sql:String) : ReportingRequest
}

case class DefaultMahaCalciteSqlParser(registry:Registry) extends MahaCalciteSqlParser with Logging {

  override def parse(sql: String): ReportingRequest = {
    require(!StringUtils.isEmpty(sql), MahaCalciteSqlParserError("Sql can not be empty", sql))
    val parser: SqlParser = SqlParser.create(sql)
    try {
      var sqlNode: SqlNode = null
      sqlNode = parser.parseQuery
      sqlNode match {
        case sqlSelect: SqlSelect =>
          val publicFact = getCube(sqlSelect.getFrom)
          require(publicFact.isDefined,MahaCalciteSqlParserError(s"Failed to find the cube ${sqlSelect.getFrom}", sql))
          val fields = getSelectList(sqlSelect.getSelectList, publicFact.get)
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

  def getSelectList(sqlNode: SqlNode, publicFact: PublicFact) : IndexedSeq[Field] = {
    sqlNode match {
      case sqlNodeList: SqlNodeList =>
       if (sqlNodeList.size() > 0) {

       }
      case e=>
    }
    IndexedSeq(null)
  }

  def getCube(sqlNode: SqlNode) : Option[PublicFact] = {
    sqlNode match {
      case sqlIdentifier: SqlIdentifier =>
        registry.getFact()
        registry.getFact(sqlIdentifier.getSimple.toLowerCase)
      case e=>
        throw new IllegalArgumentException(s"missing case ${e} in get cube method")
    }
  }
}
