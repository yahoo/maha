// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.report

import java.io._
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardOpenOption}

import com.fasterxml.jackson.core.{JsonEncoding, JsonGenerator}
import com.fasterxml.jackson.databind.ObjectMapper
import com.yahoo.maha.core.query.{Query, QueryRowList, Row, RowList}
import com.yahoo.maha.core.{Column, ColumnInfo, DimColumnInfo, FactColumnInfo}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Created by hiral on 5/5/16.
 */

object JsonRowList {
  private final val logger: Logger = LoggerFactory.getLogger(classOf[JsonRowList])
  private final val objectMapper: ObjectMapper = new ObjectMapper
  private final val TOTAL_ROWS: String = "TotalRows"
  private final val ROW_COUNT: String = "rowCount"

  def jsonGenerator(outputStream: OutputStream): JsonGenerator = {
    objectMapper.getFactory.createGenerator(outputStream, JsonEncoding.UTF8)
  }

  def jsonGenerator(writer: Writer): JsonGenerator = {
    objectMapper.getFactory.createGenerator(writer)
  }

  def from(rowList: QueryRowList, injectTotalRowsOption: Option[Integer], jsonGenerator: JsonGenerator, compatibilityMode: Boolean) : JsonRowList = {
    val jsonRowList = DefaultJsonRowList(rowList.query, rowList.subQuery, injectTotalRowsOption, PassThroughJsonGeneratorProvider(jsonGenerator), compatibilityMode)
    jsonRowList.withLifeCycle {
      rowList.foreach(jsonRowList.addRow)
    }
    jsonRowList
  }

  def jsonRowList(jsonGenerator: JsonGenerator, injectTotalRowsOption: Option[Integer], compatibilityMode: Boolean) : Query => QueryRowList = (q) => {
    DefaultJsonRowList(q, IndexedSeq.empty, injectTotalRowsOption, PassThroughJsonGeneratorProvider(jsonGenerator), compatibilityMode)
  }
}

trait JsonRowList extends QueryRowList {
  def query: Query
  protected def subQuery: IndexedSeq[Query]
  protected def injectTotalRowsOption: Option[Integer]
  protected def jsonGeneratorProvider: JsonGeneratorProvider
  protected def compatibilityMode: Boolean

  import JsonRowList._
  private[this] val combinedAliasColumnMap = query.aliasColumnMap ++ subQuery.flatMap(_.aliasColumnMap)
  private[this] final val requestModel = query.queryContext.requestModel
  private[this] val numCols: Int = {
    val addOneForTotalRows = {
      if (requestModel.includeRowCount) {
        if (injectTotalRowsOption.isDefined) 0 else 1
      } else 0
    }
    requestModel.requestCols.size + addOneForTotalRows
  }
  protected var started = false
  protected var ended = false
  private[this] var rowsWritten: Int = 0

  protected lazy val jsonGenerator: JsonGenerator = jsonGeneratorProvider.newJsonGenerator

  override protected def start() : Unit = {
    if(!started) {
      started = true
      val outputColumnNames = query.queryContext.requestModel.reportingRequest.selectFields.map(f => f.alias.getOrElse(f.field))
      writeHeader(jsonGenerator, injectTotalRowsOption, outputColumnNames, getAliasColumnTypeMap(requestModel.requestCols, combinedAliasColumnMap))
      if(compatibilityMode) {
        writeFieldsArray(jsonGenerator, injectTotalRowsOption, outputColumnNames, getAliasColumnTypeMap(requestModel.requestCols, combinedAliasColumnMap))
      }
      jsonGenerator.writeFieldName("rows")
      jsonGenerator.writeStartArray()
    }
  }

  override protected def end() : Unit = {
    if(started && !ended) {
      try {
        jsonGenerator.writeEndArray()
        if (requestModel.isAsyncRequest) {
          jsonGenerator.writeFieldName(ROW_COUNT)
          if (injectTotalRowsOption.isDefined) {
            jsonGenerator.writeNumber(injectTotalRowsOption.get)
          } else {
            jsonGenerator.writeNumber(rowsWritten)
          }
        }
        writeDebug()
      } catch {
        case t: Throwable =>
          JsonRowList.logger.error("Failed on end", t)
          throw t
      }
      ended = true
    }
  }

  def addRow(r: Row): Unit = {
    writeRow(r)
  }

  override def addRow(r: Row, er: Option[Row] = None): Unit = {
    postResultRowOperation(r, er)
    writeRow(r)
  }

  override def isEmpty : Boolean = true
  override def foreach(fn: Row => Unit) : Unit = {
    throw new UnsupportedOperationException("foreach not supported on JsonRowList")
  }
  override def map[T](fn: Row => T) : Iterable[T] = {
    throw new UnsupportedOperationException("map not supported on JsonRowList")
  }

  private def getAliasColumnTypeMap(requestedCols: IndexedSeq[ColumnInfo], combinedAliasColumnMap: Map[String, Column]): IndexedSeq[String] = {
    val columnTypeList: IndexedSeq[String] = requestedCols.map { field =>
      getColumnType(field, combinedAliasColumnMap.contains(field.alias) && combinedAliasColumnMap(field.alias).isKey)
    }
    if (query.queryContext.requestModel.includeRowCount) {
      columnTypeList :+ "CONSTANT"
    } else {
      columnTypeList
    }
  }

  private def getColumnType(columnInfo: ColumnInfo, isKey: Boolean): String = {
    if (columnInfo.isInstanceOf[DimColumnInfo] || (columnInfo.isInstanceOf[FactColumnInfo] && isKey) || ("Day" == columnInfo.alias) || ("Hour" == columnInfo.alias)) {
      "DIM"
    }
    else if (columnInfo.isInstanceOf[FactColumnInfo]) {
      "FACT"
    }
    else {
      "CONSTANT"
    }
  }

  private def writeFieldsArray(jsonGenerator: JsonGenerator, injectTotalRowsOption: Option[Integer], requestedCols: IndexedSeq[String], columnTypeList: IndexedSeq[String]): Unit = {
    jsonGenerator.writeFieldName("fields")
    jsonGenerator.writeStartArray()
    var i: Int = 0
    while (i < requestedCols.size) {
      val field: String = requestedCols(i)
      val fieldColumnType: String = columnTypeList(i)
      jsonGenerator.writeStartObject()
      jsonGenerator.writeFieldName("fieldName")
      jsonGenerator.writeString(field)
      jsonGenerator.writeFieldName("fieldType")
      if(fieldColumnType == null) {
        jsonGenerator.writeString("CONSTANT")
      } else {
        jsonGenerator.writeString(fieldColumnType)
      }
      jsonGenerator.writeEndObject()
      i += 1
    }
    if (requestModel.includeRowCount) {
      jsonGenerator.writeStartObject()
      jsonGenerator.writeFieldName("fieldName")
      jsonGenerator.writeString(TOTAL_ROWS)
      jsonGenerator.writeFieldName("fieldType")
      jsonGenerator.writeString("CONSTANT")
      jsonGenerator.writeEndObject()
    }
    jsonGenerator.writeEndArray()
  }

  private def writeHeader(jsonGenerator: JsonGenerator, injectTotalRowsOption: Option[Integer], requestedCols: IndexedSeq[String], columnTypeList: IndexedSeq[String]) {
    jsonGenerator.writeFieldName("header")
    jsonGenerator.writeStartObject()
    jsonGenerator.writeFieldName("cube")
    jsonGenerator.writeString(requestModel.cube)
    writeFieldsArray(jsonGenerator, injectTotalRowsOption, requestedCols, columnTypeList)
    if(requestModel.isSyncRequest) {
      jsonGenerator.writeFieldName("maxRows")
      jsonGenerator.writeNumber(requestModel.maxRows)
    }
    jsonGenerator.writeEndObject()
  }

  private def writeRow(row: Row) {
    require(started, "cannot write row without starting lifecycle")
    try {
      jsonGenerator.writeStartArray()
      var i: Int = 0
      while (i < numCols) {
        val value: Any = row.getValue(i)
        jsonGenerator.writeObject(value)
        i += 1
      }
      if (requestModel.includeRowCount && injectTotalRowsOption.isDefined) {
        jsonGenerator.writeObject(injectTotalRowsOption.get)
      }
      jsonGenerator.writeEndArray()
      rowsWritten += 1
    }
    catch {
      case e: IOException =>
        logger.error("Failed on transforming row data : " + row, e)
    }
  }

  private def writeDebug() : Unit = {
    if (requestModel.isDebugEnabled) {
      jsonGenerator.writeFieldName("debug")
      jsonGenerator.writeStartObject()

      jsonGenerator.writeFieldName("fields")
      jsonGenerator.writeStartArray()
      for ((k, v) <- combinedAliasColumnMap) {
        val fieldDataType: String = v.dataType.jsonDataType
        jsonGenerator.writeStartObject()
        jsonGenerator.writeFieldName("fieldName")
        jsonGenerator.writeString(k)
        jsonGenerator.writeFieldName("dataType")
        jsonGenerator.writeString(fieldDataType)
        jsonGenerator.writeEndObject()
      }
      if (requestModel.includeRowCount) {
        jsonGenerator.writeStartObject()
        jsonGenerator.writeFieldName("fieldName")
        jsonGenerator.writeString(TOTAL_ROWS)
        jsonGenerator.writeFieldName("dataType")
        jsonGenerator.writeString("integer")
        jsonGenerator.writeEndObject()
      }
      jsonGenerator.writeEndArray()

      jsonGenerator.writeFieldName("drivingQuery")
      jsonGenerator.writeStartObject()
      jsonGenerator.writeFieldName("tableName")
      jsonGenerator.writeString(query.tableName)
      jsonGenerator.writeFieldName("engine")
      jsonGenerator.writeString(query.engine.toString)
      jsonGenerator.writeEndObject()

      if (subQuery.nonEmpty) {
        jsonGenerator.writeFieldName("subQuery")
        jsonGenerator.writeStartArray()
        subQuery.foreach {
          query =>
            jsonGenerator.writeStartObject()
            jsonGenerator.writeFieldName("tableName")
            jsonGenerator.writeString(query.tableName)
            jsonGenerator.writeFieldName("engine")
            jsonGenerator.writeString(query.engine.toString)
            jsonGenerator.writeEndObject()
        }
        jsonGenerator.writeEndArray()
      }
      jsonGenerator.writeEndObject()
    }
  }

}



case class DefaultJsonRowList(query: Query
                       , override val subQuery: IndexedSeq[Query]
                       , injectTotalRowsOption: Option[Integer]
                       , jsonGeneratorProvider: JsonGeneratorProvider
                       , compatibilityMode: Boolean
                        ) extends JsonRowList

object FileJsonRowList {
  private final val logger: Logger = LoggerFactory.getLogger(classOf[FileJsonRowList])
  def fileJsonRowList(file: File, injectTotalRowsOption: Option[Integer], compatibilityMode: Boolean) : Query => QueryRowList = (q) => {
    FileJsonRowList(q, IndexedSeq.empty, injectTotalRowsOption, FileJsonGeneratorProvider(file), compatibilityMode)
  }
}

case class FileJsonRowList(query: Query
                       , override val subQuery: IndexedSeq[Query]
                       , injectTotalRowsOption: Option[Integer]
                       , jsonGeneratorProvider: JsonGeneratorProvider
                       , compatibilityMode: Boolean
                      ) extends JsonRowList {

  override def start(): Unit = {
    if(!started) {
      jsonGenerator.writeStartObject()
      super.start()
    }
  }

  override def end(): Unit = {
    if(started && !ended) {
      try {
        super.end()
        jsonGenerator.writeEndObject()
      } catch {
        case t:Throwable =>
          FileJsonRowList.logger.error("Failed on end", t)
          throw t
      } finally {
        jsonGenerator.flush()
        jsonGenerator.close()
      }
    }
  }
}

trait JsonGeneratorProvider {
  def newJsonGenerator: JsonGenerator
}

case class FileJsonGeneratorProvider(file: File) extends JsonGeneratorProvider {
  def newJsonGenerator: JsonGenerator = {
    if(file.exists() && file.length() > 0) {
      Files.write(file.toPath, Array[Byte](), StandardOpenOption.TRUNCATE_EXISTING) // Clear file
    }
    val fos = new FileOutputStream(file.getAbsoluteFile, true)
    val jsonGeneratorTry = safeCloseable(fos)(new OutputStreamWriter(_, StandardCharsets.UTF_8))
      .flatMap(safeCloseable(_)(new BufferedWriter(_)))
      .flatMap(safeCloseable(_)(JsonRowList.jsonGenerator(_)))
    require(jsonGeneratorTry.isSuccess, "Failed to create json generator safely")
    jsonGeneratorTry.get
  }
}

case class PassThroughJsonGeneratorProvider(jsonGenerator: JsonGenerator) extends JsonGeneratorProvider {
  def newJsonGenerator: JsonGenerator = jsonGenerator
}