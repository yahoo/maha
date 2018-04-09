// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.api.jersey

import java.io.OutputStream
import javax.ws.rs.core.StreamingOutput

import com.fasterxml.jackson.core.{JsonEncoding, JsonGenerator}
import com.fasterxml.jackson.databind.ObjectMapper
import com.yahoo.maha.core._
import com.yahoo.maha.core.query.RowList
import com.yahoo.maha.core.request.ReportingRequest

object JsonStreamingOutput {
  val objectMapper: ObjectMapper = new ObjectMapper()
}

trait RowListToJsonStream {
  def reportingRequest: ReportingRequest
  def dimCols: Set[String]
  def rowList: RowList
  def engine: Engine
  def factName: String
  def ingestionTimeUpdaterMap : Map[Engine, IngestionTimeUpdater]
}

case class JsonStreamingOutput(reportingRequest: ReportingRequest,
                               dimCols : Set[String],
                               rowList: RowList,
                               engine: Engine,
                               factName : String,
                               ingestionTimeUpdaterMap : Map[Engine, IngestionTimeUpdater] = Map.empty) extends StreamingOutput with RowListToJsonStream {

  val ingestionTimeUpdater:IngestionTimeUpdater = ingestionTimeUpdaterMap.get(engine).getOrElse(NoopIngestionTimeUpdater(engine, engine.toString))

  override def write(outputStream: OutputStream): Unit = {
    val jsonGenerator: JsonGenerator = JsonStreamingOutput.objectMapper.getFactory().createGenerator(outputStream, JsonEncoding.UTF8)
    jsonGenerator.writeStartObject() // {
    writeHeader(jsonGenerator, rowList.columns)
    writeDataRows(jsonGenerator)
    jsonGenerator.writeEndObject() // }
    jsonGenerator.flush()
    jsonGenerator.close()
  }

  private def writeHeader(jsonGenerator: JsonGenerator, columns: IndexedSeq[ColumnInfo]) {
    jsonGenerator.writeFieldName("header") // "header":
    jsonGenerator.writeStartObject() // {
    val ingestionTimeOption = ingestionTimeUpdater.getIngestionTime(factName)
    if (ingestionTimeOption.isDefined) {
      jsonGenerator.writeFieldName("lastIngestTime")
      jsonGenerator.writeString(ingestionTimeOption.get)
      jsonGenerator.writeFieldName("source")
      jsonGenerator.writeString(factName)
    }
    jsonGenerator.writeFieldName("cube") // "cube":
    jsonGenerator.writeString(reportingRequest.cube) // <cube_name>
    jsonGenerator.writeFieldName("fields") // "fields":
    jsonGenerator.writeStartArray() // [

    columns.foreach {
      columnInfo => {
        val columnType: String = {
          if (columnInfo.isInstanceOf[DimColumnInfo] || dimCols.contains(columnInfo.alias) || "Hour".equals(columnInfo.alias) || "Day".equals(columnInfo.alias))
            "DIM"
          else if (columnInfo.isInstanceOf[FactColumnInfo])
            "FACT"
          else
            "CONSTANT"
        }
        jsonGenerator.writeStartObject()
        jsonGenerator.writeFieldName("fieldName") // "fieldName":
        jsonGenerator.writeString(columnInfo.alias) // <display_field>
        jsonGenerator.writeFieldName("fieldType") // "fieldType":
        jsonGenerator.writeString(if (columnType == null) "CONSTANT" else columnType) // <field_type>
        jsonGenerator.writeEndObject() // }
      }
    }
    jsonGenerator.writeEndArray() // ]
    jsonGenerator.writeFieldName("maxRows")
    jsonGenerator.writeNumber(reportingRequest.rowsPerPage)
    jsonGenerator.writeEndObject()
  }

  private def writeDataRows(jsonGenerator: JsonGenerator): Unit = {
    jsonGenerator.writeFieldName("rows") // "rows":
    jsonGenerator.writeStartArray() // [
    val numColumns = rowList.columns.size

    rowList.foreach {
      row => {
        jsonGenerator.writeStartArray()
        var i = 0
        while(i < numColumns) {
          jsonGenerator.writeObject(row.getValue(i))
          i+=1
        }
        jsonGenerator.writeEndArray()
      }
    }
    jsonGenerator.writeEndArray() // ]
  }

}
