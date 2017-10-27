// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.api.jersey

import java.io.OutputStream
import javax.ws.rs.core.StreamingOutput

import com.fasterxml.jackson.core.{JsonEncoding, JsonGenerator}
import com.fasterxml.jackson.databind.ObjectMapper
import com.yahoo.maha.core.{ColumnInfo, DimColumnInfo, FactColumnInfo, RequestModel}
import com.yahoo.maha.core.query.{RowList}

object JsonStreamingOutput {
  val objectMapper: ObjectMapper = new ObjectMapper()
}

case class JsonStreamingOutput(requestModel:RequestModel, rowList: RowList) extends StreamingOutput {

  override def write(outputStream: OutputStream): Unit = {
    val jsonGenerator: JsonGenerator = JsonStreamingOutput.objectMapper.getFactory().createGenerator(outputStream, JsonEncoding.UTF8)
    jsonGenerator.writeStartObject() // {
    writeHeader(jsonGenerator, requestModel.requestCols)
    writeDataRows(jsonGenerator)
    jsonGenerator.writeEndObject() // }
    jsonGenerator.flush()
    jsonGenerator.close()
  }

  private def writeHeader(jsonGenerator: JsonGenerator, requestedCols: IndexedSeq[ColumnInfo]) {
    jsonGenerator.writeFieldName("header") // "header":
    jsonGenerator.writeStartObject() // {

    jsonGenerator.writeFieldName("cube") // "cube":
    jsonGenerator.writeString(requestModel.cube) // <cube_name>
    jsonGenerator.writeFieldName("fields") // "fields":
    jsonGenerator.writeStartArray() // [

    requestedCols.foreach {
      columnInfo => {
        val isKey: Boolean = {
          if (rowList.query.aliasColumnMap.contains(columnInfo.alias))
            rowList.query.aliasColumnMap(columnInfo.alias).isKey
          else
            false
        }
        val columnType: String = {
          if ((columnInfo.isInstanceOf[DimColumnInfo] && isKey) || (columnInfo.isInstanceOf[FactColumnInfo] && isKey) || "Hour".equals(columnInfo.alias) || "Day".equals(columnInfo.alias))
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
    jsonGenerator.writeNumber(requestModel.maxRows)
    jsonGenerator.writeEndObject()
  }

  private def writeDataRows(jsonGenerator: JsonGenerator): Unit = {
    jsonGenerator.writeFieldName("rows") // "rows":
    jsonGenerator.writeStartArray() // [
    val numColumns = requestModel.requestCols.size

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
