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
import com.yahoo.maha.service.curators.{CuratorResult, DefaultCurator}
import org.slf4j.{Logger, LoggerFactory}

object JsonStreamingOutput {
  val objectMapper: ObjectMapper = new ObjectMapper()
  val logger: Logger = LoggerFactory.getLogger(classOf[JsonStreamingOutput])
  val ROW_COUNT : String = "ROW_COUNT"
}

case class JsonStreamingOutput(resultList: IndexedSeq[CuratorResult],
                               ingestionTimeUpdaterMap : Map[Engine, IngestionTimeUpdater] = Map.empty) extends StreamingOutput {


  override def write(outputStream: OutputStream): Unit = {
    val jsonGenerator: JsonGenerator = JsonStreamingOutput.objectMapper.getFactory().createGenerator(outputStream, JsonEncoding.UTF8)
    jsonGenerator.writeStartObject() // {
    val renderDefaultFirst = resultList.take(1)
    renderDefaultFirst.foreach {
      result =>
        defaultCuratorRender(result, jsonGenerator)
    }
    jsonGenerator.writeFieldName("curators") //"curators" :
    jsonGenerator.writeStartObject() //{
    resultList.foreach {
      result =>
        if (result.curator.name != DefaultCurator.name && !result.curator.isSingleton) {
          curatorRender(result, jsonGenerator)
        }
    }
    jsonGenerator.writeEndObject() //}

    jsonGenerator.writeEndObject() // }
    jsonGenerator.flush()
    jsonGenerator.close()
  }

  private def defaultCuratorRender(curatorResult: CuratorResult, jsonGenerator: JsonGenerator): Unit = {
    if(curatorResult.curator.name == DefaultCurator.name || curatorResult.curator.isSingleton) {
      if(curatorResult.requestResultTry.isSuccess) {
        val qpr = curatorResult.requestResultTry.get.queryPipelineResult
        val rowCountOption = curatorResult.requestResultTry.get.rowCountOption
        val engine = qpr.queryChain.drivingQuery.engine
        val tableName = qpr.queryChain.drivingQuery.tableName
        val ingestionTimeUpdater:IngestionTimeUpdater = ingestionTimeUpdaterMap
          .getOrElse(qpr.queryChain.drivingQuery.engine, NoopIngestionTimeUpdater(engine, engine.toString))
        val dimCols : Set[String]  = if(curatorResult.requestModelReference.model.bestCandidates.isDefined) {
          curatorResult.requestModelReference.model.bestCandidates.get.publicFact.dimCols.map(_.alias)
        } else Set.empty

        val includeRowCount: Boolean = curatorResult.requestModelReference.model.includeRowCount && rowCountOption.isDefined
        writeHeader(jsonGenerator
          , qpr.rowList.columns
          , curatorResult.requestModelReference.model.reportingRequest
          , ingestionTimeUpdater
          , tableName
          , dimCols
          , includeRowCount
        )
        writeDataRows(jsonGenerator, qpr.rowList, rowCountOption, includeRowCount)
      } else {
        //log error
      }

    }
  }

  private def curatorRender(curatorResult: CuratorResult, jsonGenerator: JsonGenerator) : Unit = {
    if(curatorResult.requestResultTry.isSuccess) {
      val qpr = curatorResult.requestResultTry.get.queryPipelineResult
      val engine = qpr.queryChain.drivingQuery.engine
      val tableName = qpr.queryChain.drivingQuery.tableName
      val ingestionTimeUpdater:IngestionTimeUpdater = ingestionTimeUpdaterMap
        .getOrElse(qpr.queryChain.drivingQuery.engine, NoopIngestionTimeUpdater(engine, engine.toString))
      val dimCols : Set[String]  = if(curatorResult.requestModelReference.model.bestCandidates.isDefined) {
        curatorResult.requestModelReference.model.bestCandidates.get.publicFact.dimCols.map(_.alias)
      } else Set.empty
      jsonGenerator.writeFieldName(curatorResult.curator.name) // "curatorName":
      jsonGenerator.writeStartObject() //{
      jsonGenerator.writeFieldName("result") // "result":
      jsonGenerator.writeStartObject() //{
      writeHeader(jsonGenerator
        , qpr.rowList.columns
        , curatorResult.requestModelReference.model.reportingRequest
        , ingestionTimeUpdater
        , tableName
        , dimCols
        , false
      )
      writeDataRows(jsonGenerator, qpr.rowList, None, false)
      jsonGenerator.writeEndObject() //}
      jsonGenerator.writeEndObject() //}

    }
  }

  private def writeHeader(jsonGenerator: JsonGenerator
                          , columns: IndexedSeq[ColumnInfo]
                          , reportingRequest: ReportingRequest
                          , ingestionTimeUpdater: IngestionTimeUpdater
                          , tableName: String
                          , dimCols: Set[String]
                          , includeRowCount: Boolean
                         ) {
    jsonGenerator.writeFieldName("header") // "header":
    jsonGenerator.writeStartObject() // {
    val ingestionTimeOption = ingestionTimeUpdater.getIngestionTime(tableName)
    if (ingestionTimeOption.isDefined) {
      jsonGenerator.writeFieldName("lastIngestTime")
      jsonGenerator.writeString(ingestionTimeOption.get)
      jsonGenerator.writeFieldName("source")
      jsonGenerator.writeString(tableName)
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
    if (includeRowCount) {
      jsonGenerator.writeStartObject() // {
      jsonGenerator.writeFieldName("fieldName") // "fieldName":
      jsonGenerator.writeString(JsonStreamingOutput.ROW_COUNT)
      jsonGenerator.writeFieldName("fieldType") // "fieldType":

      jsonGenerator.writeString("CONSTANT")
      jsonGenerator.writeEndObject() // }

    }
    jsonGenerator.writeEndArray() // ]
    jsonGenerator.writeFieldName("maxRows")
    jsonGenerator.writeNumber(reportingRequest.rowsPerPage)
    jsonGenerator.writeEndObject()
  }

  private def writeDataRows(jsonGenerator: JsonGenerator, rowList: RowList, rowCountOption: Option[Int], includeRowCount: Boolean): Unit = {
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
        if (includeRowCount) {
          jsonGenerator.writeObject(rowCountOption.get)
        }
        jsonGenerator.writeEndArray()
      }
    }
    jsonGenerator.writeEndArray() // ]
  }

}
