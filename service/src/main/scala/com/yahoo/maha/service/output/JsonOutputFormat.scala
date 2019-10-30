// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.output

import java.io.OutputStream

import com.fasterxml.jackson.core.{JsonEncoding, JsonGenerator}
import com.fasterxml.jackson.databind.ObjectMapper
import com.yahoo.maha.core.fact.DruidRowCountFactCol
import com.yahoo.maha.core.query.{InMemRowList, QueryRowList, RowList}
import com.yahoo.maha.core.request.ReportingRequest
import com.yahoo.maha.core.{ColumnInfo, DimColumnInfo, Engine, FactColumnInfo}
import com.yahoo.maha.service.{RequestCoordinatorResult, RequestResult}
import com.yahoo.maha.service.curators.{Curator, DefaultCurator, RowCountCurator}
import com.yahoo.maha.service.datasource.{IngestionTimeUpdater, NoopIngestionTimeUpdater}
import org.json4s.JValue
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by hiral on 4/11/18.
  */
object JsonOutputFormat {
  val objectMapper: ObjectMapper = new ObjectMapper()
  val logger: Logger = LoggerFactory.getLogger(classOf[JsonOutputFormat])
  val ROW_COUNT : String = "ROW_COUNT"
  val defaultRenderSet : Set[String] = Set(DefaultCurator.name, RowCountCurator.name)
}
case class JsonOutputFormat(requestCoordinatorResult: RequestCoordinatorResult,
                            ingestionTimeUpdaterMap : Map[Engine, IngestionTimeUpdater] = Map.empty) {


  def writeStream(outputStream: OutputStream): Unit = {
    val jsonGenerator: JsonGenerator = JsonOutputFormat.objectMapper.getFactory().createGenerator(outputStream, JsonEncoding.UTF8)
    jsonGenerator.writeStartObject() // {
    val headOption = requestCoordinatorResult.orderedList.headOption

    if(headOption.exists(_.isSingleton)) {
      renderDefault(headOption.get.name, requestCoordinatorResult, jsonGenerator, None)
    } else if(requestCoordinatorResult.successResults.contains(DefaultCurator.name)) {
      val rowCountOption = RowCountCurator.getRowCount(requestCoordinatorResult.mahaRequestContext)
      renderDefault(DefaultCurator.name, requestCoordinatorResult, jsonGenerator, rowCountOption)
      jsonGenerator.writeFieldName("curators") //"curators" :
      jsonGenerator.writeStartObject() //{
      //remove default render curators
      val curatorList = requestCoordinatorResult.orderedList.filterNot(c => JsonOutputFormat.defaultRenderSet(c.name))
      curatorList.foreach(renderCurator(_, requestCoordinatorResult, jsonGenerator))
      jsonGenerator.writeEndObject() //}
    } else if(requestCoordinatorResult.successResults.contains(RowCountCurator.name)) { // for RowCountCurator
      val rowCountOption = RowCountCurator.getRowCount(requestCoordinatorResult.mahaRequestContext)
      renderDefault(RowCountCurator.name, requestCoordinatorResult, jsonGenerator, rowCountOption)
      jsonGenerator.writeFieldName("curators") //"curators" :
      jsonGenerator.writeStartObject() //{
      val curatorList = requestCoordinatorResult.orderedList
      curatorList.foreach(renderCurator(_, requestCoordinatorResult, jsonGenerator, rowCountOption))
      jsonGenerator.writeEndObject() //}
    }

    jsonGenerator.writeEndObject() // }
    jsonGenerator.flush()
    jsonGenerator.close()
  }

  private def renderDefault(curatorName: String, requestCoordinatorResult: RequestCoordinatorResult, jsonGenerator: JsonGenerator, rowCountOption:Option[Int]): Unit = {
    if(requestCoordinatorResult.successResults.contains(curatorName)
      && requestCoordinatorResult.curatorResult.contains(curatorName)) {
      val curatorResult = requestCoordinatorResult.curatorResult(curatorName)
      val requestResult = requestCoordinatorResult.successResults(curatorName)
      val qpr = requestResult.queryPipelineResult
      val engine = qpr.queryChain.drivingQuery.engine
      val tableName = qpr.queryChain.drivingQuery.tableName
      val ingestionTimeUpdater:IngestionTimeUpdater = ingestionTimeUpdaterMap
        .getOrElse(qpr.queryChain.drivingQuery.engine, NoopIngestionTimeUpdater(engine, engine.toString))
      val dimCols : Set[String] = if (curatorResult.requestModelReference.model.bestCandidates.isDefined) {
        curatorResult.requestModelReference.model.bestCandidates.get.publicFact.dimCols.map(_.alias)
      } else Set.empty

      //check if DruidRowCountFactCol is used
      val druidRowCountAlias = getDruidRowCountAlias(rowCountOption, requestResult)

      writeHeader(jsonGenerator
        , qpr.rowList.columns
        , curatorResult.requestModelReference.model.reportingRequest
        , ingestionTimeUpdater
        , tableName
        , dimCols
        , true
        , qpr.pagination
        , druidRowCountAlias
      )
      writeDataRows(jsonGenerator, qpr.rowList, rowCountOption, curatorResult.requestModelReference.model.reportingRequest, druidRowCountAlias)
    }
  }

  private def renderCurator(curator: Curator, requestCoordinatorResult: RequestCoordinatorResult, jsonGenerator: JsonGenerator, rowCountOption: Option[Int] = None) : Unit = {
    if(requestCoordinatorResult.successResults.contains(curator.name)
      && requestCoordinatorResult.curatorResult.contains(curator.name)) {
      val curatorResult = requestCoordinatorResult.curatorResult(curator.name)
      val requestResult = requestCoordinatorResult.successResults(curator.name)
      val qpr = requestResult.queryPipelineResult
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

      //check if DruidRowCountFactCol is used
      val druidRowCountAlias = getDruidRowCountAlias(rowCountOption, requestResult)

      writeHeader(jsonGenerator
        , qpr.rowList.columns
        , curatorResult.requestModelReference.model.reportingRequest
        , ingestionTimeUpdater
        , tableName
        , dimCols
        , false
        , qpr.pagination
        , druidRowCountAlias
      )
      writeDataRows(jsonGenerator, qpr.rowList, None, curatorResult.requestModelReference.model.reportingRequest, druidRowCountAlias)
      if (!rowCountOption.isEmpty) {
        jsonGenerator.writeFieldName("rowCount")
        jsonGenerator.writeNumber(rowCountOption.get)
      }
      jsonGenerator.writeEndObject() //}
      jsonGenerator.writeEndObject() //}

    } else if(requestCoordinatorResult.failureResults.contains(curator.name)) {
      val curatorError = requestCoordinatorResult.failureResults(curator.name)
      if(requestCoordinatorResult.mahaRequestContext.reportingRequest.isDebugEnabled){
        JsonOutputFormat.logger.info(curatorError.toString)
      }

      jsonGenerator.writeFieldName(curatorError.curator.name) // "curatorName":
      jsonGenerator.writeStartObject() //{
      jsonGenerator.writeFieldName("error") // "error":
      jsonGenerator.writeStartObject() //{
      jsonGenerator.writeFieldName("message")
      jsonGenerator.writeString(curatorError.error.throwableOption.map(_.getMessage).filterNot(_ == null).getOrElse(curatorError.error.message))
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
                          , isDefault: Boolean
                          , pagination: Map[Engine, JValue]
                          , druidRowCountAlias: Option[String] = None
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


    columns.filterNot(c => druidRowCountAlias.getOrElse(None).equals(c.alias)).foreach {
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
    if (reportingRequest.includeRowCount) {
      jsonGenerator.writeStartObject() // {
      jsonGenerator.writeFieldName("fieldName") // "fieldName":
      jsonGenerator.writeString(JsonOutputFormat.ROW_COUNT)
      jsonGenerator.writeFieldName("fieldType") // "fieldType":

      jsonGenerator.writeString("CONSTANT")
      jsonGenerator.writeEndObject() // }

    }
    jsonGenerator.writeEndArray() // ]
    jsonGenerator.writeFieldName("maxRows")
    jsonGenerator.writeNumber(reportingRequest.rowsPerPage)
    if(reportingRequest.isDebugEnabled) {
      jsonGenerator.writeFieldName("debug")
      jsonGenerator.writeStartObject()
      if(isDefault && reportingRequest.isTestEnabled) {
        jsonGenerator.writeFieldName("testName")
        jsonGenerator.writeString(reportingRequest.getTestName.get)
      }
      if(isDefault && reportingRequest.hasLabels) {
        jsonGenerator.writeFieldName("labels")
        val labels = reportingRequest.getLabels
        jsonGenerator.writeStartArray()
        labels.foreach {
          label =>
            jsonGenerator.writeString(label)
        }
        jsonGenerator.writeEndArray()
      }
      jsonGenerator.writeEndObject()
    }
    if(pagination.nonEmpty) {
      jsonGenerator.writeFieldName("pagination")
      jsonGenerator.writeStartObject()
      pagination.foreach {
        case (engine, jvalue) =>
          import org.json4s.jackson.JsonMethods._
          jsonGenerator.writeFieldName(engine.toString)
          jsonGenerator.writeRawValue(compact(render(jvalue)))
      }
      jsonGenerator.writeEndObject()
    }
    jsonGenerator.writeEndObject()
  }

  private def writeDataRows(jsonGenerator: JsonGenerator, rowList: RowList, rowCountOption: Option[Int], reportingRequest:ReportingRequest, druidRowCountAlias: Option[String] = None): Unit = {
    jsonGenerator.writeFieldName("rows") // "rows":
    jsonGenerator.writeStartArray() // [
    val numColumns = rowList.columns.size
    val rowListSize: Option[Int] = rowList match {
      case inMemRowList: InMemRowList => Option(inMemRowList.size)
      case _ => None
    }

    rowList.foreach {
      row => {
        jsonGenerator.writeStartArray()
        var i = 0
        while(i < numColumns) {
          if(druidRowCountAlias.getOrElse(None) != None && rowList.columns(i).alias != druidRowCountAlias.get) {
            jsonGenerator.writeObject(row.getValue(i))
          }
          i+=1
        }
        if (reportingRequest.includeRowCount && rowCountOption.isDefined) {
          jsonGenerator.writeObject(rowCountOption.get)
        } else if(reportingRequest.includeRowCount && row.aliasMap.contains(QueryRowList.ROW_COUNT_ALIAS)) {
          jsonGenerator.writeObject(row.getValue(QueryRowList.ROW_COUNT_ALIAS))
        } else if(reportingRequest.includeRowCount && rowListSize.isDefined) {
              jsonGenerator.writeObject(rowListSize.get)
        }
        jsonGenerator.writeEndArray()
      }
    }
    jsonGenerator.writeEndArray() // ]
  }

  private def getDruidRowCountAlias(rowCountOption: Option[Int], requestResult: RequestResult): Option[String] = {
    if(!rowCountOption.isEmpty) {
      val initalFactBestCandidate = requestResult.queryPipelineResult.queryPipeline.factBestCandidate.get
      val rowCountName = initalFactBestCandidate.fact.factCols.filter {
        col => col.isInstanceOf[DruidRowCountFactCol]
      }.foldLeft(Some("")){(_, cur) => Some(cur.name)}
      val res = initalFactBestCandidate.publicFact.nameToAliasColumnMap(rowCountName.get)
      if(res.toSeq.size != 0) {
        Some(res.toSeq(0))
      } else None
    } else None
  }
}
