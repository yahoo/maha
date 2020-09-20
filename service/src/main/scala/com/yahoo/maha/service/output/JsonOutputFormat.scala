// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.output

import java.io.OutputStream

import com.fasterxml.jackson.core.{JsonEncoding, JsonGenerator}
import com.fasterxml.jackson.databind.ObjectMapper
import com.yahoo.maha.core.query.{InMemRowList, QueryAttribute, QueryAttributes, QueryPipelineResult, QueryRowList, QueryStatsAttribute, RowList}
import com.yahoo.maha.core.request.{ReportingRequest, RowCountQuery}
import com.yahoo.maha.core.{ColumnInfo, DimColumnInfo, Engine, FactColumnInfo, RequestModelResult}
import com.yahoo.maha.service.{MahaRequestContext, RequestCoordinatorResult}
import com.yahoo.maha.service.curators.{Curator, CuratorError, CuratorResult, DefaultCurator, RowCountCurator}
import com.yahoo.maha.service.datasource.{IngestionTimeUpdater, NoopIngestionTimeUpdater}
import org.json4s.JValue
import org.slf4j.{Logger, LoggerFactory}

/**
 * Created by hiral on 4/11/18.
 */
object JsonOutputFormat {
  val objectMapper: ObjectMapper = new ObjectMapper()
  val logger: Logger = LoggerFactory.getLogger(classOf[JsonOutputFormat])
  val ROW_COUNT: String = "ROW_COUNT"
  val defaultRenderSet: Set[String] = Set(DefaultCurator.name)
}

trait DebugRenderer {
  def render(mahaRequestContext: MahaRequestContext, jsonGenerator: JsonGenerator): Unit
}

class NoopDebugRenderer extends DebugRenderer {
  def render(mahaRequestContext: MahaRequestContext, jsonGenerator: JsonGenerator): Unit = {}
}

case class JsonOutputFormat(requestCoordinatorResult: RequestCoordinatorResult
                            , ingestionTimeUpdaterMap: Map[Engine, IngestionTimeUpdater] = Map.empty
                            , debugRenderer: Option[DebugRenderer] = None
                           ) {


  def writeStream(outputStream: OutputStream): Unit = {
    val jsonGenerator: JsonGenerator = JsonOutputFormat.objectMapper.getFactory().createGenerator(outputStream, JsonEncoding.UTF8)
    jsonGenerator.writeStartObject() // {
    val headOption = requestCoordinatorResult.orderedList.headOption

    if (headOption.exists(_.isSingleton)) {
      renderDefault(headOption.get.name, requestCoordinatorResult, jsonGenerator, None)
    } else if (requestCoordinatorResult.successResults.contains(DefaultCurator.name)) {
      val rowCountOption = RowCountCurator.getRowCount(requestCoordinatorResult.mahaRequestContext)
      renderDefault(DefaultCurator.name, requestCoordinatorResult, jsonGenerator, rowCountOption)
      jsonGenerator.writeFieldName("curators") //"curators" :
      jsonGenerator.writeStartObject() //{
      //remove default render curators
      val curatorList = requestCoordinatorResult.orderedList.filterNot(c => JsonOutputFormat.defaultRenderSet(c.name))
      curatorList.foreach(renderCurator(_, requestCoordinatorResult, jsonGenerator))
      jsonGenerator.writeEndObject() //}
    }

    jsonGenerator.writeEndObject() // }
    jsonGenerator.flush()
    jsonGenerator.close()
  }

  private def renderDefault(curatorName: String, requestCoordinatorResult: RequestCoordinatorResult, jsonGenerator: JsonGenerator, rowCountOption: Option[Int]): Unit = {
    if (requestCoordinatorResult.successResults.contains(curatorName)) {
      val curatorAndRequestResult = requestCoordinatorResult.successResults(curatorName).head //only 1 result for default
      val requestResults = curatorAndRequestResult.requestResult
      val curatorResult = curatorAndRequestResult.curatorResult
      val qpr = requestResults.queryPipelineResult
      val engine = qpr.queryChain.drivingQuery.engine
      val tableName = qpr.queryChain.drivingQuery.tableName
      val ingestionTimeUpdater: IngestionTimeUpdater = ingestionTimeUpdaterMap
        .getOrElse(qpr.queryChain.drivingQuery.engine, NoopIngestionTimeUpdater(engine, engine.toString))
      val dimCols: Set[String] = if (curatorResult.requestModelReference.model.bestCandidates.isDefined) {
        curatorResult.requestModelReference.model.bestCandidates.get.publicFact.dimCols.map(_.alias)
      } else Set.empty
      val engineStats = qpr.queryAttributes.getAttributeOption(QueryAttributes.QueryStats)
      writeHeader(jsonGenerator
        , qpr.rowList.columns
        , curatorResult.requestModelReference.model.reportingRequest
        , ingestionTimeUpdater
        , tableName
        , dimCols
        , true
        , qpr.pagination
        , engineStats
      )
      writeDataRows(jsonGenerator, qpr.rowList, rowCountOption, curatorResult.requestModelReference.model.reportingRequest)
    }
  }

  private def renderCurator(curator: Curator, requestCoordinatorResult: RequestCoordinatorResult, jsonGenerator: JsonGenerator): Unit = {
    val curatorAndRequestResults = requestCoordinatorResult.successResults.getOrElse(curator.name, IndexedSeq.empty)
    val curatorErrors = requestCoordinatorResult.failureResults.getOrElse(curator.name, IndexedSeq.empty)
    val numResults = curatorAndRequestResults.size + curatorErrors.size

    if (numResults > 1) {
      jsonGenerator.writeFieldName(curator.name) // "curatorName":
      jsonGenerator.writeStartObject() //{
      if (curatorAndRequestResults.nonEmpty) {
        jsonGenerator.writeFieldName("results") // "results":
        jsonGenerator.writeStartArray(curatorAndRequestResults.size) //[
        curatorAndRequestResults.foreach {
          crr =>
            renderSuccessResult(jsonGenerator, crr.curatorResult.index, crr.requestResult.queryPipelineResult
              , crr.curatorResult.requestModelReference)
        }
        jsonGenerator.writeEndArray() //]
      }
      if (curatorErrors.nonEmpty) {
        jsonGenerator.writeFieldName("errors") // "errors":
        jsonGenerator.writeStartArray(curatorErrors.size) //[
        curatorErrors.foreach {
          ce =>
            renderFailureResult(jsonGenerator, ce, ce.index)
        }
        jsonGenerator.writeEndArray() //]
      }
      jsonGenerator.writeEndObject() //}

    } else {
      if (requestCoordinatorResult.successResults.contains(curator.name)) {
        val curatorAndRequestResult = curatorAndRequestResults.head //only 1 result possible
        val requestResult = curatorAndRequestResult.requestResult
        val curatorResult = curatorAndRequestResult.curatorResult
        val qpr = requestResult.queryPipelineResult
        jsonGenerator.writeFieldName(curatorResult.curator.name) // "curatorName":
        jsonGenerator.writeStartObject() //{
        jsonGenerator.writeFieldName("result") // "result":
        renderSuccessResult(jsonGenerator, None, qpr, curatorResult.requestModelReference)
        jsonGenerator.writeEndObject() //}

      } else if (requestCoordinatorResult.failureResults.contains(curator.name)) {
        val curatorError = curatorErrors.head //only 1 error possible
        if (requestCoordinatorResult.mahaRequestContext.reportingRequest.isDebugEnabled) {
          JsonOutputFormat.logger.info(curatorError.toString)
        }
        jsonGenerator.writeFieldName(curatorError.curator.name) // "curatorName":
        jsonGenerator.writeStartObject() //{
        jsonGenerator.writeFieldName("error") // "error":
        renderFailureResult(jsonGenerator, curatorError, None)
        jsonGenerator.writeEndObject() //}
      }
    }
  }

  private def renderSuccessResult(jsonGenerator: JsonGenerator, index: Option[Int]
                                  , qpr: QueryPipelineResult, requestModelReference: RequestModelResult): Unit = {
    val engine = qpr.queryChain.drivingQuery.engine
    val tableName = qpr.queryChain.drivingQuery.tableName
    val ingestionTimeUpdater: IngestionTimeUpdater = ingestionTimeUpdaterMap
      .getOrElse(qpr.queryChain.drivingQuery.engine, NoopIngestionTimeUpdater(engine, engine.toString))
    val dimCols: Set[String] = if (requestModelReference.model.bestCandidates.isDefined) {
      requestModelReference.model.bestCandidates.get.publicFact.dimCols.map(_.alias)
    } else Set.empty
    jsonGenerator.writeStartObject() //{
    if (index.isDefined) {
      jsonGenerator.writeFieldName("index")
      jsonGenerator.writeNumber(index.get)
    }
    val engineStats = qpr.queryAttributes.getAttributeOption(QueryAttributes.QueryStats)
    writeHeader(jsonGenerator
      , qpr.rowList.columns
      , requestModelReference.model.reportingRequest
      , ingestionTimeUpdater
      , tableName
      , dimCols
      , false
      , qpr.pagination
      , engineStats
    )
    writeDataRows(jsonGenerator, qpr.rowList, None, requestModelReference.model.reportingRequest)
    jsonGenerator.writeEndObject() //}
  }

  private def renderFailureResult(jsonGenerator: JsonGenerator
                                  , curatorError: CuratorError
                                  , index: Option[Int]): Unit = {
    if (requestCoordinatorResult.mahaRequestContext.reportingRequest.isDebugEnabled) {
      JsonOutputFormat.logger.info(curatorError.toString)
    }
    jsonGenerator.writeStartObject() //{
    if (index.isDefined) {
      jsonGenerator.writeFieldName("index")
      jsonGenerator.writeNumber(curatorError.index.get)
    }
    jsonGenerator.writeFieldName("message")
    jsonGenerator.writeString(curatorError.error.throwableOption.map(_.getMessage).filterNot(_ == null).getOrElse(curatorError.error.message))
    jsonGenerator.writeEndObject() //}
  }

  private def writeHeader(jsonGenerator: JsonGenerator
                          , columns: IndexedSeq[ColumnInfo]
                          , reportingRequest: ReportingRequest
                          , ingestionTimeUpdater: IngestionTimeUpdater
                          , tableName: String
                          , dimCols: Set[String]
                          , isDefault: Boolean
                          , pagination: Map[Engine, JValue]
                          , engineStatsOption: Option[QueryAttribute]
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
    if (reportingRequest.isDebugEnabled) {
      jsonGenerator.writeFieldName("debug")
      jsonGenerator.writeStartObject()
      if (isDefault && reportingRequest.isTestEnabled) {
        jsonGenerator.writeFieldName("testName")
        jsonGenerator.writeString(reportingRequest.getTestName.get)
      }
      if (isDefault && reportingRequest.hasLabels) {
        jsonGenerator.writeFieldName("labels")
        val labels = reportingRequest.getLabels
        jsonGenerator.writeStartArray()
        labels.foreach {
          label =>
            jsonGenerator.writeString(label)
        }
        jsonGenerator.writeEndArray()
      }
      if (engineStatsOption.isDefined) {
        val engineStats = engineStatsOption.get
        engineStats match {
          case QueryStatsAttribute(stats) =>
            jsonGenerator.writeFieldName("engineStats")
            jsonGenerator.writeStartArray()
            stats.getStats.foreach {
              s =>
                jsonGenerator.writeStartObject()
                jsonGenerator.writeFieldName("engine")
                jsonGenerator.writeString(s.engine.toString)
                jsonGenerator.writeFieldName("tableName")
                jsonGenerator.writeString(s.tableName)
                jsonGenerator.writeFieldName("queryTime")
                jsonGenerator.writeObject(s.endTime - s.startTime)
                jsonGenerator.writeEndObject()
            }
            jsonGenerator.writeEndArray()
        }
      }
      if(debugRenderer.isDefined) {
        debugRenderer.get.render(requestCoordinatorResult.mahaRequestContext, jsonGenerator)
      }
      jsonGenerator.writeEndObject()
    }
    if (pagination.nonEmpty) {
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

  private def writeDataRows(jsonGenerator: JsonGenerator, rowList: RowList, rowCountOption: Option[Int], reportingRequest: ReportingRequest): Unit = {
    jsonGenerator.writeFieldName("rows") // "rows":
    jsonGenerator.writeStartArray() // [
    val numColumns = rowList.columns.size
    val rowListSize: Option[Int] = rowList match {
      case inMemRowList: InMemRowList => Option(inMemRowList.size)
      case _ => None
    }

    if (reportingRequest.queryType == RowCountQuery && rowList.isEmpty) {
      jsonGenerator.writeStartArray()
      jsonGenerator.writeObject(0)
      jsonGenerator.writeEndArray()
    } else {
      rowList.foreach {
        row => {
          jsonGenerator.writeStartArray()
          var i = 0
          while (i < numColumns) {
            jsonGenerator.writeObject(row.getValue(i))
            i += 1
          }
          if (reportingRequest.includeRowCount && rowCountOption.isDefined) {
            jsonGenerator.writeObject(rowCountOption.get)
          } else if (reportingRequest.includeRowCount && row.aliasMap.contains(QueryRowList.ROW_COUNT_ALIAS)) {
            jsonGenerator.writeObject(row.getValue(QueryRowList.ROW_COUNT_ALIAS))
          } else if (reportingRequest.includeRowCount && rowListSize.isDefined) {
            jsonGenerator.writeObject(rowListSize.get)
          }
          jsonGenerator.writeEndArray()
        }
      }
    }
    jsonGenerator.writeEndArray() // ]
  }

}
