// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.executor.druid

import java.io.Closeable
import java.math.MathContext
import java.nio.charset.StandardCharsets
import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.asynchttpclient.Response
import com.yahoo.maha.core._
import com.yahoo.maha.core.fact.{FactColumn, TierUrl}
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.query.druid._
import grizzled.slf4j.Logging
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._

import scala.collection.concurrent.TrieMap
import scala.util.{Failure, Try}

/**
  * Created by hiral on 12/22/15.
  */

trait AuthHeaderProvider {
  def getAuthHeaders: Map[String, String]
}

class NoopAuthHeaderProvider extends AuthHeaderProvider {
  override def getAuthHeaders = Map.empty
}

case class DruidQueryExecutorConfig(maxConnectionsPerHost: Int
                                    , maxConnections: Int
                                    , connectionTimeout: Int
                                    , timeoutRetryInterval: Int
                                    , timeoutThreshold: Int
                                    , degradationConfigName: String
                                    , url: String, headers: Option[Map[String, String]] = None
                                    , readTimeout: Int
                                    , requestTimeout: Int
                                    , pooledConnectionIdleTimeout: Int
                                    , timeoutMaxResponseTimeInMs: Int
                                    , enableRetryOn500: Boolean
                                    , retryDelayMillis: Int
                                    , maxRetry: Int
                                    , enableFallbackOnUncoveredIntervals: Boolean = false
                                    , sslContextVersion: String = "TLSv1.2"
                                    , commaSeparatedCipherSuitesList: String = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384,TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,TLS_RSA_WITH_AES_128_GCM_SHA256,TLS_RSA_WITH_AES_256_GCM_SHA384,TLS_RSA_WITH_AES_128_CBC_SHA256,TLS_RSA_WITH_AES_256_CBC_SHA256,TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA"
                                    , allowPartialIfResultExceedsMaxRowLimit: Boolean = false
                                   )

object DruidQueryExecutor extends Logging {

  val timeStampFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").withZoneUTC()
  val DRUID_RESPONSE_CONTEXT = "X-Druid-Response-Context"
  val UNCOVERED_INTERVAL_VALUE = "uncoveredIntervals"

  implicit val formats = DefaultFormats

  val dateTimeFormatters = new TrieMap[String, DateTimeFormatter]()

  val mathContextCache = CacheBuilder
    .newBuilder()
    .maximumSize(100)
    .concurrencyLevel(Runtime.getRuntime.availableProcessors())
    .build(new CacheLoader[java.lang.Integer, MathContext]() {
      override def load(key: java.lang.Integer): MathContext = {
        new MathContext(key, java.math.RoundingMode.HALF_EVEN)
      }
    })

  def parseHelper(grain: Grain, row: Row, outputAlias: String, resultValue: JValue, aliasColumnMap: Map[String, Column], transformers: List[ResultSetTransformer]): Unit = {
    if (aliasColumnMap.contains(outputAlias)) {
      val column = aliasColumnMap(outputAlias)
      transformers.foreach(transformer => {
        if (transformer.canTransform(outputAlias, column)) {
          val transformedValue = extractField(resultValue)
          if (DruidQuery.replaceMissingValueWith.equals(transformedValue)) {
            throw new IllegalStateException(DruidQuery.replaceMissingValueWith)
          }
          val tv = transformer.transform(grain, outputAlias, column, transformedValue)
          row.addValue(outputAlias, tv)
        }
      })
    }
  }

  def processResult[T <: QueryRowList](query: Query
                                       , transformers: List[ResultSetTransformer]
                                       , getRow: List[JField] => Row
                                       , getEphemeralRow: List[JField] => Row
                                       , rowList: T
                                       , jsonString: String
                                       , eventObject: List[JField]
                                       , factsMappedToAlias: Boolean = true
                                      ): Unit = {
    val aliasColumnMap = query.aliasColumnMap
    val ephemeralAliasColumnMap = query.ephemeralAliasColumnMap
    val row = getRow(eventObject)
    val ephemeralRow: Option[Row] = if (!rowList.ephemeralColumnNames.isEmpty) {
      Option(getEphemeralRow(eventObject))
    } else {
      None
    }
    val resultAliasToOutputAliasMap = if (factsMappedToAlias) {
      aliasColumnMap.map { case (alias, col) => alias -> alias }
    } else {
      aliasColumnMap.map {
        case (alias, col) =>
          if (col.isInstanceOf[FactColumn]) {
            col.alias.getOrElse(col.name) -> alias
          } else {
            alias -> alias
          }
      }
    }
    eventObject.foreach {
      case (resultAlias, resultValue) =>
        if (resultAliasToOutputAliasMap.contains(resultAlias)) {
          val outputAlias = resultAliasToOutputAliasMap(resultAlias)
          if (rowList.columnNames.contains(outputAlias)) {
            parseHelper(query.queryContext.requestModel.queryGrain.getOrElse(DailyGrain),
              row, outputAlias, resultValue, aliasColumnMap, transformers)
          } else {
            if (query.queryContext.requestModel.isDebugEnabled) {
              info(s"Skipping result from druid which is not in columnNames : $resultAlias : $resultValue")
            }
          }
        } else if (rowList.ephemeralColumnNames.contains(resultAlias) && ephemeralRow.isDefined) {
          parseHelper(query.queryContext.requestModel.queryGrain.getOrElse(DailyGrain),
            ephemeralRow.get, resultAlias, resultValue, ephemeralAliasColumnMap, transformers)

        } else {
          if (query.queryContext.requestModel.isDebugEnabled) {
            info(s"Skipping result from druid which could not be mapped to outputAlias or ephemeral column names : $resultAlias : $resultValue")
          }
        }
    }
    try {
      rowList.addRow(row, ephemeralRow)
    } catch {
      case e: Exception =>
        error(s"Failed to add row to rowList $row, response json $jsonString,  Query: ${query.asString}")
        throw e
    }
  }

  def parseJsonAndPopulateResultSet[T <: QueryRowList](query: Query, response: Response, rowList: T, getRow: List[JField] => Row, getEphemeralRow: List[JField] => Row,
                                                       transformers: List[ResultSetTransformer], allowPartialIfResultExceedsMaxRowLimit:Boolean): Option[JValue] = {
    var pagination: Option[JValue] = None
    val jsonString: String = response.getResponseBody(StandardCharsets.UTF_8)

    if (query.queryContext.requestModel.isDebugEnabled) {
      info("received http response " + jsonString)
    }
    require(response.getStatusCode == 200, s"received status code from druid is ${response.getStatusCode} instead of 200 : $jsonString")

    val si: Int = query.queryContext.requestModel.startIndex
    val startIndex: Int = if (si < 0) {
      0
    } else {
      si
    }
    if (query.queryContext.requestModel.isDebugEnabled) {
      info(s"starIndex=$startIndex")
    }

    var rowsCount: Int = 0
    query match {
      case TimeseriesDruidQuery(_, _, _, _, _, _) =>
        val json = parse(jsonString)
        val grainFieldsAliasColumnMap: Map[String, Column] = query.aliasColumnMap.filter { case (alias, col) => Grain.grainFields(alias) }
        var injectedGrainFields: List[(String, JValue)] = List.empty
        json match {
          case JArray(rows) =>
            if (query.queryContext.requestModel.isDebugEnabled) {
              info(s"Timeseries rows.size=${rows.size}")
            }
            rows.foreach {
              case JObject(cols) =>
                rowsCount += 1
                cols.foreach {
                  case (alias, JString(sTimestamp)) if alias == "timestamp" && grainFieldsAliasColumnMap.nonEmpty =>
                    //need to inject timestamp
                    injectedGrainFields = grainFieldsAliasColumnMap.map {
                      case (grainAlias, col) =>
                        //we already know this grain alias exists
                        val formattedString: String = col.dataType match {
                          case DateType(format) if format.isDefined =>
                            val formatter = DateTransformer.getDateTimeFormatter(format.get)
                            formatter.print(timeStampFormatter.parseDateTime(sTimestamp))
                          case _ =>
                            Grain.getGrainByField(grainAlias).get.toFormattedString(timeStampFormatter.parseDateTime(sTimestamp))
                        }
                        (grainAlias, JString(formattedString))
                    }.toList
                  case (alias, jvalue) if alias == "result" =>
                    jvalue match {
                      case JObject(eventObject) =>
                        processResult(query, transformers, getRow, getEphemeralRow, rowList, jsonString, eventObject ++ injectedGrainFields)
                        injectedGrainFields = List.empty
                      case unmatched => throw new UnsupportedOperationException(s"Unexpected field in timeseries json response : $unmatched")
                    }
                  case other => //ignore
                    if (query.queryContext.requestModel.isDebugEnabled) {
                      info(s"unhandled field : $other")
                    }
                    throw new UnsupportedOperationException(s"Unexpected field in timeseries json response : $other")
                }
              case nonJobject => throw new UnsupportedOperationException(s"Unexpected field in timeseries json response : $nonJobject")
            }
          case other => throw new UnsupportedOperationException(s"Unexpected field in timeseries json response : $other")
        }

      case TopNDruidQuery(_, _, _, _, _, _) =>
        val json = parse(jsonString)
        json match {
          case JArray(rows) =>
            if (query.queryContext.requestModel.isDebugEnabled) {
              info(s"TopN rows.size=${rows.size}")
            }
            rows.foreach {
              case JObject(jobject) =>
                jobject.foreach {
                  case (alias, jvalue) if alias == "timestamp" => // TODO timestamp support
                  case (alias, jvalue) if alias == "result" =>
                    jvalue match {
                      case JArray(resultRows) =>
                        if (query.queryContext.requestModel.isDebugEnabled) {
                          info(s"results size=${resultRows.size}")
                        }
                        resultRows.drop(startIndex).foreach {
                          case JObject(eventObject) =>
                            rowsCount += 1
                            processResult(query, transformers, getRow, getEphemeralRow, rowList, jsonString, eventObject)
                          case other => throw new UnsupportedOperationException(s"Unexpected field in TopNDruidQuery json response : $other")
                        }
                      case other => throw new UnsupportedOperationException(s"Unexpected field in TopNDruidQuery json response : $other")
                    }
                  case other => //ignore
                    if (query.queryContext.requestModel.isDebugEnabled) {
                      info(s"unhandled field : $other")
                    }
                    throw new UnsupportedOperationException(s"Unexpected field in TopNDruidQuery json response : $other")
                }
              case other => throw new UnsupportedOperationException(s"Unexpected field in TopNDruidQuery json response : $other")
            }
          case other => throw new UnsupportedOperationException(s"Unexpected field in TopNDruidQuery json response : $other")
        }

      case GroupByDruidQuery(_, _, _, _, _, _, _) =>
        val json = parse(jsonString)
        json match {
          case JArray(rows) =>
            if (query.queryContext.requestModel.isDebugEnabled) {
              info(s"GroupBy rows.size=${rows.size}")
            }

            val dropStartIndexRows = rows.drop(startIndex)
            val len = dropStartIndexRows.size
            val maxRows = query.queryContext.requestModel.maxRows
            val finalRows = if (maxRows > 0 && len > maxRows) {
              dropStartIndexRows.dropRight(len - maxRows)
            } else dropStartIndexRows

            finalRows.foreach {
              case JObject(jobject) =>
                rowsCount += 1
                jobject.foreach {
                  case (alias, jvalue) if alias == "timestamp" || alias == "version" => //TODO timestamp support
                  case (alias, jvalue) if alias == "event" =>
                    jvalue match {
                      case JObject(eventObject) =>
                        processResult(query, transformers, getRow, getEphemeralRow, rowList, jsonString, eventObject)
                      case other => throw new UnsupportedOperationException(s"Unexpected field in GroupByDruidQuery json response : $other")
                    }
                  case other => //ignore
                    if (query.queryContext.requestModel.isDebugEnabled) {
                      info(s"unhandled field : $other")
                    }
                    throw new UnsupportedOperationException(s"Unexpected field in GroupByDruidQuery json response : $other")
                }
              case other => throw new UnsupportedOperationException(s"Unexpected field in GroupByDruidQuery json response : $other")
            }
          case other => throw new UnsupportedOperationException(s"Unexpected field in GroupByDruidQuery json response : $other")
        }
      case ScanDruidQuery(_, _, _, _, _, _) =>
        val json = parse(jsonString)
        json match {
          case JArray(rows) =>
            if (query.queryContext.requestModel.isDebugEnabled) {
              info(s"ScanQuery rows.size=${rows.size}")
            }
            rows.foreach {
              case JObject(jobject) =>
                jobject.foreach {
                  case (alias, jvalue) if alias == "timestamp" => // TODO timestamp support
                  case (alias, jvalue) if alias == "result" =>
                    jvalue match {
                      case JObject(resultFields) =>
                        resultFields.foreach {
                          case (rfAlias, rfJValue) if rfAlias == "pagingIdentifiers" =>
                            pagination = Option(JObject((rfAlias, rfJValue)))
                          case (rfAlias, rfJValue) if rfAlias == "events" =>
                            rfJValue match {
                              case JArray(eventsRows) =>
                                if (query.queryContext.requestModel.isDebugEnabled) {
                                  info(s"events size=${eventsRows.size}")
                                }
                                eventsRows.foreach {
                                  case JObject(eventsFields) =>
                                    eventsFields.foreach {
                                      case (efAlias, efJValue) if efAlias == "event" =>
                                        efJValue match {
                                          case JObject(eventObject) =>
                                            rowsCount += 1
                                            processResult(query, transformers, getRow, getEphemeralRow, rowList, jsonString, eventObject, factsMappedToAlias = false)
                                          case other => throw new UnsupportedOperationException(s"Unexpected field in SelectDruidQuery json response : $other")
                                        }
                                      case other =>
                                        if (query.queryContext.requestModel.isDebugEnabled) {
                                          info(s"ignore field from eventsFields : $other")
                                        }
                                    }
                                  case other => throw new UnsupportedOperationException(s"Unexpected field in SelectDruidQuery json response : $other")

                                }

                              case other => throw new UnsupportedOperationException(s"Unexpected field in SelectDruidQuery json response : $other")
                            }
                          case other => //ignore
                            if (query.queryContext.requestModel.isDebugEnabled) {
                              info(s"ignoring unhandled field : $other")
                            }
                        }
                      case other => throw new UnsupportedOperationException(s"Unexpected field in SelectDruidQuery json response : $other")
                    }
                  case other => //ignore
                    if (query.queryContext.requestModel.isDebugEnabled) {
                      info(s"unhandled field : $other")
                    }
                    throw new UnsupportedOperationException(s"Unexpected field in SelectDruidQuery json response : $other")
                }
              case other => throw new UnsupportedOperationException(s"Unexpected field in SelectDruidQuery json response : $other")
            }
          case other => throw new UnsupportedOperationException(s"Unexpected field in SelectDruidQuery json response : $other")
        }
      case other => throw new UnsupportedOperationException(s"Druid Query type not supported $other")
    }
    if (query.queryContext.requestModel.isDebugEnabled) {
      info(s"rowsCount=$rowsCount")
    }
    if (query.isInstanceOf[DruidQuery[_]]) {
      val druidQuery = query.asInstanceOf[DruidQuery[_]]
      if (!druidQuery.isPaginated &&  !allowPartialIfResultExceedsMaxRowLimit) {
        require(rowsCount < druidQuery.maxRows
          , s"Non paginated query fails rowsCount < maxRows, partial result possible : rowsCount=$rowsCount maxRows=${druidQuery.maxRows}")
      }
    }
    pagination
  }

  def extractField[T <: JValue](field: T): Any = {
    field match {
      case JBool(boolean) => boolean
      case JString(_) => field.values
      case JDecimal(_) => field.values
      case JInt(_) => field.values
      case JDouble(_) => field.values
      case JLong(_) => field.values
      case JNull | JNothing => null
      case _ => throw new UnsupportedOperationException("unsupported field type")
    }
  }
}

class DruidQueryExecutor(config: DruidQueryExecutorConfig, lifecycleListener: ExecutionLifecycleListener,
                         transformers: List[ResultSetTransformer] = ResultSetTransformer.DEFAULT_TRANSFORMS,
                         authHeaderProvider: AuthHeaderProvider = new NoopAuthHeaderProvider) extends QueryExecutor with Logging with Closeable {
  val engine: Engine = DruidEngine
  val httpUtils = new HttpUtils(ClientConfig
    .getConfig(
      config.maxConnectionsPerHost
      , config.maxConnections
      , config.connectionTimeout
      , config.timeoutRetryInterval
      , config.timeoutThreshold
      , config.degradationConfigName
      , config.readTimeout
      , config.requestTimeout
      , config.pooledConnectionIdleTimeout
      , config.timeoutMaxResponseTimeInMs
      , config.sslContextVersion
      , config.commaSeparatedCipherSuitesList
    )
    , config.enableRetryOn500
    , config.retryDelayMillis
    , config.maxRetry
  )
  var url = config.url

  override def close(): Unit = httpUtils.close()

  def checkUncoveredIntervals(query: Query, response: Response, config: DruidQueryExecutorConfig): Unit = {
    val requestModel = query.queryContext.requestModel
    val latestDate: DateTime = FilterDruid.getMaxDate(requestModel.utcTimeDayFilter, DailyGrain)

    val hasSingleHourFilter =
      requestModel.localTimeHourFilter.isDefined &&
        requestModel.localTimeHourFilter.get.asValues.split(",").distinct.length == 1 &&
        requestModel.utcTimeDayFilter.asValues.split(",").distinct.length == 1
    val hasSingleDayFilter =
      requestModel.utcTimeDayFilter.asValues.split(",").distinct.length == 1


    if (config.enableFallbackOnUncoveredIntervals
      && latestDate.isBeforeNow()
      && response.getHeaders().contains(DruidQueryExecutor.DRUID_RESPONSE_CONTEXT)
      && response.getHeader(DruidQueryExecutor.DRUID_RESPONSE_CONTEXT).contains(DruidQueryExecutor.UNCOVERED_INTERVAL_VALUE)) {
      logger.error(s"uncoveredIntervals Found: ${response.getHeader(DruidQueryExecutor.DRUID_RESPONSE_CONTEXT)} in source table : ${query.tableName}, query on single hour: $hasSingleHourFilter, query on single day: $hasSingleDayFilter")
    }
  }

  def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes): QueryResult[T] = {
    val acquiredQueryAttributes = lifecycleListener.acquired(query, queryAttributes)
    val debugEnabled = query.queryContext.requestModel.isDebugEnabled
    if (!acceptEngine(query.engine)) {
      throw new UnsupportedOperationException(s"DruidQueryExecutor does not support query with engine=${query.engine}")
    } else {
      val isFactDriven = query.queryContext.requestModel.isFactDriven
      if (isFactDriven && query.queryContext.asInstanceOf[FactQueryContext].factBestCandidate.fact.annotations.collect { case a: TierUrl => a }.nonEmpty)
        url = query.queryContext.asInstanceOf[FactQueryContext]
          .factBestCandidate.fact
          .annotations.filter(p => p.isInstanceOf[TierUrl]).head
          .asInstanceOf[TierUrl].url

      val headersWithAuthHeader = if (config.headers.isDefined) {
        Some(config.headers.get ++ authHeaderProvider.getAuthHeaders)
      } else {
        Some(authHeaderProvider.getAuthHeaders)
      }
      if (debugEnabled) {
        info(s"Running query : ${query.asString}")
        info(s"headersWithAuthHeader : ${headersWithAuthHeader}")
      }
      rowList match {
        case rl if rl.isInstanceOf[IndexedRowList] =>
          val irl = rl.asInstanceOf[IndexedRowList]
          val performJoin = irl.size > 0
          var pagination: Option[JValue] = null
          val result = Try {
            val response: Response = httpUtils.post(url, httpUtils.POST, headersWithAuthHeader, Some(query.asString))

            checkUncoveredIntervals(query, response, config)

            pagination = DruidQueryExecutor.parseJsonAndPopulateResultSet(query, response, irl, (fieldList: List[JField]) => {
              val indexName = irl.rowGrouping.indexAlias
              val fieldListMap = fieldList.toMap
              val rowSet = if (performJoin) {
                if (fieldListMap.contains(indexName)) {
                  val indexValue = fieldListMap.get(indexName).get
                  val field = DruidQueryExecutor.extractField(indexValue)
                  if (field == null) {
                    error(s"Druid has null value : ${response.getResponseBody()}")
                    Set(irl.newRow)
                  } else {
                    irl.getRowByIndex(RowGrouping(field.toString, List.empty))
                  }
                } else {
                  Set(irl.newRow)
                }
              } else {
                Set(irl.newRow)
              }
              if (rowSet.size > 0) {
                rowSet.head
              }
              else irl.newRow
            }, (fieldList: List[JField]) => {
              irl.newEphemeralRow
            }, transformers, config.allowPartialIfResultExceedsMaxRowLimit)


          }
          result match {
            case Failure(e) =>
              Try(lifecycleListener.failed(query, acquiredQueryAttributes, e))
              error(s"Exception occurred while executing druid query", e)
              QueryResult(rl, acquiredQueryAttributes, QueryResultStatus.FAILURE, Option(e))
            case _ =>
              if (debugEnabled) {
                info(s"rowList.size=${irl.size}, rowList.updatedSet=${irl.updatedSize}")
              }
              QueryResult(rl, acquiredQueryAttributes, QueryResultStatus.SUCCESS, pagination = pagination)
          }

        case rl if rl.isInstanceOf[QueryRowList] =>
          val qrl = rl.asInstanceOf[QueryRowList]
          var pagination: Option[JValue] = None
          val result = Try {
            val response = httpUtils.post(url, httpUtils.POST, headersWithAuthHeader, Some(query.asString))

            checkUncoveredIntervals(query, response, config)

            pagination = DruidQueryExecutor.parseJsonAndPopulateResultSet(query, response, qrl, (fieldList: List[JField]) => {
              qrl.newRow
            }, (fieldList: List[JField]) => {
              qrl.newEphemeralRow
            }, transformers, config.allowPartialIfResultExceedsMaxRowLimit)

          }
          result match {
            case Failure(e) =>
              Try(lifecycleListener.failed(query, acquiredQueryAttributes, e))
              error(s"Exception occurred while executing druid query", e)
              QueryResult(rl, acquiredQueryAttributes, QueryResultStatus.FAILURE, Option(e))
            case _ =>
              QueryResult(rl, lifecycleListener.completed(query, acquiredQueryAttributes), QueryResultStatus.SUCCESS, pagination = pagination)
          }
      }
    }
  }
}



