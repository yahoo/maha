// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.executor.druid

import java.io.Closeable
import java.math.MathContext
import java.nio.charset.StandardCharsets
import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.ning.http.client.Response
import com.yahoo.maha.core._
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.query.druid.{DruidQuery, GroupByDruidQuery, TimeseriesDruidQuery, TopNDruidQuery}
import grizzled.slf4j.Logging
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormatter
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._
import scala.collection.concurrent.TrieMap
import scala.util.{Failure, Try}

/**
 * Created by hiral on 12/22/15.
 */
case class DruidQueryExecutorConfig(maxConnectionsPerHost:Int
                                    , maxConnections:Int
                                    , connectionTimeout:Int
                                    , timeoutRetryInterval:Int
                                    , timeoutThreshold:Int
                                    , degradationConfigName:String
                                    , url: String ,headers : Option[Map[String, String]] = None
                                    , readTimeout:Int
                                    , requestTimeout:Int
                                    , pooledConnectionIdleTimeout:Int
                                    , timeoutMaxResponseTimeInMs:Int
                                    , enableRetryOn500:Boolean
                                    , retryDelayMillis:Int
                                    , maxRetry: Int
                                    , enableFallbackOnUncoveredIntervals: Boolean = false
                                     )

object DruidQueryExecutor extends Logging {

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

  def parseHelper(grain: Grain, row:Row,resultAlias:String,resultValue:JValue,aliasColumnMap:Map[String,Column], transformers: List[ResultSetTransformers]): Unit ={
    if(aliasColumnMap.contains(resultAlias)) {
      val column = aliasColumnMap(resultAlias)
      transformers.foreach(transformer => {
        if (transformer.canTransform(resultAlias, column)) {
          val transformedValue = extractField(resultValue)
          if(DruidQuery.replaceMissingValueWith.equals(transformedValue)) {
            throw new IllegalStateException(DruidQuery.replaceMissingValueWith)
          }
          val tv = transformer.transform(grain, resultAlias, column, transformedValue)
          row.addValue(resultAlias, tv)
        }
      })
    }
  }

  def parseJsonAndPopulateResultSet[T <: RowList](query:Query,response:Response,rowList: T, getRow: List[JField] => Row, getEphemeralRow: List[JField] => Row,
                                                  transformers: List[ResultSetTransformers]  ) : Unit ={
    val jsonString : String = response.getResponseBody(StandardCharsets.UTF_8.displayName())

    if(query.queryContext.requestModel.isDebugEnabled) {
      info("received http response " + jsonString)
    }
    require(response.getStatusCode == 200, s"received status code from druid is ${response.getStatusCode} instead of 200 : $jsonString")

    val aliasColumnMap = query.aliasColumnMap
    val ephemeralAliasColumnMap = query.ephemeralAliasColumnMap
    val si: Int = query.queryContext.requestModel.startIndex
    val startIndex: Int = if(si < 0) {
      0
    } else {
      si
    }

    query match {
      case TimeseriesDruidQuery(_,_,_,_) =>
        val json =parse(jsonString)
        json  match {
          case JArray(rows) =>
            if(query.queryContext.requestModel.isDebugEnabled) {
              info(s"Timeseries rows.size=${rows.size}")
            }
            rows.foreach {
              case JObject(cols) =>
                cols.foreach {
                  case (alias, jvalue) if alias == "timestamp" =>  // TODO timestamp support
                  case (alias, jvalue) if alias == "result" =>
                    jvalue match {
                      case JObject(resultCols) => val row = getRow(resultCols)
                        resultCols.foreach {
                          case (resultAlias, resultValue) =>
                            if(rowList.columnNames.contains(resultAlias)) {
                              parseHelper(query.queryContext.requestModel.queryGrain.getOrElse(DailyGrain),
                                row,resultAlias,resultValue,aliasColumnMap, transformers)
                            } else {
                              if(query.queryContext.requestModel.isDebugEnabled) {
                                info(s"Skipping result from druid which is not in columnNames : $resultAlias : $resultValue")
                              }
                            }
                        }
                        rowList.addRow(row)
                      case unmatched => throw new UnsupportedOperationException(s"Unexpected field in timeseries json response : $unmatched")
                    }
                  case a => throw new UnsupportedOperationException(s"Unexpected field in timeseries json response : $a")
                }
              case nonJobject => new UnsupportedOperationException(s"Unexpected field in timeseries json response : $nonJobject")
            }
          case other => throw new UnsupportedOperationException(s"Unexpected field in timeseries json response : $other")
        }

      case TopNDruidQuery(_,_,_,_)=>
        val json = parse(jsonString)
        json match {
          case JArray(rows) =>
            if(query.queryContext.requestModel.isDebugEnabled) {
              info(s"TopN rows.size=${rows.size}")
            }
            rows.drop(startIndex).foreach {
              case JObject(jobject) =>
                jobject.foreach{
                  case (alias, jvalue) if alias=="timestamp" =>  // TODO timestamp support
                  case (alias,jvalue) if alias=="result" =>
                    jvalue match{
                      case JArray(resultRows)  =>
                        resultRows.foreach{
                          case JObject(resultVal) =>
                            val row = getRow(resultVal)
                            resultVal.foreach{
                              case (resultAlias, resultValue) =>
                                if(rowList.columnNames.contains(resultAlias)) {
                                  parseHelper(query.queryContext.requestModel.queryGrain.getOrElse(DailyGrain),
                                    row,resultAlias,resultValue,aliasColumnMap, transformers)
                                } else {
                                  if(query.queryContext.requestModel.isDebugEnabled) {
                                    info(s"Skipping result from druid which is not in columnNames : $resultAlias : $resultValue")
                                  }
                                }
                              case other => throw new UnsupportedOperationException(s"Unexpected field in TopNDruidQuery json response : $other")
                            }
                            rowList.addRow(row)
                          case other => throw new UnsupportedOperationException(s"Unexpected field in TopNDruidQuery json response : $other")
                        }
                      case other => throw new UnsupportedOperationException(s"Unexpected field in TopNDruidQuery json response : $other")
                    }
                  case other => throw new UnsupportedOperationException(s"Unexpected field in TopNDruidQuery json response : $other")
                }
              case other => throw new UnsupportedOperationException(s"Unexpected field in TopNDruidQuery json response : $other")
            }
          case other => throw new UnsupportedOperationException(s"Unexpected field in TopNDruidQuery json response : $other")
        }

      case GroupByDruidQuery(_,_,_,_,_)=>
        val json = parse(jsonString)
        json match {
          case JArray(rows) =>
            if(query.queryContext.requestModel.isDebugEnabled) {
              info(s"GroupBy rows.size=${rows.size}")
            }
            rows.drop(startIndex).foreach {
              case JObject(jobject) =>
                jobject.foreach{
                  case (alias, jvalue) if (alias == "timestamp" || alias == "version") => //TODO timestamp support
                  case (alias,jvalue) if alias=="event" =>
                    jvalue match{
                      case JObject(eventObject)  =>
                        val row =getRow(eventObject)
                        val ephemeralRow: Option[Row] = if(!rowList.ephemeralColumnNames.isEmpty) {
                          Option(getEphemeralRow(eventObject))
                        } else {
                          None
                        }
                        eventObject.foreach{
                          case (resultAlias, resultValue) =>
                            if(rowList.columnNames.contains(resultAlias)) {
                              parseHelper(query.queryContext.requestModel.queryGrain.getOrElse(DailyGrain),
                                row, resultAlias, resultValue, aliasColumnMap, transformers)
                            } else if(rowList.ephemeralColumnNames.contains(resultAlias) && ephemeralRow.isDefined) {
                              parseHelper(query.queryContext.requestModel.queryGrain.getOrElse(DailyGrain),
                                ephemeralRow.get, resultAlias, resultValue, ephemeralAliasColumnMap, transformers)
                            } else {
                              if(query.queryContext.requestModel.isDebugEnabled) {
                                info(s"Skipping result from druid which is not in columnNames : $resultAlias : $resultValue")
                              }
                            }
                          case other => throw new UnsupportedOperationException(s"Unexpected field in GroupByDruidQuery json response : $other")
                        }
                        try {
                          rowList.addRow(row, ephemeralRow)
                        } catch {
                          case e: Exception =>
                            error(s"Failed to add row to rowList $row, response json $jsonString,  Query: ${query.asString}")
                            throw e
                        }
                      case other => throw new UnsupportedOperationException(s"Unexpected field in GroupByDruidQuery json response : $other")
                    }
                  case other => throw new UnsupportedOperationException(s"Unexpected field in GroupByDruidQuery json response : $other")
                }
              case other => throw new UnsupportedOperationException(s"Unexpected field in GroupByDruidQuery json response : $other")
            }
          case other => throw new UnsupportedOperationException(s"Unexpected field in GroupByDruidQuery json response : $other")
        }
      case other => new UnsupportedOperationException(s"Druid Query type not supported $other")
    }
  }

  def extractField[T<:JValue](field:T):Any ={
    field match{
      case JBool(boolean) => boolean
      case JString(_) => field.values
      case JDecimal(_) => field.values
      case JInt(_) => field.values
      case JDouble(_) => field.values
      case JLong(_) => field.values
      case JNull|JNothing => null
      case _ => throw new  UnsupportedOperationException("unsupported field type")
    }
  }
}

class DruidQueryExecutor(config:DruidQueryExecutorConfig , lifecycleListener: ExecutionLifecycleListener,
                         transformers: List[ResultSetTransformers] = ResultSetTransformers.DEFAULT_TRANSFORMS ) extends QueryExecutor with Logging with Closeable {
  val engine: Engine = DruidEngine
  val httpUtils = new HttpUtils(ClientConfig
    .getConfig(
      config.maxConnectionsPerHost
      ,config.maxConnections
      ,config.connectionTimeout
      ,config.timeoutRetryInterval
      ,config.timeoutThreshold
      ,config.degradationConfigName
      ,config.readTimeout
      ,config.requestTimeout
      ,config.pooledConnectionIdleTimeout
      ,config.timeoutMaxResponseTimeInMs
    )
    , config.enableRetryOn500
    , config.retryDelayMillis
    , config.maxRetry
  )
  val url = config.url

  val headers = config.headers


  override def close(): Unit = httpUtils.close()

  def checkUncoveredIntervals(query : Query, response : Response, config: DruidQueryExecutorConfig) : Unit = {
    val requestModel = query.queryContext.requestModel
    val latestDate : DateTime = FilterDruid.getMaxDate(requestModel.utcTimeDayFilter, DailyGrain)
    if (config.enableFallbackOnUncoveredIntervals
      && latestDate.isBeforeNow()
      && response.getHeaders().containsKey(DruidQueryExecutor.DRUID_RESPONSE_CONTEXT)
      && response.getHeader(DruidQueryExecutor.DRUID_RESPONSE_CONTEXT).contains(DruidQueryExecutor.UNCOVERED_INTERVAL_VALUE)) {
      val exception = new IllegalStateException("Druid data missing, identified in uncoveredIntervals")
      logger.error(s"uncoveredIntervals Found: ${response.getHeader(DruidQueryExecutor.DRUID_RESPONSE_CONTEXT)}")
      //throw exception // will add-back in a week, assuming few enough intervals are found.
    }
  }

  def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes) : QueryResult[T] = {
    val acquiredQueryAttributes = lifecycleListener.acquired(query, queryAttributes)
    val debugEnabled = query.queryContext.requestModel.isDebugEnabled
    if(!acceptEngine(query.engine)) {
      throw new UnsupportedOperationException(s"DruidQueryExecutor does not support query with engine=${query.engine}")
    }else {
      if (debugEnabled) {
        info(s"Running query : ${query.asString}")
      }
      rowList match {
        case rl if rl.isInstanceOf[IndexedRowList] =>
          val irl = rl.asInstanceOf[IndexedRowList]
          val isFactDriven = query.queryContext.requestModel.isFactDriven
          val performJoin = irl.size > 0
          val result = Try {
            val response : Response= httpUtils.post(url,httpUtils.POST,headers,Some(query.asString))

            val temp = checkUncoveredIntervals(query, response, config)

            DruidQueryExecutor.parseJsonAndPopulateResultSet(query,response,rl,(fieldList:List[JField] ) =>{
              val indexName =irl.indexAlias
              val fieldListMap = fieldList.toMap
              val rowSet = if(performJoin) {
                if(fieldListMap.contains(indexName)) {
                  val indexValue =fieldListMap.get(indexName).get
                  val field = DruidQueryExecutor.extractField(indexValue)
                  if (field == null) {
                    error(s"Druid has null value : ${response.getResponseBody()}")
                    Set(rowList.newRow)
                  } else {
                    irl.getRowByIndex(field)
                  }
                } else {
                  Set(rowList.newRow)
                }
              } else {
                Set(rowList.newRow)
              }
              if(rowSet.size > 0) {
                rowSet.head
              }
              else rowList.newRow
            }, (fieldList: List[JField]) =>{
              rowList.newEphemeralRow
            }, transformers)

          }
          result match{
            case Failure(e) =>
              error(s"Exception occurred while executing druid query", e)
              Try(lifecycleListener.failed(query, acquiredQueryAttributes, e))
              throw e
            case _ =>
              if (debugEnabled) {
                info(s"rowList.size=${irl.size}, rowList.updatedSet=${irl.updatedSize}")
              }
              QueryResult(rl, lifecycleListener.completed(query, acquiredQueryAttributes), QueryResultStatus.SUCCESS)
          }

        case rl =>
          val result = Try {
            val response = httpUtils.post(url,httpUtils.POST,headers,Some(query.asString))
            
            val temp = checkUncoveredIntervals(query, response, config)

            DruidQueryExecutor.parseJsonAndPopulateResultSet(query,response,rl,(fieldList: List[JField]) =>{
              rowList.newRow
            }, (fieldList: List[JField]) =>{
              rowList.newEphemeralRow
            }, transformers)

          }
          result match {
            case Failure(e) =>
              error(s"Exception occurred while executing druid query", e)
              Try(lifecycleListener.failed(query, acquiredQueryAttributes, e))
              e match {
                case ise: IllegalStateException => QueryResult(rl, lifecycleListener.completed(query, acquiredQueryAttributes), QueryResultStatus.FAILURE, ise.getMessage)
                case _ => throw e
              }
            case _ =>
              QueryResult(rl, lifecycleListener.completed(query, acquiredQueryAttributes), QueryResultStatus.SUCCESS)
          }
      }
    }
  }
}



