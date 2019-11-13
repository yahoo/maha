// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.executor.presto

import java.sql.{ResultSetMetaData}
import com.yahoo.maha.core._
import com.yahoo.maha.core.query.{ResultSetTransformer, _}
import com.yahoo.maha.jdbc.JdbcConnection
import grizzled.slf4j.Logging

import scala.util.{Failure, Try}

trait PrestoQueryTemplate {
  def buildFinalQuery(query: String,  queryContext: QueryContext, queryAttributes: QueryAttributes): String
}

class PrestoQueryExecutor(jdbcConnection: JdbcConnection,
                          prestoQueryTemplate: PrestoQueryTemplate,
                          lifecycleListener: ExecutionLifecycleListener,
                          transformers: List[ResultSetTransformer] = ResultSetTransformer.DEFAULT_TRANSFORMS) extends QueryExecutor with Logging {
  val engine: Engine = PrestoEngine

  val columnValueExtractor = new ColumnValueExtractor

  def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes) : QueryResult[T] = {
    val acquiredQueryAttributes = lifecycleListener.acquired(query, queryAttributes)
    val debugEnabled = query.queryContext.requestModel.isDebugEnabled
    if(!acceptEngine(query.engine)) {
      throw new UnsupportedOperationException(s"PrestoQueryExecutor does not support query with engine=${query.engine}")
    } else {
      var rowCount = 0
      rowList match {
        case rl if rl.isInstanceOf[QueryRowList] =>
          val qrl = rl.asInstanceOf[QueryRowList]
          var metaData: ResultSetMetaData = null
          val columnIndexMap = new collection.mutable.HashMap[String, Int]
          val aliasColumnMap = query.aliasColumnMap
          val finalQuery = prestoQueryTemplate.buildFinalQuery(query.asString, query.queryContext, queryAttributes)
          if (debugEnabled) {
            info(s"Running query : $finalQuery")
          }
          val result: Try[Unit] = jdbcConnection.queryForObject(finalQuery) { resultSet =>
            if (resultSet.next()) {
              if (metaData == null) {
                metaData = resultSet.getMetaData
                var count = 1
                while (count <= metaData.getColumnCount) {
                  //get alias
                  val alias = metaData.getColumnLabel(count).toLowerCase
                  columnIndexMap += alias -> count
                  if (debugEnabled) {
                    info(s"metaData index=$count label=$alias")
                  }
                  count += 1
                }
                if (debugEnabled) {
                  info(s"columnIndexMap from metaData ${columnIndexMap.toList.sortBy(_._1).mkString("\n", "\n", "\n")}")
                  info(s"aliasColumnMap from query ${aliasColumnMap.mapValues(_.name).toList.sortBy(_._1).mkString("\n", "\n", "\n")}")
                }
              }

              //process all columns
              do {
                val row = qrl.newRow
                for {
                  (alias, column) <- aliasColumnMap
                } {
                  val index = columnIndexMap(alias)
                  if (debugEnabled) {
                    info(s"Raw value for column: ${column.name} at index: $index: ${resultSet.getObject(index)}")
                  }
                  val value = columnValueExtractor.getColumnValue(index, column, resultSet)
                  var transformedValue = value
                  transformers.foreach(transformer => {
                    val grain = query.queryContext.requestModel.queryGrain.getOrElse(DailyGrain)
                    val columnAlias = query.asInstanceOf[PrestoQuery].columnHeaders.apply(index-1)
                    if (transformer.canTransform(columnAlias, column)) {
                      transformedValue = transformer.transform(grain, columnAlias, column, transformedValue)
                    }
                  })
                  row.addValue(index-1, transformedValue)
                }
                qrl.addRow(row)
                rowCount += 1
              } while (resultSet.next())
              if (debugEnabled) {
                info(s"rowCount = $rowCount")
              }
            }
          }
          result match {
            case Failure(e) =>
              Try(lifecycleListener.failed(query, acquiredQueryAttributes, e))
              error(s"Failed query : ${query.asString}")
              QueryResult(rl, acquiredQueryAttributes, QueryResultStatus.FAILURE, Option(e))
            case _ =>
              if (debugEnabled) {
                info(s"Successfully retrieved results from Presto: $rowCount")
              }
              QueryResult(rowList, lifecycleListener.completed(query, acquiredQueryAttributes), QueryResultStatus.SUCCESS)
          }
      }
    }
  }
}
