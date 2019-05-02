// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.executor.oracle

import java.sql.ResultSetMetaData

import com.yahoo.maha.core._
import com.yahoo.maha.core.query._
import com.yahoo.maha.jdbc.JdbcConnection
import grizzled.slf4j.Logging

import scala.util.{Failure, Try}

/**
 * Created by hiral on 12/22/15.
 */

class OracleQueryExecutor(jdbcConnection: JdbcConnection, lifecycleListener: ExecutionLifecycleListener) extends QueryExecutor with Logging {
  val engine: Engine = OracleEngine

  val columnValueExtractor = new ColumnValueExtractor

  def execute[T <: RowList](query: Query, rowList: T, queryAttributes: QueryAttributes) : QueryResult[T] = {
    val acquiredQueryAttributes = lifecycleListener.acquired(query, queryAttributes)
    val debugEnabled = query.queryContext.requestModel.isDebugEnabled
    if(!acceptEngine(query.engine)) {
      throw new UnsupportedOperationException(s"OracleQueryExecutor does not support query with engine=${query.engine}")
    } else {
      var rowCount = 0
      rowList match {
        //if we have a indexed row list that's populated, we need to perform join
        case rl if rl.isInstanceOf[IndexedRowList] =>
          //do default row processing
          val irl = rl.asInstanceOf[IndexedRowList]
          val columnIndexMap = new collection.mutable.HashMap[String, Int]
          val aliasColumnMap = query.aliasColumnMap
          var index = -1
          val indexColumn = aliasColumnMap(irl.rowGrouping.indexAlias)

          if(debugEnabled) {
            info(s"Running query : ${query.asString}")
          }
          val result : Try[Unit] = jdbcConnection.queryForObject(query.asString) { resultSet =>
            if (resultSet.next()) {
              var metaData: ResultSetMetaData = null
              if (metaData == null) {
                metaData = resultSet.getMetaData
                var count = 1
                while (count <= metaData.getColumnCount) {
                  //get alias
                  val alias = metaData.getColumnLabel(count)
                  if (alias == irl.rowGrouping.indexAlias) {
                    index = count
                    columnIndexMap += alias -> count
                  } else {
                    columnIndexMap += alias -> count
                  }
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


              do {
                //get existing index row or create new one
                val rowSet = {
                  val indexValue = columnValueExtractor.getColumnValue(index, indexColumn, resultSet)
                  val rowSet = irl.getRowByIndex(RowGrouping(indexValue.toString, List(indexValue.toString)))
                  //no row, create one
                  if(rowSet.isEmpty) {
                    val r = irl.newRow
                    r.addValue(irl.rowGrouping.indexAlias, indexValue)
                    irl.updateRow(r)
                    Set(r)
                  } else {
                    rowSet
                  }
                }

                val aliasValueMap =
                  for {
                    (alias, column) <- aliasColumnMap
                  } yield {
                    val index = columnIndexMap(alias)
                    val value = columnValueExtractor.getColumnValue(index, column, resultSet)
                    (alias, value)
                  }

                rowSet.foreach {
                  row =>
                    aliasValueMap.foreach {
                      entry =>
                        row.addValue(entry._1, entry._2)
                    }
                }
                rowCount += 1
              } while (resultSet.next())
              if(debugEnabled) {
                info(s"rowCount = $rowCount, rowList.size = ${irl.size}, rowList.updatedSize = ${irl.updatedSize}")
              }
            }
          }
          result match {
            case Failure(e) =>
              Try(lifecycleListener.failed(query, acquiredQueryAttributes, e))
              error(s"Failed query : ${query.asString}")
              QueryResult(rl, acquiredQueryAttributes, QueryResultStatus.FAILURE, Option(e))
            case _ =>
              QueryResult(rl, lifecycleListener.completed(query, acquiredQueryAttributes), QueryResultStatus.SUCCESS)
          }
        case rl if rl.isInstanceOf[QueryRowList] =>
          val qrl = rl.asInstanceOf[QueryRowList]
          var metaData : ResultSetMetaData = null
          val columnIndexMap = new collection.mutable.HashMap[String, Int]
          val aliasColumnMap = query.aliasColumnMap
          if(debugEnabled) {
            info(s"Running query : ${query.asString}")
          }
          val result : Try[Unit] = jdbcConnection.queryForObject(query.asString) { resultSet =>
            if (resultSet.next()) {
              if (metaData == null) {
                metaData = resultSet.getMetaData
                var count = 1
                while (count <= metaData.getColumnCount) {
                  //get alias
                  val alias = metaData.getColumnLabel(count)
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
                  val value = columnValueExtractor.getColumnValue(index, column, resultSet)
                  row.addValue(alias, value)
                }
                qrl.addRow(row)
                rowCount += 1
              } while (resultSet.next())
              if(debugEnabled) {
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
              QueryResult(rl, lifecycleListener.completed(query, acquiredQueryAttributes), QueryResultStatus.SUCCESS)
          }
      }
    }
  }
}
