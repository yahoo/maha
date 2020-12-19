// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.executor.bigquery

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import com.github.vertical_blank.sqlformatter.scala.{FormatConfig, SqlDialect, SqlFormatter}
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.ServiceOptions
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.JobId
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.QueryJobConfiguration
import com.yahoo.maha.core._
import com.yahoo.maha.core.query._
import grizzled.slf4j.Logging
import java.io.FileInputStream
import java.net.{Authenticator, PasswordAuthentication}
import java.util.UUID
import scala.util.Try

private case class ProxyCredentials(
  @JsonProperty("proxy_user") username: String,
  @JsonProperty("proxy_pass") password: String
)

case class BigqueryQueryExecutorConfig(
  gcpCredentialsFilePath: String,
  gcpProjectId: String,
  enableProxy: Boolean,
  proxyCredentialsFilePath: Option[String],
  proxyHost: Option[String],
  proxyPort: Option[String],
  retries: Int
) extends Logging {

  private final val HttpsProxyHost = "https.proxyHost"
  private final val HttpsProxyPort = "https.proxyPort"
  private final val DisabledAuthSchemes = "jdk.http.auth.tunneling.disabledSchemes"

  private val mapper = {
    val om = new ObjectMapper(new YAMLFactory()) with ScalaObjectMapper
    om.registerModule(DefaultScalaModule)
    om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    om
  }

  def buildBigqueryClient(): BigQuery = {
    val credentials = ServiceAccountCredentials.fromStream(new FileInputStream(gcpCredentialsFilePath))

    val bigqueryClient: BigQuery = BigQueryOptions
      .newBuilder()
      .setCredentials(credentials)
      .setProjectId(gcpProjectId)
      .setRetrySettings(ServiceOptions.getDefaultRetrySettings().toBuilder().setMaxAttempts(retries).build())
      .build()
      .getService

    bigqueryClient
  }

  def configureProxy(): Unit = {
    if (enableProxy && proxyCredentialsFilePath.isDefined && proxyHost.isDefined && proxyHost.isDefined) {
      val proxyCredentials = mapper.readValue[ProxyCredentials](
        new FileInputStream(proxyCredentialsFilePath.get)
      )

      System.setProperty(HttpsProxyHost, proxyHost.get)
      System.setProperty(HttpsProxyPort, proxyPort.get)
      System.setProperty(DisabledAuthSchemes, "")

      Authenticator.setDefault(new Authenticator() {
        override protected def getPasswordAuthentication =
          new PasswordAuthentication(proxyCredentials.username, proxyCredentials.password.toCharArray)
      })
    } else {
      info("Not using proxy for BigQuery Query Execution")
    }
  }
}

class BigqueryQueryExecutor(
  config: BigqueryQueryExecutorConfig,
  lifecycleListener: ExecutionLifecycleListener
) extends QueryExecutor
  with Logging {

  val engine: Engine = BigqueryEngine
  config.configureProxy()
  lazy val bigqueryClient = config.buildBigqueryClient()

  def execute[T <: RowList](
    query: Query,
    rowList: T,
    queryAttributes: QueryAttributes
  ): QueryResult[T] = {
    val acquiredQueryAttributes = lifecycleListener.acquired(query, queryAttributes)
    val debugEnabled = query.queryContext.requestModel.isDebugEnabled

    if (!acceptEngine(query.engine)) {
      throw new UnsupportedOperationException(
        s"BigqueryQueryExecutor does not support query with engine=${query.engine}"
      )
    }

    val formattedQuery = SqlFormatter.format(
      query.asString,
      FormatConfig(dialect = SqlDialect.StandardSQL)
    )

    if (debugEnabled) {
      info(s"Running query : $formattedQuery")
    }

    val queryJobConfig = QueryJobConfiguration
      .newBuilder(formattedQuery)
      .setUseLegacySql(false)
      .build()

    val jobId = JobId.of(UUID.randomUUID.toString)
    val jobInfo = JobInfo.newBuilder(queryJobConfig).setJobId(jobId).build
    val queryJob = bigqueryClient.create(jobInfo).waitFor()
    val aliasColumnMap = query.aliasColumnMap
    val queryColumns = query.asInstanceOf[BigqueryQuery].columnHeaders

    rowList match {
      case rl if rl.isInstanceOf[QueryRowList] =>
        val queryRowList = rl.asInstanceOf[QueryRowList]

        if (queryJob == null) {
          error(s"Failed query : $formattedQuery")
          val exception = new RuntimeException("BigQuery Job doesn't exist: " + jobId)
          Try(lifecycleListener.failed(query, acquiredQueryAttributes, exception))
          QueryResult(rl, acquiredQueryAttributes, QueryResultStatus.FAILURE, Option(exception))
        } else if (queryJob.getStatus.getError() != null) {
          error(s"Failed query : $formattedQuery")
          val exception = new RuntimeException("BigQuery Job Failed: " + jobId)
          Try(lifecycleListener.failed(query, acquiredQueryAttributes, exception))
          QueryResult(rl, acquiredQueryAttributes, QueryResultStatus.FAILURE, Option(exception))
        } else {
          val result = queryJob.getQueryResults()
          val resultColumns = result.getSchema().getFields()
          if (resultColumns.size != queryColumns.size)
            throw new RuntimeException("Mismatch in number of query and result columns")

          val rowCount = result.getTotalRows()
          if (debugEnabled) info(s"rowCount = $rowCount")

          result.iterateAll().forEach { row =>
            val resultRow = queryRowList.newRow
            for ((alias, column) <- aliasColumnMap) {
              val index = resultColumns.getIndex(alias)
              val result = row.get(index)
              val columnAlias = queryColumns(index)
              val processedResult = column.dataType match {
                case DateType(_) | TimestampType(_) =>
                  result.getStringValue
                case IntType(_, _, _, _, _) =>
                  result.getNumericValue
                case DecType(_, _, _, _, _, _) =>
                  result.getDoubleValue
                case StrType(_, _, _) =>
                  result.getStringValue
                case any => throw new UnsupportedOperationException(s"Unhandled data type for column value extraction : $any")
              }
              resultRow.addValue(columnAlias, processedResult)
            }
            queryRowList.addRow(resultRow)
          }
          QueryResult(rl, lifecycleListener.completed(query, acquiredQueryAttributes), QueryResultStatus.SUCCESS)
        }
      case _ => throw new RuntimeException("Unsupported RowList Type for BigQuery execution")
    }
  }
}
