// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.executor.bigquery

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import com.github.vertical_blank.sqlformatter.scala.{FormatConfig, SqlDialect, SqlFormatter}
import com.google.api.client.http.apache.v2.ApacheHttpTransport
import com.google.api.services.bigquery.Bigquery
import com.google.auth.http.HttpTransportFactory
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.ServiceOptions
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.JobId
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.http.HttpTransportOptions
import com.yahoo.maha.core._
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.request.{Parameter, RequestIdValue}
import grizzled.slf4j.Logging
import java.io.FileInputStream
import java.util.UUID
import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.{BasicCredentialsProvider, DefaultHttpRequestRetryHandler, ProxyAuthenticationStrategy}
import org.apache.http.impl.conn.DefaultProxyRoutePlanner
import scala.util.Try

private case class ProxyCredentials(
  @JsonProperty("proxy_user") userName: String,
  @JsonProperty("proxy_pass") password: String
)

case class BigqueryQueryExecutorConfig(
  gcpCredentialsFilePath: String,
  gcpProjectId: String,
  enableProxy: Boolean,
  proxyCredentialsFilePath: Option[String],
  proxyHost: Option[String],
  proxyPort: Option[String],
  disableRpc: Option[Boolean],
  connectionTimeoutMs: Int,
  readTimeoutMs: Int,
  retries: Int
) extends Logging {

  private val mapper = {
    val om = new ObjectMapper(new YAMLFactory()) with ScalaObjectMapper
    om.registerModule(DefaultScalaModule)
    om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    om
  }

  def buildBigqueryClient(): Option[BigQuery] = {
    if (!disableRpc.getOrElse(false)) {
      val builder = BigQueryOptions
        .newBuilder()
        .setProjectId(gcpProjectId)
        .setRetrySettings(
          ServiceOptions
            .getDefaultRetrySettings()
            .toBuilder()
            .setMaxAttempts(retries)
            .build()
        )

      val fileInputStream = new FileInputStream(gcpCredentialsFilePath)
      val bigqueryClient =
        try {
          googleApiHttpTransportFactory()
            .fold(builder.setCredentials(ServiceAccountCredentials.fromStream(fileInputStream))) {
              proxyTransport =>
                builder
                  .setTransportOptions(
                    HttpTransportOptions
                      .newBuilder()
                      .setHttpTransportFactory(proxyTransport)
                      .setConnectTimeout(connectionTimeoutMs)
                      .setReadTimeout(readTimeoutMs)
                      .build()
                  )
                  .setCredentials(ServiceAccountCredentials.fromStream(fileInputStream, proxyTransport))
            }.build().getService
        } finally {
          fileInputStream.close()
        }
      Some(bigqueryClient)
    } else None
  }

  private[this] def googleApiHttpTransportFactory(): Option[HttpTransportFactory] = {
    if (enableProxy && proxyCredentialsFilePath.isDefined && proxyHost.isDefined && proxyHost.isDefined) {
      info(s"Setting up proxied Google API Http Transport")
      val fileInputStream = new FileInputStream(proxyCredentialsFilePath.get)
      try {
        val proxyCredentials = mapper.readValue[ProxyCredentials](fileInputStream)
        val proxyHostDetails = new HttpHost(proxyHost.get, proxyPort.get.toInt)
        val proxyRoutePlanner = new DefaultProxyRoutePlanner(proxyHostDetails);
        val credentialsProvider = new BasicCredentialsProvider
        credentialsProvider.setCredentials(
          new AuthScope(proxyHostDetails.getHostName, proxyHostDetails.getPort),
          new UsernamePasswordCredentials(
            proxyCredentials.userName,
            proxyCredentials.password
          )
        )
        val mHttpClient = ApacheHttpTransport.newDefaultHttpClientBuilder
          .setRoutePlanner(proxyRoutePlanner)
          .setRetryHandler(new DefaultHttpRequestRetryHandler)
          .setProxyAuthenticationStrategy(ProxyAuthenticationStrategy.INSTANCE)
          .setDefaultCredentialsProvider(credentialsProvider)
          .build()

        val mHttpTransport = new ApacheHttpTransport(mHttpClient)
        Some(() => mHttpTransport)
      } finally {
        fileInputStream.close()
      }
    } else None
  }
}

class BigqueryQueryExecutor(
  config: BigqueryQueryExecutorConfig,
  lifecycleListener: ExecutionLifecycleListener
) extends QueryExecutor
  with Logging {

  val engine: Engine = BigqueryEngine
  val bigqueryClient: Option[BigQuery] = config.buildBigqueryClient()

  def execute[T <: RowList](
    query: Query,
    rowList: T,
    queryAttributes: QueryAttributes
  ): QueryResult[T] = {
    val acquiredQueryAttributes = lifecycleListener.acquired(query, queryAttributes)
    val requestModel = query.queryContext.requestModel
    val debugEnabled = requestModel.isDebugEnabled

    if (!acceptEngine(query.engine)) {
      throw new UnsupportedOperationException(
        s"BigqueryQueryExecutor does not support query with engine=${query.engine}"
      )
    }

    if (!bigqueryClient.isDefined) {
      throw new UnsupportedOperationException(
        s"BigqueryQueryExecutor cannot execute queries when RPC calls are disabled"
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

    val requestId = requestModel
      .additionalParameters
      .getOrElse(Parameter.RequestId, RequestIdValue(UUID.randomUUID().toString))
      .asInstanceOf[RequestIdValue]
      .value

    val jobId = JobId.of(requestId)
    val jobInfo = JobInfo.newBuilder(queryJobConfig).setJobId(jobId).build
    val queryJob = bigqueryClient.get.create(jobInfo).waitFor()
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

          if (debugEnabled) info(s"rowCount = ${result.getTotalRows()}")

          result.iterateAll().forEach { row =>
            val resultRow = queryRowList.newRow
            for ((alias, column) <- aliasColumnMap) {
              val index = resultColumns.getIndex(alias)
              val columnValue = row.get(index)
              val columnAlias = queryColumns(index)
              val processedResult = column.dataType match {
                case DateType(_) | TimestampType(_) =>
                  columnValue.getStringValue
                case IntType(_, _, _, _, _) =>
                  columnValue.getNumericValue
                case DecType(_, _, _, _, _, _) =>
                  columnValue.getDoubleValue
                case StrType(_, _, _, _) =>
                  columnValue.getStringValue
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
