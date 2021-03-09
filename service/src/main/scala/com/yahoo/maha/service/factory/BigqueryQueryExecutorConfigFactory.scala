// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import com.yahoo.maha.executor.bigquery.BigqueryQueryExecutorConfig
import com.yahoo.maha.service.MahaServiceConfig.MahaConfigResult
import com.yahoo.maha.service.request._
import com.yahoo.maha.service.{MahaServiceConfig, MahaServiceConfigContext}
import org.json4s.JValue
import scalaz.syntax.applicative._

class DefaultBigqueryQueryExecutorConfigFactory extends BigqueryQueryExecutorConfigFactory {
  """
    |{
    |"gcpCredentialsFilePath": "/path/to/credentials/file",
    |"gcpProjectId": "testProjectId",
    |"enableProxy": true,
    |"proxyCredentialsFilePath": "/path/to/proxy/file",
    |"proxyHost": "test.proxy.host.com",
    |"proxyPort": "3128",
    |"disableRpc": false,
    |"connectionTimeoutMs": 30000,
    |"readTimeoutMs": 60000,
    |"retries": 5
    |}
  """.stripMargin

  override def fromJson(configJson: JValue)(implicit context: MahaServiceConfigContext): MahaConfigResult[BigqueryQueryExecutorConfig] =  {
    import org.json4s.scalaz.JsonScalaz._

    val gcpCredentialsFilePathResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("gcpCredentialsFilePath")(configJson)
    val gcpProjectIdResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("gcpProjectId")(configJson)
    val enableProxyResult: MahaServiceConfig.MahaConfigResult[Boolean] = fieldExtended[Boolean]("enableProxy")(configJson)
    val proxyCredentialsFilePathResult: MahaServiceConfig.MahaConfigResult[Option[String]] = fieldExtended[Option[String]]("proxyCredentialsFilePath")(configJson)
    val proxyHostResult: MahaServiceConfig.MahaConfigResult[Option[String]] = fieldExtended[Option[String]]("proxyHost")(configJson)
    val proxyPortResult: MahaServiceConfig.MahaConfigResult[Option[String]] = fieldExtended[Option[String]]("proxyPort")(configJson)
    val disableRpcResult: MahaServiceConfig.MahaConfigResult[Option[Boolean]] = fieldExtended[Option[Boolean]]("disableRpc")(configJson)
    val connectionTimeoutMsResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("connectionTimeoutMs")(configJson)
    val readTimeoutMsResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("readTimeoutMs")(configJson)
    val retriesResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("retries")(configJson)

    (gcpCredentialsFilePathResult |@| gcpProjectIdResult |@| enableProxyResult |@|
      proxyCredentialsFilePathResult |@| proxyHostResult |@| proxyPortResult |@|
      disableRpcResult |@| connectionTimeoutMsResult |@| readTimeoutMsResult |@| retriesResult) {
      case (
        gcpCredentialsFilePath,
        gcpProjectId,
        enableProxy,
        proxyCredentialsFilePath,
        proxyHost,
        proxyPort,
        disableRpc,
        connectionTimeoutMs,
        readTimeoutMs,
        retries
      ) =>
        BigqueryQueryExecutorConfig(
          gcpCredentialsFilePath,
          gcpProjectId,
          enableProxy,
          proxyCredentialsFilePath,
          proxyHost,
          proxyPort,
          disableRpc,
          connectionTimeoutMs,
          readTimeoutMs,
          retries
        )
    }
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}
