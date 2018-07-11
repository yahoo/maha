// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import com.yahoo.maha.core.request._
import com.yahoo.maha.executor.druid.DruidQueryExecutorConfig
import com.yahoo.maha.service.{MahaServiceConfig, MahaServiceConfigContext}
import com.yahoo.maha.service.MahaServiceConfig.MahaConfigResult
import org.json4s.JValue
import scalaz.syntax.applicative._


/**
 * Created by pranavbhole on 01/06/17.
 */
class DefaultDruidQueryExecutorConfigFactory extends DruidQueryExecutorConfigFactory {

  """
    |{
    |"maxConnectionsPerHost" : 100,
    |"maxConnections" : 10000,
    |"connectionTimeout": 140000,
    |"timeoutRetryInterval" : 100,
    |"timeoutThreshold" : 9000,
    |"degradationConfigName" : "TestConfig",
    |"url" : "http://broker.druid.test.maha.com",
    |"headers": [{"key" : "value"}],
    |"readTimeout" : 10000,
    |"requestTimeout" : 10000,
    |"pooledConnectionIdleTimeout" : 10000,
    |"timeoutMaxResponseTimeInMs" : 30000,
    |"enableRetryOn500" : true,
    |"retryDelayMillis" : 1000,
    |"maxRetry" : 3,
    |"enableFallbackOnUncoveredIntervals" : false
    |}
  """.stripMargin

  override def fromJson(configJson: JValue)(implicit context: MahaServiceConfigContext): MahaConfigResult[DruidQueryExecutorConfig] =  {
    import org.json4s.scalaz.JsonScalaz._
    val maxConnectionsPerHostResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("maxConnectionsPerHost")(configJson)
    val maxConnectionsResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("maxConnections")(configJson)
    val connectionTimeoutResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("connectionTimeout")(configJson)
    val timeoutRetryIntervalResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("timeoutRetryInterval")(configJson)
    val timeoutThresholdResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("timeoutThreshold")(configJson)
    val degradationConfigNameResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("degradationConfigName")(configJson)
    val urlResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("url")(configJson)
    val headersOptionResult: MahaServiceConfig.MahaConfigResult[Option[Map[String, String]]] = fieldExtended[Option[Map[String, String]]]("headers")(configJson)
    val readTimeoutResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("readTimeout")(configJson)
    val requestTimeoutResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("requestTimeout")(configJson)
    val pooledConnectionIdleTimeoutResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("pooledConnectionIdleTimeout")(configJson)
    val timeoutMaxResponseTimeInMsResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("timeoutMaxResponseTimeInMs")(configJson)
    val enableRetryOn500Result: MahaServiceConfig.MahaConfigResult[Boolean] = fieldExtended[Boolean]("enableRetryOn500")(configJson)
    val retryDelayMillisResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("retryDelayMillis")(configJson)
    val maxRetryResult: MahaServiceConfig.MahaConfigResult[Int] = fieldExtended[Int]("maxRetry")(configJson)
    val enableFallbackOnUncoveredIntervalsResult : MahaServiceConfig.MahaConfigResult[Boolean] = fieldExtended[Boolean]("enableFallbackOnUncoveredIntervals")(configJson)

    val tupleLeftResult = (maxConnectionsPerHostResult |@| maxConnectionsResult |@| connectionTimeoutResult |@| timeoutRetryIntervalResult
      |@| timeoutThresholdResult |@| degradationConfigNameResult |@| urlResult |@| headersOptionResult) {
       (maxConnectionsPerHost, maxConnections, connectionTimeout, timeoutRetryInterval
       , timeoutThreshold, degradationConfigName, url, headersOption) => (maxConnectionsPerHost, maxConnections, connectionTimeout, timeoutRetryInterval
       , timeoutThreshold, degradationConfigName, url, headersOption)
      }
    val tupleRightResult = (readTimeoutResult |@| requestTimeoutResult |@| pooledConnectionIdleTimeoutResult |@| timeoutMaxResponseTimeInMsResult
      |@|  enableRetryOn500Result |@| retryDelayMillisResult |@| maxRetryResult |@| enableFallbackOnUncoveredIntervalsResult) {
      (readTimeout, requestTimeout, pooledConnectionIdleTimeout, timeoutMaxResponseTimeInMs,
       enableRetryOn500, retryDelayMillis, maxRetry, enableFallbackOnUncoveredIntervals) => (readTimeout, requestTimeout, pooledConnectionIdleTimeout, timeoutMaxResponseTimeInMs,
       enableRetryOn500, retryDelayMillis, maxRetry, enableFallbackOnUncoveredIntervals)
      }

    (tupleLeftResult |@| tupleRightResult) {
     case ((maxConnectionsPerHost, maxConnections, connectionTimeout, timeoutRetryInterval
       , timeoutThreshold, degradationConfigName, url, headersOption),(readTimeout, requestTimeout, pooledConnectionIdleTimeout, timeoutMaxResponseTimeInMs,
       enableRetryOn500, retryDelayMillis, maxRetry, enableFallbackOnUncoveredIntervals)) =>

     new DruidQueryExecutorConfig(maxConnectionsPerHost,
      maxConnections,
      connectionTimeout,
      timeoutRetryInterval,
      timeoutThreshold,
      degradationConfigName,
      url,
      headersOption,
      readTimeout,
      requestTimeout,
      pooledConnectionIdleTimeout,
      timeoutMaxResponseTimeInMs,
      enableRetryOn500,
      retryDelayMillis,
      maxRetry, enableFallbackOnUncoveredIntervals)
    }
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}
