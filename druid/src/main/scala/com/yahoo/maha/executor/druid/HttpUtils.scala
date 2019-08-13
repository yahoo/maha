// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.executor.druid

/**
 * Created by vivekch on 3/2/16.
 */

import java.io.Closeable
import javax.net.ssl.SSLContext

import com.ning.http.client.{AsyncHttpClient, AsyncHttpClientConfig, Response}
import com.yahoo.maha.executor.druid.filters.TimeoutThrottlingFilter
import grizzled.slf4j.Logging
import org.apache.http.HttpHeaders
import org.apache.http.entity.ContentType;

class HttpUtils(config:AsyncHttpClientConfig, enableRetryOn500: Boolean, retryDelayMillis: Int, maxRetry: Int) extends Logging with Closeable {

  sealed trait RequestMethod
  case object GET extends RequestMethod
  case object POST extends RequestMethod
  private[this] val client = new AsyncHttpClient(config)
  private[this] def sendRequest(url:String
                                , method:RequestMethod
                                ,headers:Option[Map[String, String]] = None
                                ,payload:Option[String] = None
                                , debugEnabled: Boolean = false
                                 ):Response = {

    def execute(): Response = {
      val requestBuilder: AsyncHttpClient#BoundRequestBuilder = method match {
        case GET =>
          client.prepareGet(url)
        case POST =>
          if (payload.isDefined) {
            client.preparePost(url).setBody(payload.get)
          } else {
            client.preparePost(url)
          }
        case a => throw new UnsupportedOperationException(s"Unsupported method : $a")
      }

      if (headers.isDefined) {
        val map = headers.get
        for ((key, value) <- map) {
          requestBuilder.addHeader(key, value)
        }
      }
      requestBuilder.addHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString)
      val request = requestBuilder.build()
      requestBuilder.execute().get()
    }

    var tryExecute = execute()
    var count = 0
    var retry = false
    do {
      if (tryExecute.getStatusCode == 500 && enableRetryOn500) {
        if(count < maxRetry) {
          count += 1
          error(s"Got 500 response, retrying, retry count=$count : ${tryExecute.getResponseBody}")
          Thread.sleep(retryDelayMillis)
          tryExecute = execute()
          retry = true
        } else {
          retry = false
        }
      } else {
        retry = false
      }
    } while(retry)
    tryExecute
  }


  override def close(): Unit = client.closeAsynchronously()

  def get(url:String,method:RequestMethod,headers:Option[Map[String, String]] = None ): Response =
  {
    sendRequest(url,method,headers)
  }

  def post(url:String
           , method:RequestMethod
           ,headers:Option[Map[String, String]] = None
           ,payload:Option[String] = None): Response ={
    sendRequest(url,method,headers,payload)
  }

}

object ClientConfig{

  def getConfig(maxConnectionsPerHost:Int
                , maxConnections:Int
                , connectionTimeout:Int
                , timeoutRetryInterval:Int
                , timeoutThreshold:Int
                , degradationConfig:String
                , readTimeout:Int
                , requestTimeout:Int
                , pooledConnectionIdleTimeout:Int
                , timeoutMaxResponseTimeInMs:Int
                , sslContextVersion:String
                , commaSeparatedCipherSuitesList:String
                , customizeBuilder: Option[(AsyncHttpClientConfig.Builder) => Unit] = None
               ): AsyncHttpClientConfig ={
    val builder =  new AsyncHttpClientConfig.Builder()
    val sslContext: SSLContext = SSLContext.getInstance(sslContextVersion)
    sslContext.init(null, null, null)
    customizeBuilder.foreach(_(builder))
    builder.setAllowPoolingConnections(true)
      .setMaxConnectionsPerHost(maxConnectionsPerHost)
      .setMaxConnections(maxConnections)
      .setConnectTimeout(connectionTimeout)
      .setReadTimeout(readTimeout)
      .setRequestTimeout(requestTimeout)
      .setPooledConnectionIdleTimeout(pooledConnectionIdleTimeout)
      .addRequestFilter(new TimeoutThrottlingFilter(timeoutThreshold = timeoutThreshold,
        timeoutRetryInterval = timeoutRetryInterval,
        timeoutMaxResponseTime= timeoutMaxResponseTimeInMs))
      .setAllowPoolingSslConnections(true)
      .setCompressionEnforced(true)
      .setSSLContext(sslContext)
      .setEnabledProtocols(Array(sslContextVersion))
      .setEnabledCipherSuites(commaSeparatedCipherSuitesList.split(","))
    builder.build()
  }
}
