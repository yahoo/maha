// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.executor.druid

/**
 * Created by vivekch on 3/2/16.
 */

import java.io.{Closeable, FileInputStream}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}
import com.yahoo.maha.executor.druid.filters.TimeoutThrottlingFilter
import grizzled.slf4j.Logging
import io.netty.handler.ssl.{SslContext, SslContextBuilder}
import org.apache.http.HttpHeaders
import org.apache.http.entity.ContentType
import org.asynchttpclient.{AsyncHttpClient, AsyncHttpClientConfig, BoundRequestBuilder, DefaultAsyncHttpClient, DefaultAsyncHttpClientConfig, Response}

import java.security.spec.PKCS8EncodedKeySpec
import java.security.{KeyFactory, KeyStore, PrivateKey};

class HttpUtils(config:AsyncHttpClientConfig, enableRetryOn500: Boolean, retryDelayMillis: Int, maxRetry: Int) extends Logging with Closeable {

  sealed trait RequestMethod
  case object GET extends RequestMethod
  case object POST extends RequestMethod
  private[this] val client = new DefaultAsyncHttpClient(config)
  private[this] def sendRequest(url:String
                                , method:RequestMethod
                                ,headers:Option[Map[String, String]] = None
                                ,payload:Option[String] = None
                                , debugEnabled: Boolean = false
                                 ):Response = {

    def execute(): Response = {
      val requestBuilder:BoundRequestBuilder = method match {
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
          error(s"Got 500 response, exhausted retries with $count/$maxRetry : ${tryExecute.getResponseBody}")
          retry = false
        }
      } else {
        retry = false
      }
    } while(retry)
    tryExecute
  }


  override def close(): Unit = client.close()

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
                , customizeBuilder: Option[(DefaultAsyncHttpClientConfig.Builder) => Unit] = None
               ): AsyncHttpClientConfig ={
    val builder =  new DefaultAsyncHttpClientConfig.Builder()
    //val sslContext: SslContext = SslContextBuilder.forClient().build()
    // test with other mtls
    val clientTrustStorePath = ""
    val clientTrustStorePassword = ""
    val clientPublicCertPath = ""
    val clientPrivateKeyPath = ""

    def getCertificate(path: String): java.security.cert.Certificate = {
      val inputStream = new FileInputStream(path)
      val certificate = java.security.cert.CertificateFactory.getInstance("X.509").generateCertificate(inputStream)
      inputStream.close()
      certificate
    }

    def getPrivateKey(path: String, keyFactory: KeyFactory): PrivateKey = {
      val inputStream = new FileInputStream(path)
      val privateKeyBytes = readPemFile(inputStream)
      inputStream.close()
      val privateKeySpec = new PKCS8EncodedKeySpec(privateKeyBytes)
      keyFactory.generatePrivate(privateKeySpec)
    }

    def readPemFile(inputStream: FileInputStream): Array[Byte] = {
      val pemBytes = Stream.continually(inputStream.read).takeWhile(_ != -1).map(_.toByte).toArray
      val header = "-----BEGIN PRIVATE KEY-----"
      val footer = "-----END PRIVATE KEY-----"
      val pemString = new String(pemBytes)
      val pemWithoutHeaderAndFooter = pemString
        .stripPrefix(header)
        .stripSuffix(footer)
        .replaceAll("\\s", "")
      javax.xml.bind.DatatypeConverter.parseBase64Binary(pemWithoutHeaderAndFooter)
    }

    // Load the trust store
    val trustStore = KeyStore.getInstance("JKS")
    trustStore.load(new FileInputStream(clientTrustStorePath), clientTrustStorePassword.toCharArray)
    val trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    trustManagerFactory.init(trustStore)

    // Load the client certificate and key
    val keyFactory = KeyFactory.getInstance("RSA")
    val privateKey = getPrivateKey(clientPrivateKeyPath, keyFactory)
    val certificate = getCertificate(clientPublicCertPath)

    // Load the client certificate and key
    val keyStore = KeyStore.getInstance("PKCS12")
    keyStore.load(null)
    keyStore.setCertificateEntry("cert", certificate)
    keyStore.setKeyEntry("key", privateKey, null, Array(getCertificate(clientPublicCertPath)))
    val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    keyManagerFactory.init(keyStore, null)

    val sslContext: SslContext = SslContextBuilder.forClient()
      .trustManager(trustManagerFactory)
      .keyManager(keyManagerFactory)
      .build()

    customizeBuilder.foreach(_(builder))
    builder
      .setMaxConnectionsPerHost(maxConnectionsPerHost)
      .setMaxConnections(maxConnections)
      .setConnectTimeout(connectionTimeout)
      .setReadTimeout(readTimeout)
      .setRequestTimeout(requestTimeout)
      .setPooledConnectionIdleTimeout(pooledConnectionIdleTimeout)
      .addRequestFilter(new TimeoutThrottlingFilter(timeoutThreshold = timeoutThreshold,
        timeoutRetryInterval = timeoutRetryInterval,
        timeoutMaxResponseTime= timeoutMaxResponseTimeInMs))
      .setCompressionEnforced(true)
      .setSslContext(sslContext)
      .setEnabledProtocols(Array(sslContextVersion))
      .setEnabledCipherSuites(commaSeparatedCipherSuitesList.split(","))
    builder.build()
  }
}
