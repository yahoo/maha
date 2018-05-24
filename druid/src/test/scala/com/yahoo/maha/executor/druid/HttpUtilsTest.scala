// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.executor.druid

import java.net.InetSocketAddress

import cats.effect.IO
import com.yahoo.maha.core.query.NumberTransformer
import grizzled.slf4j.Logging
import org.http4s.server.blaze.BlazeBuilder
import org.scalatest._

class HttpUtilsTest extends FunSuite with Matchers with BeforeAndAfterAll with Logging with TestWebService {
  val maxConnectionsPerHost:Int = 5
  val maxConnections:Int=50
  val connectionTimeout:Int=5000
  val timeoutRetryInterval=5000
  val timeoutThreshold:Int=5000
  val readTimeout = 30000
  val requestTimeout = 29000
  val pooledConnectionIdleTimeout = 50000
  val timeoutMaxResponseTimeInMs = 50000
  val sslContextVersion = "TLSv1.2"
  val commaSeparatedCipherSuitesList = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384,TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,TLS_RSA_WITH_AES_128_GCM_SHA256,TLS_RSA_WITH_AES_256_GCM_SHA384,TLS_RSA_WITH_AES_128_CBC_SHA256,TLS_RSA_WITH_AES_256_CBC_SHA256,TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA"
  val degradationConfig:String= "CONFIG"
  var server: org.http4s.server.Server[IO] = null

  override def beforeAll{
    super.beforeAll()
    val builder = BlazeBuilder[IO].mountService(service,"/mock").bindSocketAddress(new InetSocketAddress("localhost",5544) )
    server = builder.start.unsafeRunSync()
    info("Started blaze server")
  }

  test("Successful ResultSet conversion") {
    val bigDecimalTransformer = new NumberTransformer
    val bigDecimal : BigDecimal = 0.0
    val floatInput : Float = 1
    assert(bigDecimalTransformer.extractBigDecimal(floatInput).getClass == bigDecimal.getClass, "Output should be a BigDecimal, but found " + bigDecimalTransformer.extractBigDecimal(floatInput).getClass)
    val longInput : Long = 10
    assert(bigDecimalTransformer.extractBigDecimal(longInput).getClass == bigDecimal.getClass, "Output should be a BigDecimal, but found " + bigDecimalTransformer.extractBigDecimal(longInput).getClass)
    val intInput : Int = 53
    assert(bigDecimalTransformer.extractBigDecimal(intInput).getClass == bigDecimal.getClass, "Output should be a BigDecimal, but found " + bigDecimalTransformer.extractBigDecimal(intInput).getClass)
    val fallthroughStringInput : String = "Should not be returned"
    assert(bigDecimalTransformer.extractBigDecimal(fallthroughStringInput).getClass == bigDecimal.getClass, "Output should be a BigDecimal, but found " + bigDecimalTransformer.extractBigDecimal(fallthroughStringInput).getClass)
  }

  test("successful POST call test") {
    val config = ClientConfig
      .getConfig(maxConnectionsPerHost
        , maxConnections
        , connectionTimeout
        , timeoutRetryInterval
        , timeoutThreshold
        , degradationConfig
        , readTimeout
        , requestTimeout
        , pooledConnectionIdleTimeout
        , timeoutMaxResponseTimeInMs
        , sslContextVersion
        , commaSeparatedCipherSuitesList
        , Option{ builder => builder.setFollowRedirect(true)}
      )
    assert(config.isFollowRedirect === true)
    val httpUtils = new HttpUtils(config, true, 500, 3)
    val response  = httpUtils.post("http://localhost:5544/mock/groupby",httpUtils.POST)
    
    assert(response.getStatusCode==200)
  }

  test("successful POST call test with Payload") {
    val topN = """[{
                 |	"timestamp": "2013-08-31T00:00:00.000Z",
                 |	"result": [{
                 |		"Keyword ID": 14,
                 |    "Keyword Value":1,
                 |		"Impressions": 10669
                 |	},{
                 |		"Keyword ID": 13,
                 |		"Impressions": 106,
                 |    "Keyword Value":30
                 |	}]
                 |}]""".stripMargin
    val httpUtils = new HttpUtils(ClientConfig.getConfig(maxConnectionsPerHost,maxConnections,connectionTimeout,timeoutRetryInterval,timeoutThreshold, degradationConfig,readTimeout,requestTimeout,pooledConnectionIdleTimeout,timeoutMaxResponseTimeInMs,sslContextVersion,commaSeparatedCipherSuitesList), true, 500, 3)
    val response  = httpUtils.post("http://localhost:5544/mock/groupby",httpUtils.POST,None,Some(topN))
    
    assert(response.getStatusCode==200)
  }


  test("successful GET call test") {
    
    val httpUtils = new HttpUtils(ClientConfig.getConfig(maxConnectionsPerHost,maxConnections,connectionTimeout,timeoutRetryInterval,timeoutThreshold, degradationConfig,readTimeout,requestTimeout,pooledConnectionIdleTimeout,timeoutMaxResponseTimeInMs,sslContextVersion,commaSeparatedCipherSuitesList), true, 500, 3)
    val response  = httpUtils.post("http://localhost:5544/mock/topn",httpUtils.GET)
    
    assert(response.getStatusCode==200)
  }

  test("Invalid url call test"){
    
    val httpUtils = new HttpUtils(ClientConfig.getConfig(maxConnectionsPerHost,maxConnections,connectionTimeout,timeoutRetryInterval,timeoutThreshold, degradationConfig,readTimeout,requestTimeout,pooledConnectionIdleTimeout,timeoutMaxResponseTimeInMs,sslContextVersion,commaSeparatedCipherSuitesList), true, 500, 3)
    val response  = httpUtils.get("http://localhost:5544/mock/invalid",httpUtils.GET)
    
    assert(response.getStatusCode==404)
  }

  test("GET call test with headers"){
    
    val httpUtils = new HttpUtils(ClientConfig.getConfig(maxConnectionsPerHost,maxConnections,connectionTimeout,timeoutRetryInterval,timeoutThreshold, degradationConfig,readTimeout,requestTimeout,pooledConnectionIdleTimeout,timeoutMaxResponseTimeInMs,sslContextVersion,commaSeparatedCipherSuitesList), true, 500, 3)
    var headers = Map("Content-Type"->"Application/Json")
    val response  = httpUtils.get("http://localhost:5544/mock/topn",httpUtils.GET,Some(headers))
    
    assert(response.getStatusCode==200)
  }

  test("Unsupported HTTP method"){
    
    val httpUtils = new HttpUtils(ClientConfig.getConfig(maxConnectionsPerHost,maxConnections,connectionTimeout,timeoutRetryInterval,timeoutThreshold, degradationConfig,readTimeout,requestTimeout,pooledConnectionIdleTimeout,timeoutMaxResponseTimeInMs,sslContextVersion,commaSeparatedCipherSuitesList), true, 500, 3)
    val headers = Map("Content-Type"->"Application/Json")
    val thrown = intercept[UnsupportedOperationException]{
    httpUtils.get("http://localhost:5544/mock/topn",null,Some(headers))
    }
    assert(thrown.getMessage.contains("Unsupported method"))
  }

  test("successfully retry on 500"){
    
    assert(failFirstBoolean.get() === false)
    val httpUtils = new HttpUtils(ClientConfig.getConfig(maxConnectionsPerHost,maxConnections,connectionTimeout,timeoutRetryInterval,timeoutThreshold, degradationConfig,readTimeout,requestTimeout,pooledConnectionIdleTimeout,timeoutMaxResponseTimeInMs,sslContextVersion,commaSeparatedCipherSuitesList), true, 500, 3)
    val response  = httpUtils.post("http://localhost:5544/mock/failFirst",httpUtils.POST)
    
    assert(response.getStatusCode==200)
    assert(failFirstBoolean.get() === true)
  }

  test("exhaust max retry on 500"){
    
    val httpUtils = new HttpUtils(ClientConfig.getConfig(maxConnectionsPerHost,maxConnections,connectionTimeout,timeoutRetryInterval,timeoutThreshold, degradationConfig,readTimeout,requestTimeout,pooledConnectionIdleTimeout,timeoutMaxResponseTimeInMs,sslContextVersion,commaSeparatedCipherSuitesList), true, 500, 3)
    val response  = httpUtils.post("http://localhost:5544/mock/fail",httpUtils.POST)
    
    assert(response.getStatusCode==500)
    httpUtils.close()
  }

  override def afterAll{
    info("Stopping blaze server")
    server.shutdown
  }

}
