// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.executor.druid

import java.net.InetSocketAddress

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
  val degradationConfig:String= "CONFIG"
  var server: org.http4s.server.Server = null

  override def beforeAll{
    super.beforeAll()
    val builder = BlazeBuilder.mountService(service,"/mock").bindSocketAddress(new InetSocketAddress("localhost",5544) )
    server = builder.run
    info("Started blaze server")
  }

  test("successful POST call test") {
    val httpUtils = new HttpUtils(ClientConfig.getConfig(maxConnectionsPerHost,maxConnections,connectionTimeout,timeoutRetryInterval,timeoutThreshold, degradationConfig,readTimeout,requestTimeout,pooledConnectionIdleTimeout,timeoutMaxResponseTimeInMs), true, 500, 3)
    val response  = httpUtils.post("http://localhost:5544/mock/groupby",httpUtils.POST)
    println("POST Response "+response.getResponseBody)
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
    val httpUtils = new HttpUtils(ClientConfig.getConfig(maxConnectionsPerHost,maxConnections,connectionTimeout,timeoutRetryInterval,timeoutThreshold, degradationConfig,readTimeout,requestTimeout,pooledConnectionIdleTimeout,timeoutMaxResponseTimeInMs), true, 500, 3)
    val response  = httpUtils.post("http://localhost:5544/mock/groupby",httpUtils.POST,None,Some(topN))
    println("POST Response "+response.getResponseBody)
    assert(response.getStatusCode==200)
  }


  test("successful GET call test") {
    println("started test")
    val httpUtils = new HttpUtils(ClientConfig.getConfig(maxConnectionsPerHost,maxConnections,connectionTimeout,timeoutRetryInterval,timeoutThreshold, degradationConfig,readTimeout,requestTimeout,pooledConnectionIdleTimeout,timeoutMaxResponseTimeInMs), true, 500, 3)
    val response  = httpUtils.post("http://localhost:5544/mock/topn",httpUtils.GET)
    println("GET Response "+response.getResponseBody)
    assert(response.getStatusCode==200)
  }

  test("Invalid url call test"){
    println("started test")
    val httpUtils = new HttpUtils(ClientConfig.getConfig(maxConnectionsPerHost,maxConnections,connectionTimeout,timeoutRetryInterval,timeoutThreshold, degradationConfig,readTimeout,requestTimeout,pooledConnectionIdleTimeout,timeoutMaxResponseTimeInMs), true, 500, 3)
    val response  = httpUtils.get("http://localhost:5544/mock/invalid",httpUtils.GET)
    println("Response body "+response.getResponseBody)
    assert(response.getStatusCode==404)
  }

  test("GET call test with headers"){
    println("started test")
    val httpUtils = new HttpUtils(ClientConfig.getConfig(maxConnectionsPerHost,maxConnections,connectionTimeout,timeoutRetryInterval,timeoutThreshold, degradationConfig,readTimeout,requestTimeout,pooledConnectionIdleTimeout,timeoutMaxResponseTimeInMs), true, 500, 3)
    var headers = Map("Content-Type"->"Application/Json")
    val response  = httpUtils.get("http://localhost:5544/mock/topn",httpUtils.GET,Some(headers))
    println("Response body "+response.getResponseBody)
    assert(response.getStatusCode==200)
  }

  test("Unsupported HTTP method"){
    println("started test")
    val httpUtils = new HttpUtils(ClientConfig.getConfig(maxConnectionsPerHost,maxConnections,connectionTimeout,timeoutRetryInterval,timeoutThreshold, degradationConfig,readTimeout,requestTimeout,pooledConnectionIdleTimeout,timeoutMaxResponseTimeInMs), true, 500, 3)
    val headers = Map("Content-Type"->"Application/Json")
    val thrown = intercept[UnsupportedOperationException]{
    httpUtils.get("http://localhost:5544/mock/topn",null,Some(headers))
    }
    assert(thrown.getMessage.contains("Unsupported method"))
  }

  test("successfully retry on 500"){
    println("started test")
    assert(failFirstBoolean.get() === false)
    val httpUtils = new HttpUtils(ClientConfig.getConfig(maxConnectionsPerHost,maxConnections,connectionTimeout,timeoutRetryInterval,timeoutThreshold, degradationConfig,readTimeout,requestTimeout,pooledConnectionIdleTimeout,timeoutMaxResponseTimeInMs), true, 500, 3)
    val response  = httpUtils.post("http://localhost:5544/mock/failFirst",httpUtils.POST)
    println("POST Response "+response.getResponseBody)
    assert(response.getStatusCode==200)
    assert(failFirstBoolean.get() === true)
  }

  test("exhaust max retry on 500"){
    println("started test")
    val httpUtils = new HttpUtils(ClientConfig.getConfig(maxConnectionsPerHost,maxConnections,connectionTimeout,timeoutRetryInterval,timeoutThreshold, degradationConfig,readTimeout,requestTimeout,pooledConnectionIdleTimeout,timeoutMaxResponseTimeInMs), true, 500, 3)
    val response  = httpUtils.post("http://localhost:5544/mock/fail",httpUtils.POST)
    println("POST Response "+response.getResponseBody)
    assert(response.getStatusCode==500)
  }

  override def afterAll{
    info("Stopping blaze server")
    server.shutdown
  }

}
