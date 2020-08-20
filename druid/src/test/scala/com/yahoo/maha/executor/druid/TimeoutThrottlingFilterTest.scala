// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.executor.druid

import java.net.{InetSocketAddress, URI}
import java.util.concurrent.ExecutionException

import cats.effect.IO
import com.yahoo.maha.executor.druid.filters.{ServiceUnavailableException, TimeoutMillsStore, TimeoutThrottlingFilter}
import grizzled.slf4j.Logging
import org.asynchttpclient.filter.FilterException
import org.asynchttpclient.{DefaultAsyncHttpClient, DefaultAsyncHttpClientConfig}
import org.http4s.HttpService
import org.http4s.dsl.io._
import org.http4s.server.blaze.BlazeBuilder
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.mutable
import scala.util.Try

/**
 * Created by pranavbhole on 28/06/17.
 */
class TimeoutThrottlingFilterTest extends FunSuite with Matchers with BeforeAndAfterAll with Logging {

  var server: org.http4s.server.Server[IO] = null

  val service = HttpService[IO] {
    case GET -> Root / ("endpoint") =>
      val endpoint =
        """[
          |{"emptyJson" : true}
          |  ]""".stripMargin
      Ok(endpoint)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val builder = BlazeBuilder[IO].mountService(service, "/mock").bindSocketAddress(new InetSocketAddress("localhost", 16367))
    server = builder.start.unsafeRunSync()
    info("Started blaze server")
  }

  override def afterAll {
    info("Stopping blaze server")
    server.shutdown
  }

  test("Test TimeoutMillsStore") {
    val window = 1000
    val thresholdCount = 10
    val retryInterval = 100

    val timeoutMillsStore = TimeoutMillsStore(window, thresholdCount, retryInterval)
    for (i <- 1 to thresholdCount + 1) {
      timeoutMillsStore.addTimeout
      Thread.sleep(1)
    }
    val availability = Try(timeoutMillsStore.checkAvailability("/endpoint"))
    
    assert(availability.isFailure)
    assert(availability.failed.get.toString.contains("ServiceUnavailableException, timeout threshold count exceeded 11 > 10 url:/endpoint"))

    Thread.sleep(retryInterval)

    val availability1 = Try(timeoutMillsStore.checkAvailability("/endpoint"))
    assert(availability1.isSuccess)
  }

  test("TimeoutThrottlingFilterTest") {

    val targetURI = new URI("http://localhost:" + server.address.getPort + "/mock/endpoint");

    // Build http client
    val config = new DefaultAsyncHttpClientConfig.Builder()
      .setCompressionEnforced(false)
      .setMaxConnectionsPerHost(5)
      .setMaxConnections(5)
      .setRequestTimeout(600000)
      .addRequestFilter(new TimeoutThrottlingFilter(1000L, 2, 1000L, 1))
      .build();

    val asyncHttpClient = new DefaultAsyncHttpClient(config);

    // Send 1 good request.
    val f = asyncHttpClient.prepareGet(targetURI.toASCIIString()).execute();
    Thread.sleep(3000)
    val response = f.get();
    val statusCode = response.getStatusCode();
    assert(statusCode == 200)


    // Bottleneck the service with threshold timeouts
    for (i <- 1 to 3) {
     val result = Try {
        val f = asyncHttpClient.prepareGet(new URI("http://localhost:0/echo").toASCIIString()).execute();
        // We block here to give chance to return the status code.
        val response = f.get();
        
      }
      assert(result.isFailure)
    }


    try {
      asyncHttpClient.prepareGet(targetURI.toASCIIString()).execute().get()
    } catch {
      case e: ExecutionException =>
        val fe = e.getCause()
        
        assert(fe.getCause.isInstanceOf[ServiceUnavailableException])
    }

    //Wait for service becomes available: waiting for window
    Thread.sleep(1000)

    val trynow = Try(asyncHttpClient.prepareGet(targetURI.toASCIIString()).execute().get())
    
    assert(trynow.isSuccess)
  }

  test("Test thread safeness of TimeoutThrottlingFilter") {

    val targetURI = new URI("http://localhost:" + server.address.getPort + "/mock/endpoint");

    val filter = new TimeoutThrottlingFilter(1000L, 2, 1000L)
    // Build http client
    val config = new DefaultAsyncHttpClientConfig.Builder()
      .setCompressionEnforced(false)
      .setMaxConnectionsPerHost(5)
      .setMaxConnections(5)
      .setRequestTimeout(600000)
      .addRequestFilter(filter)
      .build();

    val asyncHttpClient = new DefaultAsyncHttpClient(config)

    val testNingThreads = new mutable.ListBuffer[TestAsyncThread]()

    for (i <- 1 to 5) {
      testNingThreads += TestAsyncThread(asyncHttpClient, new URI(s"http://localhost:${server.address.getPort+1}/bad"))
    }

    var filterExceptionCount = 0
    var connectionExceptionCount = 0
    testNingThreads.foreach {
      t=>
        val bombard = Try(t.run())
        
        assert(bombard.failed.get.getCause.isInstanceOf[java.net.ConnectException]
         || bombard.failed.get.getCause.isInstanceOf[FilterException])

        bombard.failed.get.getCause match {
          case a:FilterException=>
              filterExceptionCount+=1
          case a:java.net.ConnectException=>
              connectionExceptionCount+=1
          case _=>
            assert(false)
        }
    }
    testNingThreads.foreach(t=> t.join())
    assert(filterExceptionCount >= 1)
    assert(connectionExceptionCount >= 3)
    assert(filterExceptionCount + connectionExceptionCount == 5)

    val result = Try(asyncHttpClient.prepareGet(targetURI.toASCIIString()).execute().get())
    assert(result.isFailure)
    
    val filterException = result.failed.get.getCause
    assert(filterException.isInstanceOf[FilterException])
    assert(filterException.getCause.isInstanceOf[ServiceUnavailableException])

    //Wait for service becomes available: waiting for window
    Thread.sleep(1000)

    val trynow = Try(asyncHttpClient.prepareGet(targetURI.toASCIIString()).execute().get())
    
    assert(trynow.isSuccess)
  }

}

case class TestAsyncThread(asyncHttpClient: DefaultAsyncHttpClient, url :URI) extends Thread {
  override def run(): Unit =  {
    asyncHttpClient.prepareGet(url.toASCIIString()).execute().get()
  }
}
