// Copyright 2018, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service

import java.net.{InetSocketAddress, ServerSocket}
import java.nio.file.Paths
import java.util.UUID

import com.google.common.io.Closer
import com.yahoo.maha.core.{DailyGrain, OracleEngine}
import com.yahoo.maha.core.ddl.OracleDDLGenerator
import com.yahoo.maha.core.registry.Registry
import com.yahoo.maha.jdbc.JdbcConnection
import com.yahoo.maha.service.utils.MahaConstants
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import grizzled.slf4j.Logging
import org.apache.log4j.MDC
import org.http4s.server.blaze.BlazeBuilder
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.FunSuite
import cats.effect.IO
import org.http4s._
import org.http4s.dsl.io._

/**
 * Created by pranavbhole on 21/03/18.
 */
trait BaseMahaServiceTest extends FunSuite with Logging {
  protected var dataSource: Option[HikariDataSource] = None
  protected var jdbcConnection: Option[JdbcConnection] = None
  protected val closer : Closer = Closer.create()

  final val REGISTRY = "er"
  protected[this] val fromDate : String = DailyGrain.toFormattedString(DateTime.now(DateTimeZone.UTC).minusDays(7))
  protected[this] val toDate : String = DailyGrain.toFormattedString(DateTime.now(DateTimeZone.UTC))
  val ddlGenerator : OracleDDLGenerator = new OracleDDLGenerator
  val h2dbId : String = UUID.randomUUID().toString.replace("-","")

  initJdbcToH2()

  protected[this] val druidPort:Int  = {
    try {
      val socket = new ServerSocket(0)
      val freePort = socket.getLocalPort
      socket.close()
      freePort
    } catch {
      case e:Exception=>
        error("Failed to find the free port, trying default port")
        11223
    }
  }

  private[this] val replacementTuplesInConfigJson =   List(("H2DBID", h2dbId),
    ("http://localhost:druidLocalPort/mock/studentPerf", s"http://localhost:$druidPort/mock/studentPerf"))
  protected[this] val mahaServiceResult : MahaServiceConfig.MahaConfigResult[MahaServiceConfig] = getConfigFromFileWithReplacements("mahaServiceExampleJson.json", replacementTuplesInConfigJson )

  require(mahaServiceResult.isSuccess, mahaServiceResult.toString())

  val mahaServiceConfig : MahaServiceConfig = mahaServiceResult.toOption.get
  val mahaService : MahaService = DefaultMahaService(mahaServiceConfig)

  //For Kafka Logging init
  MDC.put(MahaConstants.REQUEST_ID, "123Request")
  MDC.put(MahaConstants.USER_ID,"abc")

  assert(mahaServiceConfig.registry.get("er").isDefined)
  val erRegistryConfig : RegistryConfig = mahaServiceConfig.registry.get("er").get
  val erRegistry : Registry = erRegistryConfig.registry
  assert(erRegistry.isCubeDefined("student_performance"))
  assert(erRegistry.getDimension("student").isDefined)
  assert(erRegistry.getDimension("class").isDefined)

  def initJdbcToH2(): Unit = {
    val config = new HikariConfig()
    config.setJdbcUrl(s"jdbc:h2:mem:$h2dbId;MODE=Oracle;DB_CLOSE_DELAY=-1")
    config.setUsername("sa")
    config.setPassword("h2.test.database.password")
    config.setMaximumPoolSize(1)
    dataSource = Option(new HikariDataSource(config))
    jdbcConnection = dataSource.map(JdbcConnection(_))
    assert(jdbcConnection.isDefined)
  }

  protected[this] def getJsonStringFromFile(fileName: String) : String = {
    val absolutePath : String = Paths.get(getUserDir + "/src/test/resources/" + fileName).toString
    scala.io.Source.fromFile(absolutePath)
      .getLines()
      .mkString
  }

  protected[this] def getConfigFromFileWithReplacements(fileName: String, replacements: List[(String, String)]) : MahaServiceConfig.MahaConfigResult[MahaServiceConfig] = {
    val jsonString : String = getJsonStringFromFile(fileName)
    var finalString = jsonString
    replacements.foreach {
      case (a,b) => finalString = finalString.replaceAll(a,b)
    }

    MahaServiceConfig.fromJson(finalString.getBytes("utf-8"))
  }

  protected[this] def getUserDir : String = {
    //val userDir = System.getProperty("user.dir")
    val userDir = "/Users/surabhip/git/maha/service/"
    s"$userDir"
  }

  def createTables(): Unit = {
    // Create Tables
    erRegistry.factMap.values.foreach {
      publicFact =>
        publicFact.factList.filter(_.engine == OracleEngine).foreach {
          fact=>
            val ddl = ddlGenerator.toDDL(fact)
            val result = jdbcConnection.get.executeUpdate(ddl)
            if(result.isFailure) {
              val throwable = result.failed.get
              error("Failed to create tables", throwable)
              throw throwable
            }
        }
    }
    erRegistry.dimMap.values.foreach {
      publicDim =>
        publicDim.dimList.foreach {
          fact=>
            val ddl = ddlGenerator.toDDL(fact)
            assert(jdbcConnection.get.executeUpdate(ddl).isSuccess)
        }
    }
  }

  val service = HttpService[IO] {

      case POST->  Root /("studentPerf") =>
      val response=s"""[{
                     |	"version": "v1",
                     |	"timestamp": "${toDate}T00:00:01.300Z",
                     |	"event": {
                     | 		"Section ID": 100,
                     |		"Student ID": 213,
                     |		"Class ID": "200",
                     |		"Total Marks": 99
                     |	}
                     |}]""".stripMargin
      Ok(response)

    case POST -> Root / ("empty")=>
      val response="[]"
      Ok(response)
  }

  val server: org.http4s.server.Server[IO] = {
    logger.info(s"Starting blaze server on port : $druidPort")
    val builder = BlazeBuilder[IO].mountService(service, "/mock").bindSocketAddress(new InetSocketAddress("localhost", druidPort))
    builder.start.unsafeRunSync()
  }

}
