// Copyright 2018, Yahoo Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.

package com.yahoo.maha.job.service

import java.util.UUID

import com.google.common.io.Closer
import com.yahoo.maha.core.DailyGrain
import com.yahoo.maha.jdbc.JdbcConnection
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.{FunSuite, Matchers}

/*
    Created by pranavbhole on 8/29/18
*/
trait BaseJobServiceTest extends FunSuite with Matchers {

  protected var dataSource: Option[HikariDataSource] = None
  protected var jdbcConnection: Option[JdbcConnection] = None
  protected val closer : Closer = Closer.create()

  final val REGISTRY = "er"
  protected[this] val fromDate : String = DailyGrain.toFormattedString(DateTime.now(DateTimeZone.UTC).minusDays(7))
  protected[this] val toDate : String = DailyGrain.toFormattedString(DateTime.now(DateTimeZone.UTC))
  val h2dbId : String = UUID.randomUUID().toString.replace("-","")

  initJdbcToH2()

  val mahaJobWorkerTable =
    s"""
       | create table maha_worker_job(
       | jobId NUMBER(10) PRIMARY KEY,
       | jobType VARCHAR(100),
       | jobStatus VARCHAR(100),
       | jobResponse VARCHAR(100),
       | numAcquired NUMBER(2),
       | createdTimestamp TIMESTAMP,
       | acquiredTimestamp TIMESTAMP,
       | endedTimestamp TIMESTAMP,
       | jobParentId NUMBER(10),
       | jobRequest VARCHAR(100),
       | hostname VARCHAR(100),
       | cubeName VARCHAR(100),
       | isDeleted NUMBER(1)
       | );
     """.stripMargin
  val now = new DateTime()

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

  val result = jdbcConnection.get.execute(mahaJobWorkerTable)
  assert(result.isSuccess, s"Failed to create job table $result")

}
