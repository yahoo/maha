// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.jdbc

import java.sql.{Timestamp, Date}
import java.util.UUID

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

/**
 * Created by hiral on 1/26/16.
 */
class JdbcConnectionTest extends FunSuite with Matchers with BeforeAndAfterAll {
  private var dataSource: HikariDataSource = null
  private var jdbcConnection: JdbcConnection = null
  private val staticTimestamp = System.currentTimeMillis()

  override protected def beforeAll(): Unit = {
    val config = new HikariConfig()
    config.setJdbcUrl("jdbc:h2:mem:" + UUID.randomUUID().toString.replace("-",
      "") + ";MODE=Oracle;DB_CLOSE_DELAY=-1")
    config.setUsername("sa")
    config.setPassword("sa")
    config.setMaximumPoolSize(1)
    dataSource = new HikariDataSource(config)
    jdbcConnection = new JdbcConnection(dataSource)
  }

  override protected def afterAll(): Unit = {
    dataSource.close()
  }
  
  test("create table") {
    val result = jdbcConnection.execute("CREATE TABLE dummy (string VARCHAR2(244), num INT, decimalValue DECIMAL, dt DATE, ts TIMESTAMP)")
    assert(result.isSuccess && result.toOption.get === false)
  }
  
  test("insert rows") {
    val string: String = "string"
    val num: Int = 1
    val decimal: Double = 299991.99999999888888888
    val dt: Date = new Date(staticTimestamp)
    val ts: Timestamp = new Timestamp(staticTimestamp)

    val result = jdbcConnection.executeUpdate("INSERT INTO dummy (string, num, decimalValue, dt, ts) VALUES (?, ?, ?, ?, ?)", 
      Seq(string, num, decimal, dt, ts))

    assert(result.isSuccess && result.toOption.get === 1)
  }
  
  test("read rows") {
    jdbcConnection.queryForObject("SELECT * FROM dummy") {
      rs =>
        val s = rs.getString(0)
        val n = rs.getInt(1)
        val d = rs.getDouble(2)
        val dt = rs.getDate(3)
        val ts = rs.getTimestamp(4)
        assert(s === "string")
        assert(n === 1)
        assert(d === 299991.99999999888888888)
        assert(dt.getTime === staticTimestamp)
        assert(ts.getTime === staticTimestamp)

    }
    
  }
}
