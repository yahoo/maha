// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.jdbc

import java.sql.{Date, ResultSet, Timestamp}
import java.util.UUID

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.util.Try

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
    val result = jdbcConnection.execute("CREATE TABLE dummy (string VARCHAR2(244), num INT, decimalValue DECIMAL, dt DATE, ts TIMESTAMP, lo BIGINT, bool BIT, fl REAL, by TINYINT, sh SMALLINT, da DATE)")
    assert(result.isSuccess && result.toOption.get === false)
    val resultBool : Try[Boolean] = jdbcConnection.execute("CREATE TABLE dummy2 (string VARCHAR2(244), num INT, decimalValue DECIMAL, dt DATE, ts TIMESTAMP)")
    assert(resultBool.isSuccess && resultBool.toOption.get === false)
  }

  test("create table with interpolated input") {
    val interpolatedInput : SqlAndArgs = SqlAndArgs("CREATE TABLE dummy3 (string VARCHAR2(244))", Seq())
    val interpolatedResult : Try[Boolean] = jdbcConnection.execute(interpolatedInput)
    assert(interpolatedResult.isSuccess && interpolatedResult.toOption.get === false)
  }
  
  test("insert rows") {
    val string: String = "string"
    val num: Int = 1
    val decimal: Double = 299991.99999999888888888
    val dt: Date = new Date(staticTimestamp)
    val ts: Timestamp = new Timestamp(staticTimestamp)
    val lo: Long = 1
    val bool: Boolean = true
    val fl: Float = 1
    val by: Byte = 1
    val sh: Short = 4
    val da: java.util.Date = new java.util.Date(staticTimestamp)

    val updateSqlAndArgs : SqlAndArgs = SqlAndArgs("INSERT INTO dummy (string, num, decimalValue, dt, ts, lo, bool, fl, by, sh, da) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      Seq(string, num, decimal, dt, ts, lo, bool, fl, by, sh, da))
    val result = jdbcConnection.executeUpdate(updateSqlAndArgs)
    assert(result.isSuccess && result.toOption.get === 1)
  }

  test("Update DB with new table") {
    val resultInt : Try[Int] = jdbcConnection.executeUpdate(s"CREATE TABLE student_grade_sheet (total_marks NUMBER(0) NULL, year NUMBER(3) NOT NULL, obtained_marks NUMBER(0) NULL, date DATE NOT NULL, comment VARCHAR2(0 CHAR) NOT NULL, section_id NUMBER(3) NOT NULL, class_id NUMBER(0) NOT NULL, student_id NUMBER(0) NOT NULL)")
    assert(resultInt.isSuccess && resultInt.toOption.get === 0)
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

  test("mapSingle on an already limited query") {
    val limitedQueryableArgs : SqlAndArgs = SqlAndArgs("SELECT * FROM student_grade_sheet LIMIT 1",
      Seq())
    val mappedLimitedRows = jdbcConnection.mapSingle(limitedQueryableArgs)
    assert(mappedLimitedRows.isSuccess, "Limit 1 already present test condition failure")
  }

  test("query table for a list") {
    val queriedParameterList = jdbcConnection.queryForList("SELECT * FROM student_grade_sheet",
      Seq())
    assert(queriedParameterList.isSuccess, "queried list returned false")
  }

  test("map a query") {
    val queryableArgs : SqlAndArgs = SqlAndArgs("SELECT * FROM student_grade_sheet",
      Seq())
    val queriedList = jdbcConnection.mapQuery(queryableArgs)
    assert(queriedList.isSuccess, "mapping of query returned false")
  }

  test("map a single row") {
    val queryableArgs : SqlAndArgs = SqlAndArgs("SELECT * FROM student_grade_sheet",
      Seq())
    val mappedSingle = jdbcConnection.mapSingle(queryableArgs)
    assert(mappedSingle.isSuccess, "Mapped args failed to add LIMIT 1 condition")
  }

  test("Verify SqlAndArgs can execute stripMargin") {
    val queryableArgs : SqlAndArgs = SqlAndArgs("SELECT * FROM student_grade_sheet ",
      Seq()).stripMargin
    assert(queryableArgs.sql == "SELECT * FROM student_grade_sheet ")
  }

  test("Get RowData on a query") {
    val queryableArgs : SqlAndArgs = SqlAndArgs("SELECT * FROM dummy",
      Seq())
    val moreQueriedArgs : SqlAndArgs = SqlAndArgs("LIMIT 1",
      Seq())
    val extraQueryableArgs : SqlAndArgs = queryableArgs + moreQueriedArgs
    val result = jdbcConnection.executeQuery(queryableArgs)
    val resultSet : ResultSet = result.get
    jdbcConnection.queryForObject("SELECT * FROM dummy") {
      rs =>
        val obj = RowData(rs)
        obj.apply(0)
        obj.toIterator(rs => rs.first()).next()
    }
    jdbcConnection.queryForObject(extraQueryableArgs) {
      rs =>
        val obj = RowData(rs)
        obj.apply("string")
    }
    jdbcConnection.queryForObject(extraQueryableArgs) {
      rs =>
        val obj = RowData(rs)
        val iter = obj.toIterator(rs => rs)
        iter.next()
    }
  }
}
