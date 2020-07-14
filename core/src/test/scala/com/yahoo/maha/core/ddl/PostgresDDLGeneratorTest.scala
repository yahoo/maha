// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.ddl

import java.util.UUID

import com.yahoo.maha.core._
import com.yahoo.maha.jdbc.JdbcConnection
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

/**
 * Created by shengyao on 1/29/16.
 */

class PostgresDDLGeneratorTest extends BaseDDLGeneratorTest {

  val postgresDDLGenerator = new PostgresDDLGenerator

  private var dataSource: HikariDataSource = null
  private var jdbcConnection: JdbcConnection = null

  override protected def beforeAll(): Unit = {
    val config = new HikariConfig()
    config.setJdbcUrl("jdbc:h2:mem:" + UUID.randomUUID().toString.replace("-",
      "") + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1")
    config.setUsername("sa")
    config.setPassword("sa")
    config.setMaximumPoolSize(1)
    dataSource = new HikariDataSource(config)
    jdbcConnection = new JdbcConnection(dataSource)
  }

  override protected def afterAll(): Unit = {
    dataSource.close()
  }

  def removePartitionBy(ddl : String) : String = {
    val partitionString1 = """PARTITION BY LIST(stats_date)
                             |( PARTITION p_default VALUES(TO_DATE('01-JAN-1970 00:00:00', 'DD-MON-YYYY HH24:MI:SS'))
                             |)
                             |;""".stripMargin
    ddl.replace(partitionString1, ";")
  }

  test("test ddl for fact") {
    val postgresFacts = pubFact.factList.filter(f => f.engine.equals(PostgresEngine))
    val ddlMap : Map[String, String] = postgresFacts.map(fact => fact.name -> removePartitionBy(postgresDDLGenerator.toDDL(fact))).toMap
    assert(ddlMap.keySet.contains("pg_ad_k_stats"),
      "Postgres DDL Generator should generate ddl for postgres table cb_ad_k_stats")
    assert(!ddlMap.keySet.contains("ad_k_stats"),
      "Postgres DDL Generator should not generate ddl for hive table ad_k_stats")

    ddlMap.foreach {
      case(fact, ddl) =>
        println(ddl)
        val result = jdbcConnection.execute(ddl)
        assert(result.isSuccess && result.toOption.get === false, result.failed.toString)
    }
  }

  test("test ddl for dimension") {
    val postgresDims = pubDim.dimList.filter(d => d.engine.equals(PostgresEngine))
    val ddlMap : Map[String, String] = postgresDims.map(dim => dim.name -> removePartitionBy(postgresDDLGenerator.toDDL(dim))).toMap
    assert(ddlMap.keySet.contains("postgres_advertiser"),
      "Postgres DDL Generator should generate ddl for postgres table postgres_advertiser")
    assert(!ddlMap.keySet.contains("cache_advertiser"),
      "Postgres DDL Generator should not generate ddl for hive table cache_advertiser")

    ddlMap.foreach {
      case(dim, ddl) =>
        println(ddl)
        val result = jdbcConnection.execute(ddl)
        assert(result.isSuccess && result.toOption.get === false, result.failed.toString)
    }
  }



}
