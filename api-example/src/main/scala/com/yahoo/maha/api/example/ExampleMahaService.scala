// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.api.example

import java.util.UUID

import com.google.common.base.Charsets
import com.google.common.io.Resources
import com.yahoo.maha.core.OracleEngine
import com.yahoo.maha.core.ddl.OracleDDLGenerator
import com.yahoo.maha.jdbc.{Seq, _}
import com.yahoo.maha.service.{DefaultMahaService, MahaService, MahaServiceConfig}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import grizzled.slf4j.Logging
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object ExampleMahaService extends Logging {

  private var dataSource: Option[HikariDataSource] = None
  private var jdbcConnection: Option[JdbcConnection] = None
  val h2dbId = UUID.randomUUID().toString.replace("-","")
  val today: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now())
  val yesterday: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(1))

  val daysBack11 = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(11))

  def initJdbcToH2(): Unit = {
    val config = new HikariConfig()
    config.setJdbcUrl(s"jdbc:h2:mem:$h2dbId;MODE=Oracle;DB_CLOSE_DELAY=-1")
    config.setUsername("sa")
    config.setPassword("h2.test.database.password")
    config.setMaximumPoolSize(2)
    dataSource = Option(new HikariDataSource(config))
    jdbcConnection = dataSource.map(new JdbcConnection(_))
    assert(jdbcConnection.isDefined, "Failed to connect to h2 local server")
  }

  def getMahaService: MahaService = {
    val url = Resources.getResource("maha-service-config.json")
    val jsonString = Resources.toString(url, Charsets.UTF_8).replaceAll("h2dbId", s"$h2dbId")

    initJdbcToH2()

    val mahaServiceResult = MahaServiceConfig.fromJson(jsonString.getBytes("utf-8"))
    if (mahaServiceResult.isFailure) {
      mahaServiceResult.leftMap {
        res=>
          error(s"Failed to launch Example MahaService, MahaService Error list is: ${res.list.map(er=> er.message -> er.source).toList}")
      }
    }

    // Mandatory step of registering all the schemas used in the definitions
    ExampleSchema.register()

    val mahaServiceConfig = mahaServiceResult.toOption.get
    val mahaService: MahaService = new DefaultMahaService(mahaServiceConfig)
    stageStudentData(mahaServiceConfig)
    mahaService
  }

  def stageStudentData(mahaServiceConfig: MahaServiceConfig) : Unit = {

    val ddlGenerator = new OracleDDLGenerator
    val erRegistryConfig = mahaServiceConfig.registry.get("student").get
    val erRegistry= erRegistryConfig.registry
    erRegistry.factMap.values.foreach {
      publicFact =>
        publicFact.factList.filter(_.engine == OracleEngine).foreach {
          fact =>
            val ddl = ddlGenerator.toDDL(fact)
            assert (jdbcConnection.get.executeUpdate(ddl).isSuccess)
        }
    }
    erRegistry.dimMap.values.foreach {
      dim=>
        dim.dimList.filter(_.engine == OracleEngine).foreach {
          dimTable=>
            val ddl = ddlGenerator.toDDL(dimTable)
            assert (jdbcConnection.get.executeUpdate(ddl).isSuccess, s"failed to create table ${ddl}")
        }
    }

    val insertSql =
      """
        INSERT INTO student_grade_sheet (myyear, section_id, student_id, class_id, total_marks, mydate, mycomment)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      """

    val rows: List[Seq[Any]] = List(
      Seq(1, 100, 213, 200, 125, today.toString, "some comment 1"),
        Seq(2, 100, 213, 198, 120, yesterday.toString, "some comment 2"),
          Seq(1, 100, 213, 200, 110, daysBack11.toString, "some comment 1"),
      Seq(2, 100, 213, 198, 98, daysBack11.toString, "some comment 2")
    )

    rows.foreach {
      row =>
        val result = jdbcConnection.get.executeUpdate(insertSql, row)
        assert(result.isSuccess)
    }
    var count = 0
    jdbcConnection.get.queryForObject("select * from student_grade_sheet") {
      rs =>
        while (rs.next()) {
          count += 1
        }
    }
    assert(rows.size == count)

    val insertDimSql =
      """
        INSERT INTO student (id, name, status, admitted_year, department_id)
        VALUES (?, ?, ?, ?, ?)
      """
    val row = Seq(213, "Maha Student 1", "Full Time", 2015, 1)
    val dimResult = jdbcConnection.get.executeUpdate(insertDimSql, row)
    assert(dimResult.isSuccess, s"failed to execute the ${insertDimSql}, ${dimResult}")
  }
}
