// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.api.jersey.example

import java.io.File
import java.util.UUID

import com.yahoo.maha.core.ddl.OracleDDLGenerator
import com.yahoo.maha.jdbc.{JdbcConnection, List, Seq}
import com.yahoo.maha.service.{DefaultMahaService, MahaService, MahaServiceConfig}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import grizzled.slf4j.Logging
import org.apache.commons.io.FileUtils
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object ExampleMahaService extends Logging {

  val REGISTRY_NAME = "academic";

  private var dataSource: Option[HikariDataSource] = None
  private var jdbcConnection: Option[JdbcConnection] = None
  val h2dbId = UUID.randomUUID().toString.replace("-","")
  val today: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now())
  val yesterday: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now().minusDays(1))

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

  def getMahaService(scope: String = "main"): MahaService = {
    val jsonString = FileUtils.readFileToString(new File(s"src/$scope/resources/maha-service-config.json"))
      .replaceAll("h2dbId", s"$h2dbId")

    initJdbcToH2()

    val mahaServiceResult = MahaServiceConfig.fromJson(jsonString.getBytes("utf-8"))
    if (mahaServiceResult.isFailure) {
      mahaServiceResult.leftMap {
        res=>
          error(s"Failed to launch Example MahaService, MahaService Error list is: ${res.list.toList}")
      }
    }
    val mahaServiceConfig = mahaServiceResult.toOption.get
    val mahaService: MahaService = new DefaultMahaService(mahaServiceConfig)
    stageStudentData(mahaServiceConfig)
    mahaService
  }

  def stageStudentData(mahaServiceConfig: MahaServiceConfig) : Unit = {

    val ddlGenerator = new OracleDDLGenerator
    val erRegistryConfig = mahaServiceConfig.registry.get(ExampleMahaService.REGISTRY_NAME).get
    val erRegistry= erRegistryConfig.registry
    erRegistry.factMap.values.foreach {
      publicFact =>
        publicFact.factList.foreach {
          fact=>
            val ddl = ddlGenerator.toDDL(fact)
            val result = jdbcConnection.get.executeUpdate(ddl)
            assert(result.isSuccess, result)
        }
    }

    val insertSql =
      """
        INSERT INTO student_grade_sheet (myyear, section_id, student_id, class_id, total_marks, mydate, mycomment)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      """

    val rows: List[Seq[Any]] = List(
      Seq(1, 100, 213, 200, 125, ExampleMahaService.today, "some comment")
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
  }
}
