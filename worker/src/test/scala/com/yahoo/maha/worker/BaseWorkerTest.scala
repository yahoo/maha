package com.yahoo.maha.worker

import java.sql.ResultSet
import java.util.UUID

import com.yahoo.maha.jdbc.JdbcConnection
import com.yahoo.maha.service.BaseMahaServiceTest
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.joda.time.DateTime
import org.scalatest.{FunSuite, Matchers}

/*
    Created by pranavbhole on 8/29/18
*/
trait BaseWorkerTest extends BaseMahaServiceTest with Matchers {
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
  val result = jdbcConnection.get.execute(mahaJobWorkerTable)
  assert(result.isSuccess, s"Failed to create job table $result")

}
