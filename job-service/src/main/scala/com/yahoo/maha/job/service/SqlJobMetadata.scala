package com.yahoo.maha.job.service

import java.sql.SQLException

import com.yahoo.maha.jdbc.JdbcConnection
import com.yahoo.maha.job.service.JobStatus.JobStatus
import grizzled.slf4j.Logging
import org.joda.time.DateTime

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/*
    Created by pranavbhole on 8/31/18

    This is the example implementation of JobMetadata, mostly used by the tests
*/
case class SqlJobMetadata(jdbcConnection: JdbcConnection, tableName :String) extends JobMetadata with Logging {

  val insertSQL = s"""INSERT INTO $tableName (jobId, jobType, jobStatus,
    jobResponse, numAcquired, createdTimestamp,
    acquiredTimestamp, endedTimestamp,jobParentId,
     jobRequest, hostname, cubeName,
      isDeleted) values(?,?,?,?,?,?,?,?,?,?,?,?,?)""".stripMargin

  val deleteSql = s"UPDATE $tableName SET isDeleted=1 where jobId = ?"

  val updateJobStatusSql = s"UPDATE $tableName  SET jobStatus = ? where jobId = ?"

  val updateJobStatusWithMessageSql = s"UPDATE $tableName SET jobStatus = ? , jobResponse = ? where jobId = ?"

  val currentJobsSql= s"""SELECT count(*) as jobCount from $tableName where jobType = ? and jobStatus = ? """

  override def insertJob(job: Job): Future[Boolean] = {
    Future {
      val row = scala.collection.immutable.Seq(job.jobId, job.jobType.name, job.jobStatus.toString,
        job.jobResponse, job.numAcquired, job.createdTimestamp,
        job.acquiredTimestamp, job.endedTimestamp,job.jobParentId,
        job.jobRequest, job.hostname,job.cubeName,
        0)
      val result = jdbcConnection.executeUpdate(insertSQL, row)
      info("Result: "+result)
      if(result.isFailure) {
        error(s"Failed to create job $result")
        throw result.failed.get
      } else true
    }
  }

  override def deleteJob(jobId: Long): Future[Boolean] = Future {
    val row = scala.collection.immutable.Seq(jobId)
    val result = jdbcConnection.executeUpdate(deleteSql, row)
    info("Result: "+result)
    if(result.isFailure) {
      error(s"Failed to delete the job $result")
      false
    } else true
  }

  override def findById(jobId: Long): Future[Option[Job]] = {
    val result:Future[Option[Job]] = Future {
      val resultSetTry = jdbcConnection.queryForObject(s"SELECT * from $tableName where jobId=$jobId") {
        rs =>
          if(rs.next()) {
            val job = AsyncJob(rs.getLong("jobId"),
              JobType.fromString(rs.getString("jobType")),
              JobStatus.fromString(rs.getString("jobStatus")),
              rs.getString("jobResponse"),
              rs.getInt("numAcquired"),
              new DateTime(rs.getDate("createdTimestamp").getTime),
              new DateTime(rs.getDate("acquiredTimestamp").getTime),
              new DateTime(rs.getDate("endedTimestamp").getTime),
              rs.getLong("jobParentId"),
              rs.getString("jobRequest"),
              rs.getString("hostname"),
              rs.getString("cubeName"),
              rs.getBoolean("isDeleted")
            )
            Some(job)
          } else None
      }
      if(resultSetTry.isFailure) {
        throw resultSetTry.failed.get
      } else {
        resultSetTry.get
      }
    }
    result
  }

  override def updateJobStatus(jobId: Long, jobStatus: JobStatus): Future[Boolean] = Future {
    val row = scala.collection.immutable.Seq(jobStatus.toString, jobId)
    val result = jdbcConnection.executeUpdate(updateJobStatusSql, row)
    info("Result: "+result)
    if(result.isFailure) {
      error(s"Failed to update the job status $result")
      throw result.failed.get
    } else true
  }

  override def updateJobEnded(jobId: Long, endStatus: JobStatus, message: String): Future[Boolean] = Future {
    val row = scala.collection.immutable.Seq(endStatus.toString, message, jobId)
    val result = jdbcConnection.executeUpdate(updateJobStatusWithMessageSql, row)
    info("Result: "+result)
    if(result.isFailure) {
      error(s"Failed to update the job end status $result")
      throw result.failed.get
    } else true
  }

  override def countJobsByTypeAndStatus(jobType: JobType, jobStatus: JobStatus, jobCreatedTs: DateTime): Future[Int] = Future {
    val row = scala.collection.immutable.Seq(jobType.name, jobStatus)
    val result = jdbcConnection.queryForObject(currentJobsSql, row) {
      resultSet =>
        if(resultSet.next()) {
          val intValue = resultSet.getLong("jobCount")
          println("Int Value "+intValue)
          intValue.toInt
        } else 0
    }
    if(result.isSuccess) {
      result.get
    } else {
      throw result.failed.get
    }
  }
}
