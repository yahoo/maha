package com.yahoo.maha.worker.jobmeta

import java.sql.ResultSet

import com.yahoo.maha.jdbc
import com.yahoo.maha.jdbc.JdbcConnection
import com.yahoo.maha.worker.jobmeta.JobStatus.JobStatus
import grizzled.slf4j.Logging
import org.joda.time.DateTime
import scalaz.Failure

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Success


/*
    Created by pranavbhole on 8/29/18

    Test Job Metadata Dao with unhandled exceptions
*/
case class TestJobMetadataDao(jdbcConnection: JdbcConnection) extends JobMetadata with Logging {

  val tableName = "maha_worker_job"

  val insertSQL = s"""INSERT INTO $tableName (jobId, jobType, jobStatus,
    jobResponse, numAcquired, createdTimestamp,
    acquiredTimestamp, endedTimestamp,jobParentId,
     jobRequest, hostname, cubeName,
      isDeleted) values(?,?,?,?,?,?,?,?,?,?,?,?,?)""".stripMargin


  override def insertJob(job: Job): Future[Boolean] = {
    Future {
      val row = scala.collection.immutable.Seq(job.jobId, job.jobType, job.jobStatus.toString,
        job.jobResponse, job.numAcquired, job.createdTimestamp,
        job.acquiredTimestamp, job.endedTimestamp,job.jobParentId,
        job.jobRequest, job.hostname,job.cubeName,
        0)
      val result = jdbcConnection.executeUpdate(insertSQL, row)
      info("Result: "+result)
      if(result.isFailure) {
        error(s"Failed to create job $result")
        false
      } else true
    }
  }

  override def deleteJob(job: Job): Future[Boolean] = Future.never

  override def findById(jobId: Long): Future[Option[Job]] = {
    val result:Future[Option[Job]] = Future {
      val resultSetTry = jdbcConnection.queryForObject(s"SELECT * from maha_worker_job") {
        rs =>
          if(rs.next()) {
            val job = AsyncJob(rs.getLong("jobId"),
              rs.getString("jobType"),
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

  override def updateJobStatus(jobId: Long, jobStatus: JobStatus): Future[Boolean] = Future(true)

  override def updateJobEnded(jobId: Long, endStatus: JobStatus, message: String): Future[Boolean] = Future(true)

  override def countJobsByTypeAndStatus(jobType: JobType, jobStatus: JobStatus, jobCreatedTs: DateTime): Future[Int] = Future(1)

  def getMap(queryResult: ResultSet) : List[Map[String, AnyRef]] = {
    val rowCount =
      if (queryResult.last()) {
        queryResult.getRow
      }
      else 0

    queryResult.beforeFirst()
    val md = queryResult.getMetaData
    val colNames = (1 to md.getColumnCount) map md.getColumnName

    val rows:List[Map[String, AnyRef]] =
      for {i <- List.range(0, rowCount)} yield {
        queryResult.next()
        val results = colNames map(n => queryResult.getObject(n))
        Map(colNames zip results:_*)
      }
    rows
  }

}
