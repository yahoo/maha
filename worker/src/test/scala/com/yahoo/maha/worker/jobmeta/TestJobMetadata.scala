package com.yahoo.maha.worker.jobmeta

import com.yahoo.maha.worker.jobmeta.JobStatus.JobStatus
import org.joda.time.DateTime
import java.sql.Timestamp

import com.zaxxer.hikari.HikariDataSource
import grizzled.slf4j.Logging
import slick.lifted.{ProvenShape, TableQuery, Tag}
import slick.jdbc.OracleProfile.api._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
/*
    Created by pranavbhole on 8/27/18
*/
//(Long, String, String, String, Int, DateTime, DateTime, DateTime, Long, String, String, String, Boolean)
class MahaWorkerJob(tag: Tag) extends Table[Job](tag, "maha_worker_job") {
  implicit def dateTime  =
    MappedColumnType.base[DateTime, Timestamp](
      dt => new Timestamp(dt.getMillis),
      ts => new DateTime(ts.getTime)
    )
  implicit def jobStatus  =
    MappedColumnType.base[DateTime, Timestamp](
      dt => new Timestamp(dt.getMillis),
      ts => new DateTime(ts.getTime)
    )

  def jobId                        = column[Long]("jobId", O.PrimaryKey)
  def jobType                      = column[String]("jobType")
  def jobStatus                    = column[JobStatus]("jobStatus")
  def jobResponse                  = column[String]("jobResponse")
  def numAcquired                     = column[Int]("numAcquired")
  def createdTimestamp          = column[DateTime] ("createdTimestamp"   , O.SqlType("DATE"))
  def acquiredTimestamp         = column[DateTime] ("createdTimestamp"   , O.SqlType("DATE"))
  def endedTimestamp            = column[DateTime] ("createdTimestamp"   , O.SqlType("DATE"))
  def jobParentId                   = column[Long]("jobId", O.PrimaryKey, O.SqlType("NUMBER"))
  def jobRequest                   = column[String]("jobStatus"  , O.SqlType("VARCHAR2(4000 CHAR)"))
  def hostname                     = column[String]("jobStatus"  , O.SqlType("VARCHAR2(500 CHAR)"))
  def cubeName                     = column[String]("jobStatus"  , O.SqlType("VARCHAR2(64 CHAR)"))
  def isDeleted                   = column[Boolean]("jobStatus"  , O.SqlType("VARCHAR2(64 CHAR)"))

  def * : ProvenShape[Job]= (jobId, jobType, jobStatus, jobResponse, numAcquired, createdTimestamp, acquiredTimestamp, endedTimestamp, jobParentId, jobRequest, hostname, cubeName, isDeleted) <> (AsyncJob.tupled, AsyncJob.unapply)
}

/*

 */
class TestJobMetadataDao(databaseDef: HikariDataSource) extends TableQuery(new MahaWorkerJob(_)) with JobMetadata {

  val db = Database.forDataSource(databaseDef, Some(10))
  val tableQuery = new TableQuery(tag => new MahaWorkerJob(tag))

  override def insertJob(job: AsyncJob): Boolean =  {
    db.run(
      {
        val insertAction = this += job
        if(isDebugEnabled)
          logger.info(s"Insert Statements: ${insertAction.statements.mkString("\n")}")

        insertAction
      })
    mahaWorkerJobs += job
    s"""
       |INSERT INTO $tableName (jobId,jobType,jobStatus,jobParentId,jobRequest,hostname,cubeName)
       |values(${job.jobId}, ${job.jobType}, ${job.jobStatus}, ${job.jobParentId}, ${job.jobRequest}, ${job.hostname}, ${job.cubeName});
     """.stripMargin
  }

  override def deleteJob(job: Job): Boolean = {
    s"""
       |ALTER $tableName UPDATE is_deleted = 'Y' WHERE jobId = ${job.jobId};
     """.stripMargin
  }

  override def findById(jobId: Long): Job  = {

  }

  override def updateJobStatus(jobId: Long, jobStatus: JobStatus): Boolean = ???

  override def updateJobEnded(jobId: Long, endStatus: JobStatus, message: String): Boolean = ???

  override def countJobsByTypeAndStatus(jobType: JobType, jobStatus: JobStatus, jobCreatedTs: DateTime): Int = ???
}
