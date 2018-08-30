// Copyright 2018, Oath Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.

package com.yahoo.maha.worker.state.actor

import java.net.InetAddress

import akka.actor.Actor
import com.yahoo.maha.core.{DruidEngine, Engine, OracleEngine}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import grizzled.slf4j.Logging
import scala.collection.mutable

/*
    Created by hiral on 8/13/18
*/
sealed trait ExecutionType
case object AsyncExecution extends ExecutionType
case object SyncExecution extends ExecutionType

sealed trait WorkerStateActorMessage

sealed trait StatefulWorkerStateActorMessage extends WorkerStateActorMessage {
  def updateSyncEngineStats(map: Map[Engine, EngineState]) : Unit
  def updateSyncUserStats(map: mutable.Map[String, UserState]) : Unit
  def updateSyncJobsStats(jobsState: JobsState) : Unit
}

case class JobStarted(executionType: ExecutionType, jobId: Long, engine: Engine, cost: Long, estimatedRows:Long, userId: String) extends StatefulWorkerStateActorMessage {
  def updateSyncEngineStats(map: Map[Engine, EngineState]) : Unit = {
    executionType match {
      case SyncExecution =>
        map.get(engine).foreach(_.addJob(cost, estimatedRows))
      case _ =>
    }
  }
  def updateSyncUserStats(map: mutable.Map[String, UserState]) : Unit = {
    executionType match {
      case SyncExecution =>
        val userStateOption = map.get(userId)
        if (userStateOption.isDefined) {
          userStateOption.foreach(_.addJob(cost, estimatedRows))
        } else {
          val userState = UserState(userId)
          userState.addJob(cost, estimatedRows)
          map += (userId -> userState)
        }
      case _ =>
    }
  }
  def updateSyncJobsStats(jobsState: JobsState) : Unit = {
    jobsState.addJob(cost, estimatedRows)
  }
}

case class JobEnded(executionType: ExecutionType, jobId: Long, engine: Engine, cost: Long, estimatedRows: Long, userId: String) extends StatefulWorkerStateActorMessage {
  def updateSyncEngineStats(map: Map[Engine, EngineState]) : Unit = {
    executionType match {
      case SyncExecution =>
        map.get(engine).foreach(_.removeJob(cost, estimatedRows))
      case _ =>
    }
  }
  def updateSyncUserStats(map: mutable.Map[String, UserState]) : Unit = {
    executionType match {
      case SyncExecution =>
        val userStateOption = map.get(userId)
        if (userStateOption.isDefined) {
          userStateOption.foreach(_.removeJob(cost, estimatedRows))
        } else {
          //nothing to remove
        }
      case _ =>
    }
  }
  def updateSyncJobsStats(jobsState: JobsState) : Unit = {
    jobsState.removeJob(cost, estimatedRows)
  }
}

case object AllEngineStats extends WorkerStateActorMessage
case object AllUserStats extends WorkerStateActorMessage
case class GetEngineStats(engine: Engine) extends WorkerStateActorMessage
case class GetUserStats(userId: String) extends WorkerStateActorMessage
case object GetJobsStats extends WorkerStateActorMessage
case class SimulateDelay(delay: Int) extends WorkerStateActorMessage

object CommonState {
  val hostName: String = InetAddress.getLocalHost.getHostName
}
trait CommonState {
  protected[this] var countJobs: Int = 0
  protected[this] var countJobsWithCost: Long = 0
  protected[this] var costJobs: Long = 0
  protected[this] var estimatedRowsJobs: Long = 0

  def hostName: String = CommonState.hostName

  def addJob(cost:Long, estimatedRows: Long): Unit = {
    countJobs += 1
    if(cost != Long.MaxValue) {
      countJobsWithCost += 1
      costJobs += cost
      estimatedRowsJobs += estimatedRows
    }
  }

  def removeJob(cost:Long, estimatedRows: Long): Unit = {
    countJobs -= 1
    if(cost != Long.MaxValue) {
      countJobsWithCost -= 1
      costJobs -= cost
      estimatedRowsJobs -= estimatedRows
    }
  }
}

case class EngineState(engine: Engine) extends CommonState with Logging {
  override def toString: String = {
    val json = "workerState" ->
      ("hostname" -> hostName) ~
        ("engine" -> engine.toString) ~
        ("countJobs" -> countJobs) ~
        ("countJobsWithCost" -> countJobsWithCost) ~
        ("countJobsWithoutCost" -> (countJobs - countJobsWithCost) ) ~
        ("costJobs" -> costJobs) ~
        ("estimatedRowsJobs" -> estimatedRowsJobs)
    val workerStateStr = compact(render(json))
    workerStateStr
  }
}

case class UserState(userId: String) extends CommonState with Logging {
  override def toString: String = {
    val json = "workerState" ->
      ("hostname" -> hostName) ~
        ("userId" -> userId) ~
        ("countJobs" -> countJobs) ~
        ("countJobsWithCost" -> countJobsWithCost) ~
        ("countJobsWithoutCost" -> (countJobs - countJobsWithCost) ) ~
        ("costJobs" -> costJobs) ~
        ("estimatedRowsJobs" -> estimatedRowsJobs)
    val workerStateStr = compact(render(json))
    workerStateStr
  }
}

class JobsState extends CommonState with Logging {
  override def toString: String = {
    val json = "workerState" ->
      ("hostname" -> hostName) ~
        ("countJobs" -> countJobs) ~
        ("countJobsWithCost" -> countJobsWithCost) ~
        ("countJobsWithoutCost" -> (countJobs - countJobsWithCost) ) ~
        ("costJobs" -> costJobs) ~
        ("estimatedRowsJobs" -> estimatedRowsJobs)
    val workerStateStr = compact(render(json))
    workerStateStr
  }
}

case class WorkerStateActorConfig(name: String = "maha-worker-state")
class WorkerStateActor(config: WorkerStateActorConfig) extends Actor with Logging  {

  val syncEngineStats :Map[Engine, EngineState] =
    Map(OracleEngine -> EngineState(OracleEngine), DruidEngine -> EngineState(DruidEngine))

  val syncUserStats : scala.collection.mutable.Map[String, UserState] = new mutable.HashMap[String, UserState]()

  val syncJobsStats : JobsState = new JobsState

  override def preStart(): Unit = {
    info(s"Starting actor ${getClass.getName}")
  }

  override def receive: Receive = {
    case statefulMessage: StatefulWorkerStateActorMessage =>
      statefulMessage.updateSyncEngineStats(syncEngineStats)
      statefulMessage.updateSyncUserStats(syncUserStats)
      statefulMessage.updateSyncJobsStats(syncJobsStats)
    case AllEngineStats =>
      sender ! syncEngineStats.valuesIterator.toList
    case AllUserStats =>
      sender ! syncUserStats.valuesIterator.toList
    case GetEngineStats(engine) =>
      sender ! syncEngineStats.get(engine)
    case GetUserStats(userId) =>
      sender ! syncUserStats.get(userId)
    case GetJobsStats =>
      sender ! syncJobsStats
    case SimulateDelay(delay) =>
      Thread.sleep(delay)
    case any =>
      info(s"Received unknown message $any")
  }

  override def postStop(): Unit = {
    info(s"Stopping actor ${getClass.getName}")
  }
}
