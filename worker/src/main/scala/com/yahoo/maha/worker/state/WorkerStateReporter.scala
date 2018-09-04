// Copyright 2018, Oath Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.

package com.yahoo.maha.worker.state

/*
    Created by pranavbhole on 8/13/18
*/
import java.io.File

import akka.actor.{Actor, ActorPath, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}
import com.yahoo.maha.core.Engine
import com.yahoo.maha.worker.state.actor._
import grizzled.slf4j.Logging

object WorkerStateReporter extends Logging {

  // Use a bounded mailbox to prevent memory leaks in the rare case when jobs get piled up to be processed by the actor
  val defaultConfig: Config = ConfigFactory.parseString(
    """
      |akka.actor.nonblocking_bounded_mailbox {
      |  mailbox-type = akka.dispatch.NonBlockingBoundedMailbox
      |  mailbox-capacity = 10000
      |}
      |akka {
      |  loggers = ["akka.event.slf4j.Slf4jLogger"]
      |  loglevel = "INFO"
      |}
      |""".stripMargin)

}



case class WorkerStateReporter(akkaConf: String) extends Logging {

  val config: Config = {
    val file = new File(akkaConf)
    if(file.exists() && file.canRead) {
      info(s"Using akka conf file : ${file.getAbsolutePath}")
      ConfigFactory.parseFile(file)
    } else {
      info("Using default akka config")
      WorkerStateReporter.defaultConfig
    }
  }
  val system = ActorSystem("maha-workers", config)
  lazy val workerStateActorPath: ActorPath = {
    val actorConfig = WorkerStateActorConfig()
    val props: Props = Props(classOf[WorkerStateActor], actorConfig).withMailbox("akka.actor.nonblocking_bounded_mailbox")
    val path = system.actorOf(props, actorConfig.name).path
    info(s"Created WorkerStateActor: $path")
    path
  }

  def jobStarted(executionType: ExecutionType, jobId: Long, engine: Engine, cost: Long, estimatedRows: Long, userId: String): Unit = {
    sendMessage(JobStarted(executionType, jobId, engine, cost, estimatedRows, userId))
  }

  def jobEnded(executionType: ExecutionType, jobId: Long, engine: Engine, cost: Long, estimatedRows: Long, userId: String): Unit = {
    sendMessage(JobEnded(executionType, jobId, engine, cost, estimatedRows, userId))
  }

  def sendMessage(actorMessage:WorkerStateActorMessage) = {
    try {
      system.actorSelection(workerStateActorPath).tell(actorMessage, Actor.noSender)
    } catch {
      case t: Throwable =>
        warn(s"Failed to send $actorMessage message to WorkerStateActor", t)
    }
  }
}
