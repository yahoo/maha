// Copyright 2018, Yahoo Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.

package com.yahoo.maha.worker.state

import com.yahoo.maha.core.{DruidEngine, OracleEngine}
import com.yahoo.maha.worker.state.actor._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
/*
    Created by pranavbhole on 8/30/18
*/
class WorkerStateReporterTest extends AnyFunSuite with Matchers {

  val workerStateReporter = WorkerStateReporter("test-config")

  val workerStateReporterFromFile = WorkerStateReporter("src/main/test/resources/akkaActorConfigTest.conf")

  test("Test workerStateReporter") {

    workerStateReporter.jobStarted(SyncExecution, 12345, OracleEngine, 10000, 900, "maha-test-user")
    workerStateReporter.jobEnded(SyncExecution, 12345, OracleEngine, 10000, 900, "maha-test-user")

    workerStateReporterFromFile.jobStarted(SyncExecution, 12345, OracleEngine, 10000, 900, "maha-test-user")
    workerStateReporterFromFile.jobEnded(SyncExecution, 12345, OracleEngine, 10000, 900, "maha-test-user")

    workerStateReporter.sendMessage(AllEngineStats)
    workerStateReporter.sendMessage(GetEngineStats(DruidEngine))
    workerStateReporter.sendMessage(AllEngineStats)
    workerStateReporter.sendMessage(GetUserStats(""))
    workerStateReporter.sendMessage(GetJobsStats)
    workerStateReporter.sendMessage(SimulateDelay(1))

    val system = workerStateReporter.system
    val workerStateActorSelection = system.actorSelection(workerStateReporter.workerStateActorPath)
    val workerStateActorFuture = workerStateActorSelection.resolveOne(1000 millis)

    workerStateActorFuture.onComplete {
      actorTry =>
        assert(actorTry.isSuccess)
    }
    import scala.concurrent.duration._
    Await.result(workerStateActorFuture, 500 millis)

    assert(workerStateActorFuture.isCompleted)
  }


}
