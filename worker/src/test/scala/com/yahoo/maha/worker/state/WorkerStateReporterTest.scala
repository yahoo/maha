package com.yahoo.maha.worker.state

import com.yahoo.maha.core.OracleEngine
import com.yahoo.maha.worker.state.actor.{SyncExecution, WorkerStateActor}
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
/*
    Created by pranavbhole on 8/30/18
*/
class WorkerStateReporterTest extends  FunSuite with Matchers {

  val workerStateReporter = WorkerStateReporter("test-config")

  test("Test workerStateReporter") {
    workerStateReporter.jobStarted(SyncExecution, 12345, OracleEngine, 10000, 900, "maha-test-user")
    workerStateReporter.jobEnded(SyncExecution, 12345, OracleEngine, 10000, 900, "maha-test-user")

    val system = workerStateReporter.system
    val workerStateActorSelection = system.actorSelection(workerStateReporter.workerStateActorPath)
    val workerStateActorFuture = workerStateActorSelection.resolveOne(1000 millis)

    workerStateActorFuture.onComplete {
      actorTry =>
        assert(actorTry.isSuccess)
    }
    assert(workerStateActorFuture.isCompleted)
  }

}
