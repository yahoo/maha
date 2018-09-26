// Copyright 2018, Yahoo Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.

package com.yahoo.maha.worker.state.actor

import com.yahoo.maha.core.OracleEngine
import org.scalatest.{FunSuite, Matchers}

/*
    Created by pranavbhole on 8/30/18
*/
class WorkerStateActorTest extends FunSuite with Matchers {

  test("Test WorkerStateActor") {
    val engineState = EngineState(OracleEngine)
    assert(engineState.toString.contains("estimatedRowsJobs"))
    val userState = UserState("maha-worker")
    assert(userState.toString.contains("maha-worker"))
    val jobState = new JobsState()
    assert(jobState.toString.contains("estimatedRowsJobs"))
  }
}
