package com.yahoo.maha.core.error

import com.yahoo.maha.core.DailyGrain
import com.yahoo.maha.core.request.SyncRequest
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ErrorTest extends AnyFunSuite with Matchers{
  test("Throw GranularityNotSupportedError") {
    val p = new GranularityNotSupportedError("performance", SyncRequest, DailyGrain)
    assert(p.code == 10003)
    assert(p.message == "Cube performance does not support request type : SyncRequest -> DailyGrain")
  }
}
