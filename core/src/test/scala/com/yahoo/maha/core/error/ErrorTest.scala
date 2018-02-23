package com.yahoo.maha.core.error

import com.yahoo.maha.core.DailyGrain
import com.yahoo.maha.core.request.SyncRequest
import org.scalatest.{FunSuiteLike, Matchers}

class ErrorTest extends FunSuiteLike with Matchers{
  test("Throw GranularityNotSupportedError") {
    val p = new GranularityNotSupportedError("performance", SyncRequest, DailyGrain)
    assert(p.code == 10003)
    assert(p.message == "Cube performance does not support request type : SyncRequest -> DailyGrain")
  }
}
