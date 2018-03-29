package com.yahoo.maha.service.error

import org.scalatest.{FunSuite, Matchers}

class MahaServiceExceptionTest extends FunSuite with Matchers {
  test("MahaServiceExceptionTest: MahaServiceBadRequestException") {
    val ex : MahaServiceBadRequestException = MahaServiceBadRequestException(message = "Got a bad request exception!")
    assert(ex.errorCode.equals(400), "BadRequest should throw 400")
  }

  test("MahaServiceExceptionTest: MahaServiceExecutionException") {
    val ex : MahaServiceExecutionException = MahaServiceExecutionException(message = "Got an execution exception!")
    assert(ex.errorCode.equals(500), "ServiceExecution should throw 500")
  }

}
