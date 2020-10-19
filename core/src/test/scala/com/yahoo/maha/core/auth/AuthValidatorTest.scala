package com.yahoo.maha.core.auth

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import play.api.mvc.Results

class AuthValidatorTest extends AnyFunSuite with Matchers {

  test("test DefaultAuthValidator") {
    val authValidator: AuthValidator = new DefaultAuthValidator
    authValidator.init(null)
    val validationResult: ValidationResult = authValidator.validate(null)
    assert(validationResult.success)
    assert(validationResult.user.isEmpty)
    assert(authValidator.handleAuthCallback(null) == Results.Ok)
    assert(authValidator.handleAuthFailure(null) == Results.Ok)
  }

  test("test DefaultDruidAuthHeaderProvider") {
    val druidAuthHeaderProvider: DruidAuthHeaderProvider = new DefaultDruidAuthHeaderProvider
    druidAuthHeaderProvider.init(null)
    assert(druidAuthHeaderProvider.getAuthHeaders.isEmpty)
  }
}
