package com.yahoo.maha.core.auth

import org.scalatest.{FunSuite, Matchers}

class AuthValidatorTest extends FunSuite with Matchers {

  test("test DefaultAuthValidator") {
    val authValidator: AuthValidator = new DefaultAuthValidator
    authValidator.init(null)
    val validationResult: ValidationResult = authValidator.validate(null)
    assert(validationResult.success)
    assert(validationResult.user.isEmpty)
  }

  test("test DefaultDruidAuthHeaderProvider") {
    val druidAuthHeaderProvider: DruidAuthHeaderProvider = new DefaultDruidAuthHeaderProvider
    druidAuthHeaderProvider.init(null)
    assert(druidAuthHeaderProvider.getAuthHeaders.isEmpty)
  }
}
