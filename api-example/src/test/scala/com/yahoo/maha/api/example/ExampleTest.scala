// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.api.example

import org.scalatest.FunSuite

/**
 * Created by pranavbhole on 24/10/17.
 */
class ExampleTest extends FunSuite {

  test("Test for Example Test") {
    val mahaService = ExampleMahaService.getMahaService("main")
    println(mahaService)
    assert(mahaService.getDomain("wiki").isDefined)
    assert(mahaService.getDomain("student").isDefined)
  }

}
