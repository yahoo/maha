// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.api.example

import com.yahoo.maha.api.example.ExampleSchema.{StudentSchema, WikiSchema}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Created by pranavbhole on 24/10/17.
 */
class ExampleTest extends AnyFunSuite {

  test("Test for Example Test") {
    val mahaService = ExampleMahaService.getMahaService
    val baseRequest = ExampleRequest.getRequest
    //assert(mahaService.getDomain("wiki").isDefined)
    assert(mahaService.getDomain("student").isDefined)
  }

  test("Test for example schema") {
    val mahaSchema = ExampleSchema
    val studentSchema = StudentSchema
    val wikiSchema = WikiSchema
    mahaSchema.register()
    assert(mahaSchema.registered)
    mahaSchema.register() //explore implied else clause.
  }

}
