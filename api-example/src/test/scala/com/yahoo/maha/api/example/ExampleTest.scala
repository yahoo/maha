// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.api.example

import com.yahoo.maha.api.example.ExampleSchema.{StudentSchema, WikiSchema}
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

  test("Test for example schema") {
    val mahaSchema = ExampleSchema
    val studentSchema = StudentSchema
    val wikiSchema = WikiSchema
    mahaSchema.register()
    println("Example request default config map: " + ExampleRequest.getRequest.DEFAULT_CURATOR_JSON_CONFIG_MAP)
    assert(mahaSchema.registered)
    mahaSchema.register() //explore implied else clause.
    println("Maha Schema Values: " + mahaSchema.values)
    println("Student Schema Info: " + studentSchema)
    println("Wiki Schema Info: " + wikiSchema)
  }

}
