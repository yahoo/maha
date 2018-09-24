// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.utils

import org.scalatest.{FunSuite, Matchers}
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
  * Created by panditsurabhi on 24/09/18.
  */
class DynamicConfigurationUtilsTest extends FunSuite with Matchers {

  test("getDynamicFields should return all dynamic fields in the json") {
    val jsonStr = s""""{"key1": "val1", "key2": "<%(dynamic.val2,30000)%>}""""
    val json = parse(jsonStr)
    val dynamicFields = DynamicConfigurationUtils.getDynamicFields(json)
    println(dynamicFields)
  }
}