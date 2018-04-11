// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
  * Created by hiral on 5/30/17.
  */
class DefaultResultSetTransformersFactoryTest extends BaseFactoryTest {

  test("successfully build factory from json") {
    val jsonString =     """[{}]"""

    val factoryResult = getFactory[DefaultResultSetTransformersFactory]("com.yahoo.maha.service.factory.DefaultResultSetTransformersFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val defaultResultSetTransformersFactoryResult = factory.fromJson(json)
    assert(defaultResultSetTransformersFactoryResult.isSuccess, defaultResultSetTransformersFactoryResult)
    assert(factory.supportedProperties == List.empty)
  }
}
