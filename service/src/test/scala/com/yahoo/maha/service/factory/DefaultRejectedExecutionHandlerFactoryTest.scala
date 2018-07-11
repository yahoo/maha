// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import com.yahoo.maha.service.{DefaultMahaServiceConfigContext, MahaServiceConfigContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
  * Created by hiral on 5/30/17.
  */
class DefaultRejectedExecutionHandlerFactoryTest extends BaseFactoryTest {
  implicit val context: MahaServiceConfigContext = DefaultMahaServiceConfigContext()

  test("successfully build factory from json") {
    val jsonString =     """[{}]"""

    val factoryResult = getFactory[DefaultRejectedExecutionHandlerFactory]("com.yahoo.maha.service.factory.DefaultRejectedExecutionHandlerFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val defaultRejectedExecutionHandlerFactoryResult = factory.fromJson(json)
    assert(defaultRejectedExecutionHandlerFactoryResult.isSuccess, defaultRejectedExecutionHandlerFactoryResult)
    assert(factory.supportedProperties == List.empty)
  }
}
