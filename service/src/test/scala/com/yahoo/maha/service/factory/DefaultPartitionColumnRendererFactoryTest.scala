// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
  * Created by hiral on 5/30/17.
  */
class DefaultPartitionColumnRendererFactoryTest extends BaseFactoryTest {

  test("successfully build factory from json") {
    val jsonString =     """[{}]"""

    val factoryResult = getFactory[PartitionColumnRendererFactory]("com.yahoo.maha.service.factory.DefaultPartitionColumnRendererFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val partitionColRendererResult = factory.fromJson(json)
    assert(partitionColRendererResult.isSuccess, partitionColRendererResult)
    assert(factory.supportedProperties == List.empty)
  }
}
