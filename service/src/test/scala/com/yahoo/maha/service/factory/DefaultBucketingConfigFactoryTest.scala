// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import org.scalatest.{FunSuite, Matchers}
import org.json4s._
import org.json4s.scalaz.JsonScalaz._
import org.json4s.jackson.JsonMethods._

/**
  * Created by hiral on 5/30/17.
  */
class DefaultBucketingConfigFactoryTest extends BaseFactoryTest {

  test("successfully build factory from json") {
    val jsonString =     """[{
                           |	  "cube": "mycube",
                           |		"internal": [{
                           |			"revision": 0,
                           |      "percent": 10
                           |		}, {
                           |      "revision": 1,
                           |      "percent": 90
                           |    }],
                           |		"external": [{
                           |			"revision": 0,
                           |      "percent": 90
                           |		}, {
                           |      "revision": 1,
                           |      "percent": 10
                           |		}],
                           |    "dryRun": [{
                           |			"revision": 0,
                           |      "percent": 10,
                           |      "engine" : "Oracle"
                           |		}, {
                           |      "revision": 1,
                           |      "percent": 10
                           |    }],
                           |    "userWhiteList": [{
                           |      "user" : "uid",
                           |      "revision": 0
                           |    }]
                           |}]""".stripMargin

    val factoryResult = getFactory[BucketingConfigFactory]("com.yahoo.maha.service.factory.DefaultBucketingConfigFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val bucketingConfigResult = factory.fromJson(json)
    assert(bucketingConfigResult.isSuccess, bucketingConfigResult)
  }
}
