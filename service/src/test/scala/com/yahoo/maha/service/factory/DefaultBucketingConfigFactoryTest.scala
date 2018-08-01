// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import com.yahoo.maha.core.HiveEngine
import com.yahoo.maha.core.query.Version
import com.yahoo.maha.service.{DefaultMahaServiceConfigContext, MahaServiceConfigContext}
import org.scalatest.{FunSuite, Matchers}
import org.json4s._
import org.json4s.scalaz.JsonScalaz._
import org.json4s.jackson.JsonMethods._

/**
  * Created by hiral on 5/30/17.
  */
class DefaultBucketingConfigFactoryTest extends BaseFactoryTest {
  implicit val context: MahaServiceConfigContext = DefaultMahaServiceConfigContext()


  test("successfully build factory from json") {
    val jsonString =     """{"cube":
                           |[{
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
                           |}],
                           |"queryGenerator": []}""".stripMargin

    val factoryResult = getFactory[BucketingConfigFactory]("com.yahoo.maha.service.factory.DefaultBucketingConfigFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val bucketingConfigResult = factory.fromJson(json)
    assert(bucketingConfigResult.isSuccess, bucketingConfigResult)
    assert(factory.supportedProperties == List.empty)
  }

  test("successfully build query gen bucketing config from json") {
    val jsonString =     """{"cube":
                           |[{
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
                           |}],
                           |"queryGenerator": [{
                           |	  "engine": "hive",
                           |		"internal": [{
                           |			"revision": 0,
                           |      "percent": 20
                           |		}, {
                           |      "revision": 1,
                           |      "percent": 80
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
                           |      "percent": 10
                           |		}, {
                           |      "revision": 1,
                           |      "percent": 10
                           |    }],
                           |    "userWhiteList": [{
                           |      "user" : "uid",
                           |      "revision": 0
                           |    }]
                           |}
                           |]}""".stripMargin

    val factoryResult = getFactory[BucketingConfigFactory]("com.yahoo.maha.service.factory.DefaultBucketingConfigFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val bucketingConfigResult = factory.fromJson(json)
    assert(bucketingConfigResult.isSuccess, bucketingConfigResult)
    assert(factory.supportedProperties == List.empty)
    val qgenBucketingConfig = bucketingConfigResult.toOption.get.getConfigForQueryGen(HiveEngine)
    assert(qgenBucketingConfig.isDefined)
    val externalPercent = qgenBucketingConfig.get.externalBucketPercentage
    val internalPercent = qgenBucketingConfig.get.internalBucketPercentage
    val dryRunPercent = qgenBucketingConfig.get.dryRunPercentage
    val whitelist = qgenBucketingConfig.get.userWhiteList
    assert(externalPercent(Version.v0).equals(90))
    assert(externalPercent(Version.v1).equals(10))
    assert(internalPercent(Version.v0).equals(20))
    assert(internalPercent(Version.v1).equals(80))
    assert(dryRunPercent(Version.v0).equals(10))
    assert(dryRunPercent(Version.v1).equals(10))
    assert(whitelist("uid").equals(Version.v0))
  }
}
