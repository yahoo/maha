// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.config

import org.scalatest.{FunSuite, Matchers}
import org.json4s._
import org.json4s.scalaz.JsonScalaz._
import org.json4s.jackson.JsonMethods._


/**
  * Created by hiral on 5/25/17.
  */
class JsonMahaServiceConfigTest extends FunSuite with Matchers {

  test("successfully parse json") {
    val jsonString = """{
                       |	"registryMap": {
                       |		"er": {
                       |			"factRegistrationClass": "erFact",
                       |			"dimensionRegistrationClass": "erDim",
                       |			"executors": ["e1", "e2"],
                       |			"generators": ["g1", "g2"],
                       |			"bucketingConfigName": "erBucket",
                       |			"utcTimeProviderName": "erUTC",
                       |			"parallelServiceExecutorName": "erPSE",
                       |			"dimEstimatorFactoryClass": "dimEstFactoryClass",
                       |			"dimEstimatorFactoryConfig": "dimEstFactoryConfig",
                       |			"factEstimatorFactoryClass": "factEstFactoryClass",
                       |			"factEstimatorFactoryConfig": "factEstFactoryConfig",
                       |			"defaultPublicFactRevisionMap": {"a": 1, "b": 2},
                       |			"defaultPublicDimRevisionMap": {"a": 1, "b": 2}
                       |		},
                       |		"ir": {
                       |			"factRegistrationClass": "irFact",
                       |			"dimensionRegistrationClass": "irDim",
                       |			"executors": ["e1", "e2"],
                       |			"generators": ["g1", "g2"],
                       |			"bucketingConfigName": "irBucket",
                       |			"utcTimeProviderName": "irUTC",
                       |			"parallelServiceExecutorName": "irPSE",
                       |			"dimEstimatorFactoryClass": "dimEstFactoryClass",
                       |			"dimEstimatorFactoryConfig": "dimEstFactoryConfig",
                       |			"factEstimatorFactoryClass": "factEstFactoryClass",
                       |			"factEstimatorFactoryConfig": "factEstFactoryConfig",
                       |			"defaultPublicFactRevisionMap": {"a": 1, "b": 2},
                       |			"defaultPublicDimRevisionMap": {"a": 1, "b": 2}
                       |		}
                       |	},
                       |	"executorMap": {
                       |		"e1": {
                       |			"factoryClass": "e1Class",
                       |			"config": {
                       |				"k": "v"
                       |			}
                       |		},
                       |		"e2": {
                       |			"factoryClass": "e2Class",
                       |			"config": {
                       |				"k": "v"
                       |			}
                       |		}
                       |	},
                       |	"generatorMap": {
                       |		"g1": {
                       |			"factoryClass": "g1Class",
                       |			"config": {
                       |				"k": "v"
                       |			}
                       |		},
                       |		"g2": {
                       |			"factoryClass": "g2Class",
                       |			"config": {
                       |				"k": "v"
                       |			}
                       |		}
                       |	},
                       |	"bucketingConfigMap": {
                       |		"erBucket": {
                       |			"factoryClass": "erBucketClass",
                       |			"config": {
                       |				"k": "v"
                       |			}
                       |		},
                       |		"irBucket": {
                       |			"factoryClass": "irBucketClass",
                       |			"config": {
                       |				"k": "v"
                       |			}
                       |		}
                       |
                       |	},
                       |	"utcTimeProviderMap": {
                       |		"erUTC": {
                       |			"factoryClass": "erUTCClass",
                       |			"config": {
                       |				"k": "v"
                       |			}
                       |		},
                       |		"irUTC": {
                       |			"factoryClass": "irUTCClass",
                       |			"config": {
                       |				"k": "v"
                       |			}
                       |		}
                       |	},
                       |"parallelServiceExecutorConfigMap": {
                       | "commonExec": {
                       | 			"factoryClass": "irUTCClass",
                       |			"config": {
                       |				"k": "v"
                       |			}
                       |  }
                       | }
                       |,
                       | "mahaRequestLoggingConfig" : {
                       |    "factoryClass": "com.yahoo.maha.service.factory.NoopMahaRequestLogWriterFactory",
                       |    "config" : {},
                       |    "isLoggingEnabled" : false
                       |   },
                       |"curatorMap": {
                       |      "default": {
                       |         "factoryClass": "com.yahoo.maha.service.factory.DefaultCuratorFactory",
                       |         "config": {
                       |         }
                       |      },
                       |      "timeshift": {
                       |         "factoryClass": "com.yahoo.maha.service.factory.TimeShiftCuratorFactory",
                       |         "config": {
                       |         }
                       |      }
                       |   }
                       |}""".stripMargin
    val json = parse(jsonString)
    val result = fromJSON[JsonMahaServiceConfig](json)
    assert(result.isSuccess, result)
    val config = result.toOption.get
    assert(config.registryMap.contains("er"))
    assert(config.registryMap.get("er").get.dimEstimatorFactoryClass ==  "dimEstFactoryClass")
    assert(config.registryMap.contains("ir"))
    assert(config.executorMap.contains("e1"))
    assert(config.executorMap.contains("e2"))
    assert(config.generatorMap.contains("g1"))
    assert(config.generatorMap.contains("g2"))
    assert(config.bucketingConfigMap.contains("erbucket"))
    assert(config.bucketingConfigMap.contains("irbucket"))
    assert(config.utcTimeProviderMap.contains("erutc"))
    assert(config.utcTimeProviderMap.contains("irutc"))
  }
}
