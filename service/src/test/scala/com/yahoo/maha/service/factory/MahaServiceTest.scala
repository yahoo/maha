// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import com.yahoo.maha.service.MahaServiceConfig

import scalaz.Failure

/**
 * Created by pranavbhole on 06/06/17.
 */
class MahaServiceTest extends BaseFactoryTest {
  test("Test MahaService Init and Validation test") {
    val jsonString = """{
                       |	"registryMap": {
                       |		"er": {
                       |			"factRegistrationClass": "erFact",
                       |			"dimensionRegistrationClass": "erDim",
                       |			"executors": ["e1", "e2"],
                       |			"generators": ["g1", "g2"],
                       |			"bucketingConfigName": "OtherBucket",
                       |			"utcTimeProviderName": "erUTC",
                       |			"parallelServiceExecutorName": "erPSE",
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
                       | "parallelServiceExecutorConfigMap": {
                       | "commonExec": {
                       | 			"factoryClass": "irUTCClass",
                       |			"config": {
                       |				"k": "v"
                       |			}
                       |  }
                       | }
                       | ,
                       |   "mahaRequestLoggingConfig" : {
                       |    "factoryClass": "com.yahoo.maha.service.factory.NoopMahaRequestLogWriterFactory",
                       |    "config" : {},
                       |    "isLoggingEnabled" : false
                       |   }
                       |}""".stripMargin
   val mahaServiceResult = MahaServiceConfig.fromJson(jsonString.getBytes("utf-8"))
    assert(mahaServiceResult.isFailure)
   mahaServiceResult match {
     case f@Failure(_) =>
       assert(f.e.toString().contains("Unable to find parallelServiceExecutor name erpse in map"))
       assert(f.e.toString().contains("Unable to find bucket config name otherbucket in map"))
   }
  }

  test("Test MahaService Validation: Expects Success in registry Validation should fail to load factory class") {
    val jsonString = """{
                       |	"registryMap": {
                       |		"er": {
                       |			"factRegistrationClass": "erFact",
                       |			"dimensionRegistrationClass": "erDim",
                       |			"executors": ["e1", "e2"],
                       |			"generators": ["g1", "g2"],
                       |			"bucketingConfigName": "erBucket",
                       |			"utcTimeProviderName": "erUTC",
                       |			"parallelServiceExecutorName": "erParallelExec",
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
                       | "parallelServiceExecutorConfigMap": {
                       | "erParallelExec": {
                       | 			"factoryClass": "irUTCClass",
                       |			"config": {
                       |				"k": "v"
                       |			}
                       |  },
                       |   "irParallelExec": {
                       | 			"factoryClass": "irUTCClass",
                       |			"config": {
                       |				"k": "v"
                       |			}
                       |  }
                       | }
                       | ,
                       |  "mahaRequestLoggingConfig" : {
                       |    "factoryClass": "com.yahoo.maha.service.factory.NoopMahaRequestLogWriterFactory",
                       |    "config" : {},
                       |    "isLoggingEnabled" : false
                       |   }
                       |}""".stripMargin
    val mahaServiceResult = MahaServiceConfig.fromJson(jsonString.getBytes("utf-8"))
    assert(mahaServiceResult.isFailure)
    mahaServiceResult.leftMap {
      list=>
        val errorList = list.map(a=> a.message).list.toList
        assert(errorList.containsSlice(Seq("Failed to construct factory : erBucketClass", "Failed to construct factory : irBucketClass")))
    }
  }
}
