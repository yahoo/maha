// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.bucketing

import com.yahoo.maha.core.query.Version
import com.yahoo.maha.core.{DruidEngine, Engine, HiveEngine, OracleEngine}
import com.yahoo.maha.core.registry.Registry
import org.mockito.Mockito._
import org.scalatest.FunSuite

/**
  * Created by surabhip on 8/29/16.
  */
class BucketSelectorTest extends FunSuite {

  val registry = mock(classOf[Registry])
  when(registry.defaultPublicFactRevisionMap).thenReturn(Map("test-cube" -> 1, "test-cube-2" -> 1).toMap)

  val bucketSelector = new BucketSelector(registry, TestBucketingConfig)

  test("Internal bucket test") {
    val bucketParams = new BucketParams(new UserInfo("test-user", true)) // isInternal = true
    val respTry = bucketSelector.selectBucketsForCube("test-cube", bucketParams)
    require(respTry.isSuccess, respTry)
    val response = respTry.get
    assert(response.revision == 1)
  }

  test("Default Bucketing Config Test") {
    val dbc = new DefaultBucketingConfig(Map.empty,Map.empty)
    require(dbc.isInstanceOf[BucketingConfig])
    require(dbc.asInstanceOf[BucketingConfig].getConfigForCube("performance_stats").isDefined == false)
  }

  test("External bucket test") {
    val bucketParams = new BucketParams(new UserInfo("test-user", false)) // isInternal = false
    val respTry = bucketSelector.selectBucketsForCube("test-cube", bucketParams)
    require(respTry.isSuccess, respTry)
    val response = respTry.get
    assert(response.revision == 2 || response.revision == 3)
  }

  test("DryRun bucket test") {
    val bucketParams = new BucketParams(new UserInfo("test-user", true)) // isInternal = true
    var countofRev2 = 0
    for (i <- 0 to 100) {
      val respTry = bucketSelector.selectBucketsForCube("test-cube", bucketParams)
      require(respTry.isSuccess, respTry)
      val response = respTry.get
      if (response.dryRunRevision.isDefined) {
        if (response.dryRunRevision.get == 2) countofRev2 += 1
      }
    }
    
    assert(countofRev2 > 0)
  }

  test("DryRun bucket test with dryRunRevision and forceEngine but no cube config") {
    val bucketSelector = new BucketSelector(registry, NoBucketingConfig)
    val bucketParams = new BucketParams(new UserInfo("test-user", true), dryRunRevision = Option(1), forceEngine = Option(OracleEngine)) // isInternal = true
    val respTry = bucketSelector.selectBucketsForCube("test-cube-2", bucketParams)
    assert(respTry.isSuccess, respTry)
  }

  test("DryRun bucket test with dryRunRevision and no forceEngine but no cube config") {
    val bucketSelector = new BucketSelector(registry, NoBucketingConfig)
    val bucketParams = new BucketParams(new UserInfo("test-user", true), dryRunRevision = Option(1)) // isInternal = true
    val respTry = bucketSelector.selectBucketsForCube("test-cube-2", bucketParams)
    assert(respTry.isSuccess, respTry)
  }

  test("Test for invalid cube") {
    val bucketParams = new BucketParams(new UserInfo("test-user", false)) // isInternal = false
    val respTry = bucketSelector.selectBucketsForCube("invalid-cube", bucketParams)
    require(respTry.isFailure, respTry)
    assert(respTry.failed.get.isInstanceOf[IllegalArgumentException])
    assert(respTry.failed.get.getMessage.contains("invalid-cube"))
  }


  test("Test for no dryRun config") {
    val bucketSelector = new BucketSelector(registry, TestBucketingConfigNoDryRun)
    val bucketParams = new BucketParams(new UserInfo("test-user", true)) // isInternal = true
    val respTry = bucketSelector.selectBucketsForCube("test-cube", bucketParams)
    require(respTry.isSuccess, respTry)
    assert(respTry.get.dryRunRevision.equals(None))
    assert(respTry.get.dryRunEngine.equals(None))
    assert(respTry.get.revision == 1)
  }

  test("Test for dryRun single revision config") {
    val bucketSelector = new BucketSelector(registry, TestBucketingConfigDryRunOneRevision)
    val bucketParams = new BucketParams(new UserInfo("test-user", false)) // isInternal = false
    var countOfRev1 = 0
    for (i <- 1 to 100) {
      val response = bucketSelector.selectBucketsForCube("test-cube", bucketParams)
      if (response.get.dryRunRevision.isDefined) {
        if (response.get.dryRunRevision.get == 1) countOfRev1 += 1
      }
    }
    
    assert(countOfRev1 > 0 && countOfRev1 <= 100)
  }

  test("Whitelisted user test") {
    val bucketSelector = new BucketSelector(registry, TestBucketingConfigWhiteListedOneRevision)
    val bucketParams = new BucketParams(new UserInfo("test-user", false)) // isInternal = false
    val response = bucketSelector.selectBucketsForCube("test-cube",bucketParams)
    assert(response.get.revision==1)
  }

  test("QueryGenerator bucket selection should succeed") {
    val bucketSelector = new BucketSelector(registry, TestBucketingConfigForQueryGenerator)
    val bucketParams = new BucketParams(new UserInfo("test-user", false)) // isInternal = false
    val response = bucketSelector.selectBucketsForQueryGen(HiveEngine, bucketParams)
    assert(response.get.queryGenVersion.equals(Version.v1))
    assert(response.get.dryRunQueryGenVersion.get.equals(Version.v1))
  }

  test("Version forced in bucketParams should be selected ") {
    val bucketSelector = new BucketSelector(registry, TestEmptyBucketingConfigForQueryGenerator)
    val bucketParams = new BucketParams(new UserInfo("test-user", false), forceQueryGenVersion = Option(Version.v2)) // isInternal = false
    val response = bucketSelector.selectBucketsForQueryGen(HiveEngine, bucketParams)
    assert(response.get.queryGenVersion.equals(Version.v2))
    assert(response.get.dryRunQueryGenVersion.isEmpty)
  }

  test("Invalid internal bucketing configs should throw Exception") {
    val bucketSelector = new BucketSelector(registry, TestInvalidBucketingConfigForQueryGenerator1)
    val bucketParams = new BucketParams(new UserInfo("test-user", false), forceQueryGenVersion = Option(Version.v2)) // isInternal = false
    val response = bucketSelector.selectBucketsForQueryGen(HiveEngine, bucketParams)
    assert(response.isFailure, "Expected failure while using invalid bucketing config")
    assert(response.failed.get.getMessage.contains("Total internal bucket percentage is not 100%"))
  }

  test("Invalid external bucketing configs should throw Exception") {
    val bucketSelector = new BucketSelector(registry, TestInvalidBucketingConfigForQueryGenerator2)
    val bucketParams = new BucketParams(new UserInfo("test-user", false), forceQueryGenVersion = Option(Version.v2)) // isInternal = false
    val response = bucketSelector.selectBucketsForQueryGen(HiveEngine, bucketParams)
    assert(response.isFailure, "Expected failure while using invalid bucketing config")
    assert(response.failed.get.getMessage.contains("Total external bucket percentage is not 100%"))
  }

  object TestEmptyBucketingConfigForQueryGenerator extends BucketingConfig {
    override def getConfigForCube(cube: String) = None
    override def getConfigForQueryGen(engine: Engine) = None
  }
  object TestBucketingConfigForQueryGenerator extends BucketingConfig {
    override def getConfigForCube(cube: String) = None
    override def getConfigForQueryGen(engine: Engine): Option[QueryGenBucketingConfig] = {
      Some(QueryGenBucketingConfig.builder()
      .internalBucketPercentage(Map(Version.v1 -> 100))
          .externalBucketPercentage(Map(Version.v1 -> 100))
          .dryRunPercentage(Map(Version.v1 -> 100))
          .build())
    }
  }
  object TestInvalidBucketingConfigForQueryGenerator1 extends BucketingConfig {
    override def getConfigForCube(cube: String) = None
    override def getConfigForQueryGen(engine: Engine): Option[QueryGenBucketingConfig] = {
      Some(QueryGenBucketingConfig.builder()
        .internalBucketPercentage(Map(Version.v1 -> 20))
        .externalBucketPercentage(Map(Version.v1 -> 100))
        .dryRunPercentage(Map(Version.v1 -> 100))
        .build())
    }
  }

  object TestInvalidBucketingConfigForQueryGenerator2 extends BucketingConfig {
    override def getConfigForCube(cube: String) = None
    override def getConfigForQueryGen(engine: Engine): Option[QueryGenBucketingConfig] = {
      Some(QueryGenBucketingConfig.builder()
        .internalBucketPercentage(Map(Version.v1 -> 100))
        .externalBucketPercentage(Map(Version.v1 -> 10))
        .dryRunPercentage(Map(Version.v1 -> 100))
        .build())
    }
  }

  object TestBucketingConfig extends BucketingConfig {
    override def getConfigForCube(cube: String): Option[CubeBucketingConfig] = {
      Some(CubeBucketingConfig.builder()
        .internalBucketPercentage(Map(1 -> 100, 2 -> 0))
        .externalBucketPercentage(Map(1 -> 0, 2 -> 90, 3->10))
        .dryRunPercentage(Map(1 -> (75, None), 2 -> (100, Some(DruidEngine))))
        .build())
    }
    override def getConfigForQueryGen(engine: Engine): Option[QueryGenBucketingConfig] = None
  }

  object TestBucketingConfigNoDryRun extends BucketingConfig {
    override def getConfigForCube(cube: String): Option[CubeBucketingConfig] = {
      Some(CubeBucketingConfig.builder()
        .internalBucketPercentage(Map(1 -> 100, 2 -> 0))
        .externalBucketPercentage(Map(1 -> 0, 2 -> 90, 3->10))
        .dryRunPercentage(Map(1 -> (0, None), 2 -> (0, Some(DruidEngine))))
        .build())
    }

    override def getConfigForQueryGen(engine: Engine): Option[QueryGenBucketingConfig] = None
  }

  object TestBucketingConfigDryRunOneRevision extends BucketingConfig {
    override def getConfigForCube(cube: String): Option[CubeBucketingConfig] = {
      Some(CubeBucketingConfig.builder()
        .internalBucketPercentage(Map(0 -> 100))
        .externalBucketPercentage(Map(0 -> 100))
        .dryRunPercentage(Map(1 -> (95, Some(DruidEngine))))
        .build())
    }
    override def getConfigForQueryGen(engine: Engine): Option[QueryGenBucketingConfig] = None
  }

  object TestBucketingConfigWhiteListedOneRevision extends BucketingConfig {
    override def getConfigForCube(cube: String): Option[CubeBucketingConfig] = {
      Some(CubeBucketingConfig.builder()
        .internalBucketPercentage(Map(0 -> 100))
        .externalBucketPercentage(Map(0 -> 100))
        .userWhiteList(Map("test-user" -> 1))
        .build())
    }
    override def getConfigForQueryGen(engine: Engine): Option[QueryGenBucketingConfig] = None
  }

  object NoBucketingConfig extends BucketingConfig {
    override def getConfigForCube(cube: String): Option[CubeBucketingConfig] = None
    override def getConfigForQueryGen(engine: Engine): Option[QueryGenBucketingConfig] = None
  }
}
