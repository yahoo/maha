// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.bucketing

import com.yahoo.maha.core.{DruidEngine, OracleEngine}
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
    val respTry = bucketSelector.selectBuckets("test-cube", bucketParams)
    require(respTry.isSuccess, respTry)
    val response = respTry.get
    assert(response.revision == 1)
  }

  test("Default Bucketing Config Test") {
    val dbc = new DefaultBucketingConfig(Map.empty)
    require(dbc.isInstanceOf[BucketingConfig])
    require(dbc.asInstanceOf[BucketingConfig].getConfig("performance_stats").isDefined == false)
  }

  test("External bucket test") {
    val bucketParams = new BucketParams(new UserInfo("test-user", false)) // isInternal = false
    val respTry = bucketSelector.selectBuckets("test-cube", bucketParams)
    require(respTry.isSuccess, respTry)
    val response = respTry.get
    assert(response.revision == 2 || response.revision == 3)
  }

  test("DryRun bucket test") {
    val bucketParams = new BucketParams(new UserInfo("test-user", true)) // isInternal = true
    var countofRev2 = 0
    for (i <- 0 to 100) {
      val respTry = bucketSelector.selectBuckets("test-cube", bucketParams)
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
    val respTry = bucketSelector.selectBuckets("test-cube-2", bucketParams)
    assert(respTry.isSuccess, respTry)
  }

  test("DryRun bucket test with dryRunRevision and no forceEngine but no cube config") {
    val bucketSelector = new BucketSelector(registry, NoBucketingConfig)
    val bucketParams = new BucketParams(new UserInfo("test-user", true), dryRunRevision = Option(1)) // isInternal = true
    val respTry = bucketSelector.selectBuckets("test-cube-2", bucketParams)
    assert(respTry.isSuccess, respTry)
  }

  test("Test for invalid cube") {
    val bucketParams = new BucketParams(new UserInfo("test-user", false)) // isInternal = false
    val respTry = bucketSelector.selectBuckets("invalid-cube", bucketParams)
    require(respTry.isFailure, respTry)
    assert(respTry.failed.get.isInstanceOf[IllegalArgumentException])
    assert(respTry.failed.get.getMessage.contains("invalid-cube"))
  }


  test("Test for no dryRun config") {
    val bucketSelector = new BucketSelector(registry, TestBucketingConfigNoDryRun)
    val bucketParams = new BucketParams(new UserInfo("test-user", true)) // isInternal = true
    val respTry = bucketSelector.selectBuckets("test-cube", bucketParams)
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
      val response = bucketSelector.selectBuckets("test-cube", bucketParams)
      if (response.get.dryRunRevision.isDefined) {
        if (response.get.dryRunRevision.get == 1) countOfRev1 += 1
      }
    }
    
    assert(countOfRev1 > 0 && countOfRev1 <= 100)
  }

  test("Whitelisted user test") {
    val bucketSelector = new BucketSelector(registry, TestBucketingConfigWhiteListedOneRevision)
    val bucketParams = new BucketParams(new UserInfo("test-user", false)) // isInternal = false
    val response = bucketSelector.selectBuckets("test-cube",bucketParams)
    assert(response.get.revision==1)
  }

  object TestBucketingConfig extends BucketingConfig {
    override def getConfig(cube: String): Option[CubeBucketingConfig] = {
      Some(CubeBucketingConfig.builder()
        .internalBucketPercentage(Map(1 -> 100, 2 -> 0))
        .externalBucketPercentage(Map(1 -> 0, 2 -> 90, 3->10))
        .dryRunPercentage(Map(1 -> (75, None), 2 -> (100, Some(DruidEngine))))
        .build())
    }
  }

  object TestBucketingConfigNoDryRun extends BucketingConfig {
    override def getConfig(cube: String): Option[CubeBucketingConfig] = {
      Some(CubeBucketingConfig.builder()
        .internalBucketPercentage(Map(1 -> 100, 2 -> 0))
        .externalBucketPercentage(Map(1 -> 0, 2 -> 90, 3->10))
        .dryRunPercentage(Map(1 -> (0, None), 2 -> (0, Some(DruidEngine))))
        .build())
    }
  }

  object TestBucketingConfigDryRunOneRevision extends BucketingConfig {
    override def getConfig(cube: String): Option[CubeBucketingConfig] = {
      Some(CubeBucketingConfig.builder()
        .internalBucketPercentage(Map(0 -> 100))
        .externalBucketPercentage(Map(0 -> 100))
        .dryRunPercentage(Map(1 -> (95, Some(DruidEngine))))
        .build())
    }
  }

  object TestBucketingConfigWhiteListedOneRevision extends BucketingConfig {
    override def getConfig(cube: String): Option[CubeBucketingConfig] = {
      Some(CubeBucketingConfig.builder()
        .internalBucketPercentage(Map(0 -> 100))
        .externalBucketPercentage(Map(0 -> 100))
        .userWhiteList(Map("test-user" -> 1))
        .build())
    }
  }

  object NoBucketingConfig extends BucketingConfig {
    override def getConfig(cube: String): Option[CubeBucketingConfig] = {
      None
    }
  }
}
