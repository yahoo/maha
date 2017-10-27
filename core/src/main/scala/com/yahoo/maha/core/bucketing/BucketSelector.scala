// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.bucketing

import com.yahoo.maha.core.Engine
import com.yahoo.maha.core.registry.Registry
import grizzled.slf4j.Logging

import scala.util.Try

/**
  * Created by surabhip on 8/25/16.
  */
case class BucketSelected(revision: Int, dryRunRevision: Option[Int], dryRunEngine: Option[Engine])

case class UserInfo(userId: String, isInternal: Boolean)
case class BucketParams(userInfo: UserInfo, dryRunRevision: Option[Int] = None, forceRevision: Option[Int] = None, forceEngine: Option[Engine] = None) {
  override def toString(): String = {
      s"UserId: ${userInfo.userId}, " +
      s"isInternal: ${userInfo.isInternal}, " +
      s"dryRunRevision: $dryRunRevision, " +
      s"forceRevision: $forceRevision, " +
      s"forceEngine: $forceEngine"
  }
}

class BucketSelector(registry: Registry, bucketingConfig: BucketingConfig) extends Logging {
  val random = new scala.util.Random

  def selectBuckets(cube: String, requestParams: BucketParams): Try[BucketSelected] = {
    info(s"Selecting buckets for cube: $cube with params: $requestParams")

    Try {
      require(registry.defaultPublicFactRevisionMap.contains(cube), s"Default revision not found for cube $cube in the registry")
      var revision: Int = registry.defaultPublicFactRevisionMap.get(cube).get
      var dryRunRevision: Option[Int] = None
      var dryRunEngine: Option[Engine] = None

      val cubeConfig = bucketingConfig.getConfig(cube)
      if (!cubeConfig.isDefined) {
        info(s"Bucketing config is not defined for cube: $cube")
      }

      if (requestParams.forceRevision.isDefined) {
        revision = requestParams.forceRevision.get
      } else if(cubeConfig.isDefined && cubeConfig.get.userWhiteList.contains(requestParams.userInfo.userId)) {
        revision = cubeConfig.get.userWhiteList.get(requestParams.userInfo.userId).get
      }else if (cubeConfig.isDefined) {
        revision = selectRevision(cubeConfig, requestParams)
      }

      if (requestParams.dryRunRevision.isDefined) {
        dryRunRevision = requestParams.dryRunRevision
        if (requestParams.forceEngine.isDefined) {
          dryRunEngine = requestParams.forceEngine
        } else {
          dryRunEngine = cubeConfig.get.dryRunPercentage.apply(dryRunRevision.get)._2
        }
      } else {
        val dryRunBucket = getDryRunRevision(cubeConfig, requestParams)
        dryRunRevision = dryRunBucket._1
        dryRunEngine = dryRunBucket._2
      }

      info(s"Buckets Selected: revision: $revision, dryRunRevision: $dryRunRevision dryrunEngine: $dryRunEngine")
      new BucketSelected(revision, dryRunRevision, dryRunEngine)
    }
  }

  private def selectRevision(cubeConfig: Option[CubeBucketingConfig], requestParams: BucketParams): Int = {
    if (requestParams.userInfo.isInternal) {
      cubeConfig.get.internalDistribution.sample()
    } else {
      cubeConfig.get.externalDistribution.sample()
    }
  }

  private def getDryRunRevision(config: Option[CubeBucketingConfig], requestParams: BucketParams): (Option[Int], Option[Engine]) = {
    if (config.isDefined) {
      config.get.dryRunPercentage.foreach {
        case (revision, tuple) => {
          val percentage = tuple._1
          val engine = tuple._2
          val bucket = random.nextInt(100)
          if (bucket < percentage) {
            return (Some(revision), engine)
          }
        }
      }
    }
    (None, None)
  }
}
