// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.bucketing

import com.yahoo.maha.core.Engine
import com.yahoo.maha.core.registry.Registry
import grizzled.slf4j.Logging
import com.yahoo.maha.core.query.{V0, Version}
import org.apache.commons.lang3.StringUtils._

import scala.util.Try

/**
  * Created by surabhip on 8/25/16.
  */
case class CubeBucketSelected(revision: Int, dryRunRevision: Option[Int], dryRunEngine: Option[Engine])
case class QueryGenBucketSelected(queryGenVersion: Version, dryRunQueryGenVersion: Option[Version])

case class UserInfo(userId: String, isInternal: Boolean)
object UserInfo {
  def empty: UserInfo = new UserInfo(EMPTY, false)
}

case class BucketParams(userInfo: UserInfo = UserInfo.empty, dryRunRevision: Option[Int] = None, forceRevision: Option[Int] = None, forceEngine: Option[Engine] = None, forceQueryGenVersion: Option[Version] = None) {
  override def toString(): String = {
      s"UserId: ${userInfo.userId}, " +
      s"isInternal: ${userInfo.isInternal}, " +
      s"dryRunRevision: $dryRunRevision, " +
      s"forceRevision: $forceRevision, " +
      s"forceEngine: $forceEngine" +
      s"forceQueryGenVersion: $forceQueryGenVersion"
  }
}

class BucketSelector(registry: Registry, bucketingConfig: BucketingConfig) extends Logging {
  val randomCube = new scala.util.Random
  val randomQgen = new scala.util.Random

  def selectCubeBuckets(cube: String, requestParams: BucketParams): Try[CubeBucketSelected] = {
    info(s"Selecting buckets for cube: $cube with params: $requestParams")

    Try {
      require(registry.defaultPublicFactRevisionMap.contains(cube), s"Default revision not found for cube $cube in the registry")
      var revision: Int = registry.defaultPublicFactRevisionMap.get(cube).get
      var dryRunRevision: Option[Int] = None
      var dryRunEngine: Option[Engine] = None

      val cubeConfig = bucketingConfig.getConfigForCube(cube)
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
          dryRunEngine = cubeConfig.flatMap(_.dryRunPercentage.get(dryRunRevision.get)).flatMap(_._2)
        }
      } else {
        val dryRunBucket = getDryRunRevision(cubeConfig, requestParams)
        dryRunRevision = dryRunBucket._1
        dryRunEngine = dryRunBucket._2
      }

      info(s"Buckets Selected: revision: $revision, dryRunRevision: $dryRunRevision dryrunEngine: $dryRunEngine")
      new CubeBucketSelected(revision, dryRunRevision, dryRunEngine)
    }
  }

  def selectQueryGenBuckets(engine: Engine, requestParams: BucketParams): Try[QueryGenBucketSelected] = {
    info(s"Selecting buckets for engine: $engine with params: $requestParams")
    Try {
      var queryGenVersion: Version = V0
      var dryRunQueryGenVersion: Option[Version] = None

      val qgenConfig = bucketingConfig.getConfigForQueryGen(engine)
      if (!qgenConfig.isDefined) {
        info(s"Bucketing config for query generator is not defined for engine: $engine: $qgenConfig")
      }
      if(requestParams.forceQueryGenVersion.isDefined) {
        queryGenVersion = requestParams.forceQueryGenVersion.get
      } else if(qgenConfig.isDefined && qgenConfig.get.userWhiteList.contains(requestParams.userInfo.userId)) {
        queryGenVersion = qgenConfig.get.userWhiteList.get(requestParams.userInfo.userId).get
      } else if (qgenConfig.isDefined) {
        queryGenVersion = selectVersion(qgenConfig, requestParams)
      }
      dryRunQueryGenVersion = getDryRunVersion(qgenConfig, requestParams)

      new QueryGenBucketSelected(queryGenVersion, dryRunQueryGenVersion)
    }
  }

  private def selectRevision(cubeConfig: Option[CubeBucketingConfig], requestParams: BucketParams): Int = {
    if (requestParams.userInfo.isInternal) {
      cubeConfig.get.internalDistribution.sample()
    } else {
      cubeConfig.get.externalDistribution.sample()
    }
  }

  private def selectVersion(queryGenConfig: Option[QueryGenBucketingConfig], requestParams: BucketParams): Version = {
    if (requestParams.userInfo.isInternal) {
      Version.of(queryGenConfig.get.internalDistribution.sample())
    } else {
      Version.of(queryGenConfig.get.externalDistribution.sample())
    }
  }

  private def getDryRunVersion(config: Option[QueryGenBucketingConfig], requestParams: BucketParams): Option[Version] = {
    if (config.isDefined) {
      config.get.dryRunPercentage.foreach {
        case (revision, percentage) => {
          val bucket = randomQgen.nextInt(100)
          if (bucket < percentage) {
            return Some(revision)
          }
        }
      }
    }
    None
  }

  private def getDryRunRevision(config: Option[CubeBucketingConfig], requestParams: BucketParams): (Option[Int], Option[Engine]) = {
    if (config.isDefined) {
      config.get.dryRunPercentage.foreach {
        case (revision, tuple) => {
          val percentage = tuple._1
          val engine = tuple._2
          val bucket = randomCube.nextInt(100)
          if (bucket < percentage) {
            return (Some(revision), engine)
          }
        }
      }
    }
    (None, None)
  }
}
