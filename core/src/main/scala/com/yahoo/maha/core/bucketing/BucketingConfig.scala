// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.bucketing

import com.yahoo.maha.core.Engine
import org.apache.commons.math3.distribution.EnumeratedIntegerDistribution
import org.apache.commons.math3.exception.MathArithmeticException

import scala.collection.mutable



/**
  * Created by shrav87 on 8/23/16.
  */
// Config to decide which revision of the cube the request should go to
case class CubeBucketingConfig(internalBucketPercentage:Map[Int,Int] = Map.empty, //revision,%
                               externalBucketPercentage:Map[Int,Int] = Map.empty,
                               dryRunPercentage: Map[Int,Tuple2[Int, Option[Engine]]] = Map.empty, //revision,[%dryRun, Optional Engine]
                               userWhiteList:Map[String,Int] = Map.empty // userId,rev
                            ) {
  validate()

  val internalDistribution = new EnumeratedIntegerDistribution(internalBucketPercentage.keys.toArray,
    internalBucketPercentage.values.map(percentage => percentage.toDouble/100).toArray)

  val externalDistribution = new EnumeratedIntegerDistribution(externalBucketPercentage.keys.toArray,
    externalBucketPercentage.values.map(percentage => percentage.toDouble/100).toArray)

  val dryRunDistribution: Option[EnumeratedIntegerDistribution] = {
    try {
      Some(new EnumeratedIntegerDistribution(dryRunPercentage.keys.toArray,
        dryRunPercentage.values.map(tuple => tuple._1.toDouble / 100).toArray))
    } catch {
      case e: MathArithmeticException => // All probabilities are 0
        None
    }
  }

  def validate(cube: String = "") = {
    val internalSum = internalBucketPercentage.values.sum
    require(internalSum==100,s"Total internal bucket percentage is not 100% but $internalSum")

    val externalSum = externalBucketPercentage.values.sum
    require(externalBucketPercentage.values.sum==100,s"Total external bucket percentage is not 100% but $externalSum")
  }
}

object CubeBucketingConfig {
  def builder() = new CubeBucketingConfigBuilder
}

class CubeBucketingConfigBuilder {
  private var internalBucketPercentage: Map[Int, Int] = Map.empty
  private var externalBucketPercentage: Map[Int, Int] = Map.empty
  private var dryRunPercentage: Map[Int, Tuple2[Int, Option[Engine]]] = Map.empty
  private var userWhiteList: Map[String, Int] = Map.empty

  def internalBucketPercentage(map: Map[Int, Int]): CubeBucketingConfigBuilder = {
    internalBucketPercentage = map
    this
  }

  def externalBucketPercentage(map: Map[Int, Int]): CubeBucketingConfigBuilder = {
    externalBucketPercentage = map
    this
  }

  def dryRunPercentage(map: Map[Int, Tuple2[Int, Option[Engine]]]): CubeBucketingConfigBuilder = {
    dryRunPercentage = map
    this
  }

  def userWhiteList(map: Map[String, Int]): CubeBucketingConfigBuilder = {
    userWhiteList = map
    this
  }

  def build(): CubeBucketingConfig = {
    new CubeBucketingConfig(internalBucketPercentage.toMap, externalBucketPercentage.toMap, dryRunPercentage.toMap, userWhiteList.toMap)
  }
}


trait BucketingConfig {
  def getConfig(cube: String): Option[CubeBucketingConfig]
}

class DefaultBucketingConfig(bucketingConfigMap:scala.collection.immutable.Map[String,CubeBucketingConfig]) extends BucketingConfig {
  validate()

  private[this] def validate(): Unit = {
    for((cubeName,bucketingConfig)<-bucketingConfigMap) {
      bucketingConfig.validate(cubeName)
    }
  }

  override def getConfig(cube: String): Option[CubeBucketingConfig] = {
    bucketingConfigMap.get(cube)
  }
}


