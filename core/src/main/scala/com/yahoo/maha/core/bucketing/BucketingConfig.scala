// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.bucketing

import com.yahoo.maha.core.Engine
import com.yahoo.maha.core.query.Version
import org.apache.commons.math3.distribution.{EnumeratedDistribution, EnumeratedIntegerDistribution}
import org.apache.commons.math3.exception.MathArithmeticException

import java.util
import scala.collection.JavaConverters
import scala.collection.mutable.ListBuffer
import org.apache.commons.math3.util.Pair

/**
  * Created by shrav87 on 8/23/16.
  */
// Config to decide which revision of the cube the request should go to
case class CubeBucketingConfig(internalBucketPercentage:Map[Int,Int] = Map.empty, //revision,%
                               externalBucketPercentage:Map[Int,Int] = Map.empty,
                               dryRunPercentage: Map[Int,Tuple2[Int, Option[Engine]]] = Map.empty, //revision,[%dryRun, Optional Engine]
                               userWhiteList:Map[String,Int] = Map.empty, // userId,rev
                               uriPercentage: Map[String, Int] = Map.empty
                            ) {
  validate("")

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

  var uriBuffer = new ListBuffer[Pair[String, java.lang.Double]]
  uriPercentage.foreach(
    a => {
      val myPair = new Pair(a._1, java.lang.Double.valueOf(a._2.toDouble/100))
      uriBuffer += myPair
    }
  )
  val uriDistribution: Option[EnumeratedDistribution[String]] =
    if(uriBuffer.isEmpty) None else Some(new EnumeratedDistribution[String](JavaConverters.seqAsJavaList(uriBuffer.toSeq)))

  def validate(cubeName:String) = {
    val internalSum = internalBucketPercentage.values.sum
    require(internalSum==100,s"Total internal bucket percentage is not 100% but $internalSum, cube: $cubeName")

    val externalSum = externalBucketPercentage.values.sum
    require(externalBucketPercentage.values.sum==100,s"Total external bucket percentage is not 100% but $externalSum, cube: $cubeName")
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
  private var uriConfig: Map[String, Int] = Map.empty

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

  def uriConfig(map: Map[String, Int]): CubeBucketingConfigBuilder = {
    uriConfig = map
    this
  }

  def build(): CubeBucketingConfig = {
    new CubeBucketingConfig(internalBucketPercentage.toMap, externalBucketPercentage.toMap, dryRunPercentage.toMap, userWhiteList.toMap, uriConfig.toMap)
  }
}

case class QueryGenBucketingConfig(internalBucketPercentage:Map[Version,Int] = Map.empty, //version,%
                               externalBucketPercentage:Map[Version,Int] = Map.empty,//version,%
                               dryRunPercentage: Map[Version,Int] = Map.empty, //version,%
                               userWhiteList:Map[String,Version] = Map.empty // userId,version
                              ) {
  validate("")

  val internalDistribution = new EnumeratedIntegerDistribution(internalBucketPercentage.keys.map(_.number).toArray,
    internalBucketPercentage.values.map(percentage => percentage.toDouble/100).toArray)

  val externalDistribution = new EnumeratedIntegerDistribution(externalBucketPercentage.keys.map(_.number).toArray,
    externalBucketPercentage.values.map(percentage => percentage.toDouble/100).toArray)

  val dryRunDistribution: Option[EnumeratedIntegerDistribution] = {
    try {
      Some(new EnumeratedIntegerDistribution(dryRunPercentage.keys.map(_.number).toArray,
        dryRunPercentage.values.map(percentage => percentage.toDouble/100).toArray))
    } catch {
      case e: MathArithmeticException => // All probabilities are 0
        None
    }
  }

  def validate(entityName:String) = {
    val internalSum = internalBucketPercentage.values.sum
    require(internalSum==100,s"Total internal bucket percentage is not 100% but $internalSum, entity: $entityName")

    val externalSum = externalBucketPercentage.values.sum
    require(externalBucketPercentage.values.sum == 100,s"Total external bucket percentage is not 100% but $externalSum, entity: $entityName")
  }
}

object QueryGenBucketingConfig {
  val entity = "query_gen_bucket_config"
  def builder() = new QueryGenBucketingConfigBuilder
}

class QueryGenBucketingConfigBuilder {
  private var internalBucketPercentage: Map[Version, Int] = Map.empty
  private var externalBucketPercentage: Map[Version, Int] = Map.empty
  private var dryRunPercentage: Map[Version, Int] = Map.empty
  private var userWhiteList: Map[String, Version] = Map.empty

  def internalBucketPercentage(map: Map[Version, Int]): QueryGenBucketingConfigBuilder = {
    internalBucketPercentage = map
    this
  }

  def externalBucketPercentage(map: Map[Version, Int]): QueryGenBucketingConfigBuilder = {
    externalBucketPercentage = map
    this
  }

  def dryRunPercentage(map: Map[Version, Int]): QueryGenBucketingConfigBuilder = {
    dryRunPercentage = map
    this
  }

  def userWhiteList(map: Map[String, Version]): QueryGenBucketingConfigBuilder = {
    userWhiteList = map
    this
  }

  def build(): QueryGenBucketingConfig = {
    val config = new QueryGenBucketingConfig(internalBucketPercentage.toMap, externalBucketPercentage.toMap, dryRunPercentage.toMap, userWhiteList.toMap)
    config.validate(QueryGenBucketingConfig.entity)
    config
  }
}


trait BucketingConfig {
  def getConfigForCube(cube: String): Option[CubeBucketingConfig]
  def getConfigForQueryGen(engine: Engine): Option[QueryGenBucketingConfig]
}

class DefaultBucketingConfig(cubeBucketingConfigMap:scala.collection.immutable.Map[String,CubeBucketingConfig],
                             queryGenBucketingConfigMap:scala.collection.immutable.Map[Engine,QueryGenBucketingConfig]) extends BucketingConfig {
  validate()

  def this() {
    this(Map.empty, Map.empty)
  }

  private[this] def validate(): Unit = {

    cubeBucketingConfigMap.foreach {
      case (cubeName, bucketingConfig) => bucketingConfig.validate(cubeName)
    }

    queryGenBucketingConfigMap.foreach {
      case (engine, bucketingConfig) => bucketingConfig.validate(engine.toString)
    }
  }

  override def getConfigForCube(cube: String): Option[CubeBucketingConfig] = {
    cubeBucketingConfigMap.get(cube)
  }

  override def getConfigForQueryGen(engine: Engine): Option[QueryGenBucketingConfig] = {
    queryGenBucketingConfigMap.get(engine)
  }
}


