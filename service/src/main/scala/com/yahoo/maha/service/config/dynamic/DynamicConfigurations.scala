// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.

package com.yahoo.maha.service.config.dynamic

import com.netflix.config._
import com.yahoo.maha.core.bucketing.BucketSelector
import com.yahoo.maha.service.config.JsonMahaServiceConfig._
import com.yahoo.maha.service.{DynamicMahaServiceConfig, MahaServiceConfig}
import grizzled.slf4j.Logging

import scala.collection.mutable

case class DynamicPropertyInfo(propertyKey: String, defaultValue: Object, objects: mutable.Map[String, Object])

class DynamicConfigurations(configurationSource: PolledConfigurationSource, pollIntervalMs: Int) {
  ConfigurationManager.install(
    new DynamicConfiguration(
          configurationSource,
          new FixedDelayPollingScheduler(0, pollIntervalMs, false)
    )
  )

  val map = new mutable.HashMap[String, DynamicIntProperty]()

  def addProperty(name: String, defaultValue: Int): Unit = {
    val dynamicProperty = DynamicPropertyFactory
      .getInstance()
      .getIntProperty(name, defaultValue)
    map.put(name, dynamicProperty)
  }

  def addCallbacks(dynamicServiceConfig: DynamicMahaServiceConfig, registryName: String): Unit = {
    map.values.foreach(dynamicProperty => {
      dynamicProperty.addCallback(
        DynamicWrapper.getCallback(dynamicProperty, dynamicServiceConfig, "er"))
    })
  }

  def getDynamicConfiguration(configKey: String): Option[Int] = {
    if (map.contains(configKey)) {
      Option(map(configKey).get())
    } else {
      None
    }
  }
}

object DynamicWrapper extends Logging {

  def getCallback(dynamicProperty: PropertyWrapper[_ <: Any], dynamicMahaServiceConfig: DynamicMahaServiceConfig, registryName: String): Runnable = {
    val dynamicRegistryConfig = dynamicMahaServiceConfig.registry.get(registryName).get
    val registryConfig = dynamicMahaServiceConfig.jsonMahaServiceConfig.registryMap(registryName)
    val callbackImpl = new Runnable {
      override def run(): Unit = {
        val dependentObjects = dynamicMahaServiceConfig.dynamicProperties(dynamicProperty.getName).objects
        for ((name, currentObject) <- dependentObjects) {
          info(s"Updating: $name - $currentObject")
          name match {
            case BUCKETING_CONFIG_MAP =>
              val newBucketingConfig = MahaServiceConfig.initBucketingConfig(
                dynamicMahaServiceConfig.jsonMahaServiceConfig.bucketingConfigMap)(dynamicMahaServiceConfig.context)
              require(newBucketingConfig.isSuccess, s"Failed to re-initialize bucketing config - $newBucketingConfig")
              val updatedBucketSelector = new BucketSelector(dynamicRegistryConfig.getRegistry,
                newBucketingConfig.toOption.get.get(registryConfig.bucketConfigName).get)
              dynamicRegistryConfig.updateBucketSelector(updatedBucketSelector)
              info(s"Replaced BucketSelector: ${updatedBucketSelector.bucketingConfig.getConfigForCube("student_performance").get.externalBucketPercentage}")
            case _ =>
              throw new UnsupportedOperationException(s"Unknown dynamic object name: $name")
          }
        }
      }
    }
    callbackImpl
  }

}