// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.

package com.yahoo.maha.service.config.dynamic

import java.util.function.Consumer

import com.netflix.archaius.DefaultPropertyFactory
import com.netflix.archaius.api.{Config, Property}
import com.yahoo.maha.core.bucketing.BucketSelector
import com.yahoo.maha.service.config.JsonMahaServiceConfig._
import com.yahoo.maha.service.{DynamicMahaServiceConfig, MahaServiceConfig}
import grizzled.slf4j.Logging

import scala.collection.mutable

case class DynamicPropertyInfo(propertyKey: String, defaultValue: Object, objects: mutable.Map[String, Object])

class DynamicConfigurations(configurationSource: Config) extends Logging {
  val dynamicPropertyFactory = DefaultPropertyFactory.from(configurationSource)

  val map = new mutable.HashMap[String, Property[Integer]]()

  def addProperty(name: String, defaultValue: Int): Unit = {
    val dynamicProperty = dynamicPropertyFactory.getProperty(name).asInteger(defaultValue)
    info(s"Adding dynamic property: $name defaultValue: $defaultValue")
    map.put(name, dynamicProperty)
  }

  def addCallbacks(dynamicServiceConfig: DynamicMahaServiceConfig, registryName: String): Unit = {
    map.values.foreach(dynamicProperty => {
      info(s"Adding callback for dynamic property: $dynamicProperty")
      dynamicProperty.subscribe(
        new Consumer[Integer]() {
          override def accept(t: Integer): Unit = {
            DynamicWrapper.getCallback(dynamicProperty, dynamicServiceConfig, "er").run()
          }
        })
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

  def getCallback(dynamicProperty: Property[_ <: Any], dynamicMahaServiceConfig: DynamicMahaServiceConfig, registryName: String): Runnable = {
    val dynamicRegistryConfig = dynamicMahaServiceConfig.registry.get(registryName).get
    val registryConfig = dynamicMahaServiceConfig.jsonMahaServiceConfig.registryMap(registryName)
    val callbackImpl = new Runnable {
      override def run(): Unit = {
        val dependentObjects = dynamicMahaServiceConfig.dynamicProperties(dynamicProperty.getKey).objects
        for ((name, currentObject) <- dependentObjects) {
          info(s"Updating: $name - $currentObject")
          name match {
            case BUCKETING_CONFIG_MAP =>
              val newBucketingConfig = MahaServiceConfig.initBucketingConfig(
                dynamicMahaServiceConfig.jsonMahaServiceConfig.bucketingConfigMap)(dynamicMahaServiceConfig.context)
              if(newBucketingConfig.isSuccess) {
                val updatedBucketSelector = new BucketSelector(dynamicRegistryConfig.registry,
                  newBucketingConfig.toOption.get.get(registryConfig.bucketConfigName).get)
                dynamicRegistryConfig.updateBucketSelector(updatedBucketSelector)
                info(s"Replaced BucketSelector: ${updatedBucketSelector.bucketingConfig}")
              }
            case _ =>
              throw new UnsupportedOperationException(s"Unknown dynamic object name: $name")
          }
        }
      }
    }
    callbackImpl
  }

}