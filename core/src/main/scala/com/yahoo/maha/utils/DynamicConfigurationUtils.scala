// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.utils

import java.util.regex.Pattern

import grizzled.slf4j.Logging
import org.json4s.JsonAST.JString
import org.json4s.{JField, JValue}

import scala.collection.mutable

object DynamicConfigurationUtils extends Logging {
  private val START = Pattern.quote("<%(")
  private val END = Pattern.quote(")%>")
  val DYNAMIC_CONFIG_PATTERN = Pattern.compile(s"$START(.*),(.*)$END")

  def extractDynamicFields(json: JValue): Map[String, (String, String)] = {
    val dynamicFieldMap = new mutable.HashMap[String, (String, String)]()
    val dynamicFields = json.filterField(_._2 match {
      case JString(s) => DYNAMIC_CONFIG_PATTERN.matcher(s).find()
      case _ => false})

    dynamicFields.foreach(f => {
      require(f._2.isInstanceOf[JString], s"Cannot extract dynamic property from non-string field: $f")
      implicit val formats = org.json4s.DefaultFormats
      val matcher = DYNAMIC_CONFIG_PATTERN.matcher(f._2.extract[String])
      require(matcher.find(), s"Field does not contain dynamic property $f. Pattern - $DYNAMIC_CONFIG_PATTERN")
      require(matcher.groupCount() == 2, s"Expected name and default value in dynamic property field: $f")
      val propertyKey = matcher.group(1).trim
      val defaultValue = matcher.group(2).trim
      dynamicFieldMap.put(f._1, (propertyKey, defaultValue))
    })
    dynamicFieldMap.toMap
  }

  def getDynamicFields(json: JValue): List[JField] = {
    println("Matching now...")
    json.filterField(_._2 match {
      case JString(s) => {
        println("Is string")
        DYNAMIC_CONFIG_PATTERN.matcher(s).find()
      }
      case a => {
        println("Is other: " + a)
        false
      }})
  }
}
