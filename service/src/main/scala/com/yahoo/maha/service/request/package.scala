// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service

import com.yahoo.maha.service.config.dynamic.DynamicConfigurations
import com.yahoo.maha.service.config.dynamic.DynamicConfigurationUtils._
import grizzled.slf4j.Logging
import org.json4s.{JValue, _}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.scalaz.JsonScalaz
import org.json4s.scalaz.JsonScalaz.{JSONR, _}

/**
  * Created by hiral on 2/11/16.
  */
package object request extends Logging {
  implicit val formats = org.json4s.DefaultFormats

  private var dynamicConfigurations:Option[DynamicConfigurations] = None

  def setDynamicConfigurations(value: DynamicConfigurations): Unit = dynamicConfigurations = Some(value)

  def fieldExtended[A: JSONR](name: String)(json: JValue): Result[A] = {
    val dynamicField = (extractDynamicFields(json)).filter(f => f._2._1.equals(name)).headOption
    val result = {
      if (dynamicField.isDefined && dynamicConfigurations.isDefined) {
        val defaultValue = JsonScalaz.fromJSON[A](parse(dynamicField.get._2._2))
        dynamicConfigurations.get.addProperty(dynamicField.get._1, defaultValue.toOption.get.asInstanceOf[Int])
        val dynamicValue = JsonScalaz.fromJSON[A](parse(dynamicConfigurations.get.getDynamicConfiguration(dynamicField.get._1).get.toString))
        if (dynamicValue.isSuccess) {
          dynamicValue
        } else {
          error(s"Failed to fetch dynamic config value failure: $dynamicValue. Returning default: $defaultValue")
          defaultValue
        }
      } else {
        field[A](name)(json)
      }
    }

    result.leftMap {
      nel =>
        nel.map {
          case UnexpectedJSONError(was, expected) =>
            UncategorizedError(name, s"unexpected value : $was expected : ${expected.getSimpleName}", List.empty)
          case a => a
        }
    }
  }

}
