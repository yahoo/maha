// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import java.util.regex.Pattern

import com.yahoo.maha.utils.DynamicConfigurationUtils._
import org.json4s._
import org.json4s.JValue
import scalaz.JsonScalaz
import org.json4s.scalaz.JsonScalaz._
import org.json4s.scalaz.JsonScalaz.JSONR
import org.json4s.jackson.JsonMethods.parse

/**
 * Created by hiral on 2/11/16.
 */
package object request {
  implicit val formats = org.json4s.DefaultFormats

  def fieldExtended[A: JSONR](name: String)(json: JValue): Result[A] = {
    val dynamicField = extractDynamicFields(json).get(name)
    val result = {
      if (dynamicField.isDefined) {
        JsonScalaz.fromJSON[A](parse(dynamicField.get._2))
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
