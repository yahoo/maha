// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import org.json4s.JValue
import org.json4s.scalaz.JsonScalaz.{JSONR, _}
import scalaz.Validation

/**
 * Created by hiral on 2/11/16.
 */
package object request {
  implicit val formats = org.json4s.DefaultFormats

  def fieldExtended[A: JSONR](name: String)(json: JValue): Result[A] = {
    val result = field[A](name)(json)
    result.leftMap {
      nel =>
        nel.map {
          case UnexpectedJSONError(was, expected) =>
            UncategorizedError(name, s"unexpected value : $was expected : ${expected.getSimpleName}", List.empty)
          case a => a
        }
    }
  }

  def optionalFieldExtended[A: JSONR](name: String, default: A)(json: JValue): Result[A] = {
    val result = field[A](name)(json)
    if(result.isSuccess)
      result
    else {
      Validation.success[Error, A](default).asInstanceOf[Result[A]]
    }
  }

}
