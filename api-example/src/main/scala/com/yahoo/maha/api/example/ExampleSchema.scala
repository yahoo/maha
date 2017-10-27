// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.api.example

import com.yahoo.maha.core.Schema
import enumeratum.EnumEntry.{Snakecase, Uppercase}
import enumeratum.{Enum, EnumEntry}

sealed abstract class ExampleSchema (override val entryName: String) extends EnumEntry with Snakecase with Uppercase with Schema

object ExampleSchema extends Enum[ExampleSchema] {
  val values = findValues
  var registered = false

  case object StudentSchema extends ExampleSchema("student")

  case object WikiSchema extends ExampleSchema("wiki")

  def register() : Unit = {
    values.synchronized {
      if(!registered) {
        values.foreach(Schema.register)
        registered = true
      }
    }
  }
}
