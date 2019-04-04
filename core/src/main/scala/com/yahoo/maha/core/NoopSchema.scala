// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import enumeratum.{Enum, EnumEntry}
import enumeratum.EnumEntry.{Snakecase, Uppercase}

/*
  NoopSchema should be used to restrict user queries from hitting intermediate rollups.
 */
sealed abstract class NoopSchema (override val entryName: String) extends EnumEntry with Snakecase with Uppercase with Schema

object NoopSchema extends Enum[NoopSchema] {
  val values = findValues
  var registered = false

  case object NoopSchema extends NoopSchema("noop_schema")

  values.foreach(Schema.register)
}
