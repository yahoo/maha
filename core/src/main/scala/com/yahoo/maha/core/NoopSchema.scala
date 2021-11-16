// Copyright 2021, Yahoo Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

/**
  * Created by ritvikj on 4/4/19.
  */

import enumeratum.{Enum, EnumEntry}
import enumeratum.EnumEntry.{Snakecase, Uppercase}

/*
  NoopSchema should be used to prevent user queries from hitting intermediate rollups that are not valid tables. Such
  rollups are only used as an intermediate step for different child rollups and do not represent actual tables in the
  database. Restricting these rollups to NoopSchema ensures that no user query actually hits these non-existent tables.
 */
sealed abstract class NoopSchema (override val entryName: String) extends EnumEntry with Snakecase with Uppercase with Schema

object NoopSchema extends Enum[NoopSchema] {
  val values = findValues
  var registered = false

  case object NoopSchema extends NoopSchema("noop_schema")

  values.foreach(Schema.register)
}
