package com.yahoo.maha.core

import enumeratum.{Enum, EnumEntry}
import enumeratum.EnumEntry.{Snakecase, Uppercase}

sealed abstract class NoopSchema (override val entryName: String) extends EnumEntry with Snakecase with Uppercase with Schema

object NoopSchema extends Enum[NoopSchema] {
  val values = findValues
  var registered = false

  case object NoopSchema extends NoopSchema("noop_schema")

  values.foreach(Schema.register)
}
