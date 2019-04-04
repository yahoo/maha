// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

import enumeratum.{Enum, EnumEntry}
import enumeratum.EnumEntry.{Snakecase, Uppercase}

/**
 * Created by hiral on 3/28/16.
 */
trait Schema {
  def entryName: String
  final override def toString : String = entryName
}

object Schema {
  private[this] var schemaMap : Map[String, Schema] = Map.empty
  def register(schema: Schema) : Unit = {
    schemaMap.synchronized {
      require(!schemaMap.contains(schema.entryName.toLowerCase), s"Schema already registered : $schema")
      schemaMap += schema.entryName.toLowerCase -> schema
    }
  }

  def withNameInsensitiveOption(name: String) : Option[Schema] = {
    schemaMap.get(name.toLowerCase)
  }
}

sealed abstract class CoreSchema (override val entryName: String) extends EnumEntry with Snakecase with Uppercase with Schema

object CoreSchema extends Enum[CoreSchema] {
  val values = findValues
  var registered = false

  case object AdvertiserSchema extends CoreSchema("advertiser")
  case object AdvertiserLowLatencySchema extends CoreSchema("advertiser_ll")
  case object ResellerSchema extends CoreSchema("reseller")
  case object InternalSchema extends CoreSchema("internal")
  case object PublisherSchema extends CoreSchema("publisher")
  case object PublisherLowLatencySchema extends CoreSchema("publisher_ll")
  case object NoSchema extends CoreSchema("no_schema")

  def register() : Unit = {
    values.synchronized {
      if(!registered) {
        values.foreach(Schema.register)
        registered = true
      }
    }
  }
}
