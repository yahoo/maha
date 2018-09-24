// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core

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
