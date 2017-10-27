// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.serde

import java.nio.charset.StandardCharsets

trait SerDe[T] {
  def serialize(t: T): Array[Byte]
  def deserialize(bytes: Array[Byte]): T
}

object StringSerDe extends SerDe[String] {
  override def serialize(str: String): Array[Byte] = {
    require(str != null, "Cannot serialize null string")
    str.getBytes(StandardCharsets.UTF_8)
  }

  override def deserialize(bytes: Array[Byte]): String = {
    require(bytes != null, "Cannot serialize null byte array")
    new String(bytes, StandardCharsets.UTF_8)
  }

}
