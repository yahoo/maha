// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.log

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.yahoo.maha.data.StringEventBatch
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by hiral on 10/21/14.
  */
object KryoStringEventBatch {
  private val logger: Logger = LoggerFactory.getLogger(classOf[KryoStringEventBatch])
}

class KryoStringEventBatch extends Serializer[StringEventBatch] {
  KryoStringEventBatch.logger.info("Created instance of " + this.getClass.getSimpleName)

  override def write(kryo: Kryo, output: Output, stringEventBatch: StringEventBatch): Unit = {
    val size: Int = stringEventBatch.getEvents.size
    output.writeInt(size)
    stringEventBatch.getEvents.stream().forEach(output.writeString(_))
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[StringEventBatch]): StringEventBatch = {
    val size: Int = input.readInt
    val builder: StringEventBatch.Builder = new StringEventBatch.Builder(size)
    var i: Int = 0
    while ( i < size) {
      builder.add(input.readString)
      i += 1
    }
    builder.build.asInstanceOf[StringEventBatch]
  }
}
