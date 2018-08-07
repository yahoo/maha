// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.log

/**
  * Created by hiral on 10/21/14.
  */

import java.util.Collections

import com.yahoo.maha.data.{Compressor, StringEventBatch}
import org.junit.{Assert, Test}

class KafkaStringEventBatchTest {
  @Test
  def testWithNoConfiguration(): Unit = {
    val encoder = new KafkaStringEventBatchSerializer()
    encoder.configure(Collections.emptyMap(), false)
    val decoder = new KafkaStringEventBatchDeserializer()
    decoder.configure(Collections.emptyMap(), false)
    val builder = new StringEventBatch.Builder(3)
    builder.add("one")
    builder.add("two")
    builder.add("three")
    val recordList = builder.build.asInstanceOf[StringEventBatch]
    val encoded = encoder.serialize("blah", recordList)
    val decoded = decoder.deserialize("blah", encoded)
    Assert.assertEquals(recordList.getEvents.size(), decoded.getEvents.size())
    Assert.assertEquals(recordList.getEvents.get(0), decoded.getEvents.get(0))
    Assert.assertEquals(recordList.getEvents.get(1), decoded.getEvents.get(1))
    Assert.assertEquals(recordList.getEvents.get(2), decoded.getEvents.get(2))
  }

  @Test
  def testWithConfiguration(): Unit = {
    import scala.collection.JavaConverters._
    val config: java.util.Map[String, _] = Map(
      Compressor.COMPRESSOR_CODEC_PROPERTY -> "lz4hc"
      , KafkaStringEventBatchSerializer.BUFFER_MB_PROPERTY -> "2"
      , KafkaStringEventBatchDeserializer.BUFFER_MB_PROPERTY -> "2"
    ).asJava
    val encoder = new KafkaStringEventBatchSerializer()
    encoder.configure(config, false)
    val decoder = new KafkaStringEventBatchDeserializer()
    decoder.configure(config, false)
    val builder = new StringEventBatch.Builder(3)
    builder.add("one")
    builder.add("two")
    builder.add("three")
    val recordList = builder.build.asInstanceOf[StringEventBatch]
    val encoded = encoder.serialize("blah", recordList)
    val decoded = decoder.deserialize("blah", encoded)
    Assert.assertEquals(recordList.getEvents.size(), decoded.getEvents.size())
    Assert.assertEquals(recordList.getEvents.get(0), decoded.getEvents.get(0))
    Assert.assertEquals(recordList.getEvents.get(1), decoded.getEvents.get(1))
    Assert.assertEquals(recordList.getEvents.get(2), decoded.getEvents.get(2))
  }

  @Test
  def testWithBadConfiguration(): Unit = {
    import scala.collection.JavaConverters._
    val config: java.util.Map[String, _] = Map(
      Compressor.COMPRESSOR_CODEC_PROPERTY -> "blah"
      , KafkaStringEventBatchSerializer.BUFFER_MB_PROPERTY -> "abc"
      , KafkaStringEventBatchDeserializer.BUFFER_MB_PROPERTY -> "-1"
    ).asJava
    val encoder = new KafkaStringEventBatchSerializer()
    encoder.configure(config, false)
    val decoder = new KafkaStringEventBatchDeserializer()
    decoder.configure(config, false)
    val builder = new StringEventBatch.Builder(3)
    builder.add("one")
    builder.add("two")
    builder.add("three")
    val recordList = builder.build.asInstanceOf[StringEventBatch]
    val encoded = encoder.serialize("blah", recordList)
    val decoded = decoder.deserialize("blah", encoded)
    Assert.assertEquals(recordList.getEvents.size(), decoded.getEvents.size())
    Assert.assertEquals(recordList.getEvents.get(0), decoded.getEvents.get(0))
    Assert.assertEquals(recordList.getEvents.get(1), decoded.getEvents.get(1))
    Assert.assertEquals(recordList.getEvents.get(2), decoded.getEvents.get(2))
  }
}
