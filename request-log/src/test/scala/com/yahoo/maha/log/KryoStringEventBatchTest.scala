// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.log

/**
  * Created by hiral on 10/21/14.
  */

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.yahoo.maha.data.StringEventBatch
import org.junit.{Assert, Test}

class KryoStringEventBatchTest {
  @Test
  @throws[Exception]
  def test(): Unit = {
    val kryo = new Kryo
    kryo.register(classOf[StringEventBatch], new KryoStringEventBatch)
    val builder = new StringEventBatch.Builder(3)
    builder.add("one")
    builder.add("two")
    builder.add("three")
    val recordList = builder.build.asInstanceOf[StringEventBatch]
    val output = new Output(new Array[Byte](1024 * 1024 + 1))
    kryo.writeObject(output, recordList)
    //System.out.println("output.position=" + output.position)
    val input = new Input(output.getBuffer, 0, output.total.toInt)
    val resultRecordList = kryo.readObject(input, classOf[StringEventBatch])
    Assert.assertEquals(resultRecordList.getEvents.get(0), "one")
    Assert.assertEquals(resultRecordList.getEvents.get(1), "two")
    Assert.assertEquals(resultRecordList.getEvents.get(2), "three")
    val output2 = new Output(new Array[Byte](1024 * 1024 + 1))
    kryo.writeObject(output2, resultRecordList)
  }
}
