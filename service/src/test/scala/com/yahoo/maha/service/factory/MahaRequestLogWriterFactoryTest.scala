// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import com.yahoo.maha.service.utils.{KafkaMahaRequestLogWriter, NoopMahaRequestLogWriter}
import org.json4s.jackson.JsonMethods._

/**
 * Created by pranavbhole on 23/08/17.
 */
class MahaRequestLogWriterFactoryTest extends BaseFactoryTest {

  test("Noop MahaRequestLogWriterFactoryTest") {
    val jsonString =
      """
        |  {
        |  }
        |
      """.stripMargin

    val factoryResult = getFactory[MahaRequestLogWriterFactory]("com.yahoo.maha.service.factory.NoopMahaRequestLogWriterFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val generatorResult = factory.fromJson(json, true)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[NoopMahaRequestLogWriter])
  }

  test("Kafka MahaRequestLogWriterFactoryTest") {
    val jsonString =
      """
        |   {
        |      "kafkaBrokerList" : "",
        |      "bootstrapServers" : "",
        |      "producerType" : "",
        |      "serializerClass" : "" ,
        |      "requestRequiredAcks" : "",
        |      "kafkaBlockOnBufferFull" : "",
        |      "batchNumMessages" : "" ,
        |      "topicName" : "",
        |      "bufferMemory" : "",
        |      "maxBlockMs" : ""
        |    }
        |
      """.stripMargin

    val factoryResult = getFactory[MahaRequestLogWriterFactory]("com.yahoo.maha.service.factory.KafkaMahaRequestLogWriterFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val generatorResult = factory.fromJson(json, false)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[KafkaMahaRequestLogWriter])
  }

}
