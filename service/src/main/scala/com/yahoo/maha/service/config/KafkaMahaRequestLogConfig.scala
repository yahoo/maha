// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.config

import com.yahoo.maha.core.request._
import org.json4s.JValue
import org.json4s.scalaz.JsonScalaz
import org.json4s.scalaz.JsonScalaz._

/**
 * Created by pranavbhole on 16/08/17.
 */

case class JsonMahaRequestLogConfig(className: String, kafkaConfig: JValue, isLoggingEnabled : Boolean)

import scalaz.syntax.applicative._

object JsonMahaRequestLogConfig {
  import org.json4s.scalaz.JsonScalaz._

  implicit def parse: JSONR[JsonMahaRequestLogConfig] = new JSONR[JsonMahaRequestLogConfig] {
    override def read(json: JValue): Result[JsonMahaRequestLogConfig] = {
      val name: Result[String] = fieldExtended[String]("factoryClass")(json)
      val config: Result[JValue] = fieldExtended[JValue]("config")(json)
      val isLoggingEnabled: Result[Boolean] = fieldExtended[Boolean]("isLoggingEnabled")(json)
      (name |@| config |@| isLoggingEnabled) {
        (a,b,c) =>
        JsonMahaRequestLogConfig(a, b, c)
      }
    }
  }
}

case class JsonKafkaRequestLoggingConfig(kafkaBrokerList: String,
                                     bootstrapServers: String,
                                     producerType: String,
                                     serializerClass: String,
                                     requestRequiredAcks: String,
                                     kafkaBlockOnBufferFull: String,
                                     batchNumMessages: String,
                                     topicName: String,
                                     bufferMemory: String,
                                     maxBlockMs: String)


object JsonKafkaRequestLoggingConfig {
  import org.json4s.scalaz.JsonScalaz._

  implicit def parse: JSONR[JsonKafkaRequestLoggingConfig] = new JSONR[JsonKafkaRequestLoggingConfig] {
    override def read(json: JValue): Result[JsonKafkaRequestLoggingConfig] = {

      val kafkaBrokerListResult: Result[String] = fieldExtended[String]("kafkaBrokerList")(json)
      val bootstrapServersResult: Result[String] = fieldExtended[String]("bootstrapServers")(json)
      val producerTypeResult: Result[String] = fieldExtended[String]("producerType")(json)
      val serializerClassResult: Result[String] = fieldExtended[String]("serializerClass")(json)
      val requestRequiredAcksResult: Result[String] = fieldExtended[String]("requestRequiredAcks")(json)
      val kafkaBlockOnBufferFullResult: Result[String] = fieldExtended[String]("kafkaBlockOnBufferFull")(json)
      val batchNumMessagesResult: Result[String] = fieldExtended[String]("batchNumMessages")(json)
      val topicNameResult: Result[String] = fieldExtended[String]("topicName")(json)
      val bufferMemoryResult: Result[String] = fieldExtended[String]("bufferMemory")(json)
      val maxBlockMsResult: Result[String] = fieldExtended[String]("maxBlockMs")(json)

      (kafkaBrokerListResult |@| bootstrapServersResult |@| producerTypeResult |@| serializerClassResult |@| requestRequiredAcksResult |@| kafkaBlockOnBufferFullResult |@| batchNumMessagesResult |@| topicNameResult |@| bufferMemoryResult |@| maxBlockMsResult) {
        case(kafkaBrokerList, bootstrapServers, producerType, serializerClass, requestRequiredAcks, kafkaBlockOnBufferFull, batchNumMessages, topicName, bufferMemory, maxBlockMs) =>
        JsonKafkaRequestLoggingConfig(kafkaBrokerList, bootstrapServers, producerType, serializerClass, requestRequiredAcks, kafkaBlockOnBufferFull, batchNumMessages, topicName, bufferMemory, maxBlockMs)
      }
    }
  }
}


