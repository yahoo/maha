// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.utils

import java.util.Properties

import com.yahoo.maha.proto.MahaRequestLog.MahaRequestProto
import com.yahoo.maha.service.config.JsonKafkaRequestLoggingConfig
import grizzled.slf4j.Logging
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}

/**
 * Created by pranavbhole on 16/08/17.
 */

trait MahaRequestLogWriter {
  def write(reqLogBuilder: MahaRequestProto.Builder)
  def validate (reqLogBuilder: MahaRequestProto.Builder)
}

class NoopMahaRequestLogWriter extends MahaRequestLogWriter {
  override def write(reqLogBuilder: MahaRequestProto.Builder): Unit = {}
  override def validate(reqLogBuilder: MahaRequestProto.Builder): Unit = {}
}

class KafkaMahaRequestLogWriter(JsonKafkaRequestLoggingConfig: JsonKafkaRequestLoggingConfig, loggingEnabled: Boolean) extends MahaRequestLogWriter with Logging {

  val props: Properties = new Properties
  props.put("bootstrap.servers", JsonKafkaRequestLoggingConfig.bootstrapServers)
  props.put("value.serializer", JsonKafkaRequestLoggingConfig.serializerClass)
  props.put("key.serializer", JsonKafkaRequestLoggingConfig.serializerClass)
  props.put("request.required.acks", JsonKafkaRequestLoggingConfig.requestRequiredAcks)
  props.put("producer.type", JsonKafkaRequestLoggingConfig.producerType)
  props.put("block.on.buffer.full", JsonKafkaRequestLoggingConfig.kafkaBlockOnBufferFull)
  props.put("batch.num.messages", JsonKafkaRequestLoggingConfig.batchNumMessages)
  props.put("buffer.memory", JsonKafkaRequestLoggingConfig.bufferMemory)
  props.put("max.block.ms", JsonKafkaRequestLoggingConfig.maxBlockMs)

  var kafkaProducer :org.apache.kafka.clients.producer.KafkaProducer[Array[Byte],Array[Byte]] = null

  if (loggingEnabled) {
    kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer[Array[Byte],Array[Byte]](props)
  }
  val callback = new CheckErrorCallback

  def write(reqLogBuilder: MahaRequestProto.Builder) = {

    validate(reqLogBuilder)

    if (loggingEnabled) {
      try {
        val producerRecord: ProducerRecord[Array[Byte], Array[Byte]] = new ProducerRecord[Array[Byte], Array[Byte]](JsonKafkaRequestLoggingConfig.topicName, reqLogBuilder.build().toByteArray)
        kafkaProducer.send(producerRecord, callback)
      }
      catch {
        case be: Exception => {
          warn("Exception while sending kafka messages in worker.." + be.getMessage, be)
        }
      } finally {
        kafkaProducer.flush()
      }
    }
  }

  def validate (reqLogBuilder: MahaRequestProto.Builder) = {
    if(reqLogBuilder.hasJson == false || reqLogBuilder.hasRequestId == false) {
      warn(s"Message is missing the required fields [requestId, json] = [${reqLogBuilder.getRequestId} , ${reqLogBuilder.getJson}}], jobId = ${reqLogBuilder.getJobId}," +
        s" Builder: ${reqLogBuilder}")
    }
  }

  class CheckErrorCallback extends Callback {
    def onCompletion(metadata: RecordMetadata, exception: Exception) {
      if (exception != null)
        warn("Got the exception while logging request into Kafka", exception)
    }
  }

  def close (): Unit = {
    kafkaProducer.close()
  }
}

