// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.log

import java.util.Properties
import java.util.concurrent.Future
import com.google.common.util.concurrent.Futures
import com.google.protobuf.GeneratedMessageV3
import com.yahoo.maha.proto.MahaRequestLog.MahaRequestProto
import grizzled.slf4j.Logging
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}

/**
 * Created by pranavbhole on 16/08/17.
 */

trait MahaRequestLogWriter {
  def write(reqLogBuilder: MahaRequestProto)
  def validate (reqLogBuilder: MahaRequestProto)
}

class NoopMahaRequestLogWriter extends MahaRequestLogWriter {
  override def write(reqLogBuilder: MahaRequestProto): Unit = {}
  override def validate(reqLogBuilder: MahaRequestProto): Unit = {}
}

case class KafkaRequestLoggingConfig(kafkaBrokerList: String,
                                     bootstrapServers: String,
                                     producerType: String,
                                     serializerClass: String,
                                     requestRequiredAcks: String,
                                     kafkaBlockOnBufferFull: String,
                                     batchNumMessages: String,
                                     topicName: String,
                                     bufferMemory: String,
                                     maxBlockMs: String)

class KafkaMahaRequestLogWriter(kafkaRequestLoggingConfig: KafkaRequestLoggingConfig, loggingEnabled: Boolean) extends MahaRequestLogWriter with Logging {

  val props: Properties = new Properties
  props.put("bootstrap.servers", kafkaRequestLoggingConfig.bootstrapServers)
  props.put("value.serializer", kafkaRequestLoggingConfig.serializerClass)
  props.put("key.serializer", kafkaRequestLoggingConfig.serializerClass)
  props.put("request.required.acks", kafkaRequestLoggingConfig.requestRequiredAcks)
  props.put("producer.type", kafkaRequestLoggingConfig.producerType)
  props.put("block.on.buffer.full", kafkaRequestLoggingConfig.kafkaBlockOnBufferFull)
  props.put("batch.num.messages", kafkaRequestLoggingConfig.batchNumMessages)
  props.put("buffer.memory", kafkaRequestLoggingConfig.bufferMemory)
  props.put("max.block.ms", kafkaRequestLoggingConfig.maxBlockMs)

  lazy val kafkaProducer :org.apache.kafka.clients.producer.KafkaProducer[Array[Byte],Array[Byte]] = {
    if (loggingEnabled) {
      createKafkaProducer(props)
    } else {
      null
    }
  }

  val callback = new CheckErrorCallback

  val loggingDisabledError: Future[RecordMetadata] = Futures.immediateFailedFuture(new IllegalStateException("logging disabled, cannot write message, check config"))

  private[log] def createKafkaProducer(props: Properties) : org.apache.kafka.clients.producer.KafkaProducer[Array[Byte],Array[Byte]] = {
    new org.apache.kafka.clients.producer.KafkaProducer[Array[Byte], Array[Byte]](props)
  }

  private[log] def writeMessage(proto: GeneratedMessageV3): Future[RecordMetadata] = {
    if(loggingEnabled) {
      try {
        val producerRecord: ProducerRecord[Array[Byte], Array[Byte]] =
          new ProducerRecord[Array[Byte], Array[Byte]](kafkaRequestLoggingConfig.topicName, proto.toByteArray)
        kafkaProducer.send(producerRecord, callback)
      } catch {
        case be: Exception =>
          warn("Exception while sending kafka messages in worker..", be)
          Futures.immediateFailedFuture(be)
      } finally {
        kafkaProducer.flush()
      }
    } else {
      loggingDisabledError
    }
  }

  def writeMahaRequestProto(proto: MahaRequestProto): Future[RecordMetadata] = {
    validate(proto)
    val result = writeMessage(proto)
    result
  }

  def write(proto: MahaRequestProto) : Unit = {
    implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
    scala.concurrent.Future {
      writeMahaRequestProto(proto)
    }
  }

  def validate(reqLogBuilder: MahaRequestProto): Unit = {
    require(reqLogBuilder.hasJson && reqLogBuilder.hasRequestId,
      s"Message is missing the required fields requestId, json = $reqLogBuilder"
    )
  }

  class CheckErrorCallback extends Callback {
    def onCompletion(metadata: RecordMetadata, exception: Exception) {
      if (exception != null)
        warn("Got the exception while logging request into Kafka", exception)
    }
  }

  def close (): Unit = {
    if(kafkaProducer != null) {
      kafkaProducer.close()
    }
  }
}

