package com.yahoo.maha.log

import java.io.IOException
import java.util

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.yahoo.maha.data.Compressor.Codec
import com.yahoo.maha.data.{Compressor, CompressorFactory, StringEventBatch}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

/**
  * Created by hiral on 8/3/18.
  */
object KafkaStringEventBatchSerializer {
  private val logger: Logger = LoggerFactory.getLogger(classOf[KafkaStringEventBatchSerializer])
  val BUFFER_MB_PROPERTY = "stringeventbatch.serializer.buffer.mb"
  val DEFAULT_BUFFER_MB = 1
}

class KafkaStringEventBatchSerializer extends Serializer[StringEventBatch] {

  final private val kryo: Kryo = new Kryo
  private var compressor: Compressor = _
  private var encodeBuffer: Array[Byte] = _
  private var output: Output = _
  import KafkaStringEventBatchSerializer._
  kryo.addDefaultSerializer(classOf[StringEventBatch], new KryoStringEventBatch)

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    val codecValue:Any = configs.get(Compressor.COMPRESSOR_CODEC_PROPERTY)
    compressor = Option(codecValue) match {
      case Some(value) if value.isInstanceOf[String] &&
        Try(Compressor.Codec.valueOf(value.asInstanceOf[String].toUpperCase)).isSuccess =>
        CompressorFactory.getCompressor(Compressor.Codec.valueOf(value.asInstanceOf[String].toUpperCase))
      case _ =>
        CompressorFactory.getCompressor(Codec.LZ4)
    }
    val decodeValue: Any = configs.get(BUFFER_MB_PROPERTY)
    val (configuredOutput, configuredBuffer) = Option(decodeValue) match {
      case Some(value) if value.isInstanceOf[String] &&
        Try(value.asInstanceOf[String].toInt).isSuccess =>
        val mb = value.asInstanceOf[String].toInt
        val size = if(mb > 0) {
          1024 * 1024 * mb
        } else 1024 * 1024 * DEFAULT_BUFFER_MB
        (new Output(size), new Array[Byte](size))
      case _ =>
        (new Output(1024 * 1024 * DEFAULT_BUFFER_MB), new Array[Byte](1024 * 1024 * DEFAULT_BUFFER_MB))
    }
    output = configuredOutput
    encodeBuffer = configuredBuffer
    logger.warn("Using maha compression codec : {} , buffer bytes = {}"
      , compressor.codec().name(), encodeBuffer.length)
  }

  def serialize(topic: String, data: StringEventBatch): Array[Byte] = {
    kryo.writeObject(output, data)
    val ba = output.toBytes
    output.clear()
    try {
      val size = compressor.compress(ba, encodeBuffer)
      val output = new Array[Byte](size)
      System.arraycopy(encodeBuffer, 0, output, 0, size)
      output
    } catch {
      case e: IOException =>
        throw new RuntimeException(e)
    }
  }

  override def close(): Unit = {
    output = null
    encodeBuffer = null
  }

}

object KafkaStringEventBatchDeserializer {
  private val logger: Logger = LoggerFactory.getLogger(classOf[KafkaStringEventBatchDeserializer])
  val BUFFER_MB_PROPERTY = "stringeventbatch.deserializer.buffer.mb"
  val DEFAULT_BUFFER_MB = 1
}

class KafkaStringEventBatchDeserializer extends Deserializer[StringEventBatch] {

  final private val kryo: Kryo = new Kryo
  private var compressor: Compressor = _
  private var decodeBuffer: Array[Byte] = _
  import KafkaStringEventBatchDeserializer._
  kryo.addDefaultSerializer(classOf[StringEventBatch], new KryoStringEventBatch)

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    val codecValue:Any = configs.get(Compressor.COMPRESSOR_CODEC_PROPERTY)
    compressor = Option(codecValue) match {
      case Some(value) if value.isInstanceOf[String] &&
        Try(Compressor.Codec.valueOf(value.asInstanceOf[String].toUpperCase)).isSuccess =>
        CompressorFactory.getCompressor(Compressor.Codec.valueOf(value.asInstanceOf[String].toUpperCase))
      case _ =>
        CompressorFactory.getCompressor(Codec.LZ4)
    }
    val decodeValue: Any = configs.get(BUFFER_MB_PROPERTY)
    decodeBuffer = Option(decodeValue) match {
      case Some(value) if value.isInstanceOf[String] &&
        Try(value.asInstanceOf[String].toInt).isSuccess =>
        val mb = value.asInstanceOf[String].toInt
        val size = if(mb > 0) {
          1024 * 1024 * mb
        } else 1024 * 1024 * DEFAULT_BUFFER_MB
        new Array[Byte](size)
      case _ =>
        new Array[Byte](1024 * 1024 * DEFAULT_BUFFER_MB)
    }
    logger.warn("Using maha compression codec : {} , buffer bytes = {}"
      , compressor.codec().name(), decodeBuffer.length)
  }

  override def deserialize(topic: String, data: Array[Byte]): StringEventBatch = {
    try {
      val readBytes = compressor.decompress(data, decodeBuffer)
      val input = new Input(decodeBuffer, 0, readBytes)
      kryo.readObject(input, classOf[StringEventBatch])
    } catch {
      case e: IOException =>
        throw new RuntimeException(e)
    }
  }

  override def close(): Unit = {
    decodeBuffer = null
  }
}
