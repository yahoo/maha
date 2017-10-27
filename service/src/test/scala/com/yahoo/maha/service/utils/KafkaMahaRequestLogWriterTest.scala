// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.utils

import java.net.ServerSocket
import java.util
import java.util.Properties
import com.google.protobuf.ByteString
import com.yahoo.maha.proto.MahaRequestLog
import com.yahoo.maha.service.config.JsonKafkaRequestLoggingConfig
import grizzled.slf4j.Logging
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{ZkUtils, TestUtils}
import org.apache.curator.test.TestingServer
import org.apache.kafka.clients.consumer.{KafkaConsumer}
import org.apache.kafka.common.protocol.SecurityProtocol
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import collection.JavaConverters._


/**
 * Created by pranavbhole on 16/08/17.
 */
class KafkaMahaRequestLogWriterTest extends FunSuite with Matchers with BeforeAndAfterAll with Logging {

  private[this] val fromDate = "2016-02-07"
  private[this] val toDate = "2016-02-12"

  var kafkaServer: KafkaServer = null
  var zkServer: TestingServer = null

  var kafkaConsumer: KafkaConsumer[Array[Byte],Array[Byte]] = null
  var kafkaBroker: String = null
  var zkConnect: String = null
  val TOPIC = "async_req"

  var mahaRequestLogWriter: KafkaMahaRequestLogWriter = null

  override def beforeAll(): Unit = {
    //super.beforeAll()
    val zkPort = getFreePort
    try {
      zkServer = new TestingServer(zkPort, true)
    } catch {
      case e: Exception =>
        error(s"Exception while starting Zookeeper at port: $zkPort")
        e.printStackTrace()
        throw e
    }

    zkConnect = zkServer.getConnectString
    println(s"Started zookeeper at ${zkConnect}")

    //val props = TestUtils.createBrokerConfigs(1,zkServer.getConnectString).iterator.next()
    val props = TestUtils.createBrokerConfig(0, zkConnect)
    kafkaServer = TestUtils.createServer(KafkaConfig.fromProps(props))
    //kafkaServer = new KafkaServerStartable(kafkaConfig)
    kafkaServer.startup()

    val zkUtils = ZkUtils(zkConnect, 10000, 10000, false)
    TestUtils.createTopic(zkUtils, TOPIC ,1,1,Seq(kafkaServer))

    kafkaBroker = "localhost" + TestUtils.getBrokerListStrFromServers(Seq(kafkaServer),SecurityProtocol.PLAINTEXT)
    info(s"Started kafka server at $kafkaBroker")

    val jsonKafkaRequestLoggingConfig = new JsonKafkaRequestLoggingConfig(
      kafkaBroker,
      kafkaBroker,
      "test",
      "org.apache.kafka.common.serialization.ByteArraySerializer",
      "1",
      "true",
      "1",
      TOPIC,
      "999999",
      "1000"
    )

    val properties = new Properties
    properties.put("bootstrap.servers", jsonKafkaRequestLoggingConfig.bootstrapServers)
    properties.put("group.id", "test-group")
    properties.put("enable.auto.commit", "true")
    properties.put("auto.commit.interval.ms", "10")
    properties.put("socket.timeout.ms", "100")
    properties.put("consumer.timeout.ms", "100")
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer[Array[Byte],Array[Byte]](properties)

    val topics = new util.ArrayList[String]()
    topics.add(TOPIC)
    kafkaConsumer.subscribe(topics)

    mahaRequestLogWriter = new KafkaMahaRequestLogWriter(jsonKafkaRequestLoggingConfig, true)
  }

  override protected def afterAll(): Unit = {
    try {
      kafkaServer.shutdown()
      zkServer.stop()
      kafkaConsumer.close()
      mahaRequestLogWriter.close()
    } catch{
      case e =>
    }
  }

  test("RequestLogWriter Test") {
    val reqLogBuilder: MahaRequestLog.MahaRequestProto.Builder = MahaRequestLog.MahaRequestProto.newBuilder()
    reqLogBuilder.setCube("sample_cube")
    reqLogBuilder.setJobId(1234)
    reqLogBuilder.setJson(ByteString.copyFrom("{}".getBytes))
    reqLogBuilder.setRequestId("kafka-test")
    var numberOfReceivedRecords = 0

    val consumerThread = new Thread(new Runnable {
      def run() {
        var stopRequested = 20
        while(stopRequested > 0) {
          mahaRequestLogWriter.write(reqLogBuilder)
          val consumerRecords = kafkaConsumer.poll(1000)
          if(consumerRecords.count() > 0) {
            info("Received:"+consumerRecords)
            consumerRecords.asScala.foreach {
              consumerRecord =>
                numberOfReceivedRecords+= 1
                assert(consumerRecord.topic() == TOPIC)
            }
          }
          stopRequested-=1
        }
      }
    })
    consumerThread.start

    Thread.sleep(100)
    consumerThread.join()
    info("done with polling")
    assert(numberOfReceivedRecords >= 3)
    mahaRequestLogWriter.close()
    try {
      kafkaConsumer.close()
    } catch{
      case e => e.printStackTrace()
    }
  }

  private def getFreePort(): Int = {
    val s = new ServerSocket(0)
    val freePort = s.getLocalPort
    info(s"Free port: $freePort")
    s.close()
    freePort
  }
}
