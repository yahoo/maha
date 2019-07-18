// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.missing.lookup;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.MissingLookupConfig;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.TestPasswordProvider;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.TestProtobufSchemaFactory;
import io.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Period;
import org.skife.jdbi.v2.DBI;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class MissingLookupManagerTest {

    private ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Test
    public void testMissingLookupManagerTest() throws Exception {

        MissingLookupRocksDBExtractionNamespaceFactory mlenf = mock(
            MissingLookupRocksDBExtractionNamespaceFactory.class);
        DBI dbi = mock(DBI.class);
        when(mlenf.ensureDBI(any(), any())).thenReturn(dbi);
        when(mlenf.ensureKafkaProducer(any())).thenReturn(mock(KafkaProducer.class));
        doCallRealMethod().when(mlenf).process(any(), any(), any(), any(), any(), anyString());
        TestProtobufSchemaFactory protobufSchemaFactory = new TestProtobufSchemaFactory();
        TestPasswordProvider passwordProvider = new TestPasswordProvider();
        MissingLookupManager<MissingLookupRocksDBExtractionNamespaceFactory,
                TestProtobufSchemaFactory,
                TestPasswordProvider> mlm = spy(new MissingLookupManager<>());

        KafkaConsumer<String, byte[]> kafkaConsumer = mock(KafkaConsumer.class);
        List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
        MetadataStorageConnectorConfig metadataStorageConnectorConfig = objectMapper.readValue("{ \"createTables\": false,\"connectURI\": \"jdbc:oracle:thin:@testdb\",\"user\": \"test_user\",\"password\":\"test_user.db.prod.pwd\"}", MetadataStorageConnectorConfig.class);
        RocksDBExtractionNamespace extractionNamespace = new RocksDBExtractionNamespace(
                "ad_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", new MissingLookupConfig(metadataStorageConnectorConfig, "na_reporting.ad", "id", "missing_ad_lookup_topic")
        );
        byte[] byteArray = objectMapper.writeValueAsBytes(extractionNamespace);
        ConsumerRecord<String, byte[]> cr = new ConsumerRecord("abc", 1, 1, "123", byteArray);
        records.add(cr);
        TopicPartition tp = new TopicPartition("abc", 1);
        Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> map = new HashMap<>();
        map.put(tp, records);
        ConsumerRecords<String, byte[]> consumerRecords = new ConsumerRecords<>(map);
        when(kafkaConsumer.poll(anyLong())).thenReturn(consumerRecords);

        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "localhost:8092");
        kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        when(mlm.createKafkaConsumer(kafkaProperties)).thenReturn(kafkaConsumer);

        mlm.start(mlenf, protobufSchemaFactory, passwordProvider, kafkaProperties, "abc", 1, "def");
        TimeUnit.SECONDS.sleep(1);
        mlm.stop(mlenf);
        verify(mlm, times(1)).createKafkaConsumer(any());
    }
}
