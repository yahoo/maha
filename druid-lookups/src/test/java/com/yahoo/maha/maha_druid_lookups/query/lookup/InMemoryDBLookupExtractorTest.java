// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import com.google.protobuf.Message;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.java.util.emitter.service.ServiceEventBuilder;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.InMemoryDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.MissingLookupConfig;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.KafkaManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.RocksDBManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.AdProtos;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.TestProtobufSchemaFactory;
import io.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.commons.io.FileUtils;
import org.joda.time.Period;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class InMemoryDBLookupExtractorTest {
    private static final ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Test
    public void testBuildWhenDimensionValueIsEmpty()  throws Exception {
        LookupService lookupService = mock(LookupService.class);
        RocksDBManager rocksDBManager = mock(RocksDBManager.class);
        KafkaManager kafkaManager = mock(KafkaManager.class);
        ServiceEmitter serviceEmitter = mock(ServiceEmitter.class);
        InMemoryDBExtractionNamespace extractionNamespace = new InMemoryDBExtractionNamespace(
                "ad_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", null
        );
        Map<String, String> map = new HashMap<>();
        InMemoryDBLookupExtractor InMemoryDBLookupExtractor = new InMemoryDBLookupExtractor(extractionNamespace, map, lookupService, rocksDBManager, kafkaManager, new TestProtobufSchemaFactory(), serviceEmitter);
        String lookupValue = InMemoryDBLookupExtractor.apply("");
        Assert.assertNull(lookupValue);
    }

    @Test
    public void testBuildWhenDimensionValueIsNull()  throws Exception {
        LookupService lookupService = mock(LookupService.class);
        RocksDBManager rocksDBManager = mock(RocksDBManager.class);
        KafkaManager kafkaManager = mock(KafkaManager.class);
        ServiceEmitter serviceEmitter = mock(ServiceEmitter.class);
        InMemoryDBExtractionNamespace extractionNamespace = new InMemoryDBExtractionNamespace(
                "ad_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", null
        );
        Map<String, String> map = new HashMap<>();
        InMemoryDBLookupExtractor InMemoryDBLookupExtractor = new InMemoryDBLookupExtractor(extractionNamespace, map, lookupService, rocksDBManager, kafkaManager,  new TestProtobufSchemaFactory(), serviceEmitter);
        String lookupValue = InMemoryDBLookupExtractor.apply(null);
        Assert.assertNull(lookupValue);
    }

    @Test
    public void testBuildWhenCacheValueIsNull() throws Exception {

        LookupService lookupService = mock(LookupService.class);
        RocksDB db = mock(RocksDB.class);
        RocksDBManager rocksDBManager = mock(RocksDBManager.class);
        KafkaManager kafkaManager = mock(KafkaManager.class);
        ServiceEmitter serviceEmitter = mock(ServiceEmitter.class);
        when(rocksDBManager.getDB(anyString())).thenReturn(db);

        MissingLookupConfig missingLookupConfig = new MissingLookupConfig(new MetadataStorageConnectorConfig(), "", "", "some_missing_topic");
        InMemoryDBExtractionNamespace extractionNamespace = new InMemoryDBExtractionNamespace(
                "ad_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", missingLookupConfig
        );
        Map<String, String> map = new HashMap<>();
        InMemoryDBLookupExtractor InMemoryDBLookupExtractor = new InMemoryDBLookupExtractor(extractionNamespace, map, lookupService, rocksDBManager, kafkaManager,  new TestProtobufSchemaFactory(), serviceEmitter);
        MahaLookupQueryElement mahaLookupQueryElement1 = new MahaLookupQueryElement();
        mahaLookupQueryElement1.setDimension("abc");
        mahaLookupQueryElement1.setValueColumn("status");
        String lookupValue = InMemoryDBLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement1));
        verify(kafkaManager, times(1)).handleMissingLookup(any(byte[].class), anyString(), anyString());
        Assert.assertNull(lookupValue);
        InMemoryDBLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement1));
        verify(kafkaManager, times(1)).handleMissingLookup(any(byte[].class), anyString(), anyString());
        verify(serviceEmitter, times(1)).emit(any(ServiceEventBuilder.class));
    }

    @Test
    public void handleMissingLookupShouldNotBeCalledWhenNotConfigured() throws Exception {

        LookupService lookupService = mock(LookupService.class);
        RocksDB db = mock(RocksDB.class);
        RocksDBManager rocksDBManager = mock(RocksDBManager.class);
        KafkaManager kafkaManager = mock(KafkaManager.class);
        ServiceEmitter serviceEmitter = mock(ServiceEmitter.class);
        when(rocksDBManager.getDB(anyString())).thenReturn(db);

        InMemoryDBExtractionNamespace extractionNamespace = new InMemoryDBExtractionNamespace(
                "ad_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", null
        );
        Map<String, String> map = new HashMap<>();
        InMemoryDBLookupExtractor InMemoryDBLookupExtractor = new InMemoryDBLookupExtractor(extractionNamespace, map, lookupService, rocksDBManager, kafkaManager,  new TestProtobufSchemaFactory(), serviceEmitter);
        MahaLookupQueryElement mahaLookupQueryElement1 = new MahaLookupQueryElement();
        mahaLookupQueryElement1.setDimension("abc");
        mahaLookupQueryElement1.setValueColumn("status");
        String lookupValue = InMemoryDBLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement1));
        verify(kafkaManager, times(0)).handleMissingLookup(any(byte[].class), anyString(), anyString());
        Assert.assertNull(lookupValue);
        verify(serviceEmitter, times(0)).emit(any(ServiceEventBuilder.class));
    }

    @Test
    public void testBuildWhenCacheValueIsNotNull() throws Exception{

        LookupService lookupService = mock(LookupService.class);
        RocksDBManager rocksDBManager = mock(RocksDBManager.class);
        KafkaManager kafkaManager = mock(KafkaManager.class);
        ServiceEmitter serviceEmitter = mock(ServiceEmitter.class);
        Options options = null;
        RocksDB db = null;
        File tempFile = null;
        try {

            tempFile = new File(Files.createTempDir(), "rocksdblookup");
            options = new Options().setCreateIfMissing(true);
            db = RocksDB.open(options, tempFile.getAbsolutePath());

            Message msg = AdProtos.Ad.newBuilder()
                    .setId("32309719080")
                    .setTitle("some title")
                    .setStatus("ON")
                    .setLastUpdated("1470733203505")
                    .build();

            db.put("32309719080".getBytes(), msg.toByteArray());

            when(rocksDBManager.getDB(anyString())).thenReturn(db);

            InMemoryDBExtractionNamespace extractionNamespace = new InMemoryDBExtractionNamespace(
                    "ad_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", null
            );
            Map<String, String> map = new HashMap<>();
            InMemoryDBLookupExtractor InMemoryDBLookupExtractor = new InMemoryDBLookupExtractor(extractionNamespace, map, lookupService, rocksDBManager, kafkaManager,  new TestProtobufSchemaFactory(), serviceEmitter);
            MahaLookupQueryElement mahaLookupQueryElement1 = new MahaLookupQueryElement();
            mahaLookupQueryElement1.setDimension("32309719080");
            mahaLookupQueryElement1.setValueColumn("status");
            String lookupValue = InMemoryDBLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement1));
            Assert.assertEquals(lookupValue, "ON");

            MahaLookupQueryElement mahaLookupQueryElement2 = new MahaLookupQueryElement();
            mahaLookupQueryElement2.setDimension("32309719080");
            mahaLookupQueryElement2.setValueColumn("title");
            lookupValue = InMemoryDBLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement2));
            Assert.assertEquals(lookupValue, "some title");
        } finally {
            if(db != null) {
                db.close();
            }
            if(tempFile.exists()) {
                FileUtils.forceDelete(tempFile);
            }
        }
    }

    @Test
    public void testBuildWhenInvalidValueColumn() throws Exception {

        LookupService lookupService = mock(LookupService.class);
        RocksDBManager rocksDBManager = mock(RocksDBManager.class);
        KafkaManager kafkaManager = mock(KafkaManager.class);
        ServiceEmitter serviceEmitter = mock(ServiceEmitter.class);
        Options options = null;
        RocksDB db = null;
        File tempFile = null;
        try {

            tempFile = new File(Files.createTempDir(), "rocksdblookup");
            options = new Options().setCreateIfMissing(true);
            db = RocksDB.open(options, tempFile.getAbsolutePath());

            Message msg = AdProtos.Ad.newBuilder()
                .setId("32309719080")
                .setTitle("some title")
                .setStatus("ON")
                .setLastUpdated("1470733203505")
                .build();

            db.put("32309719080".getBytes(), msg.toByteArray());

            when(rocksDBManager.getDB(anyString())).thenReturn(db);

            MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
            InMemoryDBExtractionNamespace extractionNamespace =
                new InMemoryDBExtractionNamespace("ad_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", null);
            Map<String, List<String>> map = new HashMap<>();
            map.put("12345", Arrays.asList("12345", "my name", "USD", "ON"));
            InMemoryDBLookupExtractor InMemoryDBLookupExtractor = new InMemoryDBLookupExtractor(extractionNamespace, map, lookupService, rocksDBManager, kafkaManager,  new TestProtobufSchemaFactory(), serviceEmitter);
            MahaLookupQueryElement mahaLookupQueryElement1 = new MahaLookupQueryElement();
            mahaLookupQueryElement1.setDimension("12345");
            mahaLookupQueryElement1.setValueColumn("booking_country");
            String lookupValue = InMemoryDBLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement1));
            Assert.assertNull(lookupValue);
        } finally {
            if(db != null) {
                db.close();
            }
            if(tempFile.exists()) {
                FileUtils.forceDelete(tempFile);
            }
        }
    }

    @Test
    public void testDimensionOverrideMap() throws Exception {

        LookupService lookupService = mock(LookupService.class);
        RocksDBManager rocksDBManager = mock(RocksDBManager.class);
        KafkaManager kafkaManager = mock(KafkaManager.class);
        ServiceEmitter serviceEmitter = mock(ServiceEmitter.class);
        MetadataStorageConnectorConfig metadataStorageConnectorConfig = objectMapper.readValue("{ \"createTables\": false,\"connectURI\": \"jdbc:oracle:thin:@cbrptprod_bf1\",\"user\": \"na_reporting_ws\",\"password\":\"na_reporting_ws.db.prod.pwd\"}", MetadataStorageConnectorConfig.class);
        InMemoryDBExtractionNamespace extractionNamespace = new InMemoryDBExtractionNamespace(
                "ad_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", new MissingLookupConfig(metadataStorageConnectorConfig, "na_reporting.ad", "id", "missing_ad_lookup_topic")
        );
        byte[] b = objectMapper.writeValueAsBytes(extractionNamespace);
        Map<String, String> map = new HashMap<>();
        InMemoryDBLookupExtractor InMemoryDBLookupExtractor = new InMemoryDBLookupExtractor(extractionNamespace, map, lookupService, rocksDBManager, kafkaManager,  new TestProtobufSchemaFactory(), serviceEmitter);
        Map<String, String> dimensionOverrideMap = new HashMap<>();
        dimensionOverrideMap.put("12345", "something-12345");
        dimensionOverrideMap.put("6789", "something-6789");
        dimensionOverrideMap.put("", "Unknown");

        MahaLookupQueryElement mahaLookupQueryElement1 = new MahaLookupQueryElement();
        mahaLookupQueryElement1.setDimension("12345");
        mahaLookupQueryElement1.setValueColumn("name");
        mahaLookupQueryElement1.setDimensionOverrideMap(dimensionOverrideMap);
        String lookupValue = InMemoryDBLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement1));
        Assert.assertEquals(lookupValue, "something-12345");

        MahaLookupQueryElement mahaLookupQueryElement2 = new MahaLookupQueryElement();
        mahaLookupQueryElement2.setDimension("");
        mahaLookupQueryElement2.setValueColumn("name");
        mahaLookupQueryElement2.setDimensionOverrideMap(dimensionOverrideMap);
        lookupValue = InMemoryDBLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement2));
        Assert.assertEquals(lookupValue, "Unknown");
    }

    @Test
    public void testDecodeConfig() throws Exception {

        LookupService lookupService = mock(LookupService.class);
        RocksDBManager rocksDBManager = mock(RocksDBManager.class);
        KafkaManager kafkaManager = mock(KafkaManager.class);
        ServiceEmitter serviceEmitter = mock(ServiceEmitter.class);
        Options options = null;
        RocksDB db = null;
        File tempFile = null;
        try {

            tempFile = new File(Files.createTempDir(), "rocksdblookup");
            options = new Options().setCreateIfMissing(true);
            db = RocksDB.open(options, tempFile.getAbsolutePath());

            Message msg = AdProtos.Ad.newBuilder()
                .setId("32309719080")
                .setTitle("some title")
                .setStatus("ON")
                .setLastUpdated("1470733203505")
                .build();

            db.put("32309719080".getBytes(), msg.toByteArray());

            when(rocksDBManager.getDB(anyString())).thenReturn(db);

            MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
            InMemoryDBExtractionNamespace extractionNamespace =
                new InMemoryDBExtractionNamespace("ad_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", new MissingLookupConfig(metadataStorageConnectorConfig, "na_reporting.ad", "id", "missing_ad_lookup_topic"));
            Map<String, List<String>> map = new HashMap<>();
            map.put("12345", Arrays.asList("12345", "my name", "USD", "ON"));
            InMemoryDBLookupExtractor InMemoryDBLookupExtractor = new InMemoryDBLookupExtractor(extractionNamespace, map, lookupService, rocksDBManager, kafkaManager,  new TestProtobufSchemaFactory(), serviceEmitter);

            Map<String, String> dimensionOverrideMap = new HashMap<>();
            dimensionOverrideMap.put("123", "something-123");
            dimensionOverrideMap.put("6789", "something-6789");
            dimensionOverrideMap.put("", "Unknown");

            MahaLookupQueryElement mahaLookupQueryElement1 = new MahaLookupQueryElement();
            mahaLookupQueryElement1.setDimension("32309719080");
            mahaLookupQueryElement1.setValueColumn("title");
            mahaLookupQueryElement1.setDimensionOverrideMap(dimensionOverrideMap);
            DecodeConfig decodeConfig1 = new DecodeConfig();
            decodeConfig1.setColumnToCheck("title");
            decodeConfig1.setValueToCheck("some title");
            decodeConfig1.setColumnIfValueMatched("last_updated");
            decodeConfig1.setColumnIfValueNotMatched("status");
            mahaLookupQueryElement1.setDecodeConfig(decodeConfig1);
            String lookupValue = InMemoryDBLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement1));
            Assert.assertEquals(lookupValue, "1470733203505");

            MahaLookupQueryElement mahaLookupQueryElement2 = new MahaLookupQueryElement();
            mahaLookupQueryElement2.setDimension("32309719080");
            mahaLookupQueryElement2.setValueColumn("title");
            mahaLookupQueryElement2.setDimensionOverrideMap(dimensionOverrideMap);
            DecodeConfig decodeConfig2 = new DecodeConfig();
            decodeConfig2.setColumnToCheck("status");
            decodeConfig2.setValueToCheck("OFF");
            decodeConfig2.setColumnIfValueMatched("last_updated");
            decodeConfig2.setColumnIfValueNotMatched("status");
            mahaLookupQueryElement2.setDecodeConfig(decodeConfig2);
            lookupValue = InMemoryDBLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement2));
            Assert.assertEquals(lookupValue, "ON");
        } finally {
            if(db != null) {
                db.close();
            }
            if(tempFile.exists()) {
                FileUtils.forceDelete(tempFile);
            }
        }

    }

    @Test
    public void testThrowingDecodeConfig() throws Exception {

        LookupService lookupService = mock(LookupService.class);
        RocksDBManager rocksDBManager = mock(RocksDBManager.class);
        KafkaManager kafkaManager = mock(KafkaManager.class);
        ServiceEmitter serviceEmitter = mock(ServiceEmitter.class);
        Options options = null;
        RocksDB db = null;
        File tempFile = null;
        try {

            tempFile = new File(Files.createTempDir(), "rocksdblookup");
            options = new Options().setCreateIfMissing(true);
            db = RocksDB.open(options, tempFile.getAbsolutePath());

            Message msg = AdProtos.Ad.newBuilder()
                .setId("32309719080")
                .setTitle("some title")
                .setStatus("ON")
                .setLastUpdated("1470733203505")
                .build();

            db.put("32309719080".getBytes(), msg.toByteArray());

            when(rocksDBManager.getDB(anyString())).thenReturn(db);

            MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
            InMemoryDBExtractionNamespace extractionNamespace =
                new InMemoryDBExtractionNamespace("ad_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", new MissingLookupConfig(metadataStorageConnectorConfig, "na_reporting.ad", "id", "missing_ad_lookup_topic"));
            Map<String, List<String>> map = new HashMap<>();
            map.put("12345", Arrays.asList("12345", "my name", "USD", "ON"));
            InMemoryDBLookupExtractor InMemoryDBLookupExtractor = new InMemoryDBLookupExtractor(extractionNamespace, map, lookupService, rocksDBManager, kafkaManager, new TestProtobufSchemaFactory(), serviceEmitter);

            Map<String, String> dimensionOverrideMap = new HashMap<>();
            dimensionOverrideMap.put("123", "something-123");
            dimensionOverrideMap.put("6789", "something-6789");
            dimensionOverrideMap.put("", "Unknown");

            MahaLookupQueryElement mahaLookupQueryElement3 = new MahaLookupQueryElement();
            mahaLookupQueryElement3.setDimension("32309719080");
            mahaLookupQueryElement3.setValueColumn("title");
            mahaLookupQueryElement3.setDimensionOverrideMap(dimensionOverrideMap);
            DecodeConfig decodeConfig3 = new DecodeConfig();
            decodeConfig3.setColumnToCheck("title");
            decodeConfig3.setValueToCheck("some title");
            decodeConfig3.setColumnIfValueMatched("fake_column");
            decodeConfig3.setColumnIfValueNotMatched("status");
            mahaLookupQueryElement3.setDecodeConfig(decodeConfig3);
            String lookupValue = InMemoryDBLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement3));
            Assert.assertEquals(lookupValue, null);
        } finally {
            if(db != null) {
                db.close();
            }
            if(tempFile.exists()) {
                FileUtils.forceDelete(tempFile);
            }
        }
    }


}