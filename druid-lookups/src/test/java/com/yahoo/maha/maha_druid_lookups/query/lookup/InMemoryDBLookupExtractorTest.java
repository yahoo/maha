// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import com.google.protobuf.Message;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.InMemoryDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace;
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
import java.util.Map;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class InMemoryDBLookupExtractorTest {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testBuildWhenDimensionValueIsEmpty() {
        LookupService lookupService = mock(LookupService.class);
        RocksDBManager rocksDBManager = mock(RocksDBManager.class);
        KafkaManager kafkaManager = mock(KafkaManager.class);
        InMemoryDBExtractionNamespace extractionNamespace = new InMemoryDBExtractionNamespace(
                "ad_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", "some_missing_lookup_topic"
        );
        Map<String, String> map = new HashMap<>();
        InMemoryDBLookupExtractor InMemoryDBLookupExtractor = new InMemoryDBLookupExtractor(extractionNamespace, map, lookupService, rocksDBManager, kafkaManager, new TestProtobufSchemaFactory());
        String lookupValue = InMemoryDBLookupExtractor.apply("");
        Assert.assertNull(lookupValue);
    }

    @Test
    public void testBuildWhenDimensionValueIsNull() {
        LookupService lookupService = mock(LookupService.class);
        RocksDBManager rocksDBManager = mock(RocksDBManager.class);
        KafkaManager kafkaManager = mock(KafkaManager.class);
        InMemoryDBExtractionNamespace extractionNamespace = new InMemoryDBExtractionNamespace(
                "ad_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", "some_missing_lookup_topic"
        );
        Map<String, String> map = new HashMap<>();
        InMemoryDBLookupExtractor InMemoryDBLookupExtractor = new InMemoryDBLookupExtractor(extractionNamespace, map, lookupService, rocksDBManager, kafkaManager,  new TestProtobufSchemaFactory());
        String lookupValue = InMemoryDBLookupExtractor.apply(null);
        Assert.assertNull(lookupValue);
    }

    @Test
    public void testBuildWhenCacheValueIsNull() throws Exception {

        LookupService lookupService = mock(LookupService.class);
        RocksDB db = mock(RocksDB.class);
        RocksDBManager rocksDBManager = mock(RocksDBManager.class);
        KafkaManager kafkaManager = mock(KafkaManager.class);
        when(rocksDBManager.getDB(anyString())).thenReturn(db);

        InMemoryDBExtractionNamespace extractionNamespace = new InMemoryDBExtractionNamespace(
                "ad_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", "some_missing_lookup_topic"
        );
        Map<String, String> map = new HashMap<>();
        InMemoryDBLookupExtractor InMemoryDBLookupExtractor = new InMemoryDBLookupExtractor(extractionNamespace, map, lookupService, rocksDBManager, kafkaManager,  new TestProtobufSchemaFactory());
        MahaLookupQueryElement mahaLookupQueryElement1 = new MahaLookupQueryElement();
        mahaLookupQueryElement1.setDimension("abc");
        mahaLookupQueryElement1.setValueColumn("status");
        String lookupValue = InMemoryDBLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement1));
        verify(kafkaManager, times(1)).handleMissingLookup(extractionNamespace, "abc");
        Assert.assertNull(lookupValue);
    }

    @Test
    public void testBuildWhenCacheValueIsNotNull() throws Exception{

        LookupService lookupService = mock(LookupService.class);
        RocksDBManager rocksDBManager = mock(RocksDBManager.class);
        KafkaManager kafkaManager = mock(KafkaManager.class);
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
                    "ad_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", "some_missing_lookup_topic"
            );
            Map<String, String> map = new HashMap<>();
            InMemoryDBLookupExtractor InMemoryDBLookupExtractor = new InMemoryDBLookupExtractor(extractionNamespace, map, lookupService, rocksDBManager, kafkaManager,  new TestProtobufSchemaFactory());
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
    public void testDimensionOverrideMap() throws Exception {

        LookupService lookupService = mock(LookupService.class);
        RocksDBManager rocksDBManager = mock(RocksDBManager.class);
        KafkaManager kafkaManager = mock(KafkaManager.class);
        InMemoryDBExtractionNamespace extractionNamespace = new InMemoryDBExtractionNamespace(
                "ad_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", "some_missing_lookup_topic"
        );
        Map<String, String> map = new HashMap<>();
        InMemoryDBLookupExtractor InMemoryDBLookupExtractor = new InMemoryDBLookupExtractor(extractionNamespace, map, lookupService, rocksDBManager, kafkaManager,  new TestProtobufSchemaFactory());
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

}