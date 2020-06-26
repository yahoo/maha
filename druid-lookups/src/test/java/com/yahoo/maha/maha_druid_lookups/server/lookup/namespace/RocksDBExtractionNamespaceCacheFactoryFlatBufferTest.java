// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.Table;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.flatbuffer.FlatBufferValue;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.flatbuffer.ProductAd;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.flatbuffer.ProductAdWrapper;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.flatbuffer.TestFlatBufferSchemaFactory;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.AdProtos;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.TestProtobufSchemaFactory;
import org.apache.commons.io.FileUtils;
import org.joda.time.Period;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import scala.Product;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

public class RocksDBExtractionNamespaceCacheFactoryFlatBufferTest {

    @InjectMocks
    RocksDBExtractionNamespaceCacheFactory obj =
            new RocksDBExtractionNamespaceCacheFactory();

    @InjectMocks
    RocksDBExtractionNamespaceCacheFactory noopObj =
            new RocksDBExtractionNamespaceCacheFactory();

    @Mock
    RocksDBManager rocksDBManager;

    @Mock
    ServiceEmitter serviceEmitter;

    ProductAdWrapper productAdWrapper = new ProductAdWrapper();

    @BeforeTest
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        obj.rocksDBManager = rocksDBManager;
        obj.schemaFactory = new TestFlatBufferSchemaFactory();
        obj.emitter = serviceEmitter;
        noopObj.rocksDBManager = rocksDBManager;
        noopObj.schemaFactory = new TestFlatBufferSchemaFactory();
        noopObj.emitter = serviceEmitter;
    }

    @Test
    public void testUpdateCacheWithGreaterLastUpdated() throws Exception {

        Options options = null;
        RocksDB db = null;
        File tempFile = null;
        try {

            tempFile = new File(Files.createTempDir(), "rocksdblookup");
            options = new Options().setCreateIfMissing(true);
            db = RocksDB.open(options, tempFile.getAbsolutePath());

            Map<String, FlatBufferValue> map = new HashMap();
            map.put("id",  FlatBufferValue.of("32309719080"));
            map.put("title",  FlatBufferValue.of("some title"));
            map.put("status",  FlatBufferValue.of("ON"));
            map.put("description",  FlatBufferValue.of("test desc"));
            map.put("last_updated", FlatBufferValue.of("1480733203505"));

            FlatBufferBuilder flatBufferBuilder = productAdWrapper.createFlatBuffer(map);

            db.put("32309719080".getBytes(), productAdWrapper.toByteArr(flatBufferBuilder.dataBuffer()));

            when(rocksDBManager.getDB(anyString())).thenReturn(db);

            RocksDBExtractionNamespace extractionNamespace = new RocksDBExtractionNamespace(
                    "product_ad_fb_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", null
                    , "com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.CacheActionRunnerFlatBuffer");

            map.put("status", FlatBufferValue.of("OFF"));
            map.put("last_updated", FlatBufferValue.of("1480733203506"));
            FlatBufferBuilder msgFromKafkaFlatBuffer = productAdWrapper.createFlatBuffer(map);

            obj.updateCache(extractionNamespace, new HashMap<>(), "32309719080", productAdWrapper.toByteArr(msgFromKafkaFlatBuffer.dataBuffer()));

            Table productAdTable = productAdWrapper.getFlatBuffer(db.get("32309719080".getBytes()));
            ProductAd productAdUpdated  = (ProductAd) productAdTable;

            Assert.assertEquals(productAdUpdated.id(), "32309719080");
            Assert.assertEquals(productAdUpdated.description(), "test desc");
            Assert.assertEquals(productAdUpdated.status(), "OFF");
            Assert.assertEquals(extractionNamespace.getLastUpdatedTime().longValue(), 1480733203506L);
        } finally {
            if (db != null) {
                db.close();
            }
            if (tempFile.exists()) {
                FileUtils.forceDelete(tempFile);
            }
        }
    }

    @Test
    public void testNoopCacheActionRunner() throws Exception {
        Options options = null;
        RocksDB db = null;
        File tempFile = null;
        try {

            tempFile = new File(Files.createTempDir(), "rocksdblookup");
            options = new Options().setCreateIfMissing(true);
            db = RocksDB.open(options, tempFile.getAbsolutePath());

            Map<String, FlatBufferValue> map = new HashMap();
            map.put("id",  FlatBufferValue.of("32309719080"));
            map.put("title",  FlatBufferValue.of("some title"));
            map.put("status",  FlatBufferValue.of("ON"));
            map.put("description",  FlatBufferValue.of("test desc"));
            map.put("last_updated", FlatBufferValue.of("1480733203505"));

            FlatBufferBuilder flatBufferBuilder = productAdWrapper.createFlatBuffer(map);

            db.put("32309719080".getBytes(), productAdWrapper.toByteArr(flatBufferBuilder.dataBuffer()));

            when(rocksDBManager.getDB(anyString())).thenReturn(db);

            FlatBufferBuilder flatBufferBuilderFromKafka = productAdWrapper.createFlatBuffer(map);

            RocksDBExtractionNamespace extractionNamespace = new RocksDBExtractionNamespace(
                    "product_ad_fb_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", null
                    , "com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.NoopCacheActionRunner");

            noopObj.getCachePopulator("product_ad_fb_lookup", extractionNamespace, "32309719080", new HashMap<>());
            noopObj.updateCache(extractionNamespace, new HashMap<>(), "32309719080", productAdWrapper.toByteArr(flatBufferBuilderFromKafka.dataBuffer()));
            byte[] cacheVal = noopObj.getCacheValue(extractionNamespace, new HashMap<>(), "32309719080", "", Optional.empty());
            Assert.assertNull(cacheVal);
        } finally {
            if (db != null) {
                db.close();
            }
            if(tempFile.exists()) {
                FileUtils.forceDelete(tempFile);
            }
        }
    }

    @Test
    public void testUpdateCacheWithLesserLastUpdated() throws Exception{

        Options options = null;
        RocksDB db = null;
        File tempFile = null;
        try {

            tempFile = new File(Files.createTempDir(), "rocksdblookup");
            options = new Options().setCreateIfMissing(true);
            db = RocksDB.open(options, tempFile.getAbsolutePath());

            Map<String, FlatBufferValue> map = new HashMap();
            map.put("id",  FlatBufferValue.of("32309719080"));
            map.put("title",  FlatBufferValue.of("some title"));
            map.put("status",  FlatBufferValue.of("ON"));
            map.put("description",  FlatBufferValue.of("test desc"));
            map.put("last_updated", FlatBufferValue.of("1480733203505"));

            FlatBufferBuilder flatBufferBuilder = productAdWrapper.createFlatBuffer(map);

            db.put("32309719080".getBytes(), productAdWrapper.toByteArr(flatBufferBuilder.dataBuffer()));

            when(rocksDBManager.getDB(anyString())).thenReturn(db);

            RocksDBExtractionNamespace extractionNamespace = new RocksDBExtractionNamespace(
                    "product_ad_fb_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", null
                    , "com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.CacheActionRunnerFlatBuffer");

            map.put("last_updated", FlatBufferValue.of("1480733203504"));
            map.put("status",  FlatBufferValue.of("OFF"));
            FlatBufferBuilder flatBufferBuilderFromKafka = productAdWrapper.createFlatBuffer(map);

            obj.updateCache(extractionNamespace, new HashMap<>(), "32309719080", productAdWrapper.toByteArr(flatBufferBuilderFromKafka.dataBuffer()));


            Table productAdTable = productAdWrapper.getFlatBuffer(db.get("32309719080".getBytes()));
            ProductAd productAdUpdated  = (ProductAd) productAdTable;

            Assert.assertEquals(productAdUpdated.id(), "32309719080");
            Assert.assertEquals(productAdUpdated.description(), "test desc");
            Assert.assertEquals(productAdUpdated.status(), "ON"); // NOT Updated due to lesser last updated time, old record is discarded
            Assert.assertEquals(extractionNamespace.getLastUpdatedTime().longValue(), -1);
            Assert.assertEquals(productAdUpdated.lastUpdated(), "1480733203505");
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
    public void testGetCacheValue() throws Exception{

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

            RocksDBExtractionNamespace extractionNamespace = new RocksDBExtractionNamespace(
                    "product_ad_fb_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", null
                    , "com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.CacheActionRunnerFlatBuffer");

            byte[] value = obj.getCacheValue(extractionNamespace, new HashMap<>(), "32309719080", "title", Optional.empty());

            Assert.assertEquals(new String(value, UTF_8), "some title");

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
    public void testGetCacheValueWhenNull() throws Exception {

        Options options = null;
        RocksDB db = null;
        File tempFile = null;
        try {

            tempFile = new File(Files.createTempDir(), "rocksdblookup");
            options = new Options().setCreateIfMissing(true);
            db = RocksDB.open(options, tempFile.getAbsolutePath());

            when(rocksDBManager.getDB(anyString())).thenReturn(db);

            RocksDBExtractionNamespace extractionNamespace = new RocksDBExtractionNamespace(
                    "product_ad_fb_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", null
                    , "com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.CacheActionRunnerFlatBuffer");

            byte[] value = obj.getCacheValue(extractionNamespace, new HashMap<>(), "32309719080", "title", Optional.empty());

            Assert.assertEquals(value, new byte[0]);

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
