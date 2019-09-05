// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.google.common.io.Files;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.metamx.emitter.service.ServiceEmitter;
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

import java.io.File;
import java.util.HashMap;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

public class RocksDBExtractionNamespaceCacheFactoryTest {

    @InjectMocks
    RocksDBExtractionNamespaceCacheFactory obj =
            new RocksDBExtractionNamespaceCacheFactory();

    @Mock
    RocksDBManager rocksDBManager;

    @Mock
    ServiceEmitter serviceEmitter;

    @BeforeTest
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        obj.rocksDBManager = rocksDBManager;
        obj.protobufSchemaFactory = new TestProtobufSchemaFactory();
        obj.emitter = serviceEmitter;
    }

    @Test
    public void testUpdateCacheWithGreaterLastUpdated() throws Exception{

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
                    "ad_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", null
            , "com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.CacheActionRunner");

            Message msgFromKafka = AdProtos.Ad.newBuilder()
                    .setId("32309719080")
                    .setTitle("some updated title")
                    .setStatus("OFF")
                    .setLastUpdated("1480733203505")
                    .build();

            obj.updateCache(extractionNamespace, new HashMap<>(), "32309719080", msgFromKafka.toByteArray());

            Parser<Message> parser = new TestProtobufSchemaFactory().getProtobufParser(extractionNamespace.getNamespace());
            Message updatedMessage = parser.parseFrom(db.get("32309719080".getBytes()));

            Descriptors.Descriptor descriptor = new TestProtobufSchemaFactory().getProtobufDescriptor(extractionNamespace.getNamespace());
            Descriptors.FieldDescriptor field = descriptor.findFieldByName("title");

            Assert.assertEquals(updatedMessage.getField(field).toString(), "some updated title");

            field = descriptor.findFieldByName("status");
            Assert.assertEquals(updatedMessage.getField(field).toString(), "OFF");

            field = descriptor.findFieldByName("last_updated");
            Assert.assertEquals(updatedMessage.getField(field).toString(), "1480733203505");
            Assert.assertEquals(extractionNamespace.getLastUpdatedTime().longValue(), 1480733203505L);

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
    public void testNoopCacheActionRunner() throws Exception {
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
                    "ad_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", null
                    , "com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.CacheActionRunner");

            Message msgFromKafka = AdProtos.Ad.newBuilder()
                    .setId("32309719080")
                    .setTitle("some updated title")
                    .setStatus("OFF")
                    .setLastUpdated("1480733203505")
                    .build();
            extractionNamespace = new RocksDBExtractionNamespace(
                    "ad_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", null
                    , "com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.NoopCacheActionRunner");

            obj.getCachePopulator("ad_lookup", extractionNamespace, "32309719080", new HashMap<>());
            obj.updateCache(extractionNamespace, new HashMap<>(), "32309719080", msgFromKafka.toByteArray());
            byte[] cacheVal = obj.getCacheValue(extractionNamespace, new HashMap<>(), "32309719080", "", Optional.empty());
            Assert.assertEquals(null, cacheVal);
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
    public void testUpdateCacheWithLesserLastUpdated() throws Exception{

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
                    "ad_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", null
            , "com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.CacheActionRunner");

            Message msgFromKafka = AdProtos.Ad.newBuilder()
                    .setId("32309719080")
                    .setTitle("some updated title")
                    .setStatus("OFF")
                    .setLastUpdated("1460733203505")
                    .build();

            obj.updateCache(extractionNamespace, new HashMap<>(), "32309719080", msgFromKafka.toByteArray());

            Parser<Message> parser = new TestProtobufSchemaFactory().getProtobufParser(extractionNamespace.getNamespace());
            Message updatedMessage = parser.parseFrom(db.get("32309719080".getBytes()));

            Descriptors.Descriptor descriptor = new TestProtobufSchemaFactory().getProtobufDescriptor(extractionNamespace.getNamespace());
            Descriptors.FieldDescriptor field = descriptor.findFieldByName("title");

            Assert.assertEquals(updatedMessage.getField(field).toString(), "some title");

            field = descriptor.findFieldByName("status");
            Assert.assertEquals(updatedMessage.getField(field).toString(), "ON");

            field = descriptor.findFieldByName("last_updated");
            Assert.assertEquals(updatedMessage.getField(field).toString(), "1470733203505");
            Assert.assertEquals(extractionNamespace.getLastUpdatedTime().longValue(), 1460733203505L);

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
                    "ad_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", null
            , null);

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
    public void testGetCacheValueWhenNull() throws Exception{

        Options options = null;
        RocksDB db = null;
        File tempFile = null;
        try {

            tempFile = new File(Files.createTempDir(), "rocksdblookup");
            options = new Options().setCreateIfMissing(true);
            db = RocksDB.open(options, tempFile.getAbsolutePath());

            when(rocksDBManager.getDB(anyString())).thenReturn(db);

            RocksDBExtractionNamespace extractionNamespace = new RocksDBExtractionNamespace(
                    "ad_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated", null
            , null);

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
