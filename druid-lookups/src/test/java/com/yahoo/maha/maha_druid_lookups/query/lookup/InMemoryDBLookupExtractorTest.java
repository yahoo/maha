// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.google.common.io.Files;
import com.google.protobuf.Message;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.InMemoryDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.RocksDBManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.AdProtos;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.TestProtobufSchemaFactory;
import org.apache.commons.io.FileUtils;
import org.joda.time.Period;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InMemoryDBLookupExtractorTest {

    @Test
    public void testBuildWhenDimensionValueIsEmpty() {
        LookupService lookupService = mock(LookupService.class);
        RocksDBManager rocksDBManager = mock(RocksDBManager.class);
        InMemoryDBExtractionNamespace extractionNamespace = new InMemoryDBExtractionNamespace(
                "ad_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated"
        );
        Map<String, String> map = new HashMap<>();
        InMemoryDBLookupExtractor InMemoryDBLookupExtractor = new InMemoryDBLookupExtractor(extractionNamespace, map, lookupService, rocksDBManager, new TestProtobufSchemaFactory());
        String lookupValue = InMemoryDBLookupExtractor.apply("");
        Assert.assertNull(lookupValue);
    }

    @Test
    public void testBuildWhenCacheValueIsNull() {

        LookupService lookupService = mock(LookupService.class);
        RocksDB db = mock(RocksDB.class);
        RocksDBManager rocksDBManager = mock(RocksDBManager.class);
        when(rocksDBManager.getDB(anyString())).thenReturn(db);

        InMemoryDBExtractionNamespace extractionNamespace = new InMemoryDBExtractionNamespace(
                "ad_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated"
        );
        Map<String, String> map = new HashMap<>();
        InMemoryDBLookupExtractor InMemoryDBLookupExtractor = new InMemoryDBLookupExtractor(extractionNamespace, map, lookupService, rocksDBManager, new TestProtobufSchemaFactory());
        String lookupValue = InMemoryDBLookupExtractor.apply("abc\u0001status");
        Assert.assertNull(lookupValue);
    }

    @Test
    public void testBuildWhenCacheValueIsNotNull() throws Exception{

        LookupService lookupService = mock(LookupService.class);
        RocksDBManager rocksDBManager = mock(RocksDBManager.class);
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
                    "ad_lookup", "blah", "blah", new Period(), "", true, false, "ad_lookup", "last_updated"
            );
            Map<String, String> map = new HashMap<>();
            InMemoryDBLookupExtractor InMemoryDBLookupExtractor = new InMemoryDBLookupExtractor(extractionNamespace, map, lookupService, rocksDBManager, new TestProtobufSchemaFactory());
            String lookupValue = InMemoryDBLookupExtractor.apply("32309719080\u0001status");
            Assert.assertEquals(lookupValue, "ON");

            lookupValue = InMemoryDBLookupExtractor.apply("32309719080\u0001title");
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

}
