// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.RocksDBManager;
import io.druid.metadata.MetadataStorageConnectorConfig;
import org.joda.time.Period;
import org.mockito.Mock;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class JDBCLookupExtractorTest {

    private static final String CONTROL_A_SEPARATOR = "\u0001";

    @Mock
    RocksDBManager rocksDBManager;

    @Mock
    LookupService lookupService;

    @Test
    public void testBuildWhenDimensionValueIsEmpty() {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id","name","currency","status")),
                        "id", "", new Period(), true, "advertiser_lookup");

        Map<String, String> map = new HashMap<>();
        JDBCLookupExtractor JDBCLookupExtractor = new JDBCLookupExtractor(extractionNamespace, map, lookupService);
        String lookupValue = JDBCLookupExtractor.apply("");
        Assert.assertNull(lookupValue);
    }

    @Test
    public void testBuildWhenCacheValueIsNull() {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(
                        metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id","name","currency","status")),
                        "id", "", new Period(), true, "advertiser_lookup");
        Map<String, String> map = new HashMap<>();
        JDBCLookupExtractor JDBCLookupExtractor = new JDBCLookupExtractor(extractionNamespace, map, lookupService);
        String lookupValue = JDBCLookupExtractor.apply("12345\u0001name");
        Assert.assertNull(lookupValue);
    }

    @Test
    public void testBuildWhenCacheValueIsNotNull() {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(
                        metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id","name","currency","status")),
                        "id", "", new Period(), true, "advertiser_lookup");
        Map<String, String> map = new HashMap<>();
        map.put("12345", "12345" + CONTROL_A_SEPARATOR + "my name" + CONTROL_A_SEPARATOR + "USD" + CONTROL_A_SEPARATOR + "ON");
        JDBCLookupExtractor JDBCLookupExtractor = new JDBCLookupExtractor(extractionNamespace, map, lookupService);
        String lookupValue = JDBCLookupExtractor.apply("12345\u0001status");
        Assert.assertEquals(lookupValue, "ON");

        lookupValue = JDBCLookupExtractor.apply("12345\u0001name");
        Assert.assertEquals(lookupValue, "my name");

        lookupValue = JDBCLookupExtractor.apply("12345\u0001currency");
        Assert.assertEquals(lookupValue, "USD");
    }

    @Test
    public void testBuildWhenInvalidValueColumn() {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(
                        metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id","name","currency","status")),
                        "id", "", new Period(), true, "advertiser_lookup");
        Map<String, String> map = new HashMap<>();
        map.put("12345", "12345" + CONTROL_A_SEPARATOR + "my name" + CONTROL_A_SEPARATOR + "USD" + CONTROL_A_SEPARATOR + "ON");
        JDBCLookupExtractor JDBCLookupExtractor = new JDBCLookupExtractor(extractionNamespace, map, lookupService);
        String lookupValue = JDBCLookupExtractor.apply("12345\u0001booking_country");
        Assert.assertNull(lookupValue);
    }

}
