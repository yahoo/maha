// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.joda.time.Period;
import org.mockito.Mock;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.*;

public class JDBCLookupExtractorTest {

    private static final String CONTROL_A_SEPARATOR = "\u0001";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    LookupService lookupService;

    @Test
    public void testBuildWhenDimensionValueIsEmpty() {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id", "name", "currency", "status")),
                        "id", "", new Period(), true, "advertiser_lookup");

        Map<String, String> map = new HashMap<>();
        JDBCLookupExtractor JDBCLookupExtractor = new JDBCLookupExtractor(extractionNamespace, map, lookupService);
        String lookupValue = JDBCLookupExtractor.apply("");
        Assert.assertNull(lookupValue);
    }

    @Test
    public void testBuildWhenDimensionValueIsNull() {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id", "name", "currency", "status")),
                        "id", "", new Period(), true, "advertiser_lookup");

        Map<String, String> map = new HashMap<>();
        JDBCLookupExtractor JDBCLookupExtractor = new JDBCLookupExtractor(extractionNamespace, map, lookupService);
        String lookupValue = JDBCLookupExtractor.apply(null);
        Assert.assertNull(lookupValue);
    }

    @Test
    public void testBuildWhenCacheValueIsNull() throws Exception {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(
                        metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id", "name", "currency", "status")),
                        "id", "", new Period(), true, "advertiser_lookup");
        Map<String, String> map = new HashMap<>();
        JDBCLookupExtractor JDBCLookupExtractor = new JDBCLookupExtractor(extractionNamespace, map, lookupService);
        MahaLookupQueryElement mahaLookupQueryElement1 = new MahaLookupQueryElement();
        mahaLookupQueryElement1.setDimension("12345");
        mahaLookupQueryElement1.setValueColumn("name");
        String lookupValue = JDBCLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement1));
        Assert.assertNull(lookupValue);
    }

    @Test
    public void testBuildWhenCacheValueIsNotNull() throws Exception {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(
                        metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id", "name", "currency", "status")),
                        "id", "", new Period(), true, "advertiser_lookup");
        Map<String, List<String>> map = new HashMap<>();
        map.put("12345", Arrays.asList("12345", "my name", "USD", "ON"));
        JDBCLookupExtractor JDBCLookupExtractor = new JDBCLookupExtractor(extractionNamespace, map, lookupService);
        MahaLookupQueryElement mahaLookupQueryElement1 = new MahaLookupQueryElement();
        mahaLookupQueryElement1.setDimension("12345");
        mahaLookupQueryElement1.setValueColumn("status");
        String lookupValue = JDBCLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement1));
        Assert.assertEquals(lookupValue, "ON");

        MahaLookupQueryElement mahaLookupQueryElement2 = new MahaLookupQueryElement();
        mahaLookupQueryElement2.setDimension("12345");
        mahaLookupQueryElement2.setValueColumn("name");
        lookupValue = JDBCLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement2));
        Assert.assertEquals(lookupValue, "my name");

        MahaLookupQueryElement mahaLookupQueryElement3 = new MahaLookupQueryElement();
        mahaLookupQueryElement3.setDimension("12345");
        mahaLookupQueryElement3.setValueColumn("currency");
        lookupValue = JDBCLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement3));
        Assert.assertEquals(lookupValue, "USD");
    }

    @Test
    public void testBuildWhenInvalidValueColumn() throws Exception {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(
                        metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id", "name", "currency", "status")),
                        "id", "", new Period(), true, "advertiser_lookup");
        Map<String, List<String>> map = new HashMap<>();
        map.put("12345", Arrays.asList("12345", "my name", "USD", "ON"));
        JDBCLookupExtractor JDBCLookupExtractor = new JDBCLookupExtractor(extractionNamespace, map, lookupService);
        MahaLookupQueryElement mahaLookupQueryElement1 = new MahaLookupQueryElement();
        mahaLookupQueryElement1.setDimension("12345");
        mahaLookupQueryElement1.setValueColumn("booking_country");
        String lookupValue = JDBCLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement1));
        Assert.assertNull(lookupValue);
    }

    @Test
    public void testDimensionOverrideMap() throws Exception {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(
                        metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id", "name", "currency", "status")),
                        "id", "", new Period(), true, "advertiser_lookup");
        Map<String, List<String>> map = new HashMap<>();
        map.put("12345", Arrays.asList("12345", "my name", "USD", "ON"));
        JDBCLookupExtractor JDBCLookupExtractor = new JDBCLookupExtractor(extractionNamespace, map, lookupService);
        Map<String, String> dimensionOverrideMap = new HashMap<>();
        dimensionOverrideMap.put("12345", "something-12345");
        dimensionOverrideMap.put("6789", "something-6789");
        dimensionOverrideMap.put("", "Unknown");

        MahaLookupQueryElement mahaLookupQueryElement1 = new MahaLookupQueryElement();
        mahaLookupQueryElement1.setDimension("12345");
        mahaLookupQueryElement1.setValueColumn("name");
        mahaLookupQueryElement1.setDimensionOverrideMap(dimensionOverrideMap);
        String lookupValue = JDBCLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement1));
        Assert.assertEquals(lookupValue, "something-12345");

        MahaLookupQueryElement mahaLookupQueryElement2 = new MahaLookupQueryElement();
        mahaLookupQueryElement2.setDimension("");
        mahaLookupQueryElement2.setValueColumn("name");
        mahaLookupQueryElement2.setDimensionOverrideMap(dimensionOverrideMap);
        lookupValue = JDBCLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement2));
        Assert.assertEquals(lookupValue, "Unknown");
    }

    @Test
    public void testDecodeConfig() throws Exception {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(
                        metadataStorageConnectorConfig, "advertiser", new ArrayList<>(Arrays.asList("id", "name", "currency", "status")),
                        "id", "", new Period(), true, "advertiser_lookup");
        Map<String, List<String>> map = new HashMap<>();
        map.put("12345", Arrays.asList("12345", "my name", "USD", "ON"));
        JDBCLookupExtractor JDBCLookupExtractor = new JDBCLookupExtractor(extractionNamespace, map, lookupService);

        Map<String, String> dimensionOverrideMap = new HashMap<>();
        dimensionOverrideMap.put("123", "something-123");
        dimensionOverrideMap.put("6789", "something-6789");
        dimensionOverrideMap.put("", "Unknown");

        MahaLookupQueryElement mahaLookupQueryElement1 = new MahaLookupQueryElement();
        mahaLookupQueryElement1.setDimension("12345");
        mahaLookupQueryElement1.setValueColumn("name");
        mahaLookupQueryElement1.setDimensionOverrideMap(dimensionOverrideMap);
        DecodeConfig decodeConfig1 = new DecodeConfig();
        decodeConfig1.setColumnToCheck("name");
        decodeConfig1.setValueToCheck("my name");
        decodeConfig1.setColumnIfValueMatched("currency");
        decodeConfig1.setColumnIfValueNotMatched("status");
        mahaLookupQueryElement1.setDecodeConfig(decodeConfig1);
        String lookupValue = JDBCLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement1));
        Assert.assertEquals(lookupValue, "USD");


        MahaLookupQueryElement mahaLookupQueryElement2 = new MahaLookupQueryElement();
        mahaLookupQueryElement2.setDimension("12345");
        mahaLookupQueryElement2.setValueColumn("name");
        mahaLookupQueryElement2.setDimensionOverrideMap(dimensionOverrideMap);
        DecodeConfig decodeConfig2 = new DecodeConfig();
        decodeConfig2.setColumnToCheck("name");
        decodeConfig2.setValueToCheck("my unknown name");
        decodeConfig2.setColumnIfValueMatched("currency");
        decodeConfig2.setColumnIfValueNotMatched("status");
        mahaLookupQueryElement2.setDecodeConfig(decodeConfig2);
        lookupValue = JDBCLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement2));
        Assert.assertEquals(lookupValue, "ON");

    }

}