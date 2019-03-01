// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.MongoExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.MongoStorageConnectorConfig;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.FlatMultiValueDocumentProcessor;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import org.joda.time.Period;
import org.mockito.Mock;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.*;

public class MongoLookupExtractorTest {

    private static final String CONTROL_A_SEPARATOR = "\u0001";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    LookupService lookupService;

    MongoServer mongoServer;

    MongoStorageConnectorConfig mongoStorageConnectorConfig;

    MongoClient mongoClient;

    @BeforeClass
    public void setup() throws Exception {
        mongoServer = new MongoServer(new MemoryBackend());
        InetSocketAddress serverAddress = mongoServer.bind();
        String jsonConfig = String.format("{\n" +
                "\t\"hosts\": \"%s:%s\",\n" +
                "\t\"dbName\": \"mydb\",\n" +
                "\t\"user\": \"test-user\",\n" +
                "\t\"password\": {\n" +
                "\t\t\"type\": \"default\",\n" +
                "\t\t\"password\": \"mypassword\"\n" +
                "\t},\n" +
                "\t\"clientOptions\": {\n" +
                "\t\t\"connectionsPerHost\": \"3\",\n" +
                "\t\t\"socketTimeout\": \"10000\",\n" +
                "\t\t\"connectTimeout\": \"2000\"\n" +
                "\t}\n" +
                "}", serverAddress.getHostString(), serverAddress.getPort());
        mongoStorageConnectorConfig = objectMapper.readValue(jsonConfig, MongoStorageConnectorConfig.class);
        mongoClient = mongoStorageConnectorConfig.getMongoClient();
    }

    @AfterClass
    public void cleanup() throws Exception {
        mongoClient.close();
        mongoServer.shutdownNow();
    }

    @Test
    public void testBuildWhenDimensionValueIsEmpty() {

        MongoExtractionNamespace extractionNamespace =
                new MongoExtractionNamespace(mongoStorageConnectorConfig, "advertiser"
                        , "updated_at", true, new Period(), true, "advertiser_lookup"
                        , new FlatMultiValueDocumentProcessor(new ArrayList<>(Arrays.asList("name", "currency", "status")), "_id"), null);

        Map<String, String> map = new HashMap<>();
        MongoLookupExtractor MongoLookupExtractor = new MongoLookupExtractor(extractionNamespace, map, lookupService);
        String lookupValue = MongoLookupExtractor.apply("");
        Assert.assertNull(lookupValue);
    }

    @Test
    public void testBuildWhenDimensionValueIsNull() {

        MongoExtractionNamespace extractionNamespace =
                new MongoExtractionNamespace(mongoStorageConnectorConfig, "advertiser"
                        , "updated_at", true, new Period(), true, "advertiser_lookup"
                        , new FlatMultiValueDocumentProcessor(new ArrayList<>(Arrays.asList("name", "currency", "status")), "_id"), null);

        Map<String, String> map = new HashMap<>();
        MongoLookupExtractor MongoLookupExtractor = new MongoLookupExtractor(extractionNamespace, map, lookupService);
        String lookupValue = MongoLookupExtractor.apply(null);
        Assert.assertNull(lookupValue);
    }

    @Test
    public void testBuildWhenCacheValueIsNull() throws Exception {

        MongoExtractionNamespace extractionNamespace =
                new MongoExtractionNamespace(mongoStorageConnectorConfig, "advertiser"
                        , "updated_at", true, new Period(), true, "advertiser_lookup"
                        , new FlatMultiValueDocumentProcessor(new ArrayList<>(Arrays.asList("name", "currency", "status")), "_id"), null);
        Map<String, String> map = new HashMap<>();
        MongoLookupExtractor MongoLookupExtractor = new MongoLookupExtractor(extractionNamespace, map, lookupService);
        MahaLookupQueryElement mahaLookupQueryElement1 = new MahaLookupQueryElement();
        mahaLookupQueryElement1.setDimension("12345");
        mahaLookupQueryElement1.setValueColumn("name");
        String lookupValue = MongoLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement1));
        Assert.assertNull(lookupValue);
    }

    @Test
    public void testBuildWhenCacheValueIsNotNull() throws Exception {

        MongoExtractionNamespace extractionNamespace =
                new MongoExtractionNamespace(mongoStorageConnectorConfig, "advertiser"
                        , "updated_at", true, new Period(), true, "advertiser_lookup"
                        , new FlatMultiValueDocumentProcessor(new ArrayList<>(Arrays.asList("name", "currency", "status")), "_id"), null);
        Map<String, List<String>> map = new HashMap<>();
        map.put("12345", Arrays.asList("my name", "USD", "ON"));
        MongoLookupExtractor MongoLookupExtractor = new MongoLookupExtractor(extractionNamespace, map, lookupService);
        MahaLookupQueryElement mahaLookupQueryElement1 = new MahaLookupQueryElement();
        mahaLookupQueryElement1.setDimension("12345");
        mahaLookupQueryElement1.setValueColumn("status");
        String lookupValue = MongoLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement1));
        Assert.assertEquals(lookupValue, "ON");

        MahaLookupQueryElement mahaLookupQueryElement2 = new MahaLookupQueryElement();
        mahaLookupQueryElement2.setDimension("12345");
        mahaLookupQueryElement2.setValueColumn("name");
        lookupValue = MongoLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement2));
        Assert.assertEquals(lookupValue, "my name");

        MahaLookupQueryElement mahaLookupQueryElement3 = new MahaLookupQueryElement();
        mahaLookupQueryElement3.setDimension("12345");
        mahaLookupQueryElement3.setValueColumn("currency");
        lookupValue = MongoLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement3));
        Assert.assertEquals(lookupValue, "USD");
    }

    @Test
    public void testBuildWhenInvalidValueColumn() throws Exception {

        MongoExtractionNamespace extractionNamespace =
                new MongoExtractionNamespace(mongoStorageConnectorConfig, "advertiser"
                        , "updated_at", true, new Period(), true, "advertiser_lookup"
                        , new FlatMultiValueDocumentProcessor(new ArrayList<>(Arrays.asList("name", "currency", "status")), "_id"), null);
        Map<String, List<String>> map = new HashMap<>();
        map.put("12345", Arrays.asList("my name", "USD", "ON"));
        MongoLookupExtractor MongoLookupExtractor = new MongoLookupExtractor(extractionNamespace, map, lookupService);
        MahaLookupQueryElement mahaLookupQueryElement1 = new MahaLookupQueryElement();
        mahaLookupQueryElement1.setDimension("12345");
        mahaLookupQueryElement1.setValueColumn("booking_country");
        String lookupValue = MongoLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement1));
        Assert.assertNull(lookupValue);
    }

    @Test
    public void testDimensionOverrideMap() throws Exception {

        MongoExtractionNamespace extractionNamespace =
                new MongoExtractionNamespace(mongoStorageConnectorConfig, "advertiser"
                        , "updated_at", true, new Period(), true, "advertiser_lookup"
                        , new FlatMultiValueDocumentProcessor(new ArrayList<>(Arrays.asList("name", "currency", "status")), "_id"), null);
        Map<String, List<String>> map = new HashMap<>();
        map.put("12345", Arrays.asList("my name", "USD", "ON"));
        MongoLookupExtractor MongoLookupExtractor = new MongoLookupExtractor(extractionNamespace, map, lookupService);
        Map<String, String> dimensionOverrideMap = new HashMap<>();
        dimensionOverrideMap.put("12345", "something-12345");
        dimensionOverrideMap.put("6789", "something-6789");
        dimensionOverrideMap.put("", "Unknown");

        MahaLookupQueryElement mahaLookupQueryElement1 = new MahaLookupQueryElement();
        mahaLookupQueryElement1.setDimension("12345");
        mahaLookupQueryElement1.setValueColumn("name");
        mahaLookupQueryElement1.setDimensionOverrideMap(dimensionOverrideMap);
        String lookupValue = MongoLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement1));
        Assert.assertEquals(lookupValue, "something-12345");

        MahaLookupQueryElement mahaLookupQueryElement2 = new MahaLookupQueryElement();
        mahaLookupQueryElement2.setDimension("");
        mahaLookupQueryElement2.setValueColumn("name");
        mahaLookupQueryElement2.setDimensionOverrideMap(dimensionOverrideMap);
        lookupValue = MongoLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement2));
        Assert.assertEquals(lookupValue, "Unknown");
    }

    @Test
    public void testDecodeConfig() throws Exception {

        MongoExtractionNamespace extractionNamespace =
                new MongoExtractionNamespace(mongoStorageConnectorConfig, "advertiser"
                        , "updated_at", true, new Period(), true, "advertiser_lookup"
                        , new FlatMultiValueDocumentProcessor(new ArrayList<>(Arrays.asList("name", "currency", "status")), "_id"), null);
        Map<String, List<String>> map = new HashMap<>();
        map.put("12345", Arrays.asList("my name", "USD", "ON"));
        MongoLookupExtractor MongoLookupExtractor = new MongoLookupExtractor(extractionNamespace, map, lookupService);

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
        String lookupValue = MongoLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement1));
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
        lookupValue = MongoLookupExtractor.apply(objectMapper.writeValueAsString(mahaLookupQueryElement2));
        Assert.assertEquals(lookupValue, "ON");

    }

}