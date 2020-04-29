// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.emitter.service.ServiceEmitter;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import com.mongodb.client.result.UpdateResult;
import com.yahoo.maha.maha_druid_lookups.TestMongoServer;
import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.MongoExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.MongoStorageConnectorConfig;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.FlatMultiValueDocumentProcessor;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.joda.time.Period;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public class MongoExtractionNamespaceCacheFactoryTest extends TestMongoServer {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    MongoExtractionNamespaceCacheFactory obj = new MongoExtractionNamespaceCacheFactory();

    @Mock
    ServiceEmitter serviceEmitter;

    @Mock
    LookupService lookupService;

    MongoStorageConnectorConfig mongoStorageConnectorConfig;
    MongoStorageConnectorConfig badMongoStorageConnectorConfig;

    private static final String DEFAULT_COLLECTION = "advertiser";

    private static String getMongoJSONConfig(String host, int port) {
        return String.format("{\n" +
                "\t\"hosts\": \"%s:%s\",\n" +
                "\t\"dbName\": \"mydb\",\n" +
                "\t\"clientOptions\": {\n" +
                "\t\t\"connectionsPerHost\": \"3\",\n" +
                "\t\t\"socketTimeout\": \"10000\",\n" +
                "\t\t\"serverSelectionTimeout\": \"2000\",\n" +
                "\t\t\"heartbeatConnectTimeout\": \"2000\",\n" +
                "\t\t\"connectTimeout\": \"2000\"\n" +
                "\t}\n" +
                "}", host, port);
    }

    @BeforeClass
    public void setup() throws Exception {
        InetSocketAddress serverAddress = setupMongoServer(objectMapper);
        String jsonConfig = getMongoJSONConfig(serverAddress.getHostString(), serverAddress.getPort());
        String badJsonConfig = getMongoJSONConfig(serverAddress.getHostString(), serverAddress.getPort() + 1);
        mongoStorageConnectorConfig = objectMapper.readValue(jsonConfig, MongoStorageConnectorConfig.class);
        badMongoStorageConnectorConfig = objectMapper.readValue(badJsonConfig, MongoStorageConnectorConfig.class);
        createTestData("mongo_advertiser.json", DEFAULT_COLLECTION, objectMapper, mongoStorageConnectorConfig);
    }

    private void updateTestData(String jsonResource) throws Exception {
        super.updateTestData(jsonResource, DEFAULT_COLLECTION, objectMapper, mongoStorageConnectorConfig);
    }

    @AfterClass
    public void cleanup() {
        cleanupMongoServer();
    }

    @BeforeTest
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        obj.emitter = serviceEmitter;
        obj.lookupService = lookupService;
    }

    @Test
    public void testGetCacheValueWhenKeyPresent() throws Exception {
        MongoExtractionNamespace extractionNamespace =
                new MongoExtractionNamespace(mongoStorageConnectorConfig, "advertiser"
                        , "updated_at", true, new Period(), true, "advertiser_lookup"
                        , new FlatMultiValueDocumentProcessor(new ArrayList<>(Arrays.asList("name", "currency", "status")), "_id"), null);
        Map<String, List<String>> map = new HashMap<>();
        map.put("12345", Arrays.asList("my name", "USD", "ON"));
        Assert.assertEquals(obj.getCacheValue(extractionNamespace, map, "12345", "name", Optional.empty()), "my name".getBytes());
    }

    @Test
    public void testGetCacheValueWhenKeyNotPresent() throws Exception {
        MongoExtractionNamespace extractionNamespace =
                new MongoExtractionNamespace(mongoStorageConnectorConfig, "advertiser"
                        , "updated_at", true, new Period(), true, "advertiser_lookup"
                        , new FlatMultiValueDocumentProcessor(new ArrayList<>(Arrays.asList("name", "currency", "status")), "_id"), null);
        Map<String, List<String>> map = new HashMap<>();
        map.put("12345", Arrays.asList("my name", "USD", "ON"));
        Assert.assertEquals(obj.getCacheValue(extractionNamespace, map, "6789", "name", Optional.empty()), "".getBytes());
    }

    @Test
    public void testGetCacheValueWhenKeyPresentButValueColumnNotPresent() throws Exception {
        MongoExtractionNamespace extractionNamespace =
                new MongoExtractionNamespace(mongoStorageConnectorConfig, "advertiser"
                        , "updated_at", true, new Period(), true, "advertiser_lookup"
                        , new FlatMultiValueDocumentProcessor(new ArrayList<>(Arrays.asList("name", "currency", "status")), "_id"), null);
        Map<String, List<String>> map = new HashMap<>();
        map.put("12345", Arrays.asList("my name", "USD", "ON"));
        Assert.assertEquals(obj.getCacheValue(extractionNamespace, map, "6789", "blah", Optional.empty()), "".getBytes());
    }

    @Test
    public void testGetCacheValueWithDecodeConfig() throws Exception {
        MongoExtractionNamespace extractionNamespace =
                new MongoExtractionNamespace(mongoStorageConnectorConfig, "advertiser"
                        , "updated_at", true, new Period(), true, "advertiser_lookup"
                        , new FlatMultiValueDocumentProcessor(new ArrayList<>(Arrays.asList("name", "currency", "status")), "_id"), null);
        Map<String, List<String>> map = new HashMap<>();
        map.put("12345", Arrays.asList("my name", "USD", "ON"));
        DecodeConfig decodeConfig1 = new DecodeConfig();
        decodeConfig1.setColumnToCheck("name");
        decodeConfig1.setValueToCheck("my name");
        decodeConfig1.setColumnIfValueMatched("currency");
        decodeConfig1.setColumnIfValueNotMatched("status");
        Assert.assertEquals(obj.getCacheValue(extractionNamespace, map, "12345", "name", Optional.of(decodeConfig1)), "USD".getBytes());

        DecodeConfig decodeConfig2 = new DecodeConfig();
        decodeConfig2.setColumnToCheck("name");
        decodeConfig2.setValueToCheck("my unknown name");
        decodeConfig2.setColumnIfValueMatched("currency");
        decodeConfig2.setColumnIfValueNotMatched("status");
        Assert.assertEquals(obj.getCacheValue(extractionNamespace, map, "12345", "name", Optional.of(decodeConfig2)), "ON".getBytes());
    }


    @Test(expectedExceptions = com.mongodb.MongoTimeoutException.class)
    public void testReplacementOfMongoStorageConnectorConfig() throws Exception {
        Map<String, List<String>> cache = new ConcurrentHashMap<>();
        MongoExtractionNamespace extractionNamespace =
                new MongoExtractionNamespace(badMongoStorageConnectorConfig, "advertiser"
                        , "updated_at", true, new Period(), true, "advertiser_lookup"
                        , new FlatMultiValueDocumentProcessor(new ArrayList<>(Arrays.asList("name", "currency", "status")), "_id"), null);
        Callable<String> command = obj.getCachePopulator(extractionNamespace.getLookupName(), extractionNamespace, null, cache);
        command.call();
    }

    @Test
    public void testGetCachePopulator() throws Exception {
        Map<String, List<String>> cache = new ConcurrentHashMap<>();
        MongoExtractionNamespace extractionNamespace =
                new MongoExtractionNamespace(mongoStorageConnectorConfig, "advertiser"
                        , "updated_at", true, new Period(), true, "advertiser_lookup"
                        , new FlatMultiValueDocumentProcessor(new ArrayList<>(Arrays.asList("name", "currency", "status")), "_id"), null);
        Callable<String> command = obj.getCachePopulator(extractionNamespace.getLookupName(), extractionNamespace, null, cache);
        String version = command.call();
        Assert.assertTrue(cache.containsKey("5ad10906fc7b6ecac8d41080"));
        Assert.assertTrue(cache.containsKey("5ad10906fc7b6ecac8d41081"));
        Assert.assertTrue(cache.containsKey("5ad10906fc7b6ecac8d41082"));
        Assert.assertEquals(version, "1543652000");

    }

    @Test
    public void testGetCachePopulatorWithUpdatesAndAdditions() throws Exception {
        Map<String, List<String>> cache = new ConcurrentHashMap<>();
        MongoExtractionNamespace extractionNamespace =
                new MongoExtractionNamespace(mongoStorageConnectorConfig, "advertiser"
                        , "updated_at", true, new Period(), true, "advertiser_lookup"
                        , new FlatMultiValueDocumentProcessor(new ArrayList<>(Arrays.asList("name", "currency", "status")), "_id"), null);
        Callable<String> command = obj.getCachePopulator(extractionNamespace.getLookupName(), extractionNamespace, null, cache);
        String version = command.call();
        Assert.assertTrue(cache.containsKey("5ad10906fc7b6ecac8d41080"));
        Assert.assertTrue(cache.containsKey("5ad10906fc7b6ecac8d41081"));
        Assert.assertTrue(cache.containsKey("5ad10906fc7b6ecac8d41082"));
        Assert.assertEquals(version, "1543652000");
        Assert.assertEquals(cache.get("5ad10906fc7b6ecac8d41080").get(0), "123");
        Assert.assertEquals(cache.get("5ad10906fc7b6ecac8d41081").get(2), "ON");
        updateTestData("mongo_advertiser_update.json");
        version = command.call();
        Assert.assertEquals(version, "1543652001");
        Assert.assertEquals(cache.get("5ad10906fc7b6ecac8d41080").get(0), "advertiser3");
        Assert.assertEquals(cache.get("5ad10906fc7b6ecac8d41081").get(2), "OFF");
        updateTestData("mongo_advertiser_addition.json");
        Date currDate = new Date();
        version = command.call();
        Assert.assertTrue(Integer.parseInt(version) >= currDate.getTime() / 1000, String.format("%s not > %d", version, currDate.getTime() / 1000));
        Assert.assertTrue(cache.containsKey("5ad10906fc7b6ecac8d41083"));
    }

    @Test(expectedExceptions = com.mongodb.MongoTimeoutException.class)
    public void testReplacementOfMongoStorageConnectorConfigAfterMongoDBClosed() throws Exception {
        cleanupMongoServer();
        Map<String, List<String>> cache = new ConcurrentHashMap<>();
        MongoExtractionNamespace extractionNamespace =
                new MongoExtractionNamespace(badMongoStorageConnectorConfig, "advertiser"
                        , "updated_at", true, new Period(), true, "advertiser_lookup"
                        , new FlatMultiValueDocumentProcessor(new ArrayList<>(Arrays.asList("name", "currency", "status")), "_id"), null);
        Callable<String> command = obj.getCachePopulator(extractionNamespace.getLookupName(), extractionNamespace, null, cache);
        command.call();
    }
}
