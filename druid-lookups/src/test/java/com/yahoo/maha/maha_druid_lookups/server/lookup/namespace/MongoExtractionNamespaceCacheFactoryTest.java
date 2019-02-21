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
import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.MongoExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.MongoStorageConnectorConfig;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.FlatMultiValueDocumentProcessor;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import io.druid.metadata.MetadataStorageConnectorConfig;
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

public class MongoExtractionNamespaceCacheFactoryTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    MongoExtractionNamespaceCacheFactory obj = new MongoExtractionNamespaceCacheFactory();

    @Mock
    ServiceEmitter serviceEmitter;

    @Mock
    LookupService lookupService;

    MongoServer mongoServer;

    MongoStorageConnectorConfig mongoStorageConnectorConfig;

    MongoClient mongoClient;

    private static final String DEFAULT_COLLECTION = "advertiser";

    @BeforeClass
    public void setup() throws Exception {
        mongoServer = new MongoServer(new MemoryBackend());
        InetSocketAddress serverAddress = mongoServer.bind();
        String jsonConfig = String.format("{\n" +
                "\t\"hosts\": \"%s:%s\",\n" +
                "\t\"dbName\": \"mydb\",\n" +
                "\t\"clientOptions\": {\n" +
                "\t\t\"connectionsPerHost\": \"3\",\n" +
                "\t\t\"socketTimeout\": \"10000\",\n" +
                "\t\t\"connectTimeout\": \"2000\"\n" +
                "\t}\n" +
                "}", serverAddress.getHostString(), serverAddress.getPort());
        mongoStorageConnectorConfig = objectMapper.readValue(jsonConfig, MongoStorageConnectorConfig.class);
        mongoClient = mongoStorageConnectorConfig.getMongoClient();
        createTestData();
    }

    private void createTestData() throws Exception {
        MongoClient localMongoClient = mongoStorageConnectorConfig.getMongoClient();
        MongoDatabase database = localMongoClient.getDatabase(mongoStorageConnectorConfig.getDbName());
        database.createCollection(DEFAULT_COLLECTION);
        MongoCollection<Document> collection = database.getCollection(DEFAULT_COLLECTION);
        JsonNode node = objectMapper.readValue(
                ClassLoader.getSystemClassLoader().getResourceAsStream("mongo_advertiser.json"), JsonNode.class);
        for (int i = 0; i < node.size(); i++) {
            JsonNode elem = node.get(i);
            String json = objectMapper.writeValueAsString(elem);
            Document d = Document.parse(json);
            collection.insertOne(d);
        }
        for (Document d : collection.find()) {
            System.out.println(d.toJson());
        }
        localMongoClient.close();
    }

    private void updateTestData(String jsonResource) throws Exception {
        MongoClient localMongoClient = mongoStorageConnectorConfig.getMongoClient();
        MongoDatabase database = localMongoClient.getDatabase(mongoStorageConnectorConfig.getDbName());
        MongoCollection<Document> collection = database.getCollection(DEFAULT_COLLECTION);
        JsonNode node = objectMapper.readValue(
                ClassLoader.getSystemClassLoader().getResourceAsStream(jsonResource), JsonNode.class);
        for (int i = 0; i < node.size(); i++) {
            JsonNode elem = node.get(i);
            String id = elem.get("_id").get("$oid").asText();
            String json = objectMapper.writeValueAsString(elem);
            Document d = Document.parse(json);
            UpdateResult result = collection.replaceOne(new BasicDBObject("_id", new ObjectId(id)), d);
            if (result.getMatchedCount() <= 0) {
                collection.insertOne(d);
            }
        }
        for (Document d : collection.find()) {
            System.out.println(d.toJson());
        }
        localMongoClient.close();
    }

    @AfterClass
    public void cleanup() throws Exception {
        mongoClient.close();
        mongoServer.shutdownNow();
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
}
