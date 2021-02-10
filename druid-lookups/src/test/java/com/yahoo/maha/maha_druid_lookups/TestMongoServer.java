package com.yahoo.maha.maha_druid_lookups;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.MongoStorageConnectorConfig;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.net.InetSocketAddress;

public class TestMongoServer {
    MongoServer mongoServer;

    protected InetSocketAddress setupMongoServer(ObjectMapper objectMapper) throws Exception {
        mongoServer = new MongoServer(new MemoryBackend());
        return mongoServer.bind();
    }

    protected void createTestData(String jsonResource
            , String collectionName
            , ObjectMapper objectMapper
            , MongoStorageConnectorConfig mongoStorageConnectorConfig) throws Exception {
        MongoClient localMongoClient = mongoStorageConnectorConfig.getMongoClient();
        MongoDatabase database = localMongoClient.getDatabase(mongoStorageConnectorConfig.getDbName());
        database.createCollection(collectionName);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        JsonNode node = objectMapper.readValue(
                ClassLoader.getSystemClassLoader().getResourceAsStream(jsonResource), JsonNode.class);
        for (int i = 0; i < node.size(); i++) {
            JsonNode elem = node.get(i);
            String json = objectMapper.writeValueAsString(elem);
            Document d = Document.parse(json);
            collection.insertOne(d);
        }
        for (Document d : collection.find()) {
            //System.out.println(d.toJson());
        }
        localMongoClient.close();
    }

    protected void updateTestData(String jsonResource
            , String collectionName
            , ObjectMapper objectMapper
            , MongoStorageConnectorConfig mongoStorageConnectorConfig) throws Exception {
        MongoClient localMongoClient = mongoStorageConnectorConfig.getMongoClient();
        MongoDatabase database = localMongoClient.getDatabase(mongoStorageConnectorConfig.getDbName());
        MongoCollection<Document> collection = database.getCollection(collectionName);
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
            //System.out.println(d.toJson());
        }
        localMongoClient.close();
    }

    protected void cleanupMongoServer() {
        mongoServer.shutdownNow();
    }
}
