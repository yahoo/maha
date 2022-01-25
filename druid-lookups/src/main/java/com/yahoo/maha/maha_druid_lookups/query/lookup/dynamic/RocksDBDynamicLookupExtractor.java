package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.yahoo.maha.maha_druid_lookups.query.lookup.*;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupCoreSchema;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupFlatbufferSchemaSerDe;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupProtobufSchemaSerDe;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupSchema;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.*;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.*;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.*;
import org.rocksdb.*;
import java.util.*;

public class RocksDBDynamicLookupExtractor<U> extends BaseRocksDBLookupExtractor<U> {

    private static final Logger LOG = new Logger(com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.RocksDBDynamicLookupExtractor.class);
    private DynamicCacheActionRunner dynamicCacheActionRunner;
    private DynamicLookupSchemaManager schemaManager;

    public RocksDBDynamicLookupExtractor(RocksDBExtractionNamespace extractionNamespace, Map<String, U> map,
                                  LookupService lookupService, RocksDBManager rocksDBManager, KafkaManager kafkaManager,
                                  DynamicLookupSchemaManager schemaManager, ServiceEmitter serviceEmitter,
                                  DynamicCacheActionRunner cacheActionRunner) {
        super(extractionNamespace, map, lookupService, rocksDBManager, kafkaManager, serviceEmitter);
        this.dynamicCacheActionRunner = cacheActionRunner;
        this.schemaManager = schemaManager;
    }

    @Override
    public byte[] getCacheByteValue(String key, String valueColumn, Optional<DecodeConfig> decodeConfigOptional, RocksDB db) {
        return dynamicCacheActionRunner.getCacheValue(key, Optional.of(valueColumn), decodeConfigOptional, db, schemaManager, lookupService, serviceEmitter, extractionNamespace);
    }

    @Override
    public boolean canIterate() {
        return true;
    }

    @Override
    public Iterable<Map.Entry<String, String>> iterable() {
        Map<String, String> tempMap = new java.util.HashMap<>();

        try {
            final RocksDB db = rocksDBManager.getDB(extractionNamespace.getNamespace());
            RocksIterator it = db.newIterator();
            it.seekToFirst();
            while (it.isValid()) {
                Optional<DynamicLookupSchema> dynamicLookupSchemaOption = schemaManager.getSchema(extractionNamespace);
                if(!dynamicLookupSchemaOption.isPresent()) {
                    return tempMap.entrySet();
                }

                byte[] cacheByteValue = db.get(it.key());
                if (cacheByteValue == null) {
                    return tempMap.entrySet();
                }

                DynamicLookupSchema dynamicLookupSchema = dynamicLookupSchemaOption.get();
                DynamicLookupCoreSchema dynamicLookupCoreSchema = dynamicLookupSchema.getCoreSchema();
                if (dynamicLookupCoreSchema instanceof DynamicLookupProtobufSchemaSerDe) {
                    DynamicLookupProtobufSchemaSerDe dynamicLookupProtobufSchemaSerDe = (DynamicLookupProtobufSchemaSerDe) dynamicLookupCoreSchema;
                    DynamicMessage dynamicMessage = Optional.of(DynamicMessage.parseFrom(dynamicLookupProtobufSchemaSerDe.getProtobufMessageDescriptor(), cacheByteValue)).get();
                    Map<Descriptors.FieldDescriptor, Object> tempMap2 = dynamicMessage.getAllFields();
                    StringBuilder sb = new StringBuilder();
                    for (Map.Entry<Descriptors.FieldDescriptor, Object> kevVal: tempMap2.entrySet()) {
                        sb.append(kevVal.getKey().getJsonName()).append(":").append(kevVal.getValue().toString()).append("#");
                    }
                    if (sb.length() > 0) {
                        sb.setLength(sb.length() - 1);
                    }
                    StringBuilder keySb = new StringBuilder(sb);
                    String key = keySb.substring(0, sb.indexOf("#"));
                    tempMap.put(key, sb.toString());
                    it.next();
                }
                else if (dynamicLookupCoreSchema instanceof DynamicLookupFlatbufferSchemaSerDe) {
                    return tempMap.entrySet();
                }
            }
        } catch (Exception e) {
            LOG.error("Caught exception: " + e);
            LOG.warn("Returning iterable to empty map due to above exception.");
        }

        return tempMap.entrySet();
    }
}
