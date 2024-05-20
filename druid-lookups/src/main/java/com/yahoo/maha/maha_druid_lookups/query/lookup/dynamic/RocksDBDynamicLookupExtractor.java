package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.yahoo.maha.maha_druid_lookups.query.lookup.BaseRocksDBLookupExtractor;
import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupCoreSchema;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupFlatbufferSchemaSerDe;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupProtobufSchemaSerDe;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupSchema;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.KafkaManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.RocksDBManager;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import java.util.Map;
import java.util.HashMap;
import java.util.Optional;
import java.util.Set;

public class RocksDBDynamicLookupExtractor<U> extends BaseRocksDBLookupExtractor<U> {

    private static final Logger LOG = new Logger(com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.RocksDBDynamicLookupExtractor.class);
    private DynamicCacheActionRunner dynamicCacheActionRunner;
    private DynamicLookupSchemaManager schemaManager;

    private static final Map<String, String> tempMap = new HashMap<>();

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
        return false;
    }

    @Override
    public boolean canGetKeySet() {
        return false;
    }

    @Override
    public Iterable<Map.Entry<String, String>> iterable() {
        return tempMap.entrySet();
    }

    @Override
    public Set<String> keySet() {
        return null;
    }
}
