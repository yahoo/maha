package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic;

import com.yahoo.maha.maha_druid_lookups.query.lookup.*;
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
        return false;
    }

    @Override
    public Iterable<Map.Entry<String, String>> iterable() {
        return null;
    }
}
