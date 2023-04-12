// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.KafkaManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.RocksDBManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.CacheActionRunnerFlatBuffer;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.flatbuffer.FlatBufferSchemaFactory;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.rocksdb.RocksDB;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class RocksDBLookupExtractorWithFlatBuffer<U> extends BaseRocksDBLookupExtractor<U> {
    private static final Logger LOG = new Logger(RocksDBLookupExtractorWithFlatBuffer.class);

    private FlatBufferSchemaFactory schemaFactory;
    private CacheActionRunnerFlatBuffer cacheActionRunner;

    public RocksDBLookupExtractorWithFlatBuffer(RocksDBExtractionNamespace extractionNamespace, Map<String, U> map,
                                                LookupService lookupService, RocksDBManager rocksDBManager, KafkaManager kafkaManager,
                                                FlatBufferSchemaFactory schemaFactory, ServiceEmitter serviceEmitter,
                                                CacheActionRunnerFlatBuffer cacheActionRunner) {
        super(extractionNamespace, map, lookupService, rocksDBManager, kafkaManager, serviceEmitter);
        this.schemaFactory = schemaFactory;
        this.cacheActionRunner = cacheActionRunner;
    }

    @Override
    public byte[] getCacheByteValue(String key, String valueColumn, Optional<DecodeConfig> decodeConfigOptional, RocksDB db) {
        return cacheActionRunner.getCacheValue(key, Optional.of(valueColumn), decodeConfigOptional, db, schemaFactory, lookupService, serviceEmitter, extractionNamespace);
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
        return staticMap.entrySet();
    }

    @Override
    public Set<String> keySet() {
        return null;
    }
}
