// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.google.inject.Inject;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.*;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.ExtractionNameSpaceSchemaType;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.ExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.CacheActionRunner;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.CacheActionRunnerFlatBuffer;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.BaseSchemaFactory;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.flatbuffer.FlatBufferSchemaFactory;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.protobuf.ProtobufSchemaFactory;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 *
 */
public class RocksDBExtractionNamespaceCacheFactory
        implements ExtractionNamespaceCacheFactory<RocksDBExtractionNamespace, String>
{
    private static final Logger LOG = new Logger(RocksDBExtractionNamespaceCacheFactory.class);
    private static final String ZERO = "0";
    @Inject
    LookupService lookupService;
    @Inject
    RocksDBManager rocksDBManager;
    @Inject
    ProtobufSchemaFactory protobufSchemaFactory;
    @Inject
    FlatBufferSchemaFactory flatBufferSchemaFactory;
    @Inject
    ServiceEmitter emitter;
    @Inject
    DynamicLookupSchemaManager dynamicLookupSchemaManager;


    @Override
    public Callable<String> getCachePopulator(
            final String id,
            final RocksDBExtractionNamespace extractionNamespace,
            final String lastVersion,
            final Map<String, String> cache
    )
    {
        if(!extractionNamespace.isCacheEnabled()) {
            return new Callable<String>() {
                @Override
                public String call() {
                    return String.valueOf(0);
                }
            };
        }
        return new Callable<String>() {
            @Override
            public String call() {
                try {
                    String loadTime = rocksDBManager.createDB(extractionNamespace, lastVersion);
                    emitter.emit(ServiceMetricEvent.builder().build(MonitoringConstants.MAHA_LOOKUP_ROCKSDB_OPEN_SUCCESS, 1));
                    return loadTime;
                } catch(Exception e) {
                    LOG.wtf(e, "Caught exception while RocksDB creation, error: %s, lastVersion: [%s]", e.getMessage(), lastVersion);
                    emitter.emit(ServiceMetricEvent.builder().build(MonitoringConstants.MAHA_LOOKUP_ROCKSDB_OPEN_FAILURE, 1));
                    return lastVersion;
                }
            }
        };
    }


    @Override
    public void updateCache(final RocksDBExtractionNamespace extractionNamespace,
                            final Map<String, String> cache, final String key, final byte[] value) {

        RocksDB db = rocksDBManager.getDB(extractionNamespace.getNamespace());
        if (extractionNamespace.getSchemaType() == ExtractionNameSpaceSchemaType.FLAT_BUFFER) {
            ((CacheActionRunnerFlatBuffer)extractionNamespace.getCacheActionRunner()).updateCache(flatBufferSchemaFactory, key, value, db, emitter, extractionNamespace);
        } if (extractionNamespace.getSchemaType() == ExtractionNameSpaceSchemaType.DynamicSchema) {
            ((DynamicCacheActionRunner)extractionNamespace.getCacheActionRunner()).updateCache(dynamicLookupSchemaManager, key, value, db, emitter, extractionNamespace);
        } else {
            ((CacheActionRunner)extractionNamespace.getCacheActionRunner()).updateCache(protobufSchemaFactory, key, value, db, emitter, extractionNamespace);
        }
    }

    @Override
    public byte[] getCacheValue(final RocksDBExtractionNamespace extractionNamespace, final Map<String, String> cache, final String key, String valueColumn, final Optional<DecodeConfig> decodeConfigOptional) {
        RocksDB db = rocksDBManager.getDB(extractionNamespace.getNamespace());
        if (extractionNamespace.getSchemaType() == ExtractionNameSpaceSchemaType.FLAT_BUFFER) {
            return ((CacheActionRunnerFlatBuffer) extractionNamespace.getCacheActionRunner()).getCacheValue(key, Optional.of(valueColumn), decodeConfigOptional, db, flatBufferSchemaFactory, lookupService, emitter, extractionNamespace);
        }
        if (extractionNamespace.getSchemaType() == ExtractionNameSpaceSchemaType.DynamicSchema) {
            return ((DynamicCacheActionRunner) extractionNamespace.getCacheActionRunner()).getCacheValue(key, Optional.of(valueColumn), decodeConfigOptional, db, dynamicLookupSchemaManager, lookupService, emitter, extractionNamespace);
        } else {
            return ((CacheActionRunner)extractionNamespace.getCacheActionRunner()).getCacheValue(key, Optional.of(valueColumn), decodeConfigOptional, db, protobufSchemaFactory, lookupService, emitter, extractionNamespace);
        }
    }

    @Override
    public String getCacheSize(final RocksDBExtractionNamespace extractionNamespace, final Map<String, String> cache) {
        if (!extractionNamespace.isCacheEnabled()) {
            return String.valueOf(lookupService.getSize());
        }
        try {
            final RocksDB db = rocksDBManager.getDB(extractionNamespace.getNamespace());
            if(db != null) {
                return db.getProperty("rocksdb.estimate-num-keys");
            }
        } catch (RocksDBException e) {
            LOG.error(e, "RocksDBException");
            emitter.emit(ServiceMetricEvent.builder().build(MonitoringConstants.MAHA_LOOKUP_GET_CACHE_SIZE_FAILURE, 1));
        }
        return ZERO;
    }

    @Override
    public Long getLastUpdatedTime(final RocksDBExtractionNamespace extractionNamespace) {
        if (!extractionNamespace.isCacheEnabled()) {
            return lookupService.getLastUpdatedTime(new LookupService.LookupData(extractionNamespace));
        }
        return (extractionNamespace.getLastUpdatedTime() != null) ? extractionNamespace.getLastUpdatedTime() : -1L;
    }

    public void updateCacheWithDb(final RocksDBExtractionNamespace extractionNamespace,
                            RocksDB db, final String key, final byte[] value) {

        if (extractionNamespace.getSchemaType() == ExtractionNameSpaceSchemaType.FLAT_BUFFER) {
            ((CacheActionRunnerFlatBuffer)extractionNamespace.getCacheActionRunner()).updateCache(flatBufferSchemaFactory, key, value, db, emitter, extractionNamespace);
        } if (extractionNamespace.getSchemaType() == ExtractionNameSpaceSchemaType.DynamicSchema) {
            ((DynamicCacheActionRunner)extractionNamespace.getCacheActionRunner()).updateCache(dynamicLookupSchemaManager, key, value, db, emitter, extractionNamespace);
        } else {
            ((CacheActionRunner)extractionNamespace.getCacheActionRunner()).updateCache(protobufSchemaFactory, key, value, db, emitter, extractionNamespace);
        }
    }

    private BaseSchemaFactory getSchemaFactory(ExtractionNamespace extractionNamespace) {
        if (extractionNamespace.getSchemaType() == ExtractionNameSpaceSchemaType.FLAT_BUFFER) {
           return flatBufferSchemaFactory;
        } else {
            return protobufSchemaFactory;
        }
    }

}
