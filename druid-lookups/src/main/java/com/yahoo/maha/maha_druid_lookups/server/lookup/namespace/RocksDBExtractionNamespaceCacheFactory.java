// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.google.inject.Inject;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.CacheActionRunner;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.ProtobufSchemaFactory;
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
    ServiceEmitter emitter;

    private CacheActionRunner cacheActionRunner = new CacheActionRunner();

    @Override
    public Callable<String> getCachePopulator(
            final String id,
            final RocksDBExtractionNamespace extractionNamespace,
            final String lastVersion,
            final Map<String, String> cache
    )
    {
        tryResetRunnerOrLog(extractionNamespace);

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
                    LOG.error(e, "Caught exception while RocksDB creation, lastVersion: [%s]", lastVersion);
                    emitter.emit(ServiceMetricEvent.builder().build(MonitoringConstants.MAHA_LOOKUP_ROCKSDB_OPEN_FAILURE, 1));
                    return lastVersion;
                }
            }
        };
    }

    public void tryResetRunnerOrLog(RocksDBExtractionNamespace extractionNamespace) {
        try {
            if (!extractionNamespace.cacheActionRunner.isEmpty()) {
                cacheActionRunner = CacheActionRunner.class.cast(
                        Class.forName(extractionNamespace.cacheActionRunner).newInstance());
                LOG.debug("Populated a new CacheActionRunner with description " + cacheActionRunner.toString());
            }
        } catch(Exception e){
            LOG.error("Failed to get a valid cacheActionRunner.", e);
        }
    }

    @Override
    public void updateCache(final RocksDBExtractionNamespace extractionNamespace,
                            final Map<String, String> cache, final String key, final byte[] value) {
        tryResetRunnerOrLog(extractionNamespace);

        cacheActionRunner.updateCache(protobufSchemaFactory, key, value, rocksDBManager, emitter, extractionNamespace);
    }

    @Override
    public byte[] getCacheValue(final RocksDBExtractionNamespace extractionNamespace, final Map<String, String> cache, final String key, String valueColumn, final Optional<DecodeConfig> decodeConfigOptional) {
        tryResetRunnerOrLog(extractionNamespace);

        return cacheActionRunner.getCacheValue(key, valueColumn, decodeConfigOptional, rocksDBManager, protobufSchemaFactory, lookupService, emitter, extractionNamespace);
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

}
