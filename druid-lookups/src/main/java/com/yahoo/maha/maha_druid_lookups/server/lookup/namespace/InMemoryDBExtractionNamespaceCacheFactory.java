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
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.InMemoryDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.ProtobufSchemaFactory;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 *
 */
public class InMemoryDBExtractionNamespaceCacheFactory
        implements ExtractionNamespaceCacheFactory<InMemoryDBExtractionNamespace>
{
    private static final Logger LOG = new Logger(InMemoryDBExtractionNamespaceCacheFactory.class);
    private static final String ZERO = "0";
    @Inject
    LookupService lookupService;
    @Inject
    RocksDBManager rocksDBManager;
    @Inject
    ProtobufSchemaFactory protobufSchemaFactory;
    @Inject
    ServiceEmitter emitter;

    @Override
    public Callable<String> getCachePopulator(
            final String id,
            final InMemoryDBExtractionNamespace extractionNamespace,
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
                    emitter.emit(ServiceMetricEvent.builder().build(MonitoringConstants.MAHA_LOOKUP_ROCKSDB_OPEN_SUCESS, 1));
                    return loadTime;
                } catch(Exception e) {
                    LOG.error(e, "Caught exception while RocksDB creation, lastVersion: [%s]", lastVersion);
                    emitter.emit(ServiceMetricEvent.builder().build(MonitoringConstants.MAHA_LOOKUP_ROCKSDB_OPEN_FAILURE, 1));
                    return lastVersion;
                }
            }
        };
    }

    @Override
    public void updateCache(final InMemoryDBExtractionNamespace extractionNamespace,
                            final Map<String, String> cache, final String key, final byte[] value) {
        if (!extractionNamespace.isCacheEnabled()) {
            lookupService.update(new LookupService.LookupData(extractionNamespace, key), value);
        } else {
            try {

                Parser<Message> parser = protobufSchemaFactory.getProtobufParser(extractionNamespace.getNamespace());
                Descriptors.Descriptor descriptor = protobufSchemaFactory.getProtobufDescriptor(extractionNamespace.getNamespace());
                Descriptors.FieldDescriptor field = descriptor.findFieldByName(extractionNamespace.getTsColumn());

                Message newMessage = parser.parseFrom(value);
                Long newLastUpdated = Long.valueOf(newMessage.getField(field).toString());

                final RocksDB db = rocksDBManager.getDB(extractionNamespace.getNamespace());
                if (db != null) {
                    byte[] cacheValue = db.get(key.getBytes());
                    if(cacheValue != null) {

                        Message messageInDB = parser.parseFrom(cacheValue);
                        Long lastUpdatedInDB = Long.valueOf(messageInDB.getField(field).toString());

                        if(newLastUpdated > lastUpdatedInDB) {
                            db.put(key.getBytes(), value);
                        }
                    } else {
                        db.put(key.getBytes(), value);
                    }
                    if(newLastUpdated > extractionNamespace.getLastUpdatedTime()) {
                        extractionNamespace.setLastUpdatedTime(newLastUpdated);
                    }
                    emitter.emit(ServiceMetricEvent.builder().build(MonitoringConstants.MAHA_LOOKUP_UPDATE_CACHE_SUCESS, 1));
                }
            } catch (Exception e) {
                LOG.error(e, "Caught exception while updating cache");
                emitter.emit(ServiceMetricEvent.builder().build(MonitoringConstants.MAHA_LOOKUP_UPDATE_CACHE_FAILURE, 1));
            }
        }
    }

    @Override
    public byte[] getCacheValue(final InMemoryDBExtractionNamespace extractionNamespace, final Map<String, String> cache, final String key) {

        try {
            if (!extractionNamespace.isCacheEnabled()) {
                return lookupService.lookup(new LookupService.LookupData(extractionNamespace, key));
            }

            final RocksDB db = rocksDBManager.getDB(extractionNamespace.getNamespace());
            if (db != null) {
                Parser<Message> parser = protobufSchemaFactory.getProtobufParser(extractionNamespace.getNamespace());
                byte[] value = db.get(key.getBytes());
                return (value == null) ? new byte[0] : parser.parseFrom(value).toByteArray();
            }
        } catch (Exception e) {
            LOG.error(e, "Caught exception while getting cache value");
            emitter.emit(ServiceMetricEvent.builder().build(MonitoringConstants.MAHA_LOOKUP_GET_CACHE_VALUE_FAILURE, 1));
        }
        return null;
    }

    @Override
    public String getCacheSize(final InMemoryDBExtractionNamespace extractionNamespace, final Map<String, String> cache) {
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
    public Long getLastUpdatedTime(final InMemoryDBExtractionNamespace extractionNamespace) {
        if (!extractionNamespace.isCacheEnabled()) {
            return lookupService.getLastUpdatedTime(new LookupService.LookupData(extractionNamespace));
        }
        return (extractionNamespace.getLastUpdatedTime() != null) ? extractionNamespace.getLastUpdatedTime() : -1L;
    }

}
