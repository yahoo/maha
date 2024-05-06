package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic;

import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.*;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.*;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.*;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.MonitoringConstants;
import org.apache.druid.java.util.common.logger.Logger;
import org.rocksdb.RocksDB;

import java.util.Optional;
//com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.DynamicCacheActionRunner

public class DynamicCacheActionRunner implements BaseCacheActionRunner {

    private static final Logger LOG = new Logger(com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.DynamicCacheActionRunner.class);

    public byte[] getCacheValue(final String key
            , Optional<String> valueColumn
            , final Optional<DecodeConfig> decodeConfigOptional
            , RocksDB db
            , DynamicLookupSchemaManager schemaManager
            , LookupService lookupService
            , ServiceEmitter emitter
            , RocksDBExtractionNamespace extractionNamespace) {
        try {
            if (!extractionNamespace.isCacheEnabled() && valueColumn.isPresent()) {
                return lookupService.lookup(new LookupService.LookupData(extractionNamespace, key, valueColumn.get(), decodeConfigOptional));
            }

            Optional<DynamicLookupSchema> dynamicLookupSchemaOption = schemaManager.getSchema(extractionNamespace);

            if (db != null && dynamicLookupSchemaOption.isPresent()) {
                DynamicLookupSchema dynamicLookupSchema = dynamicLookupSchemaOption.get();
                byte[] cacheByteValue = db.get(key.getBytes());
                if (cacheByteValue == null) {
                    return new byte[0];
                }
                return dynamicLookupSchema.getCoreSchema().getValue(valueColumn.get(),cacheByteValue, decodeConfigOptional, extractionNamespace).getBytes();
            }
        } catch (Exception e) {
            LOG.error(e, "Caught exception while getting cache value "+e.getMessage());
            emitter.emit(ServiceMetricEvent.builder().setMetric(MonitoringConstants.MAHA_LOOKUP_GET_CACHE_VALUE_FAILURE, 1));
        }
        return null;
    }

    synchronized public void updateCache(DynamicLookupSchemaManager schemaManager
            , final String key
            , final byte[] value
            , RocksDB db
            , ServiceEmitter serviceEmitter
            , RocksDBExtractionNamespace extractionNamespace) {
        if (extractionNamespace.isCacheEnabled()) {
            try {
                if (db == null) return;

                Optional<DynamicLookupSchema> dynamicLookupSchemaOption = schemaManager.getSchema(extractionNamespace);
                if(!dynamicLookupSchemaOption.isPresent()) return;
                DynamicLookupSchema dynamicLookupSchema = dynamicLookupSchemaOption.get();

                String newLastUpdatedStr = dynamicLookupSchema.getCoreSchema().getValue(extractionNamespace.getTsColumn(), value, Optional.empty(), extractionNamespace);
                Long newLastUpdated = Long.parseLong(newLastUpdatedStr);

                byte[] cacheValue = db.get(key.getBytes());
                boolean updatedCache = false;
                if (cacheValue == null) {
                    db.put(key.getBytes(), value);
                    updatedCache = true;
                } else {
                    String oldLastUpdatedStr = dynamicLookupSchema.getCoreSchema().getValue(extractionNamespace.getTsColumn(), cacheValue, Optional.empty(), extractionNamespace);
                    Long oldLastUpdated = Long.parseLong(oldLastUpdatedStr);
                    if (newLastUpdated > oldLastUpdated) {
                        db.put(key.getBytes(), value);
                        updatedCache = true;
                    }

                }
                if (newLastUpdated > extractionNamespace.getLastUpdatedTime()) {
                    extractionNamespace.setLastUpdatedTime(newLastUpdated);
                }
                if (updatedCache) {
                    serviceEmitter.emit(ServiceMetricEvent.builder().setMetric(MonitoringConstants.MAHA_LOOKUP_UPDATE_CACHE_SUCCESS, Integer.valueOf(1)));
                }
            } catch (Exception e) {
                LOG.error(e, "Caught exception while updating cache " + e.getMessage());
                serviceEmitter.emit(ServiceMetricEvent.builder().setMetric(MonitoringConstants.MAHA_LOOKUP_UPDATE_CACHE_FAILURE, Integer.valueOf(1)));
            }
        }
    }

    @Override
    public ExtractionNameSpaceSchemaType getSchemaType() {
        return ExtractionNameSpaceSchemaType.DynamicSchema;
    }

    @Override
    public String toString() {
        return "DynamicCacheActionRunner{}";
    }

}
