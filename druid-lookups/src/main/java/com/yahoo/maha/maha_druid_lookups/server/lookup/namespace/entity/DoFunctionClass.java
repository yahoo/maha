package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.ExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.MonitoringConstants;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.RocksDBManager;
import com.metamx.common.logger.Logger;
import org.rocksdb.RocksDB;

import java.util.Map;
import java.util.Optional;

public class DoFunctionClass {

    public byte[] doStuff(final RocksDBExtractionNamespace extractionNamespace,
                 final Map<String, String> cache, final String key, String valueColumn,
                 final Optional<DecodeConfig> decodeConfigOptional, Logger LOG, RocksDBManager rocksDBManager,
                 ProtobufSchemaFactory protobufSchemaFactory, LookupService lookupService, ServiceEmitter emitter){
        try {
            if (!extractionNamespace.isCacheEnabled()) {
                return lookupService.lookup(new LookupService.LookupData(extractionNamespace, key, valueColumn, decodeConfigOptional));
            }

            final RocksDB db = rocksDBManager.getDB(extractionNamespace.getNamespace());
            if (db != null) {
                Parser<Message> parser = protobufSchemaFactory.getProtobufParser(extractionNamespace.getNamespace());
                byte[] cacheByteValue = db.get(key.getBytes());
                if(cacheByteValue == null) {
                    return new byte[0];
                }
                Message message = parser.parseFrom(cacheByteValue);
                Descriptors.Descriptor descriptor = protobufSchemaFactory.getProtobufDescriptor(extractionNamespace.getNamespace());
                Descriptors.FieldDescriptor field = descriptor.findFieldByName(valueColumn);
                return (field == null) ? new byte[0] : message.getField(field).toString().getBytes();
            }
        } catch (Exception e) {
            LOG.error(e, "Caught exception while getting cache value");
            emitter.emit(ServiceMetricEvent.builder().build(MonitoringConstants.MAHA_LOOKUP_GET_CACHE_VALUE_FAILURE, 1));
        }
        return null;
    }
}
