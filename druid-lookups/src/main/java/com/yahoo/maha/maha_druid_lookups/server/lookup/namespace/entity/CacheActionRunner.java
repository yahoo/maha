package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.MonitoringConstants;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.RocksDBManager;
import com.metamx.common.logger.Logger;
import org.rocksdb.RocksDB;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

public class CacheActionRunner {

    private static final Logger LOG = new Logger(CacheActionRunner.class);

    public byte[] getCacheValue(final String key
            , Optional<String> valueColumn
            , final Optional<DecodeConfig> decodeConfigOptional
            , RocksDBManager rocksDBManager
            , ProtobufSchemaFactory protobufSchemaFactory
            , LookupService lookupService
            , ServiceEmitter emitter
            , RocksDBExtractionNamespace extractionNamespace){
        try {
            if (!extractionNamespace.isCacheEnabled() && valueColumn.isPresent()) {
                return lookupService.lookup(new LookupService.LookupData(extractionNamespace, key, valueColumn.get(), decodeConfigOptional));
            }

            final RocksDB db = rocksDBManager.getDB(extractionNamespace.getNamespace());
            if (db != null) {
                Parser<Message> parser = protobufSchemaFactory.getProtobufParser(extractionNamespace.getNamespace());
                byte[] cacheByteValue = db.get(key.getBytes());
                if(cacheByteValue == null) {
                    return new byte[0];
                }
                Message message = parser.parseFrom(cacheByteValue);
                if (valueColumn.isPresent() && !decodeConfigOptional.isPresent()) {
                    Descriptors.Descriptor descriptor = protobufSchemaFactory.getProtobufDescriptor(extractionNamespace.getNamespace());
                    Descriptors.FieldDescriptor field = descriptor.findFieldByName(valueColumn.get());
                    return (field == null) ? new byte[0] : message.getField(field).toString().getBytes();
                } else {
                    return message.toByteArray();
                }
            }
        } catch (Exception e) {
            LOG.error(e, "Caught exception while getting cache value");
            emitter.emit(ServiceMetricEvent.builder().build(MonitoringConstants.MAHA_LOOKUP_GET_CACHE_VALUE_FAILURE, 1));
        }
        return null;
    }

    public void updateCache(ProtobufSchemaFactory protobufSchemaFactory
            , final String key
            , final byte[] value
            , RocksDBManager rocksDBManager
            , ServiceEmitter serviceEmitter
            , RocksDBExtractionNamespace extractionNamespace) {
        if (extractionNamespace.isCacheEnabled()) {
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
                    serviceEmitter.emit(ServiceMetricEvent.builder().build(MonitoringConstants.MAHA_LOOKUP_UPDATE_CACHE_SUCCESS, 1));
                }
            } catch (Exception e) {
                LOG.error(e, "Caught exception while updating cache");
                serviceEmitter.emit(ServiceMetricEvent.builder().build(MonitoringConstants.MAHA_LOOKUP_UPDATE_CACHE_FAILURE, 1));
            }
        }
    }

    @Override
    public String toString() {
        return "CacheActionRunner{}";
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this);
    }
}
