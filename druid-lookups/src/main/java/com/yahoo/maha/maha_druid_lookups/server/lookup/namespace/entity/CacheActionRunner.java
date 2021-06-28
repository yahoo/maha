package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity;

import com.google.common.base.Strings;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.ExtractionNameSpaceSchemaType;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.protobuf.ProtobufSchemaFactory;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.MonitoringConstants;
import org.apache.druid.java.util.common.logger.Logger;
import org.rocksdb.RocksDB;

import java.util.Optional;

public class CacheActionRunner implements BaseCacheActionRunner {

    private static final Logger LOG = new Logger(CacheActionRunner.class);

    public byte[] getCacheValue(final String key
            , Optional<String> valueColumn
            , final Optional<DecodeConfig> decodeConfigOptional
            , RocksDB db
            , ProtobufSchemaFactory protobufSchemaFactory
            , LookupService lookupService
            , ServiceEmitter emitter
            , RocksDBExtractionNamespace extractionNamespace){
        try {
            if (!extractionNamespace.isCacheEnabled() && valueColumn.isPresent()) {
                return lookupService.lookup(new LookupService.LookupData(extractionNamespace, key, valueColumn.get(), decodeConfigOptional));
            }

            if (db != null) {
                Parser<Message> parser = protobufSchemaFactory.getProtobufParser(extractionNamespace.getNamespace());
                byte[] cacheByteValue = db.get(key.getBytes());
                if(cacheByteValue == null) {
                    return new byte[0];
                }
                Message message = parser.parseFrom(cacheByteValue);
                LOG.debug("parsed message: %s", message.toString());
                Descriptors.Descriptor descriptor = protobufSchemaFactory.getProtobufDescriptor(extractionNamespace.getNamespace());
                if (!decodeConfigOptional.isPresent()) {
                    Descriptors.FieldDescriptor field = descriptor.findFieldByName(valueColumn.get());
                    return (field == null) ? new byte[0] : message.getField(field).toString().getBytes();
                } else { //handle decodeConfig
                    return handleDecode(decodeConfigOptional.get(), message, descriptor).getBytes();
                }
            }
        } catch (Exception e) {
            LOG.error(e, "Caught exception while getting cache value");
            emitter.emit(ServiceMetricEvent.builder().build(MonitoringConstants.MAHA_LOOKUP_GET_CACHE_VALUE_FAILURE, 1));
        }
        return null;
    }

    public String handleDecode(DecodeConfig decodeConfig, Message parsedMessage, Descriptors.Descriptor descriptor) throws Exception {

        try {
            Descriptors.FieldDescriptor columnToCheckField = descriptor.findFieldByName(decodeConfig.getColumnToCheck());
            if (decodeConfig.getValueToCheck().equals(parsedMessage.getField(columnToCheckField).toString())) {
                Descriptors.FieldDescriptor columnIfValueMatchedField = descriptor.findFieldByName(decodeConfig.getColumnIfValueMatched());
                return Strings.emptyToNull(parsedMessage.getField(columnIfValueMatchedField).toString());
            } else {
                Descriptors.FieldDescriptor columnIfValueNotMatched = descriptor.findFieldByName(decodeConfig.getColumnIfValueNotMatched());
                return Strings.emptyToNull(parsedMessage.getField(columnIfValueNotMatched).toString());
            }

        } catch (Exception e ) {
            LOG.error(e, "Caught exception while handleDecode");
            throw e;
        }
    }

    synchronized public void updateCache(ProtobufSchemaFactory protobufSchemaFactory
            , final String key
            , final byte[] value
            , RocksDB db
            , ServiceEmitter serviceEmitter
            , RocksDBExtractionNamespace extractionNamespace) {
        if (extractionNamespace.isCacheEnabled()) {
            try {
                Parser<Message> parser = protobufSchemaFactory.getProtobufParser(extractionNamespace.getNamespace());
                Descriptors.Descriptor descriptor = protobufSchemaFactory.getProtobufDescriptor(extractionNamespace.getNamespace());
                Descriptors.FieldDescriptor field = descriptor.findFieldByName(extractionNamespace.getTsColumn());

                Message newMessage = parser.parseFrom(value);
                Long newLastUpdated = Long.valueOf(newMessage.getField(field).toString());

                if (db != null) {
                    byte[] cacheValue = db.get(key.getBytes());
                    if(cacheValue != null) {

                        Message messageInDB = parser.parseFrom(cacheValue);
                        Long lastUpdatedInDB = Long.valueOf(messageInDB.getField(field).toString());

                        if (newLastUpdated > lastUpdatedInDB) {
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
    public ExtractionNameSpaceSchemaType getSchemaType() {
        return ExtractionNameSpaceSchemaType.PROTOBUF;
    }

    @Override
    public String toString() {
        return "CacheActionRunner{}";
    }

}
