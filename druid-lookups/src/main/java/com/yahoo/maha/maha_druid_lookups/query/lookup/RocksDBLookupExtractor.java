// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.*;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.KafkaManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.MonitoringConstants;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.RocksDBManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.CacheActionRunner;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.ProtobufSchemaFactory;
import org.rocksdb.RocksDB;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

public class RocksDBLookupExtractor<U> extends MahaLookupExtractor {
    private static final Logger LOG = new Logger(RocksDBLookupExtractor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, U> map;
    private final RocksDBExtractionNamespace extractionNamespace;
    private RocksDBManager rocksDBManager;
    private LookupService lookupService;
    private ProtobufSchemaFactory protobufSchemaFactory;
    private KafkaManager kafkaManager;
    private ServiceEmitter serviceEmitter;
    private Cache<String, byte[]> missingLookupCache;
    private final byte[] extractionNamespaceAsByteArray;

    public RocksDBLookupExtractor(RocksDBExtractionNamespace extractionNamespace, Map<String, U> map,
                                  LookupService lookupService, RocksDBManager rocksDBManager, KafkaManager kafkaManager,
                                  ProtobufSchemaFactory protobufSchemaFactory, ServiceEmitter serviceEmitter) {
        this.extractionNamespace = extractionNamespace;
        this.map = Preconditions.checkNotNull(map, "map");
        this.rocksDBManager = rocksDBManager;
        this.kafkaManager = kafkaManager;
        this.lookupService = lookupService;
        this.protobufSchemaFactory = protobufSchemaFactory;
        this.serviceEmitter = serviceEmitter;
        this.missingLookupCache = Caffeine
                .newBuilder()
                .maximumSize(10_000)
                .expireAfterWrite(1, TimeUnit.HOURS)
                .build();
        try {
            objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            this.extractionNamespaceAsByteArray = objectMapper.writeValueAsBytes(extractionNamespace);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }

    }

    public Map<String, U> getMap() {
        return ImmutableMap.copyOf(map);
    }

    @Nullable
    public String apply(@NotNull String key, @NotNull String valueColumn, DecodeConfig decodeConfig, Map<String, String> dimensionOverrideMap) {
        LOG.info("RocksDBLookupExtractor: key: %s, valueColumn: %s", key, valueColumn);
        try {

            if (key == null) {
                return null;
            }

            if (dimensionOverrideMap != null && dimensionOverrideMap.containsKey(key)) {
                return Strings.emptyToNull(dimensionOverrideMap.get(key));
            }

            Optional<DecodeConfig> decodeConfigOptional = (decodeConfig == null) ? Optional.empty() : Optional.of(decodeConfig);
            if (!extractionNamespace.isCacheEnabled()) {
                byte[] cacheByteValue = lookupService.lookup(new LookupService.LookupData(extractionNamespace,
                        key, valueColumn, decodeConfigOptional));
                LOG.info("cache not enabled, lookup service return cacheByteValue, len = %s", cacheByteValue.length);
                return (cacheByteValue == null || cacheByteValue.length == 0) ? null : new String(cacheByteValue, UTF_8);
            } else {
                final RocksDB db = rocksDBManager.getDB(extractionNamespace.getNamespace());
                if (db == null) {
                    LOG.error("RocksDB instance is null");
                    return null;
                }
                //byte[] cacheByteValue = db.get(key.getBytes());
                //tryResetRunnerOrLog(extractionNamespace);
                byte[] cacheByteValue = extractionNamespace.getCacheActionRunner().getCacheValue(key, Optional.empty(), decodeConfigOptional, rocksDBManager, protobufSchemaFactory, lookupService, serviceEmitter, extractionNamespace);

                if (cacheByteValue == null || cacheByteValue.length == 0) {
                    // No need to call handleMissingLookup if missing dimension is already present in missingLookupCache
                    if (extractionNamespace.getMissingLookupConfig() != null
                            && !Strings.isNullOrEmpty(extractionNamespace.getMissingLookupConfig().getMissingLookupKafkaTopic())
                            && missingLookupCache.getIfPresent(key) == null) {

                        kafkaManager.handleMissingLookup(extractionNamespaceAsByteArray,
                                extractionNamespace.getMissingLookupConfig().getMissingLookupKafkaTopic(),
                                key);
                        missingLookupCache.put(key, extractionNamespaceAsByteArray);
                        serviceEmitter.emit(ServiceMetricEvent.builder().build(MonitoringConstants.MAHA_LOOKUP_PUBLISH_MISSING_LOOKUP_SUCCESS, 1));
                    }
                    return null;
                }
                LOG.info("handling decode for cacheByteValue, len: %s, value: %s", cacheByteValue.length, new String(cacheByteValue));
                return handleDecode(decodeConfig, cacheByteValue, valueColumn);
            }

        } catch (Exception e) {
            LOG.error(e, "Caught exception while lookup");
            return null;
        }
    }

    private String handleDecode(DecodeConfig decodeConfig, byte[] cacheByteValue, String valueColumn) throws Exception {

        try {

            LOG.info("extractionNamespace.getNamespace: %s" , extractionNamespace.getNamespace());
            Parser<Message> parser = protobufSchemaFactory.getProtobufParser(extractionNamespace.getNamespace());
            LOG.info("protobufSchemaFactory.getProtobufParser: %s", parser.toString());
            Message message = parser.parseFrom(cacheByteValue);
            LOG.info("parsed message: %s", message.toString());
            CodedInputStream input = CodedInputStream.newInstance(cacheByteValue);
            LOG.info("readTag(): %d", input.readTag());
            LOG.info("getLastTag(): %d", input.getLastTag());
            Descriptors.Descriptor descriptor = protobufSchemaFactory.getProtobufDescriptor(extractionNamespace.getNamespace());

            if (decodeConfig != null) {
                Descriptors.FieldDescriptor columnToCheckField = descriptor.findFieldByName(decodeConfig.getColumnToCheck());
                if (decodeConfig.getValueToCheck().equals(message.getField(columnToCheckField).toString())) {
                    Descriptors.FieldDescriptor columnIfValueMatchedField = descriptor.findFieldByName(decodeConfig.getColumnIfValueMatched());
                    LOG.info("columnIfValueMatchedField: %s", columnIfValueMatchedField.toString());
                    LOG.info("message.getField(columnIfValueMatchedField).toString() returning: %s", message.getField(columnIfValueMatchedField).toString());
                    return Strings.emptyToNull(message.getField(columnIfValueMatchedField).toString());
                } else {
                    Descriptors.FieldDescriptor columnIfValueNotMatched = descriptor.findFieldByName(decodeConfig.getColumnIfValueNotMatched());
                    LOG.info("columnIfValueNotMatched: %s", columnIfValueNotMatched.toString());
                    LOG.info("message.getField(columnIfValueNotMatched).toString() returning: %s", message.getField(columnIfValueNotMatched).toString());
                    return Strings.emptyToNull(message.getField(columnIfValueNotMatched).toString());
                }
            } else {
                Descriptors.FieldDescriptor field = descriptor.findFieldByName(valueColumn);
                LOG.info("valueColumn: %s", valueColumn.toString());
                LOG.info("message.getField(field).toString() returning: %s", message.getField(field).toString());
                return Strings.emptyToNull(message.getField(field).toString());
            }
        } catch (InvalidProtocolBufferException e ) {
            LOG.error(e, "throwing InvalidProtocolBufferException in handleDecode");
            LOG.info("new String(cacheByteValue): %s", new String(cacheByteValue));
            CodedInputStream input = CodedInputStream.newInstance(cacheByteValue);
            LOG.info("readTag(): %d", input.readTag());
            LOG.info("getLastTag(): %d", input.getLastTag());
            LOG.info("Message is not a protobuf, returning String");
            return Strings.emptyToNull(new String(cacheByteValue));
        }

    }

    @Override
    public List<String> unapply(final String value) {
        return Lists.newArrayList(Maps.filterKeys(map, new Predicate<String>() {
            @Override
            public boolean apply(@Nullable String key) {
                return map.get(key).equals(Strings.nullToEmpty(value));
            }
        }).keySet());

    }

    @Override
    public byte[] getCacheKey() {
        try {
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            outputStream.write(extractionNamespace.toString().getBytes());
            outputStream.write((byte) 0xFF);
            return outputStream.toByteArray();
        } catch (IOException ex) {
            // If ByteArrayOutputStream.write has problems, that is a very bad thing
            throw Throwables.propagate(ex);
        }
    }

   /* public void tryResetRunnerOrLog(RocksDBExtractionNamespace extractionNamespace) {
        try {
            if (cacheActionRunner == null) {
                cacheActionRunner = CacheActionRunner.class.cast(
                        Class.forName(extractionNamespace.getCacheActionRunner()).newInstance());
                LOG.info("Populated a new CacheActionRunner with description " + cacheActionRunner.toString());
            } else {
                LOG.debug("Runner is already defined.  Found " + cacheActionRunner.getClass().getName());
            }
        } catch(Exception e){
            LOG.error(e,"Failed to get a valid cacheActionRunner.");
        }
    }*/

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RocksDBLookupExtractor that = (RocksDBLookupExtractor) o;

        return map.equals(that.map);
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

}
