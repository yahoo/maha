// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.KafkaManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.MonitoringConstants;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.RocksDBManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.ProtobufSchemaFactory;
import io.druid.query.lookup.LookupExtractor;
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
        try {

            if (key == null) {
                return null;
            }

            if (dimensionOverrideMap != null && dimensionOverrideMap.containsKey(key)) {
                return Strings.emptyToNull(dimensionOverrideMap.get(key));
            }

            if (!extractionNamespace.isCacheEnabled()) {
                Optional<DecodeConfig> decodeConfigOptional = (decodeConfig == null) ? Optional.empty() : Optional.of(decodeConfig);
                byte[] cacheByteValue = lookupService.lookup(new LookupService.LookupData(extractionNamespace,
                        key, valueColumn, decodeConfigOptional));
                return (cacheByteValue == null || cacheByteValue.length == 0) ? null : new String(cacheByteValue, UTF_8);
            } else {
                final RocksDB db = rocksDBManager.getDB(extractionNamespace.getNamespace());
                if (db == null) {
                    LOG.error("RocksDB instance is null");
                    return null;
                }
                byte[] cacheByteValue = db.get(key.getBytes());
                if (cacheByteValue == null || cacheByteValue.length == 0) {
                    // No need to call handleMissingLookup if missing dimension is already present in missingLookupCache
                    if (extractionNamespace.getMissingLookupConfig() != null
                            && !Strings.isNullOrEmpty(extractionNamespace.getMissingLookupConfig().getMissingLookupKafkaTopic())
                            && missingLookupCache.getIfPresent(key) == null) {

                        kafkaManager.handleMissingLookup(extractionNamespaceAsByteArray,
                                extractionNamespace.getMissingLookupConfig().getMissingLookupKafkaTopic(),
                                key);
                        missingLookupCache.put(key, extractionNamespaceAsByteArray);
                        serviceEmitter.emit(ServiceMetricEvent.builder().build(MonitoringConstants.MAHA_LOOKUP_PUBLISH_MISSING_LOOKUP_SUCESS, 1));
                    }
                    return null;
                }

                return handleDecode(decodeConfig, cacheByteValue, valueColumn);

            }

        } catch (Exception e) {
            LOG.error(e, "Caught exception while lookup");
            return null;
        }
    }

    private String handleDecode(DecodeConfig decodeConfig, byte[] cacheByteValue, String valueColumn) throws Exception {
        Parser<Message> parser = protobufSchemaFactory.getProtobufParser(extractionNamespace.getNamespace());
        Message message = parser.parseFrom(cacheByteValue);
        Descriptors.Descriptor descriptor = protobufSchemaFactory.getProtobufDescriptor(extractionNamespace.getNamespace());

        if (decodeConfig != null) {
            Descriptors.FieldDescriptor columnToCheckField = descriptor.findFieldByName(decodeConfig.getColumnToCheck());

            if (decodeConfig.getValueToCheck().equals(message.getField(columnToCheckField).toString())) {
                Descriptors.FieldDescriptor columnIfValueMatchedField = descriptor.findFieldByName(decodeConfig.getColumnIfValueMatched());
                return Strings.emptyToNull(message.getField(columnIfValueMatchedField).toString());
            } else {
                Descriptors.FieldDescriptor columnIfValueNotMatched = descriptor.findFieldByName(decodeConfig.getColumnIfValueNotMatched());
                return Strings.emptyToNull(message.getField(columnIfValueNotMatched).toString());
            }
        } else {
            Descriptors.FieldDescriptor field = descriptor.findFieldByName(valueColumn);
            return Strings.emptyToNull(message.getField(field).toString());
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
