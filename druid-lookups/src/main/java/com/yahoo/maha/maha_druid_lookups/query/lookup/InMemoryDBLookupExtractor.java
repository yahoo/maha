// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

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
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.InMemoryDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.KafkaManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;
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
import java.util.concurrent.TimeUnit;

public class InMemoryDBLookupExtractor extends LookupExtractor
{
    private static final Logger LOG = new Logger(InMemoryDBLookupExtractor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, String> map;
    private final InMemoryDBExtractionNamespace extractionNamespace;
    private RocksDBManager rocksDBManager;
    private LookupService lookupService;
    private ProtobufSchemaFactory protobufSchemaFactory;
    private KafkaManager kafkaManager;
    private Cache<String, String> missingLookupCache;

    public InMemoryDBLookupExtractor(InMemoryDBExtractionNamespace extractionNamespace, Map<String, String> map,
                                     LookupService lookupService, RocksDBManager rocksDBManager, KafkaManager kafkaManager,
                                     ProtobufSchemaFactory protobufSchemaFactory)
    {
        this.extractionNamespace = extractionNamespace;
        this.map = Preconditions.checkNotNull(map, "map");
        this.rocksDBManager = rocksDBManager;
        this.kafkaManager = kafkaManager;
        this.lookupService = lookupService;
        this.protobufSchemaFactory = protobufSchemaFactory;
        this.missingLookupCache = Caffeine
                .newBuilder()
                .maximumSize(10_000)
                .expireAfterWrite(1, TimeUnit.HOURS)
                .build();
    }

    public Map<String, String> getMap()
    {
        return ImmutableMap.copyOf(map);
    }

    @Nullable
    @Override
    public String apply(@NotNull String val)
    {
        try {

            if("".equals(Strings.nullToEmpty(val))) {
                return null;
            }

            MahaLookupQueryElement mahaLookupQueryElement = objectMapper.readValue(val, MahaLookupQueryElement.class);
            String dimension = Strings.nullToEmpty(mahaLookupQueryElement.getDimension());
            String valueColumn = mahaLookupQueryElement.getValueColumn();
            DecodeConfig decodeConfig = mahaLookupQueryElement.getDecodeConfig();
            Map<String, String> dimensionOverrideMap = mahaLookupQueryElement.getDimensionOverrideMap();

            if(dimensionOverrideMap != null && dimensionOverrideMap.containsKey(dimension)) {
                return Strings.emptyToNull(dimensionOverrideMap.get(dimension));
            }

            byte[] cacheByteValue = null;
            if (!extractionNamespace.isCacheEnabled()) {
                cacheByteValue = lookupService.lookup(new LookupService.LookupData(extractionNamespace,
                        dimension));
            } else {
                final RocksDB db = rocksDBManager.getDB(extractionNamespace.getNamespace());
                if (db == null) {
                    LOG.error("RocksDB instance is null");
                    return null;
                }
                cacheByteValue = db.get(dimension.getBytes());
            }

            if (cacheByteValue == null || cacheByteValue.length == 0) {
                // No need to call handleMissingLookup if missing dimension is already present in missingLookupCache
                if(missingLookupCache.getIfPresent(dimension) == null) {
                    kafkaManager.handleMissingLookup(extractionNamespace, dimension);
                    missingLookupCache.put(dimension, extractionNamespace.getNamespace());
                }
                return null;
            }

            Parser<Message> parser = protobufSchemaFactory.getProtobufParser(extractionNamespace.getNamespace());
            Message message = parser.parseFrom(cacheByteValue);
            Descriptors.Descriptor descriptor = protobufSchemaFactory.getProtobufDescriptor(extractionNamespace.getNamespace());
            Descriptors.FieldDescriptor field = descriptor.findFieldByName(valueColumn);

            return Strings.emptyToNull(message.getField(field).toString());

        } catch (Exception e) {
            LOG.error(e, "Caught exception while lookup");
            return null;
        }
    }

    @Override
    public List<String> unapply(final String value)
    {
        return Lists.newArrayList(Maps.filterKeys(map, new Predicate<String>()
        {
            @Override public boolean apply(@Nullable String key)
            {
                return map.get(key).equals(Strings.nullToEmpty(value));
            }
        }).keySet());

    }

    @Override
    public byte[] getCacheKey()
    {
        try {
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            for (Map.Entry<String, String> entry : map.entrySet()) {
                final String key = entry.getKey();
                final String val = entry.getValue();
                if (!Strings.isNullOrEmpty(key)) {
                    outputStream.write(key.getBytes());
                }
                outputStream.write((byte)0xFF);
                if (!Strings.isNullOrEmpty(val)) {
                    outputStream.write(val.getBytes());
                }
                outputStream.write((byte)0xFF);
            }
            return outputStream.toByteArray();
        }
        catch (IOException ex) {
            // If ByteArrayOutputStream.write has problems, that is a very bad thing
            throw Throwables.propagate(ex);
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InMemoryDBLookupExtractor that = (InMemoryDBLookupExtractor) o;

        return map.equals(that.map);
    }

    @Override
    public int hashCode()
    {
        return map.hashCode();
    }

}
