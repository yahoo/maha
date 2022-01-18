// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.RocksDBSnapshot;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.protobuf.ProtobufSchemaFactory;
import org.apache.commons.io.FilenameUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.KafkaManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.RocksDBManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.CacheActionRunner;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

public class RocksDBLookupExtractor<U> extends BaseRocksDBLookupExtractor<U> {

    private static final Logger LOG = new Logger(RocksDBLookupExtractor.class);
    private CacheActionRunner cacheActionRunner;
    private ProtobufSchemaFactory schemaFactory;

    public RocksDBLookupExtractor(RocksDBExtractionNamespace extractionNamespace, Map<String, U> map,
                                  LookupService lookupService, RocksDBManager rocksDBManager, KafkaManager kafkaManager,
                                  ProtobufSchemaFactory schemaFactory, ServiceEmitter serviceEmitter,
                                  CacheActionRunner cacheActionRunner) {
        super(extractionNamespace, map, lookupService, rocksDBManager, kafkaManager, serviceEmitter);
        this.cacheActionRunner = cacheActionRunner;
        this.schemaFactory = schemaFactory;
    }

    @Override
    public byte[] getCacheByteValue(String key, String valueColumn, Optional<DecodeConfig> decodeConfigOptional, RocksDB db) {
       return cacheActionRunner.getCacheValue(key, Optional.of(valueColumn), decodeConfigOptional, db, schemaFactory, lookupService, serviceEmitter, extractionNamespace);
    }

    @Override
    public boolean canIterate() {
        return true;
    }

    @Override
    public Iterable<Map.Entry<String, String>> iterable() {
        Map<String, String> tempMap = new java.util.HashMap<>();

        try {
            final RocksDB db = rocksDBManager.getDB(extractionNamespace.getNamespace());
            RocksIterator it = db.newIterator();
            it.seekToFirst();
            while (it.isValid()) {
                byte[] cacheByteValue = db.get(it.key());
                Parser<Message> parser = schemaFactory.getProtobufParser(extractionNamespace.getNamespace());
                Message message = parser.parseFrom(cacheByteValue);
                Map<Descriptors.FieldDescriptor, Object> tempMap2 = message.getAllFields();
                StringBuilder sb = new StringBuilder();
                for (Map.Entry<Descriptors.FieldDescriptor, Object> kevVal: tempMap2.entrySet()) {
                    sb.append(kevVal.getKey().getJsonName()).append(":").append(kevVal.getValue().toString()).append("#");
                }
                if (sb.length() > 0) {
                    sb.setLength(sb.length() - 1);
                }
                StringBuilder keySb = new StringBuilder(sb);
                String key = keySb.substring(0, sb.indexOf("#"));
                tempMap.put(key, sb.toString());
                it.next();
            }
        } catch (Exception e) {
            LOG.error("Caught exception: " + e);
            LOG.warn("Returning iterable to empty map due to above exception.");
        }

        return tempMap.entrySet();
    }
}
