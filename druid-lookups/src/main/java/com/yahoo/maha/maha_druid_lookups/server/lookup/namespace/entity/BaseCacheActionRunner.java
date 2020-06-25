package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity;

import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.BaseSchemaFactory;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.rocksdb.RocksDB;

import java.util.Optional;
/*
 Base class for reading the value from rocksdb and parsing it according to BaseScameFactory type,
 Right now supporting two chains, ProtoBuf and FlatBuffer. It checks if routeToNextChain is true else go ahead and parse with current chain
 */
public interface BaseCacheActionRunner {

    byte[] getCacheValue(final String key
            , Optional<String> valueColumn
            , final Optional<DecodeConfig> decodeConfigOptional
            , RocksDB db
            , BaseSchemaFactory schemaFactory
            , LookupService lookupService
            , ServiceEmitter emitter
            , RocksDBExtractionNamespace extractionNamespace);

    void updateCache(BaseSchemaFactory schemaFactory
            , final String key
            , final byte[] value
            , RocksDB db
            , ServiceEmitter serviceEmitter
            , RocksDBExtractionNamespace extractionNamespace);

    void validateSchemaFactory(BaseSchemaFactory schemaFactory);
}
