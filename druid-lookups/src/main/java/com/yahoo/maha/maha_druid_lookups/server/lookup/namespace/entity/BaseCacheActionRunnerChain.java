package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity;

import com.metamx.emitter.service.ServiceEmitter;
import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.BaseSchemaFactory;
import org.rocksdb.RocksDB;

import java.util.Optional;
/*
 Base class for reading the value from rocksdb and parsing it according to BaseScameFactory type,
 Right now supporting two chains, ProtoBuf and FlatBuffer. It checks if routeToNextChain is true else go ahead and parse with current chain
 */
public interface BaseCacheActionRunnerChain {

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

    boolean routeToNextChain(BaseSchemaFactory schemaFactory);
}
