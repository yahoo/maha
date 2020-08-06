package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity;

import com.yahoo.maha.maha_druid_lookups.query.lookup.DecodeConfig;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.ExtractionNameSpaceSchemaType;
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

    ExtractionNameSpaceSchemaType getSchemaType();
}
