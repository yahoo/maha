package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema;

import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import org.apache.commons.lang3.tuple.ImmutablePair;


public interface DynamicLookupCoreSchema<K, V> {
    public SCHEMA_TYPE getSchemaType();
    public String getSchema();
    public ImmutablePair<K, V> getValue(String fieldName, byte[] dataBytes, RocksDBExtractionNamespace extractionNamespace);

}
