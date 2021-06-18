package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema;

import java.util.Map;

public interface DynamicLookupCoreSchema {
    public SCHEMA_TYPE getSchemaType();
    public Map<String, Integer> getSchema();
}
