package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic;

import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.DynamicLookupSchema;

import java.util.*;
import java.util.concurrent.*;

public class DynamicLookupSchemaManager {

    private Map<String, DynamicLookupSchema> schemaMap;

    public DynamicLookupSchemaManager() {
        schemaMap = new ConcurrentHashMap<>();
    }

    /*
     Called by Rockdb Manager to update Schema
     */
    public void updateSchema(String namespace, DynamicLookupSchema schema) {
        schemaMap.put(namespace, schema);
    }

    public Optional<DynamicLookupSchema> getSchema(String namespace) {
        if (schemaMap.containsKey(namespace)) {
            return Optional.of(schemaMap.get(namespace));
        }
        return Optional.empty();
    }
}
