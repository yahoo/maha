package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic;

import java.util.*;
import java.util.concurrent.*;

public class DynamicLookupSchemaManager {

    private Map<String, DynamicLookupSchema> schemaMap;
    public DynamicLookupSchemaManager() {
        schemaMap = new ConcurrentHashMap<>();
    }

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
