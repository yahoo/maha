package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic;

import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.*;

import java.util.*;
import java.util.concurrent.*;

public class DynamicLookupSchemaManager {

    // Lookup name to Schema
    private Map<String, DynamicLookupSchema> schemaMap;

    public DynamicLookupSchemaManager() {
        schemaMap = new ConcurrentHashMap<>();
    }

    /*
     Called by Rockdb Manager to update Schema
     */
    public void updateSchema(ExtractionNamespace extractionNamespace, DynamicLookupSchema schema) {
        schemaMap.put(extractionNamespace.getLookupName(), schema);
    }

    public Optional<DynamicLookupSchema> getSchema(ExtractionNamespace extractionNamespace) {
        String lookupName = extractionNamespace.getLookupName();
        if (schemaMap.containsKey(lookupName)) {
            return Optional.of(schemaMap.get(lookupName));
        }
        return Optional.empty();
    }
}
