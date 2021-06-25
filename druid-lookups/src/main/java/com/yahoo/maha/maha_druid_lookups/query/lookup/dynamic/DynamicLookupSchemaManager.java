package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic;

import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema.*;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.*;
import org.apache.druid.java.util.common.logger.*;

import java.util.*;
import java.util.concurrent.*;

public class DynamicLookupSchemaManager {

    private static final Logger LOG = new Logger(com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.DynamicLookupSchemaManager.class);

    // Lookup name to Schema
    private Map<String, DynamicLookupSchema> schemaMap;

    public DynamicLookupSchemaManager() {
        LOG.info("Creating DynamicLookupSchemaManager instance!!");
        schemaMap = new ConcurrentHashMap<>();
    }

    /*
     Called by Rockdb Manager to update Schema
     */
    public void updateSchema(ExtractionNamespace extractionNamespace, DynamicLookupSchema schema) {
        LOG.info("Initializing dynamic schema for lookup %s, schema: %s", extractionNamespace.getLookupName(), schema.getName());
        schemaMap.put(extractionNamespace.getLookupName(), schema);
    }

    public Optional<DynamicLookupSchema> getSchema(ExtractionNamespace extractionNamespace) {
        String lookupName = extractionNamespace.getLookupName();
        if (schemaMap.containsKey(lookupName)) {
            return Optional.of(schemaMap.get(lookupName));
        } else {
            LOG.error("Failed to find the schema for dynamic lookup %s", extractionNamespace.getLookupName());
        }
        return Optional.empty();
    }
}
