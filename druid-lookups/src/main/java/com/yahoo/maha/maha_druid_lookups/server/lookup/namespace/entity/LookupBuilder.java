package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity;

import java.util.List;
import java.util.Map;

public class LookupBuilder {

    private final Map<String, List<String>> cache;

    public LookupBuilder(Map<String, List<String>> cache) {
        this.cache = cache;
    }

    public void add(String key, List<String> value) {
        cache.put(key, value);
    }
}
