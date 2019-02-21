package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import java.util.Collections;
import java.util.Map;

public class NoopAuthHeaderFactory implements AuthHeaderFactory {
    @Override
    public Map<String, String> getAuthHeaders() {
        return Collections.emptyMap();
    }
}
