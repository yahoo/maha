package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import java.util.Collections;
import java.util.Map;

public interface AuthHeaderProvider {
    default Map<String, String> getAuthHeaders() {
        return Collections.EMPTY_MAP;
    }
}
