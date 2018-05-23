package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import java.util.Map;

public interface AuthHeaderProvider {
    Map<String, String> getAuthHeaders();
}
