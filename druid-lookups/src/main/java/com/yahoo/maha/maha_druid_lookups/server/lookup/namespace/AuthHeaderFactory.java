// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import java.util.Collections;
import java.util.Map;

public interface AuthHeaderFactory {
    default Map<String, String> getAuthHeaders() {
        return Collections.EMPTY_MAP;
    }
}
