package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic;

import com.google.inject.*;

public class DynamicLookupSchemaManagerProvider implements Provider<DynamicLookupSchemaManager> {
    @Override
    public DynamicLookupSchemaManager get() {
        return new DynamicLookupSchemaManager();
    }
}
