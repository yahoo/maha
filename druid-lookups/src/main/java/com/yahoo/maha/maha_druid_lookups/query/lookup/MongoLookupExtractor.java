// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import org.apache.druid.java.util.common.logger.Logger;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.MongoExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MongoLookupExtractor<U extends List<String>> extends OnlineDatastoreLookupExtractor {
    private static final Logger LOG = new Logger(MethodHandles.lookup().lookupClass());

    public MongoLookupExtractor(MongoExtractionNamespace extractionNamespace, Map<String, U> map, LookupService lookupService) {
        super(extractionNamespace, map, lookupService);
    }

    @Override
    protected Logger LOGGER() {
        return LOG;
    }

    @Override
    public boolean supportsAsMap() {
        return false;
    }

    @Override
    public Map<String, String> asMap() {
        return staticMap;
    }

}
