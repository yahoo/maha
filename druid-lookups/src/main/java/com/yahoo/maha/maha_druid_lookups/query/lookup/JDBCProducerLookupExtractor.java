// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.metamx.common.logger.Logger;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCProducerExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;

public class JDBCProducerLookupExtractor<U extends List<String>> extends OnlineDatastoreLookupExtractor<U> {
    private static final Logger LOG = new Logger(MethodHandles.lookup().lookupClass());

    public JDBCProducerLookupExtractor(JDBCProducerExtractionNamespace extractionNamespace, Map<String, U> map, LookupService lookupService) {
        super(extractionNamespace, map, lookupService);
    }

    @Override
    protected Logger LOGGER() {
        return LOG;
    }
}
