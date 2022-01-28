// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import org.apache.druid.java.util.common.logger.Logger;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBCLookupExtractor<U extends List<String>> extends OnlineDatastoreLookupExtractor<U> {
    private static final Logger LOG = new Logger(MethodHandles.lookup().lookupClass());

    public JDBCLookupExtractor(JDBCExtractionNamespace extractionNamespace, Map<String, U> map, LookupService lookupService) {
        super(extractionNamespace, map, lookupService);
    }

    @Override
    protected Logger LOGGER() {
        return LOG;
    }

    @Override
    public boolean canIterate() {
        return true;
    }

    @Override
    public Iterable<Map.Entry<String, String>> iterable() {
        Map<String, String> tempMap = new HashMap<>();
        int numEntriesIterated = 0;
        try {
            for (Map.Entry<String, U> entry : getMap().entrySet()) {
                StringBuilder sb = new StringBuilder();
                for (Map.Entry<String, Integer> colToIndex : getColumnIndexMap().entrySet()) {
                    sb.append(colToIndex.getKey())
                            .append(ITER_KEY_VAL_SEPARATOR)
                            .append(entry.getValue().get(colToIndex.getValue()))
                            .append(ITER_VALUE_COL_SEPARATOR);
                }
                if (sb.length() > 0) {
                    sb.setLength(sb.length() - 1);
                }
                tempMap.put(entry.getKey(), sb.toString());
                numEntriesIterated++;
                if (numEntriesIterated == ((JDBCExtractionNamespace)getExtractionNamespace()).getNumEntriesIterator()) {
                    break;
                }
            }
        } catch (Exception e) {
            LOG.error(e, "Caught exception. Returning iterable to empty map.");
        }

        return tempMap.entrySet();
    }
}
