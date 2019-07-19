// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup.namespace;

import com.google.common.collect.ImmutableMap;

import java.util.List;

public interface OnlineDatastoreExtractionNamespace extends ExtractionNamespace {

    long getPollMs();

    String getLookupName();

    String getTsColumn();

    boolean isCacheEnabled();

    String getPrimaryKeyColumn();

    ImmutableMap<String, Integer> getColumnIndexMap();

    public List<String> getColumnList();
}
