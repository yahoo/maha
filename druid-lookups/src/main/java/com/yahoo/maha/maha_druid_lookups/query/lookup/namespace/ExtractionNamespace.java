// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup.namespace;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = "mahajdbcleaderfollower", value = JDBCExtractionNamespaceWithLeaderAndFollower.class),
        @JsonSubTypes.Type(name = "mahajdbc", value = JDBCExtractionNamespace.class),
        @JsonSubTypes.Type(name = "mahainmemorydb", value = RocksDBExtractionNamespace.class),
        @JsonSubTypes.Type(name = "maharocksdb", value = RocksDBExtractionNamespace.class),
        @JsonSubTypes.Type(name = "mahamongo", value = MongoExtractionNamespace.class)
})
/**
 * Druid version 0.11.0 uses Jackson version 2.4.*, which does not have support
 * for multiple bindings (one class using two names).
 * For now, switch the two to use the binding we utilize.
 * When Druid is upgraded, revisit this class.
 */
public interface ExtractionNamespace {

    long getPollMs();

    String getLookupName();

    String getTsColumn();

    boolean isCacheEnabled();
}
