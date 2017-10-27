// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup.namespace;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = "mahajdbc", value = JDBCExtractionNamespace.class),
        @JsonSubTypes.Type(name = "mahainmemorydb", value = InMemoryDBExtractionNamespace.class)
})
public interface ExtractionNamespace {

    long getPollMs();

    String getLookupName();

}
