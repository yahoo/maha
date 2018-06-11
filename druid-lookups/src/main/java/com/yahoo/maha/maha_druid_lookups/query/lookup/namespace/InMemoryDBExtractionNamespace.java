// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup.namespace;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

@JsonTypeName("mahainmemorydb")
public class InMemoryDBExtractionNamespace implements ExtractionNamespace {

    @JsonProperty
    private final String rocksDbInstanceHDFSPath;
    @JsonProperty
    private final String lookupAuditingHDFSPath;
    @JsonProperty
    private final String namespace;
    @JsonProperty
    private final Period pollPeriod;
    @JsonProperty
    private final String kafkaTopic;
    @JsonProperty
    private boolean cacheEnabled = true;
    @JsonProperty
    private boolean lookupAuditingEnabled = false;
    @JsonProperty
    private final String lookupName;
    @JsonProperty
    private String tsColumn = "last_updated";
    @JsonProperty
    private final String missingLookupKafkaTopic;

    private Long lastUpdatedTime = -1L;

    @JsonCreator
    public InMemoryDBExtractionNamespace(@NotNull @JsonProperty(value = "namespace", required = true)
                                                 String namespace,
                                         @NotNull @JsonProperty(value = "rocksDbInstanceHDFSPath", required = true)
                                         final String rocksDbInstanceHDFSPath,
                                         @NotNull @JsonProperty(value = "lookupAuditingHDFSPath", required = true)
                                         final String lookupAuditingHDFSPath,
                                         @Min(0) @JsonProperty(value = "pollPeriod", required = true)
                                                 Period pollPeriod,
                                         @NotNull @JsonProperty(value = "kafkaTopic", required = false)
                                         final String kafkaTopic,
                                         @JsonProperty(value = "cacheEnabled", required = false)
                                         final boolean cacheEnabled,
                                         @JsonProperty(value = "lookupAuditingEnabled", required = false)
                                         final boolean lookupAuditingEnabled,
                                         @NotNull @JsonProperty(value = "lookupName", required = true)
                                         final String lookupName,
                                         @Nullable @JsonProperty(value = "tsColumn", required = false)
                                         final String tsColumn,
                                         @NotNull @JsonProperty(value = "missingLookupKafkaTopic", required = false)
                                             final String missingLookupKafkaTopic) {
        this.rocksDbInstanceHDFSPath = Preconditions.checkNotNull(rocksDbInstanceHDFSPath, "rocksDbInstanceHDFSPath");
        this.lookupAuditingHDFSPath = Preconditions.checkNotNull(lookupAuditingHDFSPath, "lookupAuditingHDFSPath");
        this.namespace = Preconditions.checkNotNull(namespace, "namespace");
        this.pollPeriod = Preconditions.checkNotNull(pollPeriod, "pollPeriod");
        this.kafkaTopic = kafkaTopic;
        this.missingLookupKafkaTopic = missingLookupKafkaTopic;
        this.cacheEnabled = cacheEnabled;
        this.lookupAuditingEnabled = lookupAuditingEnabled;
        this.lookupName = lookupName;
        this.tsColumn = tsColumn;
    }

    public String getRocksDbInstanceHDFSPath() {
        return rocksDbInstanceHDFSPath;
    }

    public String getLookupAuditingHDFSPath() {
        return lookupAuditingHDFSPath;
    }

    //@Override
    public String getNamespace() {
        return namespace;
    }

    @Override
    public long getPollMs() {
        return pollPeriod.toStandardDuration().getMillis();
    }

    @Override
    public String getLookupName() {
        return lookupName;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public String getMissingLookupKafkaTopic() {
        return missingLookupKafkaTopic;
    }

    //@Override
    public boolean isCacheEnabled() {
        return cacheEnabled;
    }

    public boolean isLookupAuditingEnabled() {
        return lookupAuditingEnabled;
    }

    public Long getLastUpdatedTime() {
        return lastUpdatedTime;
    }

    public void setLastUpdatedTime(Long lastUpdatedTime) {
        this.lastUpdatedTime = lastUpdatedTime;
    }

    public String getTsColumn()
    {
        return tsColumn;
    }

    @Override
    public String toString() {
        return String.format(
                "CdwExtractionNamespace = { namespace = %s, rocksDbInstanceHDFSPath = { %s }, pollPeriod = %s, kafkaTopic = %s, missingLookupKafkaTopic = %s }",
                namespace,
                rocksDbInstanceHDFSPath,
                pollPeriod,
                kafkaTopic,
                missingLookupKafkaTopic
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InMemoryDBExtractionNamespace namespace1 = (InMemoryDBExtractionNamespace) o;

        if (!rocksDbInstanceHDFSPath.equals(namespace1.rocksDbInstanceHDFSPath)) {
            return false;
        }
        if (!namespace.equals(namespace1.namespace)) {
            return false;
        }
        if (!kafkaTopic.equals(namespace1.kafkaTopic)) {
            return false;
        }
        if (!missingLookupKafkaTopic.equals(namespace1.missingLookupKafkaTopic)) {
            return false;
        }
        if(!tsColumn.equals(namespace1.tsColumn)) {
            return false;
        }
        if(!lookupName.equals(namespace1.lookupName)) {
            return false;
        }
        return pollPeriod.equals(namespace1.pollPeriod);
    }

    @Override
    public int hashCode() {
        int result = rocksDbInstanceHDFSPath.hashCode();
        result = 31 * result + namespace.hashCode();
        result = 31 * result + pollPeriod.hashCode();
        result = 31 * result + kafkaTopic.hashCode();
        result = 31 * result + missingLookupKafkaTopic.hashCode();
        result = 31 * result + tsColumn.hashCode();
        result = 31 * result + lookupName.hashCode();
        return result;
    }
}
