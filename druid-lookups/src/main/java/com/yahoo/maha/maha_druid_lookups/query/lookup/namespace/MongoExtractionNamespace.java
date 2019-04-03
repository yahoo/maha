// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup.namespace;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.MongoDocumentProcessor;
import org.apache.commons.lang.StringUtils;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Objects;

@JsonTypeName("mahamongo")
public class MongoExtractionNamespace implements OnlineDatastoreExtractionNamespace {
    private static final int DEFAULT_MONGO_CLIENT_RETRY_COUNT = 10;

    private final MongoStorageConnectorConfig connectorConfig;
    private final String collectionName;
    private final String tsColumn;
    private final Period pollPeriod;
    private boolean cacheEnabled = true;
    private final String lookupName;
    private final MongoDocumentProcessor documentProcessor;
    private final int mongoClientRetryCount;
    private final boolean tsColumnEpochInteger;

    private boolean firstTimeCaching = true;
    private long previousLastUpdateTime;
    private final ImmutableMap<String, Integer> columnIndexMap;

    @JsonCreator
    public MongoExtractionNamespace(
            @NotNull @JsonProperty(value = "connectorConfig", required = true) final MongoStorageConnectorConfig connectorConfig,
            @NotNull @JsonProperty(value = "collectionName", required = true) final String collectionName,
            @NotNull @JsonProperty(value = "tsColumn", required = true) final String tsColumn,
            @NotNull @JsonProperty(value = "tsColumnEpochInteger", required = true) final boolean tsColumnEpochInteger,
            @Min(0) @Nullable @JsonProperty(value = "pollPeriod", required = false) final Period pollPeriod,
            @JsonProperty(value = "cacheEnabled", required = false) final boolean cacheEnabled,
            @NotNull @JsonProperty(value = "lookupName", required = true) final String lookupName,
            @NotNull @JsonProperty(value = "documentProcessor", required = true) final MongoDocumentProcessor documentProcessor,
            @Nullable @JsonProperty(value = "mongoClientRetryCount", required = false) final Integer mongoClientRetryCount
    ) {
        this.connectorConfig = Preconditions.checkNotNull(connectorConfig, "connectorConfig");
        Preconditions.checkNotNull(connectorConfig.getConnectURI(), "connectorConfig.connectURI");
        this.documentProcessor = documentProcessor;
        Preconditions.checkArgument(
                StringUtils.isNotBlank(collectionName), "collectionName must be provided");
        Preconditions.checkArgument(
                StringUtils.isNotBlank(tsColumn), "tsColumn must be provided");
        if (mongoClientRetryCount == null || mongoClientRetryCount <= 0) {
            this.mongoClientRetryCount = DEFAULT_MONGO_CLIENT_RETRY_COUNT;
        } else {
            this.mongoClientRetryCount = mongoClientRetryCount;
        }
        this.collectionName = collectionName;
        this.tsColumn = tsColumn;
        this.tsColumnEpochInteger = tsColumnEpochInteger;
        this.pollPeriod = pollPeriod == null ? new Period(0L) : pollPeriod;
        this.cacheEnabled = cacheEnabled;
        this.lookupName = lookupName;
        int index = 0;
        ImmutableMap.Builder<String, Integer> builder = ImmutableMap.builder();
        List<String> columnList = documentProcessor.getColumnList();
        for (String col : columnList) {
            builder.put(col, index);
            index += 1;
        }
        this.columnIndexMap = builder.build();
    }

    @JsonIgnore
    public int getColumnIndex(String valueColumn) {
        if (columnIndexMap != null && valueColumn != null && columnIndexMap.containsKey(valueColumn)) {
            return columnIndexMap.get(valueColumn);
        }
        return -1;
    }

    @JsonIgnore
    public ImmutableMap<String, Integer> getColumnIndexMap() {
        return columnIndexMap;
    }

    public MongoStorageConnectorConfig getConnectorConfig() {
        return connectorConfig;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public List<String> getColumnList() {
        return documentProcessor.getColumnList();
    }

    public String getPrimaryKeyColumn() {
        return documentProcessor.getPrimaryKeyColumn();
    }

    public String getTsColumn() {
        return tsColumn;
    }

    public boolean isTsColumnEpochInteger() {
        return tsColumnEpochInteger;
    }

    public boolean isCacheEnabled() {
        return cacheEnabled;
    }

    @Override
    public long getPollMs() {
        return pollPeriod.toStandardDuration().getMillis();
    }

    @Override
    public String getLookupName() {
        return lookupName;
    }

    public MongoDocumentProcessor getDocumentProcessor() {
        return documentProcessor;
    }

    public int getMongoClientRetryCount() {
        return mongoClientRetryCount;
    }

    public boolean isFirstTimeCaching() {
        return firstTimeCaching;
    }

    private void setFirstTimeCaching(boolean value) {
        this.firstTimeCaching = value;
    }

    @JsonIgnore
    public long getPreviousLastUpdateTime() {
        return previousLastUpdateTime;
    }

    @JsonIgnore
    public void setPreviousLastUpdateTime(long previousLastUpdateTime) {
        this.previousLastUpdateTime = previousLastUpdateTime;
        setFirstTimeCaching(false);
    }

    @Override
    public String toString() {
        return "MongoExtractionNamespace{" +
                "connectorConfig=" + connectorConfig +
                ", collectionName='" + collectionName + '\'' +
                ", tsColumn='" + tsColumn + '\'' +
                ", pollPeriod=" + pollPeriod +
                ", cacheEnabled=" + cacheEnabled +
                ", lookupName='" + lookupName + '\'' +
                ", documentProcessor=" + documentProcessor +
                ", mongoClientRetryCount=" + mongoClientRetryCount +
                ", tsColumnEpochInteger=" + tsColumnEpochInteger +
                ", firstTimeCaching=" + firstTimeCaching +
                ", previousLastUpdateTime=" + previousLastUpdateTime +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MongoExtractionNamespace that = (MongoExtractionNamespace) o;
        return isCacheEnabled() == that.isCacheEnabled() &&
                getMongoClientRetryCount() == that.getMongoClientRetryCount() &&
                isTsColumnEpochInteger() == that.isTsColumnEpochInteger() &&
                Objects.equals(getConnectorConfig(), that.getConnectorConfig()) &&
                Objects.equals(getCollectionName(), that.getCollectionName()) &&
                Objects.equals(getTsColumn(), that.getTsColumn()) &&
                Objects.equals(pollPeriod, that.pollPeriod) &&
                Objects.equals(getLookupName(), that.getLookupName()) &&
                Objects.equals(getDocumentProcessor(), that.getDocumentProcessor());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getConnectorConfig(), getCollectionName(), getTsColumn(), pollPeriod, isCacheEnabled(), getLookupName(), getDocumentProcessor(), getMongoClientRetryCount(), isTsColumnEpochInteger());
    }
}

