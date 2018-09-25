// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup.namespace;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.druid.metadata.MetadataStorageConnectorConfig;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Map;

@JsonTypeName("mahajdbc")
public class JDBCExtractionNamespace implements ExtractionNamespace
{
    @JsonProperty
    private final MetadataStorageConnectorConfig connectorConfig;
    @JsonProperty
    private final String table;
    @JsonProperty
    private final String tsColumn;
    @JsonProperty
    private final Period pollPeriod;
    @JsonProperty
    private final ArrayList<String> columnList;
    @JsonProperty
    private final String primaryKeyColumn;
    @JsonProperty
    private boolean cacheEnabled = true;
    @JsonProperty
    private final String lookupName;

    private boolean firstTimeCaching = true;
    private Timestamp previousLastUpdateTimestamp;
    private final Map<String, Integer> columnIndexMap;

    @JsonCreator
    public JDBCExtractionNamespace(
            @NotNull @JsonProperty(value = "connectorConfig", required = true)
            final MetadataStorageConnectorConfig connectorConfig,
            @NotNull @JsonProperty(value = "table", required = true)
            final String table,
            @NotNull @JsonProperty(value = "columnList", required = true)
                    final ArrayList<String> columnList,
            @NotNull @JsonProperty(value = "primaryKeyColumn", required = true)
                    final String primaryKeyColumn,
            @Nullable @JsonProperty(value = "tsColumn", required = false)
            final String tsColumn,
            @Min(0) @Nullable @JsonProperty(value = "pollPeriod", required = false)
            final Period pollPeriod,
            @JsonProperty(value = "cacheEnabled", required = false)
            final boolean cacheEnabled,
            @NotNull @JsonProperty(value = "lookupName", required = true)
            final String lookupName
    )
    {
        this.connectorConfig = Preconditions.checkNotNull(connectorConfig, "connectorConfig");
        Preconditions.checkNotNull(connectorConfig.getConnectURI(), "connectorConfig.connectURI");
        this.table = Preconditions.checkNotNull(table, "table");
        this.columnList = Preconditions.checkNotNull(columnList, "columnList");
        this.primaryKeyColumn = Preconditions.checkNotNull(primaryKeyColumn, "primaryKeyColumn");
        this.tsColumn = tsColumn;
        this.pollPeriod = pollPeriod == null ? new Period(0L) : pollPeriod;
        this.cacheEnabled = cacheEnabled;
        this.lookupName = lookupName;
        int index = 0;
        ImmutableMap.Builder<String, Integer> builder = ImmutableMap.builder();
        for(String col : columnList) {
            builder.put(col, index);
            index += 1;
        }
        this.columnIndexMap = builder.build();
    }

    public int getColumnIndex(String valueColumn) {
        if(columnIndexMap != null && valueColumn != null && columnIndexMap.containsKey(valueColumn)) {
            return columnIndexMap.get(valueColumn);
        }
        return -1;
    }

    public MetadataStorageConnectorConfig getConnectorConfig()
    {
        return connectorConfig;
    }

    public String getTable()
    {
        return table;
    }

    public ArrayList<String> getColumnList() {
        return columnList;
    }

    public String getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }

    public String getTsColumn()
    {
        return tsColumn;
    }

    public boolean isCacheEnabled() {
        return cacheEnabled;
    }

    @Override
    public long getPollMs()
    {
        return pollPeriod.toStandardDuration().getMillis();
    }

    @Override
    public String getLookupName() {
        return lookupName;
    }

    public boolean isFirstTimeCaching() {
        return firstTimeCaching;
    }

    public void setFirstTimeCaching(boolean value) {
        this.firstTimeCaching = value;
    }

    public Timestamp getPreviousLastUpdateTimestamp() {
        return previousLastUpdateTimestamp;
    }

    public void setPreviousLastUpdateTimestamp(Timestamp previousLastUpdateTimestamp) {
        this.previousLastUpdateTimestamp = previousLastUpdateTimestamp;
    }

    @Override
    public String toString()
    {
        return String.format(
                "CdwJDBCExtractionNamespace = { connectorConfig = { %s }, table = %s, primaryKeyColumn = %s, columnList = %s, tsColumn = %s, pollPeriod = %s, lookupName = %s}",
                connectorConfig.toString(),
                table,
                primaryKeyColumn,
                columnList,
                tsColumn,
                pollPeriod,
                lookupName
        );
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JDBCExtractionNamespace that = (JDBCExtractionNamespace) o;

        if (!columnList.equals(that.columnList)) {
            return false;
        }
        if (!table.equals(that.table)) {
            return false;
        }
        if (!primaryKeyColumn.equals(that.primaryKeyColumn)) {
            return false;
        }
        if (tsColumn != null ? !tsColumn.equals(that.tsColumn) : that.tsColumn != null) {
            return false;
        }
        if(!lookupName.equals(that.lookupName)) {
            return false;
        }
        return pollPeriod.equals(that.pollPeriod);

    }

    @Override
    public int hashCode()
    {
        int result = connectorConfig.hashCode();
        result = 31 * result + table.hashCode();
        result = 31 * result + primaryKeyColumn.hashCode();
        result = 31 * result + columnList.hashCode();
        result = 31 * result + (tsColumn != null ? tsColumn.hashCode() : 0);
        result = 31 * result + lookupName.hashCode();
        result = 31 * result + pollPeriod.hashCode();
        return result;
    }
}

