// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup.namespace;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Properties;

@JsonTypeName("mahajdbc")
public class JDBCExtractionNamespace implements OnlineDatastoreExtractionNamespace {
    @JsonProperty
    private final MetadataStorageConnectorConfig connectorConfig;
    @JsonProperty
    private final String table;
    @JsonProperty
    private final String tsColumn;
    @JsonProperty
    private final Period pollPeriod;
    @JsonProperty
    private final ImmutableList<String> columnList;
    @JsonProperty
    private final String primaryKeyColumn;
    @JsonProperty
    private boolean cacheEnabled = true;
    @JsonProperty
    private final String lookupName;
    @JsonProperty
    private final Properties kerberosProperties;
    @JsonProperty
    private final TsColumnConfig tsColumnConfig;
    @JsonProperty
    private final boolean kerberosPropertiesEnabled;

    private boolean firstTimeCaching = true;
    private Timestamp previousLastUpdateTimestamp;
    private final ImmutableMap<String, Integer> columnIndexMap;

    @JsonCreator
    public JDBCExtractionNamespace(
            @NotNull @JsonProperty(value = "connectorConfig", required = true) final MetadataStorageConnectorConfig connectorConfig,
            @NotNull @JsonProperty(value = "table", required = true) final String table,
            @NotNull @JsonProperty(value = "columnList", required = true) final ArrayList<String> columnList,
            @NotNull @JsonProperty(value = "primaryKeyColumn", required = true) final String primaryKeyColumn,
            @Nullable @JsonProperty(value = "tsColumn", required = false) final String tsColumn,
            @Min(0) @Nullable @JsonProperty(value = "pollPeriod", required = false) final Period pollPeriod,
            @JsonProperty(value = "cacheEnabled", required = false) final boolean cacheEnabled,
            @NotNull @JsonProperty(value = "lookupName", required = true) final String lookupName,
            @JsonProperty(value = "kerberosProperties", required = false) final Properties kerberosProperties,
            @JsonProperty(value = "tsColumnConfig", required = false) final TsColumnConfig tsColumnConfig,
            @JsonProperty(value = "kerberosPropertiesEnabled", required = false) final boolean kerberosPropertiesEnabled
    ) {
        this.connectorConfig = Preconditions.checkNotNull(connectorConfig, "connectorConfig");
        Preconditions.checkNotNull(connectorConfig.getConnectURI(), "connectorConfig.connectURI");
        this.table = Preconditions.checkNotNull(table, "table");
        this.columnList = ImmutableList.copyOf(Preconditions.checkNotNull(columnList, "columnList"));
        this.primaryKeyColumn = Preconditions.checkNotNull(primaryKeyColumn, "primaryKeyColumn");
        this.tsColumn = tsColumn;
        this.pollPeriod = pollPeriod == null ? new Period(0L) : pollPeriod;
        this.cacheEnabled = cacheEnabled;
        this.lookupName = lookupName;
        this.kerberosProperties = kerberosProperties;
        this.tsColumnConfig = tsColumnConfig;
        this.kerberosPropertiesEnabled = kerberosPropertiesEnabled;
        int index = 0;
        ImmutableMap.Builder<String, Integer> builder = ImmutableMap.builder();
        for (String col : columnList) {
            builder.put(col, index);
            index += 1;
        }
        this.columnIndexMap = builder.build();
    }

    public JDBCExtractionNamespace(MetadataStorageConnectorConfig connectorConfig, String table, ArrayList<String> columnList, String primaryKeyColumn, String tsColumn, Period pollPeriod, boolean cacheEnabled, String lookupName) {
        this(connectorConfig, table, columnList, primaryKeyColumn, tsColumn, pollPeriod, cacheEnabled, lookupName, null, null, false);
    }

    public int getColumnIndex(String valueColumn) {
        if (columnIndexMap != null && valueColumn != null && columnIndexMap.containsKey(valueColumn)) {
            return columnIndexMap.get(valueColumn);
        }
        return -1;
    }

    public ImmutableMap<String, Integer> getColumnIndexMap() {
        return columnIndexMap;
    }

    public MetadataStorageConnectorConfig getConnectorConfig() {
        return connectorConfig;
    }

    public String getTable() {
        return table;
    }

    public ImmutableList<String> getColumnList() {
        return columnList;
    }

    public String getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }

    public String getTsColumn() {
        return tsColumn;
    }

    public boolean isCacheEnabled() {
        return cacheEnabled;
    }

    @Override
    public ExtractionNameSpaceSchemaType getType() {
        return ExtractionNameSpaceSchemaType.Protobuf;
    }

    @Override
    public long getPollMs() {
        return pollPeriod.toStandardDuration().getMillis();
    }

    @Override
    public String getLookupName() {
        return lookupName;
    }

    public Properties getKerberosProperties() {
        return kerberosProperties;
    }

    public boolean hasKerberosProperties() {
        return kerberosProperties != null && kerberosProperties.size() != 0;
    }

    public TsColumnConfig getTsColumnConfig() {
        return tsColumnConfig;
    }

    public boolean hasTsColumnConfig() {
        return tsColumnConfig != null;
    }

    public boolean hasSecondaryTsColumn() {
        return this.hasTsColumnConfig() && this.getTsColumnConfig().hasSecondaryTsColumn();
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

    public Period getPollPeriod() {
        return pollPeriod;
    }

    public boolean isKerberosPropertiesEnabled() {
        return kerberosPropertiesEnabled;
    }

    @Override
    public String toString() {
        return "JDBCExtractionNamespace{" +
                "connectorConfig=" + connectorConfig +
                ", table='" + table + '\'' +
                ", tsColumn='" + tsColumn + '\'' +
                ", pollPeriod=" + pollPeriod +
                ", columnList=" + columnList +
                ", primaryKeyColumn='" + primaryKeyColumn + '\'' +
                ", cacheEnabled=" + cacheEnabled +
                ", lookupName='" + lookupName + '\'' +
                ", firstTimeCaching=" + firstTimeCaching +
                ", previousLastUpdateTimestamp=" + previousLastUpdateTimestamp +
                ", kerberosProperties=" + kerberosProperties +
                ", tsColumnConfig=" + tsColumnConfig +
                ", kerberosPropertiesEnabled=" + kerberosPropertiesEnabled +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JDBCExtractionNamespace that = (JDBCExtractionNamespace) o;
        return isCacheEnabled() == that.isCacheEnabled() &&
                isKerberosPropertiesEnabled() == that.isKerberosPropertiesEnabled() &&
                Objects.equals(getConnectorConfig(), that.getConnectorConfig()) &&
                Objects.equals(getTable(), that.getTable()) &&
                Objects.equals(getTsColumn(), that.getTsColumn()) &&
                Objects.equals(pollPeriod, that.pollPeriod) &&
                Objects.equals(getColumnList(), that.getColumnList()) &&
                Objects.equals(getPrimaryKeyColumn(), that.getPrimaryKeyColumn()) &&
                Objects.equals(getLookupName(), that.getLookupName()) &&
                Objects.equals(getKerberosProperties(), that.getKerberosProperties()) &&
                Objects.equals(getTsColumnConfig(), that.getTsColumnConfig());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getConnectorConfig(), getTable(), getTsColumn(), pollPeriod, getColumnList(), getPrimaryKeyColumn(), isCacheEnabled(), getLookupName(), getKerberosProperties(), getTsColumnConfig(), isKerberosPropertiesEnabled());
    }
}

