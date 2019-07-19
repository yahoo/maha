// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup.namespace;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.ProtobufSchemaFactory;
import io.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.commons.lang.ArrayUtils;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;

@JsonTypeName("mahajdbcleaderfollower")
public class JDBCExtractionNamespaceWithLeaderAndFollower extends JDBCExtractionNamespace {
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

    private boolean firstTimeCaching = true;
    private Timestamp previousLastUpdateTimestamp;
    private final ImmutableMap<String, Integer> columnIndexMap;

    @JsonProperty
    private final String kafkaTopic;

    @JsonProperty
    private final boolean isLeader;

    @JsonProperty
    private final Properties kafkaProperties;

    @JsonCreator
    public JDBCExtractionNamespaceWithLeaderAndFollower(
            @NotNull @JsonProperty(value = "connectorConfig", required = true) final MetadataStorageConnectorConfig connectorConfig,
            @NotNull @JsonProperty(value = "table", required = true) final String table,
            @NotNull @JsonProperty(value = "columnList", required = true) final ArrayList<String> columnList,
            @NotNull @JsonProperty(value = "primaryKeyColumn", required = true) final String primaryKeyColumn,
            @Nullable @JsonProperty(value = "tsColumn", required = false) final String tsColumn,
            @Min(0) @Nullable @JsonProperty(value = "pollPeriod", required = false) final Period pollPeriod,
            @JsonProperty(value = "cacheEnabled", required = false) final boolean cacheEnabled,
            @NotNull @JsonProperty(value = "lookupName", required = true) final String lookupName,
            @JsonProperty(value = "kafkaTopic", required = true) final String kafkaTopic,
            @JsonProperty(value = "isLeader", required = true) final boolean isLeader,
            @JsonProperty(value = "kafkaProperties", required = true) final Properties kafkaProperties
            ) {
        super(connectorConfig, table, columnList, primaryKeyColumn, tsColumn, pollPeriod, cacheEnabled, lookupName);

        this.connectorConfig = Preconditions.checkNotNull(connectorConfig, "connectorConfig");
        Preconditions.checkNotNull(connectorConfig.getConnectURI(), "connectorConfig.connectURI");
        this.table = Preconditions.checkNotNull(table, "table");
        ArrayList<String> allColumnsWithTS = columnList;
        allColumnsWithTS.add(tsColumn);
        this.columnList = ImmutableList.copyOf(Preconditions.checkNotNull(allColumnsWithTS, "columnList"));
        this.primaryKeyColumn = Preconditions.checkNotNull(primaryKeyColumn, "primaryKeyColumn");
        this.tsColumn = tsColumn;
        this.pollPeriod = pollPeriod == null ? new Period(0L) : pollPeriod;
        this.cacheEnabled = cacheEnabled;
        this.lookupName = lookupName;
        int index = 0;
        ImmutableMap.Builder<String, Integer> builder = ImmutableMap.builder();
        for (String col : columnList) {
            builder.put(col, index);
            index += 1;
        }

        this.columnIndexMap = builder.build();

        this.kafkaTopic = Objects.nonNull(kafkaTopic) ? kafkaTopic : "unassigned";

        this.isLeader = Objects.nonNull(isLeader) ? isLeader : false;

        this.kafkaProperties = kafkaProperties;

        this.setPreviousLastUpdateTimestamp(new Timestamp(0L));
    }

    @Override
    public String toString() {
        return "JDBCExtractionNamespaceWithLeaderAndFollower{" +
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
                ", kafkaProperties" + kafkaProperties.toString() +
                ", isLeader=" + isLeader +
                ", kafkaTopic=" + kafkaTopic +
                '}';
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public boolean getIsLeader() {
        return isLeader;
    }

    @Override
    public ImmutableList<String> getColumnList() {
        return columnList;
    }

    @Override
    public void setPreviousLastUpdateTimestamp(Timestamp previousLastUpdateTimestamp) {
        this.previousLastUpdateTimestamp = previousLastUpdateTimestamp;
    }

    @Override
    public Timestamp getPreviousLastUpdateTimestamp() {
        return previousLastUpdateTimestamp;
    }

    @Override
    public void setFirstTimeCaching(boolean value) {
        this.firstTimeCaching = value;
    }

    @Override
    public boolean isFirstTimeCaching() {
        return firstTimeCaching;
    }

    public Properties getKafkaProperties() { return kafkaProperties; }

}

