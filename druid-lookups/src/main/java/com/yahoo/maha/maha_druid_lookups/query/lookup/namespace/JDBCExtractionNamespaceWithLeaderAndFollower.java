// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup.namespace;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Properties;

@JsonTypeName("mahajdbcleaderfollower")
public class JDBCExtractionNamespaceWithLeaderAndFollower extends JDBCExtractionNamespace {
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
            @JsonProperty(value = "kafkaProperties", required = true) final Properties kafkaProperties,
            @JsonProperty(value = "kerberosProperties", required = false) final Properties kerberosProperties,
            @JsonProperty(value = "mTLSProperties", required = false) final Properties mTLSProperties,
            @JsonProperty(value = "tsColumnConfig", required = false) final TsColumnConfig tsColumnConfig,
            @JsonProperty(value = "kerberosPropertiesEnabled", required = false) final boolean kerberosPropertiesEnabled,
            @JsonProperty(value = "mTLSPropertiesEnabled", required = false) final boolean mTLSPropertiesEnabled,
            @JsonProperty(value = "numEntriesIterator", required = false) final int numEntriesIterator
            ) {
        super(connectorConfig, table, columnList, primaryKeyColumn, tsColumn, pollPeriod, cacheEnabled, lookupName, kerberosProperties, mTLSProperties, tsColumnConfig, kerberosPropertiesEnabled, mTLSPropertiesEnabled, numEntriesIterator, null);

        this.kafkaTopic = Objects.nonNull(kafkaTopic) ? kafkaTopic : "unassigned";

        this.isLeader = Objects.nonNull(isLeader) ? isLeader : false;

        this.kafkaProperties = kafkaProperties;

        this.setPreviousLastUpdateTimestamp(new Timestamp(0L));
    }

    public JDBCExtractionNamespaceWithLeaderAndFollower(
            final MetadataStorageConnectorConfig connectorConfig,
            final String table,
            final ArrayList<String> columnList,
            final String primaryKeyColumn,
            final String tsColumn,
            final Period pollPeriod,
            final boolean cacheEnabled,
            final String lookupName,
            final String kafkaTopic,
            final boolean isLeader,
            final Properties kafkaProperties
    ) {
        this(connectorConfig, table, columnList, primaryKeyColumn, tsColumn, pollPeriod, cacheEnabled, lookupName, kafkaTopic, isLeader, kafkaProperties, null, null, null, false, false, 0);
    }

    @Override
    public String toString() {
        return "JDBCExtractionNamespaceWithLeaderAndFollower{" +
                "connectorConfig=" + getConnectorConfig() +
                ", table='" + getTable() + '\'' +
                ", tsColumn='" + getTsColumn() + '\'' +
                ", pollPeriod=" + getPollPeriod() +
                ", columnList=" + getColumnList() +
                ", primaryKeyColumn='" + getPrimaryKeyColumn() + '\'' +
                ", cacheEnabled=" + isCacheEnabled() +
                ", lookupName='" + getLookupName() + '\'' +
                ", firstTimeCaching=" + isFirstTimeCaching() +
                ", previousLastUpdateTimestamp=" + getPreviousLastUpdateTimestamp() +
                ", kafkaProperties" + kafkaProperties.toString() +
                ", isLeader=" + isLeader +
                ", kerberosProperties=" + getKerberosProperties() +
                ", tsColumnConfig=" + getTsColumnConfig() +
                '}';
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public boolean getIsLeader() {
        return isLeader;
    }

    public Properties getKafkaProperties() { return kafkaProperties; }

}

