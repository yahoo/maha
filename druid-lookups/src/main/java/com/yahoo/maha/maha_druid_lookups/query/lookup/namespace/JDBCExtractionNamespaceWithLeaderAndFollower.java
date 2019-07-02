// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup.namespace;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.metadata.MetadataStorageConnectorConfig;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Objects;

@JsonTypeName("mahajdbcleaderfollower")
public class JDBCExtractionNamespaceWithLeaderAndFollower extends JDBCExtractionNamespace {
    @JsonProperty
    private boolean cacheEnabled = true;

    private boolean firstTimeCaching = true;
    private Timestamp previousLastUpdateTimestamp;

    @JsonProperty
    private final String kafkaTopic;

    @JsonProperty
    private final boolean isLeader;

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
            @JsonProperty(value = "isLeader", required = true) final boolean isLeader
    ) {

        super(connectorConfig, table, columnList, primaryKeyColumn, tsColumn, pollPeriod, cacheEnabled, lookupName);

        this.kafkaTopic = Objects.nonNull(kafkaTopic) ? kafkaTopic : "unassigned";

        this.isLeader = Objects.nonNull(isLeader) ? isLeader : false;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public boolean getIsLeader() {
        return isLeader;
    }
}

