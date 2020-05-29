package com.yahoo.maha.maha_druid_lookups.query.lookup.namespace;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.metadata.MetadataStorageConnectorConfig;

import javax.validation.constraints.NotNull;
import java.util.Objects;

public class MissingLookupConfig {
    @JsonProperty
    private final MetadataStorageConnectorConfig connectorConfig;
    @JsonProperty
    private final String table;
    @JsonProperty
    private final String primaryKeyColumn;
    @JsonProperty
    private final String missingLookupKafkaTopic;

    @JsonCreator
    public MissingLookupConfig(
            @NotNull @JsonProperty(value = "connectorConfig", required = true)
            final MetadataStorageConnectorConfig connectorConfig,
            @NotNull @JsonProperty(value = "table", required = true)
            final String table,
            @NotNull @JsonProperty(value = "primaryKeyColumn", required = true)
            final String primaryKeyColumn,
            @NotNull @JsonProperty(value = "missingLookupKafkaTopic", required = true)
            final String missingLookupKafkaTopic
    )
    {
        this.connectorConfig = Preconditions.checkNotNull(connectorConfig, "connectorConfig");
        Preconditions.checkNotNull(connectorConfig.getConnectURI(), "connectorConfig.connectURI");
        this.table = Preconditions.checkNotNull(table, "table");
        this.primaryKeyColumn = Preconditions.checkNotNull(primaryKeyColumn, "primaryKeyColumn");
        this.missingLookupKafkaTopic = Preconditions.checkNotNull(missingLookupKafkaTopic, "primaryKeyColumn");
    }

    public MetadataStorageConnectorConfig getConnectorConfig() {
        return connectorConfig;
    }

    public String getTable() {
        return table;
    }

    public String getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }

    public String getMissingLookupKafkaTopic() {
        return missingLookupKafkaTopic;
    }

    @Override
    public String toString() {
        return "MissingLookupConfig{" +
                "connectorConfig=" + connectorConfig +
                ", table='" + table + '\'' +
                ", primaryKeyColumn='" + primaryKeyColumn + '\'' +
                ", missingLookupKafkaTopic='" + missingLookupKafkaTopic + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MissingLookupConfig that = (MissingLookupConfig) o;
        return Objects.equals(connectorConfig, that.connectorConfig) &&
                Objects.equals(table, that.table) &&
                Objects.equals(primaryKeyColumn, that.primaryKeyColumn) &&
                Objects.equals(missingLookupKafkaTopic, that.missingLookupKafkaTopic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectorConfig, table, primaryKeyColumn, missingLookupKafkaTopic);
    }
}
