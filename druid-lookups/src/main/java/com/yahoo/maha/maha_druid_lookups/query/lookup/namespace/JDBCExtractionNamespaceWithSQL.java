package com.yahoo.maha.maha_druid_lookups.query.lookup.namespace;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Properties;

@JsonTypeName("mahajdbcsql")
public class JDBCExtractionNamespaceWithSQL extends JDBCExtractionNamespace {

    private final String columnExtractionSQL;

    @JsonCreator
    public JDBCExtractionNamespaceWithSQL(
            @NotNull @JsonProperty(value = "connectorConfig", required = true) final MetadataStorageConnectorConfig connectorConfig,
            @NotNull @JsonProperty(value = "columnExtractionSQL", required = true) final String columnExtractionSQL,
            @NotNull @JsonProperty(value = "primaryKeyColumn", required = true) final String primaryKeyColumn,
            @NotNull @JsonProperty(value = "columnList", required = true) final ArrayList<String> columnList,
            @Min(0) @Nullable @JsonProperty(value = "pollPeriod", required = false) final Period pollPeriod,
            @JsonProperty(value = "cacheEnabled", required = false) final boolean cacheEnabled,
            @NotNull @JsonProperty(value = "lookupName", required = true) final String lookupName,
            @JsonProperty(value = "kerberosProperties", required = false) final Properties kerberosProperties,
            @JsonProperty(value = "mTLSProperties", required = false) final Properties mTLSProperties,
            @JsonProperty(value = "tsColumnConfig", required = false) final TsColumnConfig tsColumnConfig,
            @JsonProperty(value = "kerberosPropertiesEnabled", required = false) final boolean kerberosPropertiesEnabled,
            @JsonProperty(value = "mTLSPropertiesEnabled", required = false) final boolean mTLSPropertiesEnabled,
            @JsonProperty(value = "numEntriesIterator", required = false) final int numEntriesIterator
    ) {
        super(connectorConfig, lookupName, columnList, primaryKeyColumn, null, pollPeriod, cacheEnabled, lookupName);
        this.columnExtractionSQL = Preconditions.checkNotNull(columnExtractionSQL, "columnExtractionSQL");
    }

    public String getColumnExtractionSQL() {
        return columnExtractionSQL;
    }

    @Override
    public String toString() {
        return "JDBCExtractionNamespaceWithSQL{" +
                "connectorConfig=" + getConnectorConfig() +
                ", pollPeriod=" + getPollPeriod() +
                ", columnExtractionSQL='" + getColumnExtractionSQL() + '\'' +
                ", primaryKeyColumn='" + getPrimaryKeyColumn() + '\'' +
                ", cacheEnabled=" + isCacheEnabled() +
                ", lookupName='" + getLookupName() + '\'' +
                ", firstTimeCaching=" + isFirstTimeCaching() +
                ", previousLastUpdateTimestamp=" + getPreviousLastUpdateTimestamp() +
                ", kerberosProperties=" + getKerberosProperties() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JDBCExtractionNamespaceWithSQL that = (JDBCExtractionNamespaceWithSQL) o;
        return isCacheEnabled() == that.isCacheEnabled() &&
                isKerberosPropertiesEnabled() == that.isKerberosPropertiesEnabled() &&
                Objects.equals(getConnectorConfig(), that.getConnectorConfig()) &&
                Objects.equals(getPollPeriod(), that.getPollPeriod()) &
                Objects.equals(getPrimaryKeyColumn(), that.getPrimaryKeyColumn()) &&
                Objects.equals(getLookupName(), that.getLookupName()) &&
                Objects.equals(getKerberosProperties(), that.getKerberosProperties()) &&
                Objects.equals(getColumnExtractionSQL(), that.getColumnExtractionSQL())
                ;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getConnectorConfig(), getPollPeriod(), getColumnExtractionSQL(), getPrimaryKeyColumn(), isCacheEnabled(), getLookupName(), getKerberosProperties(), isKerberosPropertiesEnabled());
    }
}
