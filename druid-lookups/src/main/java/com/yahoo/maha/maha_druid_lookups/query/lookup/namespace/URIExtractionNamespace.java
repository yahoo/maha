// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup.namespace;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CSVParser;
import org.apache.druid.java.util.common.parsers.DelimitedParser;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathParser;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.Parser;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.net.URI;
import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

@JsonTypeName("mahauri")
public class URIExtractionNamespace implements OnlineDatastoreExtractionNamespace {
    private static final Logger LOG = new Logger(URIExtractionNamespace.class);

    long DEFAULT_MAX_HEAP_PERCENTAGE = 10;

    @JsonProperty
    private final URI uri;
    @JsonProperty
    private final URI uriPrefix;
    @JsonProperty
    private final FlatDataParser namespaceParseSpec;
    @JsonProperty
    private final String fileRegex;
    @JsonProperty
    private final Period pollPeriod;
    @JsonProperty
    private final Long maxHeapPercentage;
    @JsonProperty
    private final String lookupName;
    @JsonProperty
    private final String tsColumn;
    @JsonProperty
    private boolean cacheEnabled = true;
    @JsonProperty
    private final String primaryKeyColumn;
    @JsonProperty
    private final ImmutableList<String> columnList;
    @JsonProperty
    private final TsColumnConfig tsColumnConfig;

    private final ImmutableMap<String, Integer> columnIndexMap;
    private Timestamp previousLastUpdateTimestamp;

    @JsonCreator
    public URIExtractionNamespace(
            @JsonProperty(value = "uri", required = false)
                    URI uri,
            @JsonProperty(value = "uriPrefix", required = false)
                    URI uriPrefix,
            @JsonProperty(value = "fileRegex", required = false)
                    String fileRegex,
            @JsonProperty(value = "namespaceParseSpec", required = true)
                    FlatDataParser namespaceParseSpec,
            @Min(0) @JsonProperty(value = "pollPeriod", required = false) @Nullable
                    Period pollPeriod,
            @Deprecated
            @JsonProperty(value = "versionRegex", required = false)
                    String versionRegex,
            @JsonProperty(value = "maxHeapPercentage") @Nullable
                    Long maxHeapPercentage,
            @NotNull @JsonProperty(value = "lookupName", required = true) final String lookupName,
            @Nullable @JsonProperty(value = "tsColumn", required = false) final String tsColumn,
            @JsonProperty(value = "cacheEnabled", required = false) final boolean cacheEnabled,
            @NotNull @JsonProperty(value = "primaryKeyColumn", required = true) final String primaryKeyColumn,
            @NotNull @JsonProperty(value = "columnList", required = true) final ArrayList<String> columnList,
            @JsonProperty(value = "tsColumnConfig", required = false) final TsColumnConfig tsColumnConfig

            )
    {
        this.uri = uri;
        this.uriPrefix = uriPrefix;
        if ((uri != null) == (uriPrefix != null)) {
            throw new IAE("Either uri xor uriPrefix required");
        }
        this.namespaceParseSpec = Preconditions.checkNotNull(namespaceParseSpec, "namespaceParseSpec");
        if (pollPeriod == null) {
            // Warning because if URIExtractionNamespace is being used for lookups, any updates to the database will not
            // be picked up after the node starts. So for use casses where nodes start at different times (like streaming
            // ingestion with peons) there can be data inconsistencies across the cluster.
            LOG.warn("No pollPeriod configured for URIExtractionNamespace - entries will be loaded only once at startup");
            this.pollPeriod = Period.ZERO;
        } else {
            this.pollPeriod = pollPeriod;
        }
        this.fileRegex = fileRegex == null ? versionRegex : fileRegex;
        if (fileRegex != null && versionRegex != null) {
            throw new IAE("Cannot specify both versionRegex and fileRegex. versionRegex is deprecated");
        }

        if (uri != null && this.fileRegex != null) {
            throw new IAE("Cannot define both uri and fileRegex");
        }

        if (this.fileRegex != null) {
            try {
                Pattern.compile(this.fileRegex);
            }
            catch (PatternSyntaxException ex) {
                throw new IAE(ex, "Could not parse `fileRegex` [%s]", this.fileRegex);
            }
        }
        this.maxHeapPercentage = maxHeapPercentage == null ? DEFAULT_MAX_HEAP_PERCENTAGE : maxHeapPercentage;
        this.lookupName = lookupName;
        this.tsColumn = tsColumn;
        this.cacheEnabled = cacheEnabled;
        this.primaryKeyColumn = Preconditions.checkNotNull(primaryKeyColumn, "primaryKeyColumn");
        this.columnList = ImmutableList.copyOf(Preconditions.checkNotNull(columnList, "columnList"));
        this.tsColumnConfig = tsColumnConfig;

        int index = 0;
        ImmutableMap.Builder<String, Integer> builder = ImmutableMap.builder();
        for (String col : columnList) {
            builder.put(col, index);
            index += 1;
        }
        this.columnIndexMap = builder.build();
    }

    public String getFileRegex()
    {
        return fileRegex;
    }

    public FlatDataParser getNamespaceParseSpec()
    {
        return this.namespaceParseSpec;
    }

    public URI getURI()
    {
        return uri;
    }

    public URI getURIPrefix()
    {
        return uriPrefix;
    }

    @Override
    public long getPollMs()
    {
        return pollPeriod.toStandardDuration().getMillis();
    }

    @Override
    public String toString()
    {
        return "URIExtractionNamespace{" +
                "uri=" + uri +
                ", uriPrefix=" + uriPrefix +
                ", namespaceParseSpec=" + namespaceParseSpec +
                ", fileRegex='" + fileRegex + '\'' +
                ", pollPeriod=" + pollPeriod +
                ", maxHeapPercentage=" + maxHeapPercentage +
                ", lookupName=" + lookupName +
                '}';
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

        URIExtractionNamespace that = (URIExtractionNamespace) o;

        if (getURI() != null ? !getURI().equals(that.getURI()) : that.getURI() != null) {
            return false;
        }
        if (getURIPrefix() != null ? !getURIPrefix().equals(that.getURIPrefix()) : that.getURIPrefix() != null) {
            return false;
        }
        if (!getNamespaceParseSpec().equals(that.getNamespaceParseSpec())) {
            return false;
        }
        if (getFileRegex() != null ? !getFileRegex().equals(that.getFileRegex()) : that.getFileRegex() != null) {
            return false;
        }
        return pollPeriod.equals(that.pollPeriod) &&
                Objects.equals(maxHeapPercentage, that.maxHeapPercentage);

    }

    @Override
    public int hashCode()
    {
        int result = getURI() != null ? getURI().hashCode() : 0;
        result = 31 * result + (getURIPrefix() != null ? getURIPrefix().hashCode() : 0);
        result = 31 * result + getNamespaceParseSpec().hashCode();
        result = 31 * result + (getFileRegex() != null ? getFileRegex().hashCode() : 0);
        result = 31 * result + pollPeriod.hashCode();
        result = 31 * result * Objects.hashCode(maxHeapPercentage);
        return result;
    }

    private static class DelegateParser implements Parser<String, String>
    {
        private final Parser<String, Object> delegate;
        private final String key;
        private final String value;

        private DelegateParser(
                Parser<String, Object> delegate,
                @NotNull String key,
                @NotNull String value
        )
        {
            this.delegate = delegate;
            this.key = key;
            this.value = value;
        }

        @Override
        public Map<String, String> parseToMap(String input)
        {
            final Map<String, Object> inner = delegate.parseToMap(input);
            if (null == inner) {
                // Skip null or missing values, treat them as if there were no row at all.
                return ImmutableMap.of();
            }
            final String k = Preconditions.checkNotNull(
                    inner.get(key),
                    "Key column [%s] missing data in line [%s]",
                    key,
                    input
            ).toString(); // Just in case is long
            final Object val = inner.get(value);
            if (val == null) {
                // Skip null or missing values, treat them as if there were no row at all.
                return ImmutableMap.of();
            }
            return ImmutableMap.of(k, val.toString());
        }

        @Override
        public void setFieldNames(Iterable<String> fieldNames)
        {
            delegate.setFieldNames(fieldNames);
        }

        @Override
        public List<String> getFieldNames()
        {
            return delegate.getFieldNames();
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "format")
    @JsonSubTypes(value = {
            @JsonSubTypes.Type(name = "csv", value = CSVFlatDataParser.class),
            @JsonSubTypes.Type(name = "tsv", value = TSVFlatDataParser.class),
            @JsonSubTypes.Type(name = "customJson", value = JSONFlatDataParser.class),
            @JsonSubTypes.Type(name = "simpleJson", value = ObjectMapperFlatDataParser.class)
    })
    public interface FlatDataParser
    {
        Parser<String, String> getParser();
    }

    @JsonTypeName("csv")
    public static class CSVFlatDataParser implements FlatDataParser
    {
        private final Parser<String, String> parser;
        private final List<String> columns;
        private final String keyColumn;
        private final String valueColumn;

        @JsonCreator
        public CSVFlatDataParser(
                @JsonProperty("columns") List<String> columns,
                @JsonProperty("keyColumn") final String keyColumn,
                @JsonProperty("valueColumn") final String valueColumn,
                @JsonProperty("hasHeaderRow") boolean hasHeaderRow,
                @JsonProperty("skipHeaderRows") int skipHeaderRows
        )
        {
            Preconditions.checkArgument(
                    Preconditions.checkNotNull(columns, "`columns` list required").size() > 1,
                    "Must specify more than one column to have a key value pair"
            );

            Preconditions.checkArgument(
                    !(Strings.isNullOrEmpty(keyColumn) ^ Strings.isNullOrEmpty(valueColumn)),
                    "Must specify both `keyColumn` and `valueColumn` or neither `keyColumn` nor `valueColumn`"
            );
            this.columns = columns;
            this.keyColumn = Strings.isNullOrEmpty(keyColumn) ? columns.get(0) : keyColumn;
            this.valueColumn = Strings.isNullOrEmpty(valueColumn) ? columns.get(1) : valueColumn;
            Preconditions.checkArgument(
                    columns.contains(this.keyColumn),
                    "Column [%s] not found int columns: %s",
                    this.keyColumn,
                    Arrays.toString(columns.toArray())
            );
            Preconditions.checkArgument(
                    columns.contains(this.valueColumn),
                    "Column [%s] not found int columns: %s",
                    this.valueColumn,
                    Arrays.toString(columns.toArray())
            );
            CSVParser csvParser = new CSVParser(null, columns, hasHeaderRow, skipHeaderRows);
            csvParser.startFileFromBeginning();
            this.parser = new DelegateParser(
                    csvParser,
                    this.keyColumn,
                    this.valueColumn
            );
        }

        @VisibleForTesting
        CSVFlatDataParser(
                List<String> columns,
                String keyColumn,
                String valueColumn
        )
        {
            this(columns, keyColumn, valueColumn, false, 0);
        }

        @JsonProperty
        public List<String> getColumns()
        {
            return columns;
        }

        @JsonProperty
        public String getKeyColumn()
        {
            return this.keyColumn;
        }

        @JsonProperty
        public String getValueColumn()
        {
            return this.valueColumn;
        }

        @Override
        public Parser<String, String> getParser()
        {
            return parser;
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final CSVFlatDataParser that = (CSVFlatDataParser) o;
            return Objects.equals(columns, that.columns) &&
                    Objects.equals(keyColumn, that.keyColumn) &&
                    Objects.equals(valueColumn, that.valueColumn);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(columns, keyColumn, valueColumn);
        }

        @Override
        public String toString()
        {
            return "CSVFlatDataParser{" +
                    "columns=" + columns +
                    ", keyColumn='" + keyColumn + '\'' +
                    ", valueColumn='" + valueColumn + '\'' +
                    '}';
        }
    }

    @JsonTypeName("tsv")
    public static class TSVFlatDataParser implements FlatDataParser
    {
        private final Parser<String, String> parser;
        private final List<String> columns;
        private final String delimiter;
        private final String listDelimiter;
        private final String keyColumn;
        private final String valueColumn;

        @JsonCreator
        public TSVFlatDataParser(
                @JsonProperty("columns") List<String> columns,
                @JsonProperty("delimiter") String delimiter,
                @JsonProperty("listDelimiter") String listDelimiter,
                @JsonProperty("keyColumn") final String keyColumn,
                @JsonProperty("valueColumn") final String valueColumn,
                @JsonProperty("hasHeaderRow") boolean hasHeaderRow,
                @JsonProperty("skipHeaderRows") int skipHeaderRows
        )
        {
            Preconditions.checkArgument(
                    Preconditions.checkNotNull(columns, "`columns` list required").size() > 1,
                    "Must specify more than one column to have a key value pair"
            );
            final DelimitedParser delegate = new DelimitedParser(
                    StringUtils.emptyToNullNonDruidDataString(delimiter),
                    StringUtils.emptyToNullNonDruidDataString(listDelimiter),
                    hasHeaderRow,
                    skipHeaderRows
            );
            delegate.startFileFromBeginning();
            Preconditions.checkArgument(
                    !(Strings.isNullOrEmpty(keyColumn) ^ Strings.isNullOrEmpty(valueColumn)),
                    "Must specify both `keyColumn` and `valueColumn` or neither `keyColumn` nor `valueColumn`"
            );
            delegate.setFieldNames(columns);
            this.columns = columns;
            this.delimiter = delimiter;
            this.listDelimiter = listDelimiter;
            this.keyColumn = Strings.isNullOrEmpty(keyColumn) ? columns.get(0) : keyColumn;
            this.valueColumn = Strings.isNullOrEmpty(valueColumn) ? columns.get(1) : valueColumn;
            Preconditions.checkArgument(
                    columns.contains(this.keyColumn),
                    "Column [%s] not found int columns: %s",
                    this.keyColumn,
                    columns
            );
            Preconditions.checkArgument(
                    columns.contains(this.valueColumn),
                    "Column [%s] not found int columns: %s",
                    this.valueColumn,
                    columns
            );

            this.parser = new DelegateParser(delegate, this.keyColumn, this.valueColumn);
        }

        @VisibleForTesting
        TSVFlatDataParser(
                List<String> columns,
                String delimiter,
                String listDelimiter,
                String keyColumn,
                String valueColumn
        )
        {
            this(columns, delimiter, listDelimiter, keyColumn, valueColumn, false, 0);
        }

        @JsonProperty
        public List<String> getColumns()
        {
            return columns;
        }

        @JsonProperty
        public String getKeyColumn()
        {
            return this.keyColumn;
        }

        @JsonProperty
        public String getValueColumn()
        {
            return this.valueColumn;
        }

        @JsonProperty
        public String getListDelimiter()
        {
            return listDelimiter;
        }

        @JsonProperty
        public String getDelimiter()
        {
            return delimiter;
        }

        @Override
        public Parser<String, String> getParser()
        {
            return parser;
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final TSVFlatDataParser that = (TSVFlatDataParser) o;
            return Objects.equals(columns, that.columns) &&
                    Objects.equals(delimiter, that.delimiter) &&
                    Objects.equals(listDelimiter, that.listDelimiter) &&
                    Objects.equals(keyColumn, that.keyColumn) &&
                    Objects.equals(valueColumn, that.valueColumn);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(columns, delimiter, listDelimiter, keyColumn, valueColumn);
        }

        @Override
        public String toString()
        {
            return "TSVFlatDataParser{" +
                    "columns=" + columns +
                    ", delimiter='" + delimiter + '\'' +
                    ", listDelimiter='" + listDelimiter + '\'' +
                    ", keyColumn='" + keyColumn + '\'' +
                    ", valueColumn='" + valueColumn + '\'' +
                    '}';
        }
    }

    @JsonTypeName("customJson")
    public static class JSONFlatDataParser implements FlatDataParser
    {
        private final Parser<String, String> parser;
        private final String keyFieldName;
        private final String valueFieldName;

        @JsonCreator
        public JSONFlatDataParser(
                @JacksonInject @Json ObjectMapper jsonMapper,
                @JsonProperty("keyFieldName") final String keyFieldName,
                @JsonProperty("valueFieldName") final String valueFieldName
        )
        {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(keyFieldName), "[keyFieldName] cannot be empty");
            Preconditions.checkArgument(!Strings.isNullOrEmpty(valueFieldName), "[valueFieldName] cannot be empty");
            this.keyFieldName = keyFieldName;
            this.valueFieldName = valueFieldName;

            // Copy jsonMapper; don't want to share canonicalization tables, etc., with the global ObjectMapper.
            this.parser = new DelegateParser(
                    new JSONPathParser(
                            new JSONPathSpec(
                                    false,
                                    ImmutableList.of(
                                            new JSONPathFieldSpec(JSONPathFieldType.ROOT, keyFieldName, keyFieldName),
                                            new JSONPathFieldSpec(JSONPathFieldType.ROOT, valueFieldName, valueFieldName)
                                    )
                            ),
                            jsonMapper.copy(),
                            false
                    ),
                    keyFieldName,
                    valueFieldName
            );
        }

        @JsonProperty
        public String getKeyFieldName()
        {
            return this.keyFieldName;
        }

        @JsonProperty
        public String getValueFieldName()
        {
            return this.valueFieldName;
        }

        @Override
        public Parser<String, String> getParser()
        {
            return this.parser;
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final JSONFlatDataParser that = (JSONFlatDataParser) o;
            return Objects.equals(keyFieldName, that.keyFieldName) &&
                    Objects.equals(valueFieldName, that.valueFieldName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(keyFieldName, valueFieldName);
        }

        @Override
        public String toString()
        {
            return "JSONFlatDataParser{" +
                    "keyFieldName='" + keyFieldName + '\'' +
                    ", valueFieldName='" + valueFieldName + '\'' +
                    '}';
        }
    }

    @JsonTypeName("simpleJson")
    public static class ObjectMapperFlatDataParser implements FlatDataParser
    {
        private final Parser<String, String> parser;

        @JsonCreator
        public ObjectMapperFlatDataParser(
                final @JacksonInject @Json ObjectMapper jsonMapper
        )
        {
            // There's no point canonicalizing field names, we expect them to all be unique.
            final JsonFactory jsonFactory = jsonMapper.getFactory().copy();
            jsonFactory.configure(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES, false);

            parser = new Parser<String, String>()
            {
                @Override
                public Map<String, String> parseToMap(String input)
                {
                    try {
                        return jsonFactory.createParser(input).readValueAs(JacksonUtils.TYPE_REFERENCE_MAP_STRING_STRING);
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void setFieldNames(Iterable<String> fieldNames)
                {
                    throw new UOE("No field names available");
                }

                @Override
                public List<String> getFieldNames()
                {
                    throw new UOE("No field names available");
                }
            };
        }

        @Override
        public Parser<String, String> getParser()
        {
            return parser;
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

            return true;
        }

        @Override
        public int hashCode()
        {
            return 0;
        }

        @Override
        public String toString()
        {
            return "ObjectMapperFlatDataParser{}";
        }
    }

    @Override
    public String getLookupName() {
        return lookupName;
    }

    @Override
    public String getTsColumn() { return tsColumn; }

    @Override
    public boolean isCacheEnabled() {
        return cacheEnabled;
    }

    @Override
    public boolean isDynamicSchemaLookup() {
        return false;
    }

    @Override
    public ExtractionNameSpaceSchemaType getSchemaType() {
        return ExtractionNameSpaceSchemaType.None;
    }

    @Override
    public String getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }

    @Override
    public ImmutableMap<String, Integer> getColumnIndexMap() {
        return columnIndexMap;
    }

    @Override
    public ImmutableList<String> getColumnList() {
        return columnList;
    }

    public int getColumnIndex(String valueColumn) {
        if (columnIndexMap != null && valueColumn != null && columnIndexMap.containsKey(valueColumn)) {
            return columnIndexMap.get(valueColumn);
        }
        return -1;
    }

    public Timestamp getPreviousLastUpdateTimestamp() {
        return previousLastUpdateTimestamp;
    }

    public void setPreviousLastUpdateTimestamp(Timestamp previousLastUpdateTimestamp) {
        this.previousLastUpdateTimestamp = previousLastUpdateTimestamp;
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

    public URI getUri()
    {
        return uri;
    }

    public URI getUriPrefix()
    {
        return uriPrefix;
    }


}

