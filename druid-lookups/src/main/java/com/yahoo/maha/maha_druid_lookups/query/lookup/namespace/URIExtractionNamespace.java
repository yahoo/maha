/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
// Copyright 2022, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup.namespace;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.yahoo.maha.maha_druid_lookups.query.lookup.util.LookupUtil;
import org.apache.druid.java.util.common.IAE;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CSVParser;

import org.apache.druid.java.util.common.parsers.Parser;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.net.URI;
import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

@JsonTypeName("mahauri")
public class URIExtractionNamespace implements OnlineDatastoreExtractionNamespace {
    private static final Logger LOG = new Logger(URIExtractionNamespace.class);
    private static final LookupUtil util = new LookupUtil();

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
    private final TsColumnConfig tsColumnConfig;
    @JsonProperty
    private final String nullReplacement;

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
            @Nullable @JsonProperty(value = "columnList", required = false) final ArrayList<String> columnList,
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
        this.tsColumnConfig = tsColumnConfig;

        int index = 0;
        ImmutableMap.Builder<String, Integer> builder = ImmutableMap.builder();
        for (String col : this.namespaceParseSpec.getColumns()) {
            builder.put(col, index);
            index += 1;
        }
        this.columnIndexMap = builder.build();
        this.nullReplacement = this.namespaceParseSpec.getNullReplacement() == null ? "" : this.namespaceParseSpec.getNullReplacement();
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

    private static class DelegateParser implements Parser<String, List<String>>
    {
        private final Parser<String, Object> delegate;
        private final String key;
        private final String nullReplacement;

        private DelegateParser(
                Parser<String, Object> delegate,
                String key,
                String nullReplacement
        )
        {
            this.delegate = delegate;
            this.key = key;
            this.nullReplacement = nullReplacement;
        }

        @Override
        public Map<String, List<String>> parseToMap(String input)
        {
            final Map<String, Object> inner = delegate.parseToMap(input);
            if (null == inner) {
                // Skip null or missing values, treat them as if there were no row at all.
                return ImmutableMap.of();
            }

            final String k = inner.get(key) == null ? nullReplacement : inner.get(key).toString();

            return new HashMap<String, List<String>>(){{put(k, new ArrayList(inner.values()));}};
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
            @JsonSubTypes.Type(name = "csv", value = CSVFlatDataParser.class)
    })
    public interface FlatDataParser
    {
        Parser<String, List<String>> getParser();
        List<String> getColumns();
        String getKey();
        String getNullReplacement();
    }

    @JsonTypeName("csv")
    public static class CSVFlatDataParser implements FlatDataParser
    {
        private final Parser<String, List<String>> parser;
        private final List<String> columns;
        private final String keyColumn;
        private final String nullReplacement;

        @JsonCreator
        public CSVFlatDataParser(
                @JsonProperty("columns") List<String> columns,
                @JsonProperty("keyColumn") final String keyColumn,
                @JsonProperty("hasHeaderRow") boolean hasHeaderRow,
                @JsonProperty("skipHeaderRows") int skipHeaderRows,
                @JsonProperty("nullReplacement") String nullReplacement
        )
        {
            Preconditions.checkArgument(
                    Preconditions.checkNotNull(columns, "`columns` list required").size() > 1,
                    "Must specify more than one column to have a key value pair"
            );

            this.columns = columns;
            this.keyColumn = Strings.isNullOrEmpty(keyColumn) ? columns.get(0) : keyColumn;
            this.nullReplacement = nullReplacement == null ? "" : nullReplacement;
            Preconditions.checkArgument(
                    columns.contains(this.keyColumn),
                    "Column [%s] not found int columns: %s",
                    this.keyColumn,
                    Arrays.toString(columns.toArray())
            );
            CSVParser csvParser = new CSVParser(null, columns, hasHeaderRow, skipHeaderRows);
            csvParser.startFileFromBeginning();
            this.parser = new DelegateParser(
                    csvParser,
                    this.keyColumn,
                    this.nullReplacement
            );
        }

        @JsonProperty
        public List<String> getColumns() { return columns; }

        @JsonProperty
        public String getKey() { return keyColumn; }

        @JsonProperty
        public String getNullReplacement() { return nullReplacement; }

        @VisibleForTesting
        CSVFlatDataParser(
                List<String> columns,
                String keyColumn,
                String nullReplacement
        )
        {
            this(columns, keyColumn, false, 0, nullReplacement);
        }

        @VisibleForTesting
        CSVFlatDataParser(
                List<String> columns,
                String keyColumn
        )
        {
            this(columns, keyColumn, false, 0, "");
        }

        //@JsonProperty
        //public List<String> getColumns()
        //{
        //    return columns;
        //}

        @JsonProperty
        public String getKeyColumn()
        {
            return this.keyColumn;
        }

        @Override
        public Parser<String, List<String>> getParser()
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
                    Objects.equals(keyColumn, that.keyColumn);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(columns, keyColumn);
        }

        @Override
        public String toString()
        {
            return "CSVFlatDataParser{" +
                    "columns=" + columns +
                    ", keyColumn='" + keyColumn + '\'' +
                    '}';
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
        return namespaceParseSpec.getKey();
    }

    @Override
    public ImmutableMap<String, Integer> getColumnIndexMap() {
        return columnIndexMap;
    }

    @Override
    public ImmutableList<String> getColumnList() {
        return ImmutableList.copyOf(namespaceParseSpec.getColumns());
    }

    @Override
    public int getNumEntriesIterator() {
        return 0;
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

    @Override
    public String nullReplacement() {
        return nullReplacement;
    }
}

