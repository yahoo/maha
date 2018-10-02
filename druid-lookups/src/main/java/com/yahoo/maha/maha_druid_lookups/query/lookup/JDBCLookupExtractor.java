// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.java.util.common.logger.Logger;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;
import io.druid.query.lookup.LookupExtractor;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

public class JDBCLookupExtractor<U extends List<String>> extends LookupExtractor
{
    private static final Logger LOG = new Logger(JDBCLookupExtractor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, U> map;
    private final JDBCExtractionNamespace extractionNamespace;
    private LookupService lookupService;
    private final Map<String, Integer> columnIndexMap;

    public JDBCLookupExtractor(JDBCExtractionNamespace extractionNamespace, Map<String, U> map, LookupService lookupService)
    {
        this.extractionNamespace = extractionNamespace;
        this.map = Preconditions.checkNotNull(map, "map");
        this.lookupService = lookupService;
        int index = 0;
        ImmutableMap.Builder<String, Integer> builder = ImmutableMap.builder();
        for(String col : extractionNamespace.getColumnList()) {
            builder.put(col, index);
            index += 1;
        }
        this.columnIndexMap = builder.build();
    }

    public Map<String, U> getMap()
    {
        return ImmutableMap.copyOf(map);
    }

    @Nullable
    @Override
    public String apply(@NotNull String val)
    {

        try {

            if("".equals(Strings.nullToEmpty(val))) {
                return null;
            }

            MahaLookupQueryElement mahaLookupQueryElement = objectMapper.readValue(val, MahaLookupQueryElement.class);
            String dimension = Strings.nullToEmpty(mahaLookupQueryElement.getDimension());
            String valueColumn = mahaLookupQueryElement.getValueColumn();
            DecodeConfig decodeConfig = mahaLookupQueryElement.getDecodeConfig();
            Map<String, String> dimensionOverrideMap = mahaLookupQueryElement.getDimensionOverrideMap();

            if(dimensionOverrideMap != null && dimensionOverrideMap.containsKey(dimension)) {
                return Strings.emptyToNull(dimensionOverrideMap.get(dimension));
            }

            if (!extractionNamespace.isCacheEnabled()) {
                Optional<DecodeConfig> decodeConfigOptional = (decodeConfig == null) ? Optional.empty() : Optional.of(decodeConfig);
                byte[] cacheByteValue = lookupService.lookup(new LookupService.LookupData(extractionNamespace,
                        dimension, valueColumn, decodeConfigOptional));
                return (cacheByteValue == null || cacheByteValue.length == 0) ? null : new String(cacheByteValue, UTF_8);
            } else {
                U cacheValueArray = map.get(dimension);
                if(cacheValueArray == null) {
                    return null;
                }
                if(decodeConfig != null) {
                    return handleDecode(cacheValueArray, decodeConfig);
                }
                else {
                    int columnIndex = getColumnIndex(extractionNamespace, valueColumn);
                    if (columnIndex < 0 || columnIndex >= cacheValueArray.size()) {
                        LOG.error("Invalid columnIndex [%s], cacheValueArray is [%s]", columnIndex, cacheValueArray);
                        return null;
                    }
                    return Strings.emptyToNull(cacheValueArray.get(columnIndex));
                }
            }

        } catch(Exception e) {
            LOG.error(e, "Exception in JDBCLookupExtractor apply");
        }
        return null;
    }

    private String handleDecode(U row, DecodeConfig decodeConfig) {

        final int columnToCheckIndex = getColumnIndex(extractionNamespace, decodeConfig.getColumnToCheck());
        if (columnToCheckIndex < 0 || columnToCheckIndex >= row.size() ) {
            return null;
        }

        final String valueFromColumnToCheck = row.get(columnToCheckIndex);

        if(valueFromColumnToCheck != null && valueFromColumnToCheck.equals(decodeConfig.getValueToCheck())) {
            final int columnIfValueMatchedIndex = getColumnIndex(extractionNamespace, decodeConfig.getColumnIfValueMatched());
            if (columnIfValueMatchedIndex < 0) {
                return null;
            }
            return Strings.emptyToNull(row.get(columnIfValueMatchedIndex));
        } else {
            final int columnIfValueNotMatchedIndex = getColumnIndex(extractionNamespace, decodeConfig.getColumnIfValueNotMatched());
            if (columnIfValueNotMatchedIndex < 0) {
                return null;
            }
            return Strings.emptyToNull(row.get(columnIfValueNotMatchedIndex));
        }
    }

    private int getColumnIndex(final JDBCExtractionNamespace extractionNamespace, String valueColumn) {
        if(columnIndexMap.containsKey(valueColumn)) {
            return columnIndexMap.get(valueColumn);
        }
        return -1;
    }

    @Override
    public List<String> unapply(final String value)
    {
        return Lists.newArrayList(Maps.filterKeys(map, new Predicate<String>()
        {
            @Override public boolean apply(@Nullable String key)
            {
                return map.get(key).equals(Strings.nullToEmpty(value));
            }
        }).keySet());

    }

    @Override
    public byte[] getCacheKey()
    {
        try {
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            for (Map.Entry<String, U> entry : map.entrySet()) {
                final String key = entry.getKey();
                if (!Strings.isNullOrEmpty(key)) {
                    outputStream.write(key.getBytes());
                }
                outputStream.write((byte)0xFF);
            }
            return outputStream.toByteArray();
        }
        catch (IOException ex) {
            // If ByteArrayOutputStream.write has problems, that is a very bad thing
            throw Throwables.propagate(ex);
        }
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

        JDBCLookupExtractor that = (JDBCLookupExtractor) o;

        return map.equals(that.map);
    }

    @Override
    public int hashCode()
    {
        return map.hashCode();
    }

}
