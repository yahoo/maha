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
import com.metamx.common.logger.Logger;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;
import io.druid.query.lookup.LookupExtractor;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class JDBCLookupExtractor extends LookupExtractor
{
    private static final Logger LOG = new Logger(JDBCLookupExtractor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, String> map;
    private final JDBCExtractionNamespace extractionNamespace;
    private static final String CONTROL_A_SEPARATOR = "\u0001";
    private LookupService lookupService;
    private final Map<String, Integer> columnIndexMap;

    public JDBCLookupExtractor(JDBCExtractionNamespace extractionNamespace, Map<String, String> map, LookupService lookupService)
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

    public Map<String, String> getMap()
    {
        return ImmutableMap.copyOf(map);
    }

    @Nullable
    @Override
    public String apply(@NotNull String val)
    {

        try {

            if (Strings.isNullOrEmpty(val) || !val.contains(CONTROL_A_SEPARATOR)) {
                return null;
            }

            String[] values = val.split(CONTROL_A_SEPARATOR);
            String cacheValue;
            if (!extractionNamespace.isCacheEnabled()) {
                cacheValue = new String(lookupService.lookup(new LookupService.LookupData(extractionNamespace,
                        values[0])), "UTF-8");
            } else {
                cacheValue = map.get(values[0]);
            }

            if (Strings.isNullOrEmpty(cacheValue)) {
                return null;
            }

            List<String> row = Arrays.asList(cacheValue.split(CONTROL_A_SEPARATOR));
            if(values.length == 3) {
                DecodeConfig decodeConfig = objectMapper.readValue(values[2], DecodeConfig.class);
                return handleDecode(row, decodeConfig);
            }
            else {
                int columnIndex = getColumnIndex(extractionNamespace, values[1]);
                if (columnIndex < 0) {
                    return null;
                }
                return Strings.emptyToNull(row.get(columnIndex));
            }
        } catch(Exception e) {
            LOG.error(e, "Exception in CdwJDBCLookupExtractor apply");
        }
        return null;
    }

    private String handleDecode(List<String> row, DecodeConfig decodeConfig) {

        final int columnToCheckIndex = getColumnIndex(extractionNamespace, decodeConfig.getColumnToCheck());
        if (columnToCheckIndex < 0) {
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
            for (Map.Entry<String, String> entry : map.entrySet()) {
                final String key = entry.getKey();
                final String val = entry.getValue();
                if (!Strings.isNullOrEmpty(key)) {
                    outputStream.write(key.getBytes());
                }
                outputStream.write((byte)0xFF);
                if (!Strings.isNullOrEmpty(val)) {
                    outputStream.write(val.getBytes());
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
