// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.druid.java.util.common.logger.Logger;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.OnlineDatastoreExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

abstract public class OnlineDatastoreLookupExtractor<U extends List<String>> extends MahaLookupExtractor {
    private final Map<String, U> map;
    private final OnlineDatastoreExtractionNamespace extractionNamespace;
    private LookupService lookupService;
    private final ImmutableMap<String, Integer> columnIndexMap;

    abstract protected Logger LOGGER();

    OnlineDatastoreLookupExtractor(OnlineDatastoreExtractionNamespace extractionNamespace, Map<String, U> map, LookupService lookupService) {
        this.extractionNamespace = extractionNamespace;
        this.map = Preconditions.checkNotNull(map, "map");
        this.lookupService = lookupService;
        this.columnIndexMap = extractionNamespace.getColumnIndexMap();
    }

    public Map<String, U> getMap() {
        return ImmutableMap.copyOf(map);
    }

    @Nullable
    public String apply(@NotNull String key, @NotNull String valueColumn, DecodeConfig decodeConfig, Map<String, String> dimensionOverrideMap) {
        try {

            if (key == null) {
                return null;
            }

            if (dimensionOverrideMap != null && dimensionOverrideMap.containsKey(key)) {
                return Strings.emptyToNull(dimensionOverrideMap.get(key));
            }

            if (!extractionNamespace.isCacheEnabled()) {
                Optional<DecodeConfig> decodeConfigOptional = (decodeConfig == null) ? Optional.empty() : Optional.of(decodeConfig);
                byte[] cacheByteValue = lookupService.lookup(new LookupService.LookupData(extractionNamespace,
                        key, valueColumn, decodeConfigOptional));
                return (cacheByteValue == null || cacheByteValue.length == 0) ? null : new String(cacheByteValue, UTF_8);
            } else {
                U cacheValueArray = map.get(key);
                if (cacheValueArray == null) {
                    return null;
                }
                if (decodeConfig != null) {
                    return handleDecode(cacheValueArray, decodeConfig);
                } else {
                    int columnIndex = getColumnIndex(valueColumn);
                    if (columnIndex < 0 || columnIndex >= cacheValueArray.size()) {
                        LOGGER().error("Invalid columnIndex [%s], cacheValueArray is [%s]", columnIndex, cacheValueArray);
                        return null;
                    }
                    return Strings.emptyToNull(cacheValueArray.get(columnIndex));
                }
            }

        } catch (Exception e) {
            LOGGER().error(e, "Exception in OnlineDatastoreLookupExtractor apply");
        }
        return null;
    }

    private String handleDecode(U row, DecodeConfig decodeConfig) {

        final int columnToCheckIndex = getColumnIndex(decodeConfig.getColumnToCheck());
        if (columnToCheckIndex < 0 || columnToCheckIndex >= row.size()) {
            return null;
        }

        final String valueFromColumnToCheck = row.get(columnToCheckIndex);

        if (valueFromColumnToCheck != null && valueFromColumnToCheck.equals(decodeConfig.getValueToCheck())) {
            final int columnIfValueMatchedIndex = getColumnIndex(decodeConfig.getColumnIfValueMatched());
            if (columnIfValueMatchedIndex < 0) {
                return null;
            }
            return Strings.emptyToNull(row.get(columnIfValueMatchedIndex));
        } else {
            final int columnIfValueNotMatchedIndex = getColumnIndex(decodeConfig.getColumnIfValueNotMatched());
            if (columnIfValueNotMatchedIndex < 0) {
                return null;
            }
            return Strings.emptyToNull(row.get(columnIfValueNotMatchedIndex));
        }
    }

    private int getColumnIndex(String valueColumn) {
        if (columnIndexMap.containsKey(valueColumn)) {
            return columnIndexMap.get(valueColumn);
        }
        return -1;
    }

    @Override
    public List<String> unapply(final String value) {
        return Lists.newArrayList(Maps.filterKeys(map, new Predicate<String>() {
            @Override
            public boolean apply(@Nullable String key) {
                return map.get(key).equals(Strings.nullToEmpty(value));
            }
        }).keySet());

    }

    @Override
    public byte[] getCacheKey() {
        try {
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            outputStream.write(extractionNamespace.toString().getBytes());
            outputStream.write((byte) 0xFF);
            return outputStream.toByteArray();
        } catch (IOException ex) {
            // If ByteArrayOutputStream.write has problems, that is a very bad thing
            throw Throwables.propagate(ex);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        OnlineDatastoreLookupExtractor that = (OnlineDatastoreLookupExtractor) o;

        return map.equals(that.map);
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

}
