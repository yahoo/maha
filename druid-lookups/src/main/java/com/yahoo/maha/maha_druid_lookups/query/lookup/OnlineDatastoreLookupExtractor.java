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
import com.yahoo.maha.maha_druid_lookups.query.lookup.util.LookupUtil;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.URIExtractionNamespaceCacheFactory;
import org.apache.druid.java.util.common.logger.Logger;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.OnlineDatastoreExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.LookupService;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

abstract public class OnlineDatastoreLookupExtractor<U extends List<String>> extends MahaLookupExtractor {
    private final Map<String, U> map;
    private final OnlineDatastoreExtractionNamespace extractionNamespace;
    private LookupService lookupService;
    private final ImmutableMap<String, Integer> columnIndexMap;
    private final LookupUtil util = new LookupUtil();

    private static final Logger log = new Logger(URIExtractionNamespaceCacheFactory.class);

    protected abstract Logger LOGGER();

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
    public String apply(@Nullable String key, @NotNull String valueColumn, DecodeConfig decodeConfig, Map<String, String> dimensionOverrideMap, Map<String, String> secondaryColOverrideMap) {
        try {
            util.overrideThrowableCheck(dimensionOverrideMap, secondaryColOverrideMap);

            if (key == null) {
                return null;
            }

            if (dimensionOverrideMap != null && dimensionOverrideMap.containsKey(key)) {
                return Strings.emptyToNull(dimensionOverrideMap.get(key));
            }

            String cacheableByteString;
            if (!extractionNamespace.isCacheEnabled()) {
                Optional<DecodeConfig> decodeConfigOptional = (decodeConfig == null) ? Optional.empty() : Optional.of(decodeConfig);
                byte[] cacheByteValue = lookupService.lookup(new LookupService.LookupData(extractionNamespace,
                        key, valueColumn, decodeConfigOptional));
                String cacheByteString = (cacheByteValue == null || cacheByteValue.length == 0) ? null : new String(cacheByteValue, UTF_8).trim();
                cacheableByteString = util.populateCacheStringFromOverride(cacheByteString, secondaryColOverrideMap);
            } else {
                U cacheValueArray = map.get(key);
                if (cacheValueArray == null) {
                    return null;
                }
                if (decodeConfig != null) {
                    cacheableByteString = handleDecode(cacheValueArray, decodeConfig);
                } else {
                    int columnIndex = getColumnIndex(valueColumn);
                    if (columnIndex < 0 || columnIndex >= cacheValueArray.size()) {
                        log.error("Invalid columnIndex [%s], cacheValueArray is [%s]", columnIndex, cacheValueArray);
                        return null;
                    }
                    String cacheByteString = Strings.emptyToNull(cacheValueArray.get(columnIndex));
                    cacheableByteString = util.populateCacheStringFromOverride(cacheByteString, secondaryColOverrideMap);
                }
            }

            return cacheableByteString;

        } catch (Exception e) {
            log.error(e, "Exception in OnlineDatastoreLookupExtractor apply");
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

    @Override
    public boolean canIterate() {
        return extractionNamespace.isCacheEnabled();
    }

    @Override
    public Iterable<Map.Entry<String, String>> iterable() {
        Map<String, String> tempMap = new HashMap<>();
        int numEntriesIterated = 0;
        try {
            for (Map.Entry<String, U> entry : getMap().entrySet()) {
                StringBuilder sb = new StringBuilder();
                for (Map.Entry<String, Integer> colToIndex : columnIndexMap.entrySet()) {
                    sb.append(colToIndex.getKey())
                            .append(ITER_KEY_VAL_SEPARATOR)
                            .append(entry.getValue().get(colToIndex.getValue()))
                            .append(ITER_VALUE_COL_SEPARATOR);
                }
                if (sb.length() > 0) {
                    sb.setLength(sb.length() - 1);
                }
                tempMap.put(entry.getKey(), sb.toString());
                numEntriesIterated++;
                if (numEntriesIterated == extractionNamespace.getNumEntriesIterator()) {
                    break;
                }
            }
        } catch (Exception e) {
            log.error(e, "Caught exception. Returning iterable to empty map.");
        }

        return tempMap.entrySet();
    }
}
