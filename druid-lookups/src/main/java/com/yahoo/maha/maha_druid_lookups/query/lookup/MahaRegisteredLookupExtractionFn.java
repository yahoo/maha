// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.lookup.*;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Strings;

@JsonTypeName("mahaRegisteredLookup")
public class MahaRegisteredLookupExtractionFn implements ExtractionFn {
    private static final Logger LOG = new Logger(MahaRegisteredLookupExtractionFn.class);
    // Protected for moving to not-null by `delegateLock`
    private volatile MahaLookupExtractionFn delegate = null;
    private final Object delegateLock = new Object();
    private final LookupReferencesManager manager;
    private final String lookup;
    private final boolean retainMissingValue;
    private final String replaceMissingValueWith;
    private final boolean injective;
    private final boolean optimize;
    private final String valueColumn;
    private final DecodeConfig decodeConfig;
    private final Map<String, String> dimensionOverrideMap;
    private final Map<String, String> secondaryColOverrideMap;
    private final boolean useQueryLevelCache;
    volatile Cache<String, String> cache = null;
    private final Object cacheLock = new Object();

    @JsonCreator
    public MahaRegisteredLookupExtractionFn(
            @JacksonInject LookupReferencesManager manager,
            @JsonProperty("lookup") String lookup,
            @JsonProperty("retainMissingValue") final boolean retainMissingValue,
            @Nullable @JsonProperty("replaceMissingValueWith") final String replaceMissingValueWith,
            @JsonProperty("injective") final boolean injective,
            @JsonProperty("optimize") Boolean optimize,
            @NotNull @JsonProperty("valueColumn") String valueColumn,
            @Nullable @JsonProperty("decode") DecodeConfig decodeConfig,
            @Nullable @JsonProperty("dimensionOverrideMap") Map<String, String> dimensionOverrideMap,
            @Nullable @JsonProperty("secondaryColOverrideMap") Map<String, String> secondaryColOverrideMap,
            @Nullable @JsonProperty("useQueryLevelCache") Boolean useQueryLevelCache
    ) {
        Preconditions.checkArgument(lookup != null, "`lookup` required");
        Preconditions.checkArgument(valueColumn != null, "`valueColumn` required");
        this.manager = manager;
        this.replaceMissingValueWith = replaceMissingValueWith;
        this.retainMissingValue = retainMissingValue;
        this.injective = injective;
        this.optimize = optimize == null ? true : optimize;
        this.lookup = lookup;
        this.valueColumn = valueColumn;
        this.decodeConfig = decodeConfig;
        this.dimensionOverrideMap = dimensionOverrideMap;
        this.secondaryColOverrideMap = secondaryColOverrideMap;
        this.useQueryLevelCache = useQueryLevelCache == null ? false : useQueryLevelCache;
    }

    @JsonProperty("lookup")
    public String getLookup() {
        return lookup;
    }

    @JsonProperty("retainMissingValue")
    public boolean isRetainMissingValue() {
        return retainMissingValue;
    }

    @JsonProperty("replaceMissingValueWith")
    public String getReplaceMissingValueWith() {
        return replaceMissingValueWith;
    }

    @JsonProperty("injective")
    public boolean isInjective() {
        return injective;
    }

    @JsonProperty("optimize")
    public boolean isOptimize() {
        return optimize;
    }

    @JsonProperty("valueColumn")
    public String getValueColumn() {
        return valueColumn;
    }

    @JsonProperty("decode")
    public DecodeConfig getDecodeConfig() {
        return decodeConfig;
    }

    @JsonProperty("dimensionOverrideMap")
    public Map<String, String> getDimensionOverrideMap() {
        return dimensionOverrideMap;
    }

    @JsonProperty("secondaryColOverrideMap")
    public Map<String, String> getSecondaryColOverrideMap() {
        return secondaryColOverrideMap;
    }

    @JsonProperty("useQueryLevelCache")
    public boolean isUseQueryLevelCache() {
        return useQueryLevelCache;
    }

    @Override
    public byte[] getCacheKey() {
        try {
            final byte[] keyPrefix = getClass().getCanonicalName().getBytes();
            final byte[] lookupBytes = getLookup().getBytes();
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            outputStream.write(keyPrefix);
            outputStream.write(0xFF);
            outputStream.write(lookupBytes);
            if (getValueColumn() != null) {
                outputStream.write(getValueColumn().getBytes());
            }
            outputStream.write(0xFF);
            if (getDecodeConfig() != null) {
                outputStream.write(getDecodeConfig().getCacheKey());
            }
            outputStream.write(0xFF);
            if (getDimensionOverrideMap() != null) {
                outputStream.write(getDimensionOverrideMap().hashCode());
            }
            outputStream.write(0xFF);
            if (getSecondaryColOverrideMap() != null) {
                outputStream.write(getSecondaryColOverrideMap().hashCode());
            }
            outputStream.write(0xFF);
            outputStream.write(isUseQueryLevelCache() ? 1 : 0);
            outputStream.write(0xFF);
            outputStream.write(ensureDelegate().getCacheKey());
            return outputStream.toByteArray();
        } catch (IOException ex) {
            // If ByteArrayOutputStream.write has problems, that is a very bad thing
            throw Throwables.propagate(ex);
        }
    }

    @Override
    public String apply(Object value) {
        return apply(Objects.toString(value, null));
    }

    @Override
    public String apply(String value) {
        if ("".equals(Strings.nullToEmpty(value)))
            return null;
        if (isUseQueryLevelCache()) {
            Cache<String, String> cache = ensureCache();
            String cachedResult = cache.getIfPresent(value);
            if (cachedResult != null) {
                return cachedResult;
            } else {
                String result = ensureDelegate().apply(value);
                if (result != null) {
                    cache.put(value, result);
                }
                return result;
            }
        } else {
            return ensureDelegate().apply(value);
        }
    }

//    String getSerializedLookupQueryElement(String value) {
//        String serializedElement = "";
//        try {
//            MahaLookupQueryElement mahaLookupQueryElement = new MahaLookupQueryElement();
//            mahaLookupQueryElement.setDimension(value);
//            mahaLookupQueryElement.setValueColumn(valueColumn);
//            mahaLookupQueryElement.setDecodeConfig(decodeConfig);
//            mahaLookupQueryElement.setDimensionOverrideMap(dimensionOverrideMap);
//            serializedElement = objectMapper.writeValueAsString(mahaLookupQueryElement);
//        } catch (JsonProcessingException e) {
//            LOG.error(e, e.getMessage());
//        }
//        return serializedElement;
//    }

    @Override
    public String apply(long value) {
        return ensureDelegate().apply(value);
    }

    @Override
    public boolean preservesOrdering() {
        return ensureDelegate().preservesOrdering();
    }

    @Override
    public ExtractionType getExtractionType() {
        return ensureDelegate().getExtractionType();
    }

    Cache<String, String> ensureCache() {
        if (null == cache) {
            synchronized (cacheLock) {
                if (null == cache) {
                    this.cache = Caffeine
                            .newBuilder()
                            .maximumSize(10_000)
                            .expireAfterWrite(5, TimeUnit.MINUTES)
                            .build();
                }
            }
        }
        return cache;
    }

    private MahaLookupExtractionFn ensureDelegate() {
        if (null == delegate) {
            // http://www.javamex.com/tutorials/double_checked_locking.shtml
            synchronized (delegateLock) {
                if (null == delegate) {
                    Optional<LookupExtractorFactoryContainer> lookupExtractorFactoryContainerOptional =  manager.get(getLookup());

                    if (!lookupExtractorFactoryContainerOptional.isPresent()) {
                       throw new IllegalStateException(String.format("Lookup [%s] not found", getLookup()));
                    }
                    delegate = new MahaLookupExtractionFn(
                            lookupExtractorFactoryContainerOptional.get().getLookupExtractorFactory().get()
                            , isRetainMissingValue()
                            , getReplaceMissingValueWith()
                            , isInjective()
                            , isOptimize()
                            , valueColumn
                            , decodeConfig
                            , dimensionOverrideMap
                            , secondaryColOverrideMap
                    );
                }
            }
        }
        return delegate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MahaRegisteredLookupExtractionFn that = (MahaRegisteredLookupExtractionFn) o;

        if (isRetainMissingValue() != that.isRetainMissingValue()) {
            return false;
        }
        if (isInjective() != that.isInjective()) {
            return false;
        }
        if (isOptimize() != that.isOptimize()) {
            return false;
        }
        if (!getLookup().equals(that.getLookup())) {
            return false;
        }

        if (isUseQueryLevelCache() != that.isUseQueryLevelCache()) {
            return false;
        }

        return getReplaceMissingValueWith() != null
                ? getReplaceMissingValueWith().equals(that.getReplaceMissingValueWith())
                : that.getReplaceMissingValueWith() == null;
    }

    @Override
    public int hashCode() {
        int result = getLookup().hashCode();
        result = 31 * result + (isRetainMissingValue() ? 1 : 0);
        result = 31 * result + (getReplaceMissingValueWith() != null ? getReplaceMissingValueWith().hashCode() : 0);
        result = 31 * result + (isInjective() ? 1 : 0);
        result = 31 * result + (isOptimize() ? 1 : 0);
        result = 31 * result + (isUseQueryLevelCache() ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MahaRegisteredLookupExtractionFn{" +
                "delegate=" + delegate +
                ", lookup='" + lookup + '\'' +
                ", retainMissingValue=" + retainMissingValue +
                ", replaceMissingValueWith='" + replaceMissingValueWith + '\'' +
                ", injective=" + injective +
                ", optimize=" + optimize +
                ", valueColumn=" + valueColumn +
                ", decodeConfig=" + decodeConfig +
                ", useQueryLevelCache=" + useQueryLevelCache +
                '}';
    }

    public static MahaRegisteredLookupExtractionFn constructForExtractionDimensionSpec(
            String lookup,
            final boolean retainMissingValue,
            final String replaceMissingValueWith,
            final boolean injective,
            Boolean optimize,
            String valueColumn,
            DecodeConfig decodeConfig,
            Map<String, String> dimensionOverrideMap,
            Map<String, String> secondaryColOverrideMap,
            Boolean useQueryLevelCache
    ) {
        return new MahaRegisteredLookupExtractionFn(null, lookup, retainMissingValue, replaceMissingValueWith
                , injective, optimize, valueColumn, decodeConfig, dimensionOverrideMap, secondaryColOverrideMap
                , useQueryLevelCache);
    }

}
