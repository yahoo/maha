// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.lookup.LookupReferencesManager;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

@JsonTypeName("mahaRegisteredLookup")
public class MahaRegisteredLookupExtractionFn implements ExtractionFn
{
    // Protected for moving to not-null by `delegateLock`
    private volatile MahaLookupExtractionFn delegate = null;
    private final Object delegateLock = new Object();
    private final LookupReferencesManager manager;
    private final ObjectMapper objectMapper;
    private final String lookup;
    private final boolean retainMissingValue;
    private final String replaceMissingValueWith;
    private final boolean injective;
    private final boolean optimize;
    private final String valueColumn;
    private final DecodeConfig decodeConfig;
    private static final String CONTROL_A_SEPARATOR = "\u0001";

    @JsonCreator
    public MahaRegisteredLookupExtractionFn(
            @JacksonInject LookupReferencesManager manager,
            @JacksonInject ObjectMapper objectMapper,
            @JsonProperty("lookup") String lookup,
            @JsonProperty("retainMissingValue") final boolean retainMissingValue,
            @Nullable @JsonProperty("replaceMissingValueWith") final String replaceMissingValueWith,
            @JsonProperty("injective") final boolean injective,
            @JsonProperty("optimize") Boolean optimize,
            @Nullable @JsonProperty("valueColumn") String valueColumn,
            @Nullable @JsonProperty("decode") DecodeConfig decodeConfig
    )
    {
        Preconditions.checkArgument(lookup != null, "`lookup` required");
        this.manager = manager;
        this.objectMapper = objectMapper;
        this.replaceMissingValueWith = replaceMissingValueWith;
        this.retainMissingValue = retainMissingValue;
        this.injective = injective;
        this.optimize = optimize == null ? true : optimize;
        this.lookup = lookup;
        this.valueColumn = valueColumn;
        this.decodeConfig = decodeConfig;
    }

    @JsonProperty("lookup")
    public String getLookup()
    {
        return lookup;
    }

    @JsonProperty("retainMissingValue")
    public boolean isRetainMissingValue()
    {
        return retainMissingValue;
    }

    @JsonProperty("replaceMissingValueWith")
    public String getReplaceMissingValueWith()
    {
        return replaceMissingValueWith;
    }

    @JsonProperty("injective")
    public boolean isInjective()
    {
        return injective;
    }

    @JsonProperty("optimize")
    public boolean isOptimize()
    {
        return optimize;
    }

    @JsonProperty("valueColumn")
    public String getValueColumn()
    {
        return valueColumn;
    }

    @JsonProperty("decode")
    public DecodeConfig getDecodeConfig()
    {
        return decodeConfig;
    }

    @Override
    public byte[] getCacheKey()
    {
        final byte[] keyPrefix = getClass().getCanonicalName().getBytes();
        final byte[] lookupName = getLookup().getBytes();
        final byte[] delegateKey = ensureDelegate().getCacheKey();
        return ByteBuffer
                .allocate(keyPrefix.length + 1 + lookupName.length + 1 + delegateKey.length)
                .put(keyPrefix).put((byte) 0xFF)
                .put(lookupName).put((byte) 0xFF)
                .put(delegateKey)
                .array();
    }

    @Override
    public String apply(Object value)
    {
        return ensureDelegate().apply(value);
    }

    @Override
    public String apply(String value)
    {
        String serializedValue = value + CONTROL_A_SEPARATOR + valueColumn;
        if(decodeConfig != null) {
            try {
                serializedValue = serializedValue + CONTROL_A_SEPARATOR + objectMapper.writeValueAsString(decodeConfig);
            } catch (JsonProcessingException e) {
            }
        }
        return ensureDelegate().apply(serializedValue);
    }

    @Override
    public String apply(long value)
    {
        return ensureDelegate().apply(value);
    }

    @Override
    public boolean preservesOrdering()
    {
        return ensureDelegate().preservesOrdering();
    }

    @Override
    public ExtractionType getExtractionType()
    {
        return ensureDelegate().getExtractionType();
    }

    private MahaLookupExtractionFn ensureDelegate()
    {
        if (null == delegate) {
            // http://www.javamex.com/tutorials/double_checked_locking.shtml
            synchronized (delegateLock) {
                if (null == delegate) {
                    delegate = new MahaLookupExtractionFn(
                            Preconditions.checkNotNull(manager.get(getLookup()), "Lookup [%s] not found", getLookup()).getLookupExtractorFactory().get(),
                            isRetainMissingValue(),
                            getReplaceMissingValueWith(),
                            isInjective(),
                            isOptimize()
                    );
                }
            }
        }
        return delegate;
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
        return getReplaceMissingValueWith() != null
                ? getReplaceMissingValueWith().equals(that.getReplaceMissingValueWith())
                : that.getReplaceMissingValueWith() == null;
    }

    @Override
    public int hashCode()
    {
        int result = getLookup().hashCode();
        result = 31 * result + (isRetainMissingValue() ? 1 : 0);
        result = 31 * result + (getReplaceMissingValueWith() != null ? getReplaceMissingValueWith().hashCode() : 0);
        result = 31 * result + (isInjective() ? 1 : 0);
        result = 31 * result + (isOptimize() ? 1 : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "MahaRegisteredLookupExtractionFn{" +
                "delegate=" + delegate +
                ", lookup='" + lookup + '\'' +
                ", retainMissingValue=" + retainMissingValue +
                ", replaceMissingValueWith='" + replaceMissingValueWith + '\'' +
                ", injective=" + injective +
                ", optimize=" + optimize +
                ", valueColumn=" + valueColumn +
                ", decodeConfig=" + decodeConfig +
                '}';
    }
}
