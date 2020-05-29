// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import io.druid.query.extraction.ExtractionCacheHelper;
import io.druid.query.extraction.FunctionalExtraction;
import io.druid.query.lookup.LookupExtractor;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class MahaLookupExtractionFn extends FunctionalExtraction {
    private final LookupExtractor lookup;
    private final boolean optimize;
    // Thes are retained for auto generated hashCode and Equals
    private final boolean retainMissingValue;
    private final String replaceMissingValueWith;
    private final boolean injective;

    public MahaLookupExtractionFn(
            final LookupExtractor lookup
            , final boolean retainMissingValue
            , final String replaceMissingValueWith
            , final boolean injective
            , Boolean optimize
            , String valueColumn
            , DecodeConfig decodeConfig
            , Map<String, String> dimensionOverrideMap
    ) {
        super(
                new Function<String, String>() {
                    @Nullable
                    @Override
                    public String apply(String input) {
                        if (lookup instanceof MahaLookupExtractor) {
                            MahaLookupExtractor mahaLookupExtractor = (MahaLookupExtractor) lookup;
                            return mahaLookupExtractor.apply(Strings.nullToEmpty(input), valueColumn, decodeConfig, dimensionOverrideMap);
                        } else {
                            return lookup.apply(Strings.nullToEmpty(input));
                        }
                    }
                },
                retainMissingValue,
                replaceMissingValueWith,
                injective
        );
        this.lookup = lookup;
        this.optimize = optimize == null ? true : optimize;
        this.retainMissingValue = retainMissingValue;
        this.injective = injective;
        this.replaceMissingValueWith = replaceMissingValueWith;
    }


    @JsonProperty
    public LookupExtractor getLookup() {
        return lookup;
    }

    @Override
    @JsonProperty
    public boolean isRetainMissingValue() {
        return super.isRetainMissingValue();
    }

    @Override
    @JsonProperty
    public String getReplaceMissingValueWith() {
        return super.getReplaceMissingValueWith();
    }

    @Override
    @JsonProperty
    public boolean isInjective() {
        return super.isInjective();
    }

    @JsonProperty("optimize")
    public boolean isOptimize() {
        return optimize;
    }

    @Override
    public byte[] getCacheKey() {
        try {
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            outputStream.write(ExtractionCacheHelper.CACHE_TYPE_ID_LOOKUP);
            outputStream.write(lookup.getCacheKey());
            if (getReplaceMissingValueWith() != null) {
                outputStream.write(getReplaceMissingValueWith().getBytes());
                outputStream.write(0xFF);
            }
            outputStream.write(isInjective() ? 1 : 0);
            outputStream.write(isRetainMissingValue() ? 1 : 0);
            outputStream.write(isOptimize() ? 1 : 0);
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

        MahaLookupExtractionFn that = (MahaLookupExtractionFn) o;

        if (isOptimize() != that.isOptimize()) {
            return false;
        }
        if (isRetainMissingValue() != that.isRetainMissingValue()) {
            return false;
        }
        if (isInjective() != that.isInjective()) {
            return false;
        }
        if (getLookup() != null ? !getLookup().equals(that.getLookup()) : that.getLookup() != null) {
            return false;
        }
        return getReplaceMissingValueWith() != null
                ? getReplaceMissingValueWith().equals(that.getReplaceMissingValueWith())
                : that.getReplaceMissingValueWith() == null;

    }

    @Override
    public int hashCode() {
        int result = getLookup() != null ? getLookup().hashCode() : 0;
        result = 31 * result + (isOptimize() ? 1 : 0);
        result = 31 * result + (isRetainMissingValue() ? 1 : 0);
        result = 31 * result + (getReplaceMissingValueWith() != null ? getReplaceMissingValueWith().hashCode() : 0);
        result = 31 * result + (isInjective() ? 1 : 0);
        return result;
    }
}
