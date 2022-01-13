package com.yahoo.maha.maha_druid_lookups.query.lookup;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.Map;

abstract public class MahaLookupExtractor extends LookupExtractor {
    private static final Logger LOG = new Logger(MahaLookupExtractor.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    protected static final Map<String, String> staticMap = new java.util.HashMap<>();

    @Nullable
    public abstract String apply(@Nullable String key, @NotNull String valueColumn, DecodeConfig decodeConfig, Map<String, String> dimensionOverrideMap);

    @Nullable
    @Override
    public String apply(@Nullable String key) {
        try {
            if (key == null) {
                return null;
            }

            MahaLookupQueryElement mahaLookupQueryElement = objectMapper.readValue(key, MahaLookupQueryElement.class);
            String dimension = Strings.nullToEmpty(mahaLookupQueryElement.getDimension());
            String valueColumn = mahaLookupQueryElement.getValueColumn();
            DecodeConfig decodeConfig = mahaLookupQueryElement.getDecodeConfig();
            Map<String, String> dimensionOverrideMap = mahaLookupQueryElement.getDimensionOverrideMap();

            return apply(dimension, valueColumn, decodeConfig, dimensionOverrideMap);
        } catch (Exception e) {
            LOG.error(e, "Exception in MahaLookupExtractor apply");
        }
        return null;
    }
}
