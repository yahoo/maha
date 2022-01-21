package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class MahaLookupQueryElement {

    @JsonProperty
    public String dimension;
    @JsonProperty
    public String valueColumn;
    @JsonProperty
    public DecodeConfig decodeConfig;
    @JsonProperty
    public Map<String, String> dimensionOverrideMap;
    @JsonProperty
    public Map<String, String> secondaryColOverrideMap;

    public MahaLookupQueryElement() {
    }

    public String getDimension() {
        return dimension;
    }

    public void setDimension(String dimension) {
        this.dimension = dimension;
    }

    public String getValueColumn() {
        return valueColumn;
    }

    public void setValueColumn(String valueColumn) {
        this.valueColumn = valueColumn;
    }

    public DecodeConfig getDecodeConfig() {
        return decodeConfig;
    }

    public void setDecodeConfig(DecodeConfig decodeConfig) {
        this.decodeConfig = decodeConfig;
    }

    public Map<String, String> getDimensionOverrideMap() {
        return dimensionOverrideMap;
    }

    public Map<String, String> getSecondaryColOverrideMap() {
        return secondaryColOverrideMap;
    }

    public void setDimensionOverrideMap(Map<String, String> dimensionOverrideMap) {
        this.dimensionOverrideMap = dimensionOverrideMap;
    }

    public void setSecondaryColOverrideMap(Map<String, String> secondaryColOverrideMap) {
        this.secondaryColOverrideMap = secondaryColOverrideMap;
    }
}