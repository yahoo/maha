package com.yahoo.maha.maha_druid_lookups.query.lookup;

import java.util.Map;

public class MahaLookupQueryElement {

    String dimension;
    String valueColumn;
    DecodeConfig decodeConfig;
    Map<String, String> dimensionOverrideMap;

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

    public void setDimensionOverrideMap(Map<String, String> dimensionOverrideMap) {
        this.dimensionOverrideMap = dimensionOverrideMap;
    }
}