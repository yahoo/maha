package com.yahoo.maha.maha_druid_lookups.query.lookup.util;

import com.google.common.base.Strings;

import java.util.Map;

public class LookupUtil {
    public String NULL_VAL = "";

    public void overrideThrowableCheck(Map<String, String> dimensionOverrideMap, Map<String, String> secondaryColOverrideMap) {
        if(dimensionOverrideMap != null && !dimensionOverrideMap.isEmpty() && secondaryColOverrideMap != null && !secondaryColOverrideMap.isEmpty()){
            throw new IllegalArgumentException("Cannot populate both dimensionOverrideMap and secondaryColOverrideMap!");
        }
    }

    public String populateCacheStringFromOverride(String input, Map<String, String> overrideMap) {
        if (overrideMap != null && overrideMap.containsKey(input)) {
            return Strings.emptyToNull(overrideMap.get(input));
        }
        return input;
    }
}
