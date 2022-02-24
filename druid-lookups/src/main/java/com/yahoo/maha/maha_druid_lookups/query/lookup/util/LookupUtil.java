package com.yahoo.maha.maha_druid_lookups.query.lookup.util;

import com.google.common.base.Strings;
import org.apache.druid.java.util.common.logger.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Map;

public class LookupUtil {
    public String NULL_VAL = "NULL";
    private static final Logger LOG = new Logger(MethodHandles.lookup().lookupClass());
    private static final String OVERRIDE_MAP_ERROR_MSG = "Cannot populate both dimensionOverrideMap and secondaryColOverrideMap!";

    public void overrideThrowableCheck(Map<String, String> dimensionOverrideMap, Map<String, String> secondaryColOverrideMap) {
        if(dimensionOverrideMap != null && !dimensionOverrideMap.isEmpty() && secondaryColOverrideMap != null && !secondaryColOverrideMap.isEmpty()){
            LOG.error(OVERRIDE_MAP_ERROR_MSG);
            throw new IllegalArgumentException(OVERRIDE_MAP_ERROR_MSG);
        }
    }

    public String populateCacheStringFromOverride(String input, Map<String, String> overrideMap) {
        if (overrideMap != null && overrideMap.containsKey(input)) {
            return Strings.emptyToNull(overrideMap.get(input));
        }
        return input;
    }
}
