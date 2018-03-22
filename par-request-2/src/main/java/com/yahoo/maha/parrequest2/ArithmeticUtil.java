// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

public class ArithmeticUtil {

    public static Long addLong(Long value1, Long value2) {
        if (value1 != null) {
            if (value2 != null) {
                return value1 + value2;
            } else {
                return value1;
            }
        } else {
            return value2;
        }
    }

    public static Double addDouble(Double value1, Double value2) {
        if (value1 != null) {
            if (value2 != null) {
                return value1 + value2;
            } else {
                return value1;
            }
        } else {
            return value2;
        }
    }

    public static Double divideDouble(Double numerator, Double denominator) {
        if (numerator != null && denominator != null) {
            if (denominator != 0) {
                return numerator / denominator;
            } else {
                return 0.0;
            }
        } else {
            return null;
        }
    }

    public static Double divideLong(Long numerator, Long denominator) {
        if (numerator != null && denominator != null) {
            if (denominator != 0) {
                return (double) numerator / denominator;
            } else {
                return 0.0;
            }
        } else {
            return null;
        }
    }

    public static Double multiplyDouble(Double value1, Double value2) {
        if (value1 != null && value2 != null) {
            return value1 * value2;
        } else {
            return null;
        }
    }

    public static Long multiplyLong(Long value1, Long value2) {
        if (value1 != null && value2 != null) {
            return value1 * value2;
        } else {
            return null;
        }
    }

    public static Double subtractDouble(Double value1, Double value2) {
        if (value1 == null) {
            return null;
        }
        if (value2 == null) {
            return value1;
        }
        return value1 - value2;
    }

    public static Long subtractLong(Long value1, Long value2) {
        if (value1 == null) {
            return null;
        } else if (value2 == null) {
            return value1;
        }
        return value1 - value2;
    }

    public static Long addLongList(List<Long> input) {
        Long sum = null;
        for (Long value : input) {
            sum = addLong(sum, value);
        }
        return sum;
    }

    public static Double divideDoubleWithRounding(Double numerator, Double denominator, RoundingMode roundingMode) {
        if (numerator == null || denominator == null) {
            return null;
        }
        if (denominator == 0.0) {
            return 0.0;
        }
        BigDecimal bigDecimalValue1 = new BigDecimal(numerator);
        BigDecimal bigDecimalValue2 = new BigDecimal(denominator);
        return bigDecimalValue1.divide(bigDecimalValue2, 2, roundingMode).doubleValue();

    }

    public static Double divideLongWithRounding(Long numerator, Long denominator, RoundingMode roundingMode) {
        if (numerator == null || denominator == null) {
            return null;
        }
        if (denominator == 0L) {
            return 0.0;
        }
        BigDecimal bigDecimalValue1 = new BigDecimal(numerator);
        BigDecimal bigDecimalValue2 = new BigDecimal(denominator);
        return bigDecimalValue1.divide(bigDecimalValue2, 2, roundingMode).doubleValue();
    }

    public static Double getPositiveDouble(Double d) {
        return (d == null) ? null : (d < 0.0) ? 0.0 : d;
    }

    public static Long getPositiveLong(Long l) {
        return (l == null) ? null : (l < 0) ? 0 : l;
    }

    public static Long convertDoubleToLong(Double d) {
        return (d == null) ? null : d.longValue();
    }

    public static Double convertLongToDouble(Long l) {
        return l == null ? null : l.doubleValue();
    }
}
