// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DecodeConfig {

    @JsonProperty
    private String columnToCheck;
    @JsonProperty
    private String valueToCheck;
    @JsonProperty
    private String columnIfValueMatched;
    @JsonProperty
    private String columnIfValueNotMatched;

    public DecodeConfig() {
    }

    public String getColumnToCheck() {
        return columnToCheck;
    }

    public String getValueToCheck() {
        return valueToCheck;
    }

    public String getColumnIfValueMatched() {
        return columnIfValueMatched;
    }

    public String getColumnIfValueNotMatched() {
        return columnIfValueNotMatched;
    }

    public void setColumnToCheck(String columnToCheck) {
        this.columnToCheck = columnToCheck;
    }

    public void setValueToCheck(String valueToCheck) {
        this.valueToCheck = valueToCheck;
    }

    public void setColumnIfValueMatched(String columnIfValueMatched) {
        this.columnIfValueMatched = columnIfValueMatched;
    }

    public void setColumnIfValueNotMatched(String columnIfValueNotMatched) {
        this.columnIfValueNotMatched = columnIfValueNotMatched;
    }

    @Override
    public String toString() {
        return "DecodeConfig{" +
                "columnToCheck='" + columnToCheck + '\'' +
                ", valueToCheck='" + valueToCheck + '\'' +
                ", columnIfValueMatched='" + columnIfValueMatched + '\'' +
                ", columnIfValueNotMatched='" + columnIfValueNotMatched + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DecodeConfig that = (DecodeConfig) o;

        if (!columnToCheck.equals(that.columnToCheck)) return false;
        if (!valueToCheck.equals(that.valueToCheck)) return false;
        if (!columnIfValueMatched.equals(that.columnIfValueMatched)) return false;
        return columnIfValueNotMatched.equals(that.columnIfValueNotMatched);
    }

    @Override
    public int hashCode() {
        int result = columnToCheck.hashCode();
        result = 31 * result + valueToCheck.hashCode();
        result = 31 * result + columnIfValueMatched.hashCode();
        result = 31 * result + columnIfValueNotMatched.hashCode();
        return result;
    }
}
