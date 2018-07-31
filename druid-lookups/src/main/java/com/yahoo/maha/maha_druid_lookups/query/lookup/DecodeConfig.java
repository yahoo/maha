// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

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
        return Objects.equals(columnToCheck, that.columnToCheck) &&
                Objects.equals(valueToCheck, that.valueToCheck) &&
                Objects.equals(columnIfValueMatched, that.columnIfValueMatched) &&
                Objects.equals(columnIfValueNotMatched, that.columnIfValueNotMatched);
    }

    @Override
    public int hashCode() {

        return Objects.hash(columnToCheck, valueToCheck, columnIfValueMatched, columnIfValueNotMatched);
    }

}
