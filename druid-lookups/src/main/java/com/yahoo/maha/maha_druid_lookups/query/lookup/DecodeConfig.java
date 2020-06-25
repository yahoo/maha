// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Throwables;
import org.apache.druid.query.extraction.ExtractionCacheHelper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
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

    @JsonIgnore
    public byte[] getCacheKey() {
        try {
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            if (getColumnToCheck() != null) {
                outputStream.write(getColumnToCheck().getBytes());
            }
            outputStream.write(0xFF);
            if (getValueToCheck() != null) {
                outputStream.write(getValueToCheck().getBytes());
            }
            outputStream.write(0xFF);
            if (getColumnIfValueMatched() != null) {
                outputStream.write(getColumnIfValueMatched().getBytes());
            }
            outputStream.write(0xFF);
            if (getColumnIfValueNotMatched() != null) {
                outputStream.write(getColumnIfValueNotMatched().getBytes());
            }
            outputStream.write(0xFF);
            return outputStream.toByteArray();
        } catch (IOException ex) {
            // If ByteArrayOutputStream.write has problems, that is a very bad thing
            throw Throwables.propagate(ex);
        }
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
