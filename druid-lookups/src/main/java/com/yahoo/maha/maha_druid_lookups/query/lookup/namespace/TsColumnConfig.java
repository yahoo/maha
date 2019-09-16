package com.yahoo.maha.maha_druid_lookups.query.lookup.namespace;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang.StringUtils;

import java.util.Objects;

public class TsColumnConfig {

    public static final String VARCHAR = "varchar";
    public static final String BIGINT = "bigint";

    @JsonProperty
    private String name;
    @JsonProperty
    private String type;
    @JsonProperty
    private String format;
    @JsonProperty
    private String secondaryTsColumn;
    @JsonProperty
    private String secondaryTsColumnCondition;

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public boolean isVarchar() {
        return VARCHAR.equals(type);
    }

    public boolean isBigint() {
        return BIGINT.equals(type);
    }

    public String getFormat() {
        return format;
    }

    public String getSecondaryTsColumn() {
        return secondaryTsColumn;
    }

    public String getSecondaryTsColumnCondition() {
        return secondaryTsColumnCondition;
    }

    public boolean hasSecondaryTsColumn() {
        return StringUtils.isNotEmpty(secondaryTsColumn);
    }

    public boolean hasSecondaryTsColumnCondition() {
        return StringUtils.isNotEmpty(secondaryTsColumnCondition);
    }

    @Override
    public String toString() {
        return "TsColumnConfig{" +
                "name='" + getName() + '\'' +
                ", type='" + getType() + '\'' +
                ", format='" + getFormat() + '\'' +
                ", secondaryTsColumn='" + getSecondaryTsColumn() + '\'' +
                ", secondaryTsColumnCondition='" + getSecondaryTsColumnCondition() + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TsColumnConfig)) {
            return false;
        }

        TsColumnConfig that = (TsColumnConfig) o;

        if (getName() != null ? !getName().equals(that.getName()) : that.getName() != null) {
            return false;
        }
        if (getType() != null ? !getType().equals(that.getType()) : that.getType() != null) {
            return false;
        }
        return getFormat() != null ? getFormat().equals(that.getFormat()) : that.getFormat() == null;

    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getType(), getFormat(), getSecondaryTsColumn(), getSecondaryTsColumnCondition());
    }
}
