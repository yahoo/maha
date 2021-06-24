package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema;

public class SchemaField {
    private String field;
    private FieldDataType dataType;
    private int index;

    public SchemaField() {

    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public FieldDataType getDataType() {
        return dataType;
    }

    public void setDataType(FieldDataType dataType) {
        this.dataType = dataType;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}
