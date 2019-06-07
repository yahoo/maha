package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;

public class FlatMultiValueDocumentProcessor implements MongoDocumentProcessor {

    @JsonProperty
    private final ArrayList<String> columnList;
    @JsonProperty
    private final String primaryKeyColumn;

    @JsonCreator
    public FlatMultiValueDocumentProcessor(
            @NotNull @JsonProperty(value = "columnList", required = true) final ArrayList<String> columnList,
            @NotNull @JsonProperty(value = "primaryKeyColumn", required = true) final String primaryKeyColumn) {
        this.columnList = columnList;
        this.primaryKeyColumn = primaryKeyColumn;
        Preconditions.checkArgument(StringUtils.isNotBlank(primaryKeyColumn),
                "primaryKeyColumn cannot be blank or empty!");
        Preconditions.checkArgument(!columnList.isEmpty(), "columnList cannot be empty!");
        Preconditions.checkArgument(columnList.stream().noneMatch(StringUtils::isBlank),
                "columnList cannot have blank columns in list!");
        Preconditions.checkArgument(columnList.stream().noneMatch(s -> s.equals(primaryKeyColumn)),
                "columnList cannot have primaryKeyColumn in list!");
    }

    @Override
    public ArrayList<String> getColumnList() {
        return columnList;
    }

    @Override
    public String getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }

    private String asString(Object objectFromMongo) {
        if (objectFromMongo instanceof String) {
            return objectFromMongo.toString();
        }
        if (objectFromMongo instanceof Integer) {
            return ((Integer) objectFromMongo).toString();
        }
        if (objectFromMongo instanceof Float) {
            return ((Float) objectFromMongo).toString();
        }
        if (objectFromMongo instanceof Double) {
            return ((Double) objectFromMongo).toString();
        }
        if (objectFromMongo instanceof Boolean) {
            return ((Boolean) objectFromMongo).toString();
        }
        if (objectFromMongo instanceof Date) {
            return ((Date) objectFromMongo).toString();
        }
        if (objectFromMongo instanceof ObjectId) {
            return ((ObjectId) objectFromMongo).toHexString();
        }
        throw new RuntimeException(
                String.format("Unhanlded mongo object type : %s", objectFromMongo.getClass().getName()));
    }

    @Override
    public int process(Document document, LookupBuilder lookupBuilder) {
        Object keyObject = document.get(primaryKeyColumn);

        if (keyObject != null) {
            String keyValue = asString(keyObject);
            List<String> colValues = new ArrayList<>(columnList.size());
            for (String col : columnList) {
                Object colObject = document.get(col);
                String colValue = "";
                if (colObject != null) {
                    colValue = asString(colObject);
                }
                colValues.add(colValue);
            }
            lookupBuilder.add(keyValue, colValues);
            return 1;
        }
        return 0;
    }

    @Override
    public String toString() {
        return "FlatMultiValueDocumentProcessor{" +
                "columnList=" + columnList +
                ", primaryKeyColumn='" + primaryKeyColumn + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlatMultiValueDocumentProcessor that = (FlatMultiValueDocumentProcessor) o;
        return Objects.equals(getColumnList(), that.getColumnList()) &&
                Objects.equals(getPrimaryKeyColumn(), that.getPrimaryKeyColumn());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getColumnList(), getPrimaryKeyColumn());
    }
}
