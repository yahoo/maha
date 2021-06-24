package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema;


import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.*;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.*;
import org.apache.druid.java.util.common.logger.Logger;
import org.zeroturnaround.zip.commons.*;

import java.io.*;
import java.util.*;

public class DynamicLookupSchema {
    private static final Logger LOG = new Logger(DynamicLookupSchema.class);

    private ExtractionNameSpaceSchemaType type ;
    private String version;
    private String name;
    private List<SchemaField> schemaFieldList;

    @JsonIgnore
    private DynamicLookupCoreSchema coreSchema;

    public DynamicLookupSchema(ExtractionNameSpaceSchemaType type, String version, String name, List<SchemaField> schemaFieldList) {
        this.type = type;
        this.version = version;
        this.name = name;
        this.schemaFieldList = schemaFieldList;
    }

    public DynamicLookupSchema() {
        schemaFieldList = new ArrayList<>();
    }

    // Init Core Schema
    public void init() {

    }

    public ExtractionNameSpaceSchemaType getType() {
        return type;
    }

    public String getVersion() {
        return version;
    }

    public String getName() {
        return name;
    }

    public List<SchemaField> getSchemaFieldList() {
        return schemaFieldList;
    }

    public void setSchemaFieldList(List<SchemaField> schemaFieldList) {
        this.schemaFieldList = schemaFieldList;
    }

    @Override
    public String toString() {
        return "DynamicLookupSchema{" +
                "type=" + type +
                ", version='" + version + '\'' +
                ", name='" + name + '\'' +
                ", schemaFieldList=" + schemaFieldList +
                '}';
    }

    public static Optional<DynamicLookupSchema> parseFrom(String json) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            DynamicLookupSchema dynamicLookupSchema = mapper.readValue(json, DynamicLookupSchema.class);
            return Optional.of(dynamicLookupSchema);
        } catch (Exception e) {
            e.printStackTrace();

        }
        return Optional.empty();
    }

    public static Optional<DynamicLookupSchema> parseFrom(File schemaFile) {
        try {
            return parseFrom(FileUtils.readFileToString(schemaFile));
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Failed to read the Schema file ", e);
        }
        return Optional.empty();
    }

}
