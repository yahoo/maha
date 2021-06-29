package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema;


import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.*;
import com.google.protobuf.*;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.*;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.BaseSchemaFactory;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.flatbuffer.FlatBufferSchemaFactory;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.protobuf.ProtobufSchemaFactory;
import org.apache.druid.java.util.common.logger.Logger;
import org.zeroturnaround.zip.commons.*;

import java.io.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class DynamicLookupSchema {
    private static final Logger LOG = new Logger(DynamicLookupSchema.class);
    private static ObjectMapper mapper = new ObjectMapper();
    private static final DateTimeFormatter VERSION_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHH");

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
    public void init() throws Descriptors.DescriptorValidationException {
        coreSchema = DynamicLookupCoreSchemaFactory.buildSchema(this);
    }

    public DynamicLookupCoreSchema getCoreSchema() {
        return coreSchema;
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
                ", schemaFieldList=" + Arrays.toString(schemaFieldList.toArray()) +
                '}';
    }

    public static Optional<DynamicLookupSchema> parseFrom(String json) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
            DynamicLookupSchema dynamicLookupSchema = mapper.readValue(json, DynamicLookupSchema.class);
            dynamicLookupSchema.init();
            return Optional.of(dynamicLookupSchema);
        } catch (Exception e) {
            e.printStackTrace();

        }
        return Optional.empty();
    }

    public static Optional<DynamicLookupSchema> parseFrom(File schemaFile) {
        try {
            String schemaJson = FileUtils.readFileToString(schemaFile);
            LOG.info("Got the schema json as "+schemaJson);
            return parseFrom(schemaJson);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Failed to read the Schema file %s "+e.getMessage(), schemaFile.getAbsolutePath(), e);
        }
        return Optional.empty();
    }

    public static void toJson(String messageType, BaseSchemaFactory schemaFactory, String outputName, String outputDir) {
        String outputJsonSchemaPath = outputDir + outputName + ".json";
        DynamicLookupSchema dynamicLookupSchema = new DynamicLookupSchema();
        dynamicLookupSchema.version = LocalDateTime.now(ZoneId.of("UTC")).format(VERSION_TIME_FORMAT);
        dynamicLookupSchema.name = outputName;
        try {
            if(schemaFactory instanceof ProtobufSchemaFactory) {
                dynamicLookupSchema.type = ExtractionNameSpaceSchemaType.PROTOBUF;
                List<Descriptors.FieldDescriptor> fieldDescriptors = ((ProtobufSchemaFactory) schemaFactory).getProtobufDescriptor(messageType).getFields();
                for(Descriptors.FieldDescriptor d: fieldDescriptors) {
                    SchemaField schemaField = new SchemaField(
                            d.getJsonName(),
                            FieldDataType.valueOf(d.getType().toString().toUpperCase()), //throw exception if unsupported type
                            d.getIndex() + 1 //Field numbers must be positive integers
                    );
                    dynamicLookupSchema.schemaFieldList.add(schemaField);
                }
            } else if (schemaFactory instanceof FlatBufferSchemaFactory) {
                // implementation for flatbuffer
            } else {
                throw new Exception("unsupported schemaFactory Type: " + schemaFactory.getClass().getName());
            }
            mapper.writeValue(new File(outputJsonSchemaPath), dynamicLookupSchema);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Failed to create json Schema file for %s" + e.getMessage(), messageType, e);
        }
    }

}
