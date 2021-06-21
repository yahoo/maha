package com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.schema;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.logger.Logger;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;


public class DynamicLookupSchema {
    private static final Logger LOG = new Logger(DynamicLookupSchema.class);

    private final SCHEMA_TYPE type ;
    private final String version;
    private final String name;
    private final DynamicLookupCoreSchema dynamicLookupCoreSchema;

    private DynamicLookupSchema(Builder builder){
        this.type = builder.type;
        this.version = builder.version;
        this.name = builder.name;
        this.dynamicLookupCoreSchema = builder.dynamicLookupCoreSchema;
    }


    @Override
    public String toString(){
        return "DynamicLookupSchema{" +
                "name = " + name +
                ", type = " + type.toString() +
                ", version = " + version +
                ", coreSchema = " + dynamicLookupCoreSchema.toString() +
                " }";
    }


    public String getName(){
        return name;
    }

    public String getVersion(){
        return version;
    }

    public SCHEMA_TYPE getSchemaType(){
        return type;
    }


    public JSONObject toJson(){
        return new JSONObject();
    } // will get back to serialization later


    public static class Builder {
        protected SCHEMA_TYPE type;
        protected String version;
        protected String schemaFilePath;
        protected String name;
        protected DynamicLookupCoreSchema dynamicLookupCoreSchema;

        private void buildType(String type) {
            type = type.toUpperCase();
            try{
                this.type = SCHEMA_TYPE.valueOf(type);
            } catch (IllegalArgumentException  ex){
                LOG.error("Unknown Schema type:  " + type + ex);
                throw new IllegalArgumentException(ex);
            }
        }

        private void buildVersion(String  version) {
            this.version = version;
        }

        private void buildName(String  name) {
            this.name = name;
        }


        private void buildDynamicLookupCoreSchema(SCHEMA_TYPE type,JsonNode coreSchema){
            this.dynamicLookupCoreSchema = DynamicLookupCoreSchemaFactory.buildSchema(type, coreSchema);
        }
        public Builder setSchemaFilePath(String schemaFilePath) throws IOException {
            this.schemaFilePath = schemaFilePath;
            parseJson();
            return this;
        }

        private void parseJson() throws IOException {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode json ;

            try {
                String schemaContent = getSchemaContent();
                json = objectMapper.readTree(schemaContent);

            } catch (IOException ex){
                LOG.error("Cannot Read schema file for path " + schemaFilePath );
                throw ex;
            }


            buildVersion(getField(json,"version"));
            buildName(getField(json,"name"));
            buildType(getField(json,"type"));

            if(json.has("coreSchema")){
                buildDynamicLookupCoreSchema(type,json.get("coreSchema"));
            } else {
                throw new IllegalArgumentException("Field coreSchema not present in schema file " + schemaFilePath);
            }
        }


        private String getField(JsonNode json , String fieldName) throws IllegalArgumentException{
            if(json != null && json.has(fieldName)){
                return json.get(fieldName).textValue();
            }
            else {
                throw new IllegalArgumentException("Field " + fieldName + " not present in schema file " + schemaFilePath);
            }
        }

        private String getSchemaContent() throws IOException{
            return new String(Files.readAllBytes(Paths.get(schemaFilePath)));
        }


        public DynamicLookupSchema build(){
            return new DynamicLookupSchema(this);

        }

    }
}
